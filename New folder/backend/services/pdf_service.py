import pdfplumber
import fitz  # PyMuPDF
import base64
import io


async def extract_pdf_text(pdf_bytes: bytes) -> dict[int, str]:
    pages = {}
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for i, page in enumerate(pdf.pages):
            text = page.extract_text()
            if text:
                pages[i + 1] = text
    return pages


async def extract_pdf_tables(pdf_bytes: bytes) -> dict[int, list]:
    pages = {}
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for i, page in enumerate(pdf.pages):
            tables = page.extract_tables()
            if tables:
                pages[i + 1] = tables
    return pages


def render_pdf_page(pdf_bytes: bytes, page_num: int) -> dict:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        total_pages = len(doc)
        if page_num < 0 or page_num >= total_pages:
            raise ValueError(f"page_num {page_num} out of range (0-{total_pages - 1})")
        page = doc[page_num]
        zoom = 150 / 72
        matrix = fitz.Matrix(zoom, zoom)
        pix = page.get_pixmap(matrix=matrix)
        png_bytes = pix.tobytes("png")
        b64 = base64.b64encode(png_bytes).decode("ascii")
        return {
            "image": f"data:image/png;base64,{b64}",
            "page_num": page_num,
            "total_pages": total_pages,
        }
    finally:
        doc.close()
