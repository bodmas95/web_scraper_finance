"""
PDF Financial Statement Extractor
Integrated from bref-populator for unified UI
Extracts Income Statement, Balance Sheet, and Cash Flow from PDF annual reports
"""

import json
import tempfile
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import fitz  # PyMuPDF
from config.config import load_config
from .llm_client import get_client

# Import statement headings and configuration from extraction_config
from .extraction_config import (
    STATEMENT_HEADINGS,
    STATEMENT_LABELS,
    HEADING_SEARCH_LINES,
    MIN_TABLE_ROWS,
    MAX_CONTINUATION_PAGES
)


class PDFExtractor:
    """Extract financial statements from PDF annual reports using LLM"""
    
    def __init__(self):
        """Initialize the PDF extractor with LLM client"""
        cfg = load_config()
        self.client = get_client()
        self.model = cfg.get("LLM", "model", fallback="gpt-4o")
        self.token_usage = {"input": 0, "output": 0, "total": 0}
    
    def reset_token_usage(self):
        """Reset token usage counters"""
        self.token_usage = {"input": 0, "output": 0, "total": 0}
    
    def track_usage(self, response):
        """Track token usage from API response"""
        if hasattr(response, 'usage'):
            self.token_usage["input"] += response.usage.prompt_tokens
            self.token_usage["output"] += response.usage.completion_tokens
            self.token_usage["total"] += response.usage.total_tokens
    
    def get_token_usage(self) -> Dict[str, int]:
        """Get current token usage"""
        return self.token_usage.copy()
    
    def scan_for_candidates(self, pdf_path: str, statement_type: str) -> List[Dict]:
        """
        Scan PDF for candidate pages containing the specified statement type
        
        Args:
            pdf_path: Path to PDF file
            statement_type: One of 'income_statement', 'balance_sheet', 'cash_flow'
        
        Returns:
            List of candidate page dictionaries
        """
        headings = STATEMENT_HEADINGS.get(statement_type, [])
        candidates = []
        
        doc = fitz.open(pdf_path)
        
        for page_num in range(len(doc)):
            page = doc[page_num]
            text = page.get_text()
            lines = text.split('\n')
            
            # Check first few lines for statement heading
            search_text = '\n'.join(lines[:HEADING_SEARCH_LINES]).upper()
            
            for heading in headings:
                if heading.upper() in search_text:
                    # Count table-like rows (lines with numbers)
                    table_rows = sum(1 for line in lines if any(c.isdigit() for c in line))
                    
                    if table_rows >= MIN_TABLE_ROWS:
                        candidates.append({
                            "page_num": page_num,
                            "page_display": page_num + 1,
                            "heading_found": heading,
                            "full_text": text,
                            "table_row_count": table_rows
                        })
                        break
        
        doc.close()
        return candidates
    
    def validate_candidate_page(self, candidate: Dict) -> Dict:
        """
        Use LLM to validate if a candidate page is the actual financial statement
        
        Args:
            candidate: Candidate page dictionary
        
        Returns:
            Updated candidate with validation results
        """
        validation_prompt = f"""You are analysing a page from an annual report PDF.

A genuine financial statement page has all of the following:
  1. A statement heading such as "{candidate.get('heading_found', 'financial statement')}" near the top of the page.
  2. A financial table with labelled line items and numeric values for multiple years.

Page text:
{candidate['full_text'][:3000]}

Is this the actual primary financial statement page, or merely a reference, summary, or supplementary section?

Respond in this exact JSON format:
{{
  "is_actual_statement":  true or false,
  "confidence":           "high" or "medium" or "low",
  "reason":               "one sentence explanation",
  "has_numeric_table":    true or false
}}

Only respond with valid JSON — no other text.
"""
        
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": validation_prompt}],
            temperature=0,
            response_format={"type": "json_object"},
        )
        
        self.track_usage(response)
        result = json.loads(response.choices[0].message.content)
        
        candidate["validation"] = result
        candidate["is_confirmed"] = (
            result.get("is_actual_statement", False)
            and result.get("confidence") in ("high", "medium")
        )
        
        return candidate
    
    def find_correct_page(self, pdf_path: str, statement_type: str) -> Optional[Dict]:
        """
        Find the correct page for a financial statement
        
        Args:
            pdf_path: Path to PDF file
            statement_type: One of 'income_statement', 'balance_sheet', 'cash_flow'
        
        Returns:
            Page dictionary or None if not found
        """
        candidates = self.scan_for_candidates(pdf_path, statement_type)
        
        if not candidates:
            return None
        
        if len(candidates) == 1:
            candidates[0]["is_confirmed"] = True
            return candidates[0]
        
        # Multiple candidates - use LLM to validate
        for candidate in candidates:
            validated = self.validate_candidate_page(candidate)
            if validated["is_confirmed"]:
                return validated
        
        return None
    
    def get_continuation_pages(self, pdf_path: str, start_page_num: int, max_pages: int = None) -> str:
        """
        Get text from continuation pages after the main statement page
        
        Args:
            pdf_path: Path to PDF file
            start_page_num: Starting page number (0-indexed)
            max_pages: Maximum number of continuation pages to check
        
        Returns:
            Combined text from continuation pages
        """
        if max_pages is None:
            max_pages = MAX_CONTINUATION_PAGES
        
        doc = fitz.open(pdf_path)
        combined_text = ""
        
        for i in range(1, max_pages + 1):
            page_num = start_page_num + i
            if page_num >= len(doc):
                break
            
            page = doc[page_num]
            text = page.get_text()
            
            # Check if this page is a continuation (has numbers but no new statement heading)
            lines = text.split('\n')
            has_numbers = sum(1 for line in lines if any(c.isdigit() for c in line)) > 5
            
            # Check if page starts with a new statement heading
            search_text = '\n'.join(lines[:HEADING_SEARCH_LINES]).upper()
            is_new_statement = False
            for statement_type, headings in STATEMENT_HEADINGS.items():
                for heading in headings:
                    if heading.upper() in search_text:
                        is_new_statement = True
                        break
                if is_new_statement:
                    break
            
            # If has numbers but no new heading, it's a continuation
            if has_numbers and not is_new_statement:
                combined_text += "\n\n--- CONTINUATION PAGE ---\n\n" + text
            else:
                # Stop if we hit a new statement or non-table page
                break
        
        doc.close()
        return combined_text
    
    def extract_table_from_text(self, page_text: str) -> Dict:
        """
        Extract financial table from page text using LLM
        
        Args:
            page_text: Text content of the page
        
        Returns:
            Dictionary with rows, year_headers, and total_rows
        """
        extraction_prompt = f"""You are a financial data extraction specialist.

Below is raw text from a financial statement page in an annual report.
Extract every line item from the financial table as structured data.

Rules:
- Include all line items: revenues, expenses, subtotals, totals, per-share figures.
- Do NOT include page headers, footnotes, page numbers, or unit notes.
- Do NOT output heading rows — instead, track headings as context and attach them to data rows.
- Each data row must have a "label" and one value per year column found.
- If a data row has one or more ancestor headings, include a "parent" field containing all ancestor headings joined by " > " (outermost to innermost). Omit "parent" if the row has no ancestors.
- First identify the year column headers (e.g. 2024, 2023, 2022) from the table header row.
- The numbers in each data row appear LEFT TO RIGHT in the SAME ORDER as the year headers. The first number belongs to the first year, the second to the second year, and so on. Do not reorder them.
- Some labels are long and wrap across two lines in the PDF. In these cases the numeric values may be split: some appear beside the first line of the label and the rest beside the continuation line. Collect ALL numbers for that logical row and assign them in left-to-right order to the year columns. Do not treat the continuation line as a separate row.
- For negative values shown in parentheses like (1,234), return as -1234.
- Strip commas and currency symbols from numbers — return plain numbers.
- If a value is missing or blank for a year, use null.

LANGUAGE HANDLING:
- CRITICAL: All labels MUST be in ENGLISH.
- If the document is bilingual (e.g., Indonesian + English, Chinese + English), extract labels from the ENGLISH column/section.
- If the document is monolingual in a non-English language (Indonesian, Chinese, French, etc.), translate all labels to English.
- Common translations: "Pendapatan" → "Revenue", "Beban" → "Expenses", "Laba" → "Profit", "Rugi" → "Loss", "Aset" → "Assets", "Liabilitas" → "Liabilities", "Ekuitas" → "Equity", "Arus Kas" → "Cash Flow"

Page text:
{page_text}

Respond in this exact JSON format:
{{
  "year_headers": ["2024", "2023", "2022"],
  "rows": [
    {{"label": "Net income", "2024": 1234, "2023": 1100, "2022": 980}},
    {{"parent": "CASH FLOWS FROM OPERATING ACTIVITIES > Adjustments to reconcile net income > Changes in operating assets and liabilities", "label": "Current assets", "2024": -382, "2023": 58, "2022": -1340}}
  ]
}}

Only respond with valid JSON — no other text.
"""
        
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": extraction_prompt}],
            temperature=0,
            response_format={"type": "json_object"},
        )
        
        self.track_usage(response)
        result = json.loads(response.choices[0].message.content)
        
        year_headers = result.get("year_headers", [])
        rows = [r for r in result.get("rows", []) if r.get("label", "").strip()]
        
        return {
            "rows": rows,
            "year_headers": year_headers,
            "total_rows": len(rows)
        }
    
    def extract_table_from_image(self, image_bytes: bytes) -> Dict:
        """
        Extract financial table from page image using LLM vision
        
        Args:
            image_bytes: PNG image bytes of the page
        
        Returns:
            Dictionary with rows, year_headers, and total_rows
        """
        import base64
        
        b64 = base64.b64encode(image_bytes).decode("utf-8")
        
        image_extraction_prompt = """You are a financial data extraction specialist.

This is an image of a financial statement page from an annual report.
Extract every line item from the financial table as structured data.

Rules:
- Include all line items: revenues, expenses, subtotals, totals, per-share figures.
- Do NOT include page headers, footnotes, page numbers, or unit notes.
- Do NOT output heading rows — instead, track headings as context and attach them to data rows.
- Each data row must have a "label" and one value per year column found.
- If a data row has one or more ancestor headings, include a "parent" field containing all ancestor headings joined by " > " (outermost to innermost). Omit "parent" if the row has no ancestors.
- Year columns are identified from the column headers in the table.
- For negative values shown in parentheses like (1,234), return as -1234.
- Strip commas and currency symbols from numbers — return plain numbers.
- If a value is missing or blank for a year, use null.

LANGUAGE HANDLING:
- CRITICAL: All labels MUST be in ENGLISH.
- If the document is bilingual (e.g., Indonesian + English, Chinese + English), extract labels from the ENGLISH column/section.
- If the document is monolingual in a non-English language (Indonesian, Chinese, French, etc.), translate all labels to English.
- Common translations: "Pendapatan" → "Revenue", "Beban" → "Expenses", "Laba" → "Profit", "Rugi" → "Loss", "Aset" → "Assets", "Liabilitas" → "Liabilities", "Ekuitas" → "Equity", "Arus Kas" → "Cash Flow"

Respond in this exact JSON format:
{
  "year_headers": ["2024", "2023", "2022"],
  "rows": [
    {"label": "Net income", "2024": 1234, "2023": 1100, "2022": 980},
    {"parent": "CASH FLOWS FROM OPERATING ACTIVITIES > Changes in operating assets and liabilities", "label": "Current assets", "2024": -382, "2023": 58, "2022": -1340}
  ]
}

Only respond with valid JSON — no other text.
"""
        
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "text", "text": image_extraction_prompt},
                    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64}"}},
                ],
            }],
            temperature=0,
            response_format={"type": "json_object"},
        )
        
        self.track_usage(response)
        result = json.loads(response.choices[0].message.content)
        
        year_headers = result.get("year_headers", [])
        rows = [r for r in result.get("rows", []) if r.get("label", "").strip()]
        
        return {
            "rows": rows,
            "year_headers": year_headers,
            "total_rows": len(rows)
        }
    
    def extract_statements(
        self,
        pdf_bytes: bytes,
        statement_types: List[str],
        extraction_method: str = "text",
        company_name: str = "",
        target_year: Optional[int] = None
    ) -> Dict[str, Dict]:
        """
        Extract multiple financial statements from a PDF
        
        Args:
            pdf_bytes: PDF file bytes
            statement_types: List of statement types to extract
            extraction_method: 'text' or 'vision'
            company_name: Company name for metadata
            target_year: Target fiscal year
        
        Returns:
            Dictionary mapping statement_type to extraction results
        """
        self.reset_token_usage()
        results = {}
        
        # Save PDF to temporary file
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(pdf_bytes)
            pdf_path = tmp.name
        
        try:
            for statement_type in statement_types:
                # Find the correct page
                page_info = self.find_correct_page(pdf_path, statement_type)
                
                if not page_info:
                    continue
                
                # Extract table
                if extraction_method == "vision":
                    doc = fitz.open(pdf_path)
                    pixmap = doc[page_info["page_num"]].get_pixmap(dpi=150)
                    image_bytes = pixmap.tobytes("png")
                    doc.close()
                    table_data = self.extract_table_from_image(image_bytes)
                else:
                    # Get main page text
                    full_text = page_info["full_text"]
                    
                    # Check for continuation pages
                    continuation_text = self.get_continuation_pages(pdf_path, page_info["page_num"])
                    if continuation_text:
                        full_text += continuation_text
                    
                    table_data = self.extract_table_from_text(full_text)
                
                if table_data["rows"]:
                    results[statement_type] = {
                        "page": page_info["page_display"],
                        "page_num": page_info["page_num"],
                        "rows": table_data["rows"],
                        "year_headers": table_data.get("year_headers", []),
                        "total_rows": table_data["total_rows"],
                        "extraction_method": extraction_method,
                        "company": company_name,
                        "statement": statement_type,
                        "target_year": target_year,
                    }
        
        finally:
            # Clean up temporary file
            try:
                os.unlink(pdf_path)
            except:
                pass
        
        return results
