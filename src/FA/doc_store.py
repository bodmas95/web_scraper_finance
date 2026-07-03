"""
FA Document Store — save / list / load supporting documents via MongoDB GridFS.

Documents are stored once per (client, doc_type, filename) triple.
The filename is augmented with client name and year for uniqueness.

doc_type values: "transcript", "news", "presentation", "annual_report"
"""

import os
import sys
import gridfs
from datetime import datetime
from bson import ObjectId

_FA_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_FA_DIR, "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

DOC_TYPES = ("transcript", "news", "presentation", "annual_report")

COLLECTION = "fa_documents"


def _get_db():
    from src.cache import get_mongo_db
    return get_mongo_db()


def _get_fs():
    db = _get_db()
    return gridfs.GridFS(db)


def _build_stored_name(client_name: str, doc_type: str, original_name: str, year: int = None) -> str:
    safe_client = client_name.replace(" ", "_").replace(",", "").replace(".", "")
    year_tag = str(year) if year else datetime.now().strftime("%Y")
    return f"fa__{safe_client}__{doc_type}__{year_tag}__{original_name}"


def save_fa_document(
    file_bytes: bytes,
    original_name: str,
    client_name: str,
    doc_type: str,
    year: int = None,
) -> str:
    """
    Save a document to MongoDB GridFS. Returns the GridFS file_id as string.
    If an identical (client, doc_type, original_name) already exists, returns the existing id.
    """
    db = _get_db()
    fs = _get_fs()

    stored_name = _build_stored_name(client_name, doc_type, original_name, year)

    existing = db.fs.files.find_one({"filename": stored_name})
    if existing:
        return str(existing["_id"])

    meta = {
        "client_name": client_name,
        "doc_type": doc_type,
        "original_name": original_name,
        "year": year or datetime.now().year,
        "uploaded_at": datetime.utcnow(),
    }

    file_id = fs.put(
        file_bytes,
        filename=stored_name,
        content_type=_guess_content_type(original_name),
        metadata=meta,
    )

    db[COLLECTION].insert_one({
        "gridfs_file_id": str(file_id),
        "filename": stored_name,
        "original_name": original_name,
        "client_name": client_name,
        "doc_type": doc_type,
        "year": meta["year"],
        "uploaded_at": meta["uploaded_at"],
        "size_bytes": len(file_bytes),
    })

    return str(file_id)


def list_fa_documents(client_name: str, doc_type: str = None) -> list[dict]:
    """
    Return metadata dicts for all stored FA documents for a client.
    Optionally filter by doc_type.
    Each dict has: _id, gridfs_file_id, original_name, doc_type, year, uploaded_at, size_bytes.
    """
    db = _get_db()
    query = {"client_name": client_name}
    if doc_type:
        query["doc_type"] = doc_type
    docs = list(
        db[COLLECTION]
        .find(query)
        .sort([("doc_type", 1), ("year", -1), ("original_name", 1)])
    )
    for d in docs:
        d["_id"] = str(d["_id"])
    return docs


def load_fa_document(gridfs_file_id: str) -> bytes:
    """Load file bytes from GridFS by file_id string."""
    fs = _get_fs()
    grid_out = fs.get(ObjectId(gridfs_file_id))
    return grid_out.read()


def _guess_content_type(name: str) -> str:
    ext = os.path.splitext(name)[1].lower()
    return {
        ".pdf": "application/pdf",
        ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ".doc": "application/msword",
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ".xls": "application/vnd.ms-excel",
        ".xlsm": "application/vnd.ms-excel.sheet.macroEnabled.12",
        ".txt": "text/plain",
        ".html": "text/html",
    }.get(ext, "application/octet-stream")
