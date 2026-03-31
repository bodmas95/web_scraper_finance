"""
Common database read/write utilities.

Read-only collections : companies, sources
Write collections     : reports, ingestionLogs, fs.files, fs.chunks
"""

from datetime import datetime, timezone
from bson import ObjectId
import gridfs


# ---------------------------------------------------------------------------
# READ - company and sources (never written to by this codebase)
# ---------------------------------------------------------------------------

def get_company(db, name: str) -> dict | None:
    """
    Find a company document by name or any alias.
    Matching is case-insensitive.
    """
    return db.companies.find_one({
        "$or": [
            {"name":    {"$regex": f"^{name}$", "$options": "i"}},
            {"aliases": {"$regex": f"^{name}$", "$options": "i"}},
        ]
    })


def get_sources_for_company(db, company_id: str, source_type: str = None) -> list[dict]:
    """
    Return all active source documents for a company.

    Args:
        company_id  : string representation of the company _id
        source_type : optional filter - "WEB", "API", or "NEWS"
    """
    query = {
        "companyId": str(company_id),
        "status":    "active",
    }
    if source_type:
        query["sourceType"] = source_type.upper()
    return list(db.sources.find(query))


# ---------------------------------------------------------------------------
# WRITE - reports
# ---------------------------------------------------------------------------

def insert_report(db, doc: dict) -> str:
    """
    Insert a report document.
    Returns the inserted _id as a string.
    """
    now = datetime.now(timezone.utc)
    doc.setdefault("createdAt", now)
    doc.setdefault("updatedAt", now)
    result = db.reports.insert_one(doc)
    return str(result.inserted_id)


def update_report(db, report_id: str, fields: dict):
    """
    Update specific fields of a report document by _id.
    Always bumps updatedAt.
    """
    fields["updatedAt"] = datetime.now(timezone.utc)
    db.reports.update_one(
        {"_id": ObjectId(report_id)},
        {"$set": fields},
    )


# ---------------------------------------------------------------------------
# WRITE - ingestion_logs
# ---------------------------------------------------------------------------

def insert_ingestion_log(db, doc: dict) -> str:
    """
    Insert an ingestion log document.
    Returns the inserted _id as a string.
    """
    now = datetime.now(timezone.utc)
    doc.setdefault("createdAt", now)
    doc.setdefault("updatedAt", now)
    result = db.ingestionLogs.insert_one(doc)
    return str(result.inserted_id)


def update_ingestion_log(db, log_id: str, fields: dict):
    """
    Update specific fields of an ingestion log by _id.
    Always bumps updatedAt.
    """
    fields["updatedAt"] = datetime.now(timezone.utc)
    db.ingestionLogs.update_one(
        {"_id": ObjectId(log_id)},
        {"$set": fields},
    )


# ---------------------------------------------------------------------------
# WRITE - fs.files / fs.chunks (GridFS)
# ---------------------------------------------------------------------------

def save_bytes_to_gridfs(db, data: bytes, filename: str, metadata: dict = None) -> str:
    """
    Store binary content (PDF, Excel, etc.) in GridFS.
    Returns the file_id as a string.
    """
    fs      = gridfs.GridFS(db)
    file_id = fs.put(data, filename=filename, **(metadata or {}))
    return str(file_id)


def save_text_to_gridfs(db, text: str, filename: str, metadata: dict = None) -> str:
    """
    Store plain-text content (news articles, etc.) directly in GridFS
    without converting to any file format.
    Returns the file_id as a string.
    """
    fs      = gridfs.GridFS(db)
    file_id = fs.put(
        text.encode("utf-8"),
        filename=filename,
        content_type="text/plain",
        **(metadata or {}),
    )
    return str(file_id)
