"""
Common database read utilities.

Write operations (insert_report, update_report, insert_log, update_log,
save_bytes_to_gridfs, save_text_to_gridfs) have been moved to MongoDBClient
in src/pipeline/db.py.
"""

from datetime import datetime, timezone


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
# HKEX — file download utility
# ---------------------------------------------------------------------------

import os as _os


def download_file(url, folder, db_client=None):
    _os.makedirs(folder, exist_ok=True)

    filename = url.split("/")[-1]
    path = _os.path.join(folder, filename)

    if _os.path.exists(path):
        print(f"already exists: {filename}")
        if db_client:
            try:
                with open(path, "rb") as f:
                    file_bytes = f.read()
                print(f"saving existing local file to MongoDB: {filename}")
                db_client.save_file(file_bytes, filename)
            except Exception as e:
                print(f"failed to save existing file into MongoDB: {filename}, error: {e}")
        return

    from src.crawler.proxy_base import proxy_request
    print(f"downloading: {filename}")
    status, headers, body = proxy_request(method="GET", url=url, headers={"User-Agent": "Mozilla/5.0"})

    with open(path, "wb") as f:
        f.write(body)
    print(f"saved locally: {filename}")

    if db_client:
        try:
            print(f"saving downloaded file to MongoDB: {filename}")
            db_client.save_file(body, filename)
        except Exception as e:
            print(f"failed to save downloaded file to MongoDB: {filename}, error: {e}")
