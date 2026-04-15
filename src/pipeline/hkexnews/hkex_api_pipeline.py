"""
reads company and source info using db_utils
writes the results to reports
"""

import os
from datetime import datetime
import requests

from config.config import USER_AGENT
from src.http_client import get


def build_gridfs_metadata(
    company_doc: dict,
    source_doc: dict,
    stock_id: str,
    stock_code: str,
    report_item: dict,
) -> dict:
    """
    Build metadata to store in GridFS.

    Args:
        company_doc: Company document from companies collection.
        source_doc: Source document from source collection.
        stock_id: HKHEX stock id.
        stock_code: HKHEX symbol.
        report_item: Parsed report item.

    Returns:
        MetaData dictionary.
    """
    return {
        "companyId": str(company_doc["_id"]),
        "companyName": company_doc.get("name"),
        "sourceId": str(source_doc["_id"]),
        "source": source_doc.get("source"),
        "exchange": source_doc.get("exchange"),
        "stockid": stock_id,
        "stockCode": stock_code,
        "reportType": report_item.get("reportType"),
        "fiscalYear": report_item.get("fiscalYear"),
        "reportingId": report_item.get("reportingId"),
        "uploadAt": datetime.utcnow(),
    }


def build_ingestion_file_entry(
    file_id: str | None,
    filename: str,
    file_url: str,
    file_bytes: bytes | None,
    language: str,
    download_status: str,
    downloader_error_message: str | None = None,
) -> dict:
    """
    Build file entry for ingestionLogs and reports.

    Args:
        file_id: GridFS file id.
        filename: File name.
        file_url: Original file url.
        file_bytes: Raw file bytes.
        language: file language.
        download_status: success or failed.
        downloader_error_message: Optional error text.

    Returns:
        File metadata entry.
    """
    size_bytes = len(file_bytes) if file_bytes else 0

    return {
        "fileId": file_id,
        "fileName": filename,
        "url": file_url,
        "format": "PDF",
        "mimeType": "application/pdf",
        "sizeBytes": size_bytes,
        "language": language,
        "downloadStatus": download_status,
        "downloadedAt": datetime.utcnow(),
        "downoaderErrorMessage": downloader_error_message,
    }


def download_pdf(url: str, folder: str) -> tuple[str, bytes]:
    """
    Download PDF and store locally.

    Args:
        url: PDF URL.
        folder: Local folder path.
    Returns:
        Tuple of (filename, file_bytes).
    """
    os.makedirs(folder, exist_ok=True)

    filename = url.split("/")[-1]
    file_path = os.path.join(folder, filename)

    # Check if file already exists locally
    if os.path.exists(file_path):
        with open(file_path, "rb") as file_obj:
            return filename, file_obj.read()

    # Download using proxy-aware HTTP client
    response = get(
        url=url,
        headers={"User-Agent": USER_AGENT},
        timeout=60,
    )
    response.raise_for_status()
    body = response.content

    # Save to local file
    with open(file_path, "wb") as file_obj:
        file_obj.write(body)

    return filename, body