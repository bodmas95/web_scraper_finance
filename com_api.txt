"""
OVH API pipeline.

Handles sourceType=API: runs the OVH XBRL parser which fetches, parses, and
locally saves all XBRL filing files, then ingests every produced file into
GridFS with matching report and ingestion_log documents in MongoDB.

Files ingested per pipeline run:
  - api_filings.json          : raw filings listing from filings.xbrl.org
  Per fiscal year:
  - FY####_ixbrlviewer.html   : interactive XBRL viewer page (HTML)
  - FY####_viewer_data.json   : viewer JSON extracted from the HTML
  - FY####_report.json        : OIM xBRL-JSON (machine-readable facts)
  - ovhcloud_complete_financials.xlsx : consolidated Excel output (all years)
"""

from datetime import datetime, timezone
from pathlib import Path

from src.parser.ovh import parser as ovh_parser
from src.logging import get_logger

logger = get_logger(__name__)

_FILE_META = {
    "viewer_html": ("HTML", "text/html"),
    "viewer_json": ("JSON", "application/json"),
    "oim_json":    ("JSON", "application/json"),
}
_FILE_DISPLAY_NAMES = {
    "viewer_html": "ixbrlviewer.html",
    "viewer_json": "viewer_data.json",
    "oim_json":    "report.json",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ingest_file(client, local_path: str, stored_name: str, metadata: dict) -> tuple[str, int]:
    """Read a local file and store it in GridFS. Returns (file_id, size_bytes)."""
    data    = Path(local_path).read_bytes()
    file_id = client.save_bytes_to_gridfs(data, stored_name, metadata=metadata)
    return file_id, len(data)


def run(client, company: dict, source: dict) -> None:
    """
    Run the XBRL API pipeline for one source record.

    Steps:
      1. Insert an ingestion log with status=running.
      2. Call parser.run() which fetches, parses, and writes all files locally.
      3. Ingest each produced file into GridFS.
      4. Create a report document per fiscal year and one for the Excel.
      5. Update the ingestion log with status=success and all file records.
    """
    company_id  = str(company["_id"])
    source_id   = str(source["_id"])
    exchange    = source.get("exchange", "")
    source_name = source.get("sources", "")
    report_type = source.get("reportType", "Financial_Components")
    language    = source.get("language", "en")

    log_doc = {
        "sourceId":  source_id,
        "companyId": company_id,
        "result": {
            "status":       "running",
            "errorCode":    None,
            "errorMessage": None,
        },
        "report": {
            "fiscalYear": None,
            "reportType": report_type,
            "exchange":   exchange,
            "urls":       [source.get("sourceUrl", "")],
        },
        "files": [],
        "parse": {
            "status":       "pending",
            "parser":       "ovh_xbrl_parser",
            "parsedAt":     None,
            "errorMessage": None,
        },
    }
    log_id = client.insert_log(log_doc)
    logger.info("API pipeline started | companyId=%s sourceId=%s", company_id, source_id)

    try:
        lei      = client.get_ovh_lei(company)
        if not lei:
            logger.warning(
                "No LEI found in company tickers for %r — "
                "check that companies.tickers contains {\"lei\": \"...\"}",
                company.get("name"),
            )
        api_base = source.get("sourceUrl") or None
        parser_result = ovh_parser.run(lei=lei, api_base=api_base)
        downloaded_at = _now_iso()
        log_file_records = []

        # ------------------------------------------------------------------
        # 1. API filings listing JSON
        # ------------------------------------------------------------------
        api_listing_path = parser_result.get("api_listing")
        if api_listing_path and Path(api_listing_path).exists():
            file_id, size = _ingest_file(
                client, api_listing_path, "api_filings.json",
                metadata={
                    "companyId": company_id,
                    "sourceId":  source_id,
                    "fileType":  "api_listing",
                },
            )
            client.insert_report({
                "companyId":      company_id,
                "sourceId":       source_id,
                "exchange":       exchange,
                "source":         source_name,
                "sourceFilingId": None,
                "reportType":     "api_filings_listing",
                "fiscalYear":     None,
                "status":         "active",
                "files": [{
                    "fileId":         file_id,
                    "format":         "JSON",
                    "language":       language,
                    "url":            source.get("sourceUrl", ""),
                    "downloadStatus": "success",
                    "downloadedAt":   downloaded_at,
                }],
            })
            log_file_records.append({
                "fileId":               file_id,
                "format":               "JSON",
                "fileName":             "api_filings.json",
                "url":                  source.get("sourceUrl", ""),
                "mimeType":             "application/json",
                "sizeBytes":            size,
                "downloadStatus":       "success",
                "downloadErrorMessage": None,
            })

        # ------------------------------------------------------------------
        # 2. Per-year XBRL files  (ixbrlviewer.html, viewer_data.json, report.json)
        # ------------------------------------------------------------------
        for fy_label, fy_files in sorted(parser_result.get("per_year", {}).items()):
            fy_report_files = []

            for key, local_path in fy_files.items():
                if not local_path or not Path(local_path).exists():
                    continue

                fmt, mime          = _FILE_META.get(key, ("BIN", "application/octet-stream"))
                display_name       = _FILE_DISPLAY_NAMES.get(key, Path(local_path).name)
                stored_name        = f"{fy_label}_{display_name}"

                file_id, size = _ingest_file(
                    client, local_path, stored_name,
                    metadata={
                        "companyId":  company_id,
                        "sourceId":   source_id,
                        "fiscalYear": fy_label,
                        "fileType":   key,
                    },
                )

                fy_report_files.append({
                    "fileId":         file_id,
                    "format":         fmt,
                    "language":       language,
                    "url":            None,
                    "downloadStatus": "success",
                    "downloadedAt":   downloaded_at,
                })
                log_file_records.append({
                    "fileId":               file_id,
                    "format":               fmt,
                    "fileName":             stored_name,
                    "url":                  None,
                    "mimeType":             mime,
                    "sizeBytes":            size,
                    "downloadStatus":       "success",
                    "downloadErrorMessage": None,
                })

            if fy_report_files:
                client.insert_report({
                    "companyId":      company_id,
                    "sourceId":       source_id,
                    "exchange":       exchange,
                    "source":         source_name,
                    "sourceFilingId": f"{fy_label}_XBRL",
                    "reportType":     "xbrl_filing",
                    "fiscalYear":     fy_label,
                    "status":         "active",
                    "files":          fy_report_files,
                })

        # ------------------------------------------------------------------
        # 3. Consolidated Excel output
        # ------------------------------------------------------------------
        excel_path = parser_result.get("excel")
        if excel_path and Path(excel_path).exists():
            excel_name = Path(excel_path).name
            file_id, size = _ingest_file(
                client, excel_path, excel_name,
                metadata={
                    "companyId":  company_id,
                    "sourceId":   source_id,
                    "reportType": report_type,
                },
            )
            client.insert_report({
                "companyId":      company_id,
                "sourceId":       source_id,
                "exchange":       exchange,
                "source":         source_name,
                "sourceFilingId": None,
                "reportType":     report_type,
                "fiscalYear":     None,
                "status":         "active",
                "files": [{
                    "fileId":         file_id,
                    "format":         "XLSX",
                    "language":       language,
                    "url":            None,
                    "downloadStatus": "success",
                    "downloadedAt":   downloaded_at,
                }],
            })
            log_file_records.append({
                "fileId":               file_id,
                "format":               "XLSX",
                "fileName":             excel_name,
                "url":                  None,
                "mimeType":             "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "sizeBytes":            size,
                "downloadStatus":       "success",
                "downloadErrorMessage": None,
            })

        # ------------------------------------------------------------------
        # 4. Finalise ingestion log
        # ------------------------------------------------------------------
        client.update_log(log_id, {
            "result.status":  "success",
            "parse.status":   "success",
            "parse.parsedAt": downloaded_at,
            "files":          log_file_records,
        })
        logger.info("API pipeline completed | %d files ingested", len(log_file_records))

    except Exception as exc:
        logger.error("API pipeline failed | companyId=%s sourceId=%s error=%s",
                     company_id, source_id, exc, exc_info=True)
        client.update_log(log_id, {
            "result.status":       "error",
            "result.errorCode":    type(exc).__name__,
            "result.errorMessage": str(exc),
            "parse.status":        "error",
            "parse.errorMessage":  str(exc),
        })
        raise
