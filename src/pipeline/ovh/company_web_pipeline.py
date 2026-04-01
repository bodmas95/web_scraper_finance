"""
OVH WEB / NEWS pipeline.

Handles two source types:
  WEB  - financial PDFs from the investor-relations page
  NEWS - press-release articles from the newsroom

Inputs  : db (pymongo Database), company dict, source dict
Outputs : ingestion_log and report records in MongoDB; files in GridFS;
          local copies written via utils.py
"""

from datetime import datetime, timezone
from pathlib import Path

from src.crawler.ovh.crawler import OVHCrawler
from src.pipeline.db_utils import (
    insert_report,
    insert_ingestion_log,
    update_ingestion_log,
    save_bytes_to_gridfs,
    save_text_to_gridfs,
)
from src.pipeline.utils import (
    save_bytes,
    save_text,
    build_article_text,
    article_filename,
)
from config.config import get_section
from src.logging import get_logger

logger = get_logger(__name__)

_LOCAL_ROOT = Path(get_section("DEFAULT").get("output_path", "/opt/data/raw"))


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def run_web_pipeline(db, company: dict, source: dict) -> None:
    """
    Download financial PDFs for one WEB source record, persist them to GridFS
    and locally, and write the corresponding report + ingestion_log documents.
    """
    company_id  = str(company["_id"])
    source_id   = str(source["_id"])
    source_url  = source.get("sourceUrl", "")
    report_type = source.get("reportType", "")
    language    = source.get("language", "en")
    year_filter = source.get("filters", {}).get("year")

    log_doc = {
        "sourceId":  source_id,
        "companyId": company_id,
        "result": {
            "status":       "running",
            "errorCode":    None,
            "errorMessage": None,
        },
        "report": {
            "fiscalYear": int(year_filter) if year_filter else None,
            "reportType": report_type,
            "exchange":   source.get("exchange", ""),
            "urls":       [source_url],
        },
        "files": [],
        "parse": {
            "status":       "pending",
            "parser":       None,
            "parsedAt":     None,
            "errorMessage": None,
        },
    }
    log_id = insert_ingestion_log(db, log_doc)
    logger.info("WEB pipeline started | companyId=%s sourceId=%s url=%s",
                company_id, source_id, source_url)

    try:
        crawler = OVHCrawler(
            company  = company.get("name", ""),
            url      = source_url,
            year     = int(year_filter) if year_filter else None,
            filter   = source.get("filters"),
            metadata = {"companyId": company_id, "sourceId": source_id},
        )
        pdf_files = crawler.fetch_pdfs()

        log_file_records = []
        downloaded_at    = _now_iso()

        for f in pdf_files:
            local_path = _LOCAL_ROOT / f["fiscal_year"] / f["filename"]
            save_bytes(f["bytes"], local_path)

            file_id = save_bytes_to_gridfs(
                db,
                f["bytes"],
                f["filename"],
                metadata={
                    "companyId":  company_id,
                    "sourceId":   source_id,
                    "fiscalYear": f["fiscal_year"],
                    "docType":    f["doc_type"],
                    "url":        f["url"],
                },
            )

            report_doc = {
                "companyId":      company_id,
                "sourceId":       source_id,
                "exchange":       source.get("exchange", ""),
                "source":         source.get("sources", ""),
                "sourceFilingId": f["source_filing_id"],
                "reportType":     f["report_type"],
                "fiscalYear":     f["fiscal_year"],
                "status":         "active",
                "files": [{
                    "fileId":         file_id,
                    "format":         "PDF",
                    "language":       language,
                    "url":            f["url"],
                    "downloadStatus": "success",
                    "downloadedAt":   downloaded_at,
                }],
            }
            insert_report(db, report_doc)
            logger.debug("Report inserted | sourceFilingId=%s file=%s",
                         f["source_filing_id"], f["filename"])

            log_file_records.append({
                "fileId":               file_id,
                "format":               "PDF",
                "fileName":             f["filename"],
                "url":                  f["url"],
                "mimeType":             "application/pdf",
                "sizeBytes":            len(f["bytes"]),
                "downloadStatus":       "success",
                "downloadErrorMessage": None,
            })

        update_ingestion_log(db, log_id, {
            "result.status": "success",
            "files":         log_file_records,
        })
        logger.info("WEB pipeline completed | %d files ingested", len(log_file_records))

    except Exception as exc:
        logger.error("WEB pipeline failed | companyId=%s sourceId=%s error=%s",
                     company_id, source_id, exc, exc_info=True)
        update_ingestion_log(db, log_id, {
            "result.status":       "error",
            "result.errorCode":    type(exc).__name__,
            "result.errorMessage": str(exc),
        })
        raise


def run_news_pipeline(db, company: dict, source: dict) -> None:
    """
    Fetch news articles for one NEWS source record, store text directly in GridFS
    (no file conversion), write locally, and create report + ingestion_log documents.
    """
    company_id  = str(company["_id"])
    source_id   = str(source["_id"])
    source_url  = source.get("sourceUrl", "")
    language    = source.get("language", "en")
    year_filter = source.get("filters", {}).get("year")
    years       = [int(year_filter)] if year_filter else None

    log_doc = {
        "sourceId":  source_id,
        "companyId": company_id,
        "result": {
            "status":       "running",
            "errorCode":    None,
            "errorMessage": None,
        },
        "report": {
            "fiscalYear": int(year_filter) if year_filter else None,
            "reportType": "news",
            "exchange":   source.get("exchange", ""),
            "urls":       [source_url],
        },
        "files": [],
        "parse": {
            "status":       "pending",
            "parser":       None,
            "parsedAt":     None,
            "errorMessage": None,
        },
    }
    log_id = insert_ingestion_log(db, log_doc)
    logger.info("NEWS pipeline started | companyId=%s sourceId=%s url=%s",
                company_id, source_id, source_url)

    try:
        crawler = OVHCrawler(
            company  = company.get("name", ""),
            url      = source_url,
            year     = int(year_filter) if year_filter else None,
            filter   = source.get("filters"),
            metadata = {"companyId": company_id, "sourceId": source_id},
        )
        articles = crawler.fetch_news(years=years)

        log_file_records = []
        downloaded_at    = _now_iso()

        for article in articles:
            text  = build_article_text(article)
            fname = article_filename(article)

            local_path = _LOCAL_ROOT / "news" / fname
            save_text(text, local_path)

            file_id = save_text_to_gridfs(
                db,
                text,
                fname,
                metadata={
                    "companyId": company_id,
                    "sourceId":  source_id,
                    "date":      article.get("date", ""),
                    "url":       article.get("url", ""),
                },
            )

            report_doc = {
                "companyId":      company_id,
                "sourceId":       source_id,
                "exchange":       source.get("exchange", ""),
                "source":         source.get("sources", ""),
                "sourceFilingId": article.get("source_filing_id"),
                "reportType":     "news",
                "fiscalYear":     None,
                "status":         "active",
                "files": [{
                    "fileId":         file_id,
                    "format":         "TXT",
                    "language":       language,
                    "url":            article.get("url", ""),
                    "downloadStatus": "success",
                    "downloadedAt":   downloaded_at,
                }],
            }
            insert_report(db, report_doc)
            logger.debug("News report inserted | sourceFilingId=%s",
                         article.get("source_filing_id"))

            text_bytes = text.encode("utf-8")
            log_file_records.append({
                "fileId":               file_id,
                "format":               "TXT",
                "fileName":             fname,
                "url":                  article.get("url", ""),
                "mimeType":             "text/plain",
                "sizeBytes":            len(text_bytes),
                "downloadStatus":       "success",
                "downloadErrorMessage": None,
            })

        update_ingestion_log(db, log_id, {
            "result.status": "success",
            "files":         log_file_records,
        })
        logger.info("NEWS pipeline completed | %d articles ingested", len(log_file_records))

    except Exception as exc:
        logger.error("NEWS pipeline failed | companyId=%s sourceId=%s error=%s",
                     company_id, source_id, exc, exc_info=True)
        update_ingestion_log(db, log_id, {
            "result.status":       "error",
            "result.errorCode":    type(exc).__name__,
            "result.errorMessage": str(exc),
        })
        raise


_NEWS_URL_KEYWORDS = ("newsroom", "/news/", "news.", "press-release", "pressrelease")


def _is_news_url(url: str) -> bool:
    """Return True if the URL looks like a news/newsroom page rather than a documents page."""
    lower = url.lower()
    return any(kw in lower for kw in _NEWS_URL_KEYWORDS)


def run(db, company: dict, source: dict) -> None:
    """
    Dispatch to WEB (PDF) or NEWS pipeline.

    Primary routing: sourceType field on the source document.
    Fallback: if sourceType=WEB but the URL is a newsroom/news page,
    route to the news pipeline automatically (supports sources where
    the type was stored as WEB instead of NEWS in the database).
    """
    source_type = source.get("sourceType", "").upper()
    source_url  = source.get("sourceUrl", "")

    if source_type == "NEWS" or (source_type == "WEB" and _is_news_url(source_url)):
        run_news_pipeline(db, company, source)
    elif source_type == "WEB":
        run_web_pipeline(db, company, source)
    else:
        raise ValueError(f"company_web_pipeline does not handle sourceType={source_type!r}")
