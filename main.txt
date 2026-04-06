"""
Main entry point for the financial data ingestion pipeline.

Usage:
    python -m src.main --company ovh
    python -m src.main --company hkex
    python -m src.main --company all

For each company the pipeline:
  1. Reads the company document from MongoDB.
  2. Reads all active source documents for that company.
  3. Dispatches each source to the appropriate pipeline based on sourceType.
"""

import argparse
import sys

from src.pipeline.db import MongoDBClient
from src.pipeline.db_utils import get_company, get_sources_for_company
from src.logging import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Pipeline registry
# ---------------------------------------------------------------------------

def _get_pipeline(company_name: str, source_type: str):
    """Return the run() callable for (company, sourceType)."""
    key = (company_name.lower(), source_type.upper())

    if key[0] == "ovh":
        if key[1] in ("WEB", "NEWS"):
            from src.pipeline.ovh.company_web_pipeline import run
            return run
        if key[1] == "API":
            from src.pipeline.ovh.company_api_pipeline import run
            return run

    if key[0] == "hkex":
        if key[1] in ("WEB", "NEWS"):
            from src.pipeline.hkex.company_web_pipeline import run
            return run
        if key[1] == "API":
            from src.pipeline.hkex.company_api_pipeline import run
            return run

    raise ValueError(
        f"No pipeline registered for company={company_name!r} sourceType={source_type!r}"
    )


# ---------------------------------------------------------------------------
# Per-company runner
# ---------------------------------------------------------------------------

def run_company(db, company_name: str) -> None:
    logger.info("=" * 60)
    logger.info("Company: %s", company_name)
    logger.info("=" * 60)

    company = get_company(db, company_name)
    if not company:
        logger.error("Company %r not found in database. Skipping.", company_name)
        return

    sources = get_sources_for_company(db, str(company["_id"]))
    if not sources:
        logger.warning("No active sources found for %s.", company_name)
        return

    # Deduplicate by (sourceType, sourceUrl) — keep first occurrence
    total = len(sources)
    seen = set()
    deduped = []
    for s in sources:
        key = (s.get("sourceType", ""), s.get("sourceUrl", ""))
        if key not in seen:
            seen.add(key)
            deduped.append(s)
        else:
            logger.warning("Skipping duplicate source: type=%s url=%s id=%s",
                           key[0], key[1], s.get("_id"))
    sources = deduped

    logger.info("Found %d active source(s) (%d unique).", total, len(sources))

    for source in sources:
        source_type = source.get("sourceType", "UNKNOWN")
        source_url  = source.get("sourceUrl", "")
        logger.info("Source: %s  |  %s", source_type, source_url)

        try:
            pipeline_run = _get_pipeline(company_name, source_type)
            pipeline_run(db, company, source)
            logger.info("[OK] %s pipeline completed.", source_type)
        except ValueError as exc:
            logger.warning("[SKIP] %s", exc)
        except Exception as exc:
            logger.error("[ERROR] Pipeline failed: %s", exc, exc_info=True)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

KNOWN_COMPANIES = ["ovh", "hkex"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Financial data ingestion pipeline"
    )
    parser.add_argument(
        "--company",
        required=True,
        help=f"Company to process: {', '.join(KNOWN_COMPANIES)}, or 'all'",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.company.lower() == "all":
        companies = KNOWN_COMPANIES
    else:
        companies = [args.company.lower()]

    unknown = [c for c in companies if c not in KNOWN_COMPANIES]
    if unknown:
        logger.error("Unknown company/companies: %s. Valid options: %s, all",
                     ", ".join(unknown), ", ".join(KNOWN_COMPANIES))
        sys.exit(1)

    with MongoDBClient() as mongo:
        for company_name in companies:
            try:
                run_company(mongo.db, company_name)
            except Exception as exc:
                logger.critical("Unhandled error for %s: %s", company_name, exc, exc_info=True)

    logger.info("Pipeline complete.")


if __name__ == "__main__":
    main()
