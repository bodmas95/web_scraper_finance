"""
Main entry point for the financial data ingestion pipeline.

OVH  usage:  python main.py --company ovh
             python main.py --company all
HKEX usage:  python main.py company=01929 year=2024
             python main.py company=01929 year_from=2020 year_to=2024
             python main.py company=00700 report_type=annual year_from=2022 year_to=2024
"""

import sys


def main():
    """
    Dispatcher: route to the OVH or HKEX pipeline based on argument style.

    OVH  args use  --flag  format  (argparse).
    HKEX args use  key=value       format.
    """
    argv = sys.argv[1:]

    if not argv or any(a.startswith("--") for a in argv):
        _run_ovh()
    else:
        _run_hkex()


# =============================================================================
# OVH pipeline  (python main.py --company ovh)
# =============================================================================

def _run_ovh():
    import argparse
    from src.pipeline.db import MongoDBClient
    from src.pipeline.db_utils import get_company, get_sources_for_company
    from src.logging import get_logger

    logger = get_logger(__name__)

    def _get_pipeline(source_type):
        st = source_type.upper()
        if st in ("WEB", "NEWS"):
            from src.pipeline.ovh.company_web_pipeline import run
            return run
        if st == "API":
            from src.pipeline.ovh.company_api_pipeline import run
            return run
        raise ValueError(f"No OVH pipeline registered for sourceType={source_type!r}")

    def run_company(client, company_name):
        logger.info("=" * 60)
        logger.info("Company: %s", company_name)
        logger.info("=" * 60)

        company = get_company(client.db, company_name)
        if not company:
            logger.error("Company %r not found in database. Skipping.", company_name)
            return

        sources = get_sources_for_company(client.db, str(company["_id"]))
        if not sources:
            logger.warning("No active sources found for %s.", company_name)
            return

        total = len(sources)
        seen, deduped = set(), []
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
                pipeline_run = _get_pipeline(source_type)
                pipeline_run(client, company, source)
                logger.info("[OK] %s pipeline completed.", source_type)
            except ValueError as exc:
                logger.warning("[SKIP] %s", exc)
            except Exception as exc:
                logger.error("[ERROR] Pipeline failed: %s", exc, exc_info=True)

    parser = argparse.ArgumentParser(description="OVH financial data ingestion pipeline")
    parser.add_argument(
        "--company",
        default="ovh",
        help="Company name to look up in the database (default: ovh)",
    )
    args = parser.parse_args()

    with MongoDBClient() as mongo:
        try:
            run_company(mongo, args.company.lower())
        except Exception as exc:
            logger.critical("Unhandled error for %s: %s", args.company, exc, exc_info=True)

    logger.info("Pipeline complete.")


# =============================================================================
# HKEX pipeline  (python main.py company=01929 year=2024)
# =============================================================================

def _run_hkex():
    import os
    from datetime import datetime
    from src.crawler.hkex.crawler import HKEXCrawler
    from src.parser.hkex.parser import HKEXParser
    from src.pipeline.hkex.hkex_api_pipeline import (
        build_gridfs_metadata,
        download_pdf,
        build_ingestion_file_entry,
    )
    from src.pipeline.db import MongoDBClient
    from config.config import DOWNLOAD_DIR
    from src.logging import get_logger

    logger = get_logger(__name__)

    # ── argument parsing ──────────────────────────────────────────────────
    def parse_arguments():
        args = {}
        defaults = {
            "company":     "01929",
            "source":      "HKEX_NEWS",
            "report_type": "all",
            "year":        str(datetime.now().year),
            "year_from":   None,
            "year_to":     None,
            "help":        False,
        }
        for arg in sys.argv[1:]:
            if "=" in arg:
                key, value = arg.split("=", 1)
                args[key.strip()] = value.strip()
            elif arg in ["--help", "-h", "help"]:
                args["help"] = True
            else:
                print(f"Warning: Invalid argument format '{arg}'. Use key=value format.")
        for key, default_value in defaults.items():
            if key not in args:
                args[key] = default_value
        return args

    def show_help():
        print("""
HKEX Data Ingestion Pipeline
=============================
Usage:
    python main.py company=<symbol> [year=<year>] [report_type=<type>]
    python main.py company=<symbol> year_from=<year> year_to=<year>

Arguments:
    company=<symbol>     HKEX stock symbol (e.g., 01929, 00700)
    source=<name>        Data source name (default: HKEX_NEWS)
    report_type=<type>   annual | interim | all  (default: all)
    year=<year>          Single year (default: current year)
    year_from=<year>     Start year for range
    year_to=<year>       End year for range

Examples:
    python main.py company=01929 year=2024
    python main.py company=00288 year_from=2020 year_to=2024
    python main.py company=00700 report_type=annual year_from=2022 year_to=2024
""")

    def get_date_range(year, report_type):
        year_int = int(year)
        start_date = f"{year_int}0101"
        end_date   = f"{year_int}1231"
        return start_date, end_date

    def validate_arguments(args):
        company = args["company"]
        if not company.isdigit() or len(company) != 5:
            raise ValueError(
                f"Invalid company format: {company}. Expected 5-digit HKEX code (e.g., 01929)"
            )
        current_year = datetime.now().year
        using_range  = args.get("year_from") and args.get("year_to")
        if using_range:
            yf, yt = int(args["year_from"]), int(args["year_to"])
            if yf > yt:
                raise ValueError(f"year_from ({yf}) cannot be greater than year_to ({yt})")
            if yt - yf > 9:
                raise ValueError(f"Year range too large ({yt - yf + 1} years). Maximum is 10.")
        else:
            year_int = int(args["year"])
            if year_int < 2000 or year_int > current_year + 1:
                raise ValueError(f"Invalid year: {year_int}.")
        valid_types = ["annual", "interim", "all"]
        if args["report_type"].lower() not in valid_types:
            raise ValueError(
                f"Invalid report_type: {args['report_type']}. Must be one of: {', '.join(valid_types)}"
            )

    def run_pipeline(company, source, report_type, year):
        start_date, end_date = get_date_range(year, report_type)
        logger.info("Starting HKEX pipeline: company=%s report_type=%s year=%s",
                    company, report_type, year)

        db_client = MongoDBClient()
        crawler   = HKEXCrawler()
        parser    = HKEXParser()

        try:
            db_client.connect()

            company_doc = db_client.get_company_by_symbol(symbol=company, exchange="HKEX")
            if not company_doc:
                raise ValueError(f"Company not found for symbol: {company}")

            ticker     = db_client.get_hkex_ticker(company_doc)
            stock_code = ticker["symbol"]
            stock_id   = ticker["stockId"]

            source_doc = db_client.get_source_for_company(
                company_id=company_doc["_id"],
                source_name=source,
            )
            if not source_doc:
                raise ValueError(f"Source '{source}' not found for company: {company_doc['name']}")

            print(f"Company: {company_doc.get('name')}  stock_code={stock_code}")

            html = crawler.fetch_data(
                source_url=source_doc["sourceUrl"],
                source_filters=source_doc.get("filters", {}),
                stock_id=stock_id,
                start_date=start_date,
                end_date=end_date,
            )

            report_items = parser.extract_reports(html)

            if report_type.lower() != "all":
                report_items = [
                    item for item in report_items
                    if report_type.lower() in item.get("reportType", "").lower()
                ]

            print(f"Total reports found: {len(report_items)}")
            if not report_items:
                return 0, 0, 0

            folder = f"{DOWNLOAD_DIR}/{stock_code}"
            os.makedirs(folder, exist_ok=True)

            processed_count = failed_count = 0

            for i, report_item in enumerate(report_items, 1):
                print(f"  [{i}/{len(report_items)}] {report_item.get('title', 'Unknown')}")
                ingestion_files = []
                result = {"status": "success", "errorCode": None, "errorMessage": None}

                try:
                    filename, file_bytes = download_pdf(report_item["url"], folder)

                    gridfs_metadata = build_gridfs_metadata(
                        company_doc=company_doc,
                        source_doc=source_doc,
                        stock_id=stock_id,
                        stock_code=stock_code,
                        report_item=report_item,
                    )

                    source_file_id = db_client.save_file_to_gridfs(
                        file_bytes=file_bytes,
                        filename=filename,
                        metadata=gridfs_metadata,
                    )

                    file_entry = build_ingestion_file_entry(
                        file_id=source_file_id,
                        filename=filename,
                        file_url=report_item["url"],
                        file_bytes=file_bytes,
                        language=source_doc.get("language", "en"),
                        download_status="success",
                        downloader_error_message=None,
                    )
                    ingestion_files.append(file_entry)

                    db_client.upsert_report(
                        company_id=company_doc["_id"],
                        source_id=source_doc["_id"],
                        exchange=source_doc["exchange"],
                        source_name=source_doc["source"],
                        source_file_id=source_file_id,
                        reporting_id=report_item["reportingId"],
                        report_type=report_item["reportType"],
                        fiscal_year=report_item["fiscalYear"],
                        file_entry=file_entry,
                    )
                    processed_count += 1
                    print(f"    ✓ {filename}")

                except Exception as e:
                    failed_count += 1
                    result = {"status": "failed", "errorCode": "DOWNLOAD_ERROR", "errorMessage": str(e)}
                    file_entry = build_ingestion_file_entry(
                        file_id=None,
                        filename=report_item.get("filename", "unknown"),
                        file_url=report_item["url"],
                        file_bytes=None,
                        language=source_doc.get("language", "en"),
                        download_status="failed",
                        downloader_error_message=str(e),
                    )
                    ingestion_files.append(file_entry)
                    print(f"    ✗ {e}")

                db_client.insert_ingestion_log(
                    company_id=company_doc["_id"],
                    source_id=source_doc["_id"],
                    source_name=source_doc["source"],
                    exchange=source_doc["exchange"],
                    report_type=report_item["reportType"],
                    fiscal_year=report_item["fiscalYear"],
                    urls=[report_item["url"]],
                    files=ingestion_files,
                    result=result,
                    parser_status="success",
                    parser_error_message=None,
                )

            print(f"Done — processed={processed_count}  failed={failed_count}")
            return len(report_items), processed_count, failed_count

        finally:
            db_client.close()

    def run_pipeline_for_years(company, source, report_type, years):
        total_reports = total_processed = total_failed = 0
        successful_years = failed_years = 0
        print(f"HKEX Multi-Year Pipeline  company={company}  years={years}")
        print("=" * 60)
        for i, year in enumerate(years, 1):
            try:
                print(f"Year {i}/{len(years)}: {year}")
                yr, yp, yf = run_pipeline(company, source, report_type, str(year))
                total_reports    += yr
                total_processed  += yp
                total_failed     += yf
                successful_years += 1
            except Exception as e:
                logger.error("Error processing year %s: %s", year, e)
                failed_years += 1
        print(f"Summary: {successful_years} years OK, {failed_years} failed, "
              f"{total_processed}/{total_reports} reports downloaded")

    # ── entrypoint ────────────────────────────────────────────────────────
    try:
        args = parse_arguments()
        if args.get("help"):
            show_help()
            return
        validate_arguments(args)

        if args.get("year_from") and args.get("year_to"):
            years = list(range(int(args["year_from"]), int(args["year_to"]) + 1))
            run_pipeline_for_years(args["company"], args["source"], args["report_type"], years)
        else:
            reports_found, processed, failed = run_pipeline(
                args["company"], args["source"], args["report_type"], args["year"]
            )
            print(f"Pipeline complete — found={reports_found}  processed={processed}  failed={failed}")

    except KeyboardInterrupt:
        print("Pipeline interrupted by user")
        sys.exit(1)
    except ValueError as e:
        print(f"Invalid arguments: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
