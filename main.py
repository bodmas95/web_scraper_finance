"""
Main entry point for the financial data ingestion pipeline.

OVH usage:   python main.py --company ovh
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


# ==============================================================================
# OVH pipeline  (python main.py --company ovh)
# ==============================================================================

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
                logger.warning(
                    "Skipping duplicate source: type=%s url=%s id=%s",
                    key[0], key[1], s.get("_id")
                )

        sources = deduped
        logger.info("Found %d active source(s) (%d unique).", total, len(sources))

        for source in sources:
            source_type = source.get("sourceType", "UNKNOWN")
            source_url  = source.get("sourceUrl", "")
            logger.info("Source: %s | %s", source_type, source_url)
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


# ==============================================================================
# HKEX pipeline  (python main.py company=01929 year=2024)
# ==============================================================================

def _run_hkex():
    import sys
    import os
    from datetime import datetime, timedelta
    from src.crawler.hkexnews.crawler import HKEXCrawler
    from src.parser.hkexnews.parser import HKEXParser
    from src.pipeline.hkex.hkex_api_pipeline import build_gridfs_metadata, download_pdf, build_ingestion_file_entry
    from src.pipeline.db import MongoDBClient
    from config.config import DOWNLOAD_DIR
    from src.logging import get_logger

    def parse_arguments():
        """
        Parse command line arguments in the format key=value.

        Returns:
            dict: Parsed arguments
        """
        args = {}

        # Default values
        defaults = {
            'company': '01929',
            'source': 'HKEX_NEWS',
            'report_type': 'all',
            'year': str(datetime.now().year),
            'year_from': None,
            'year_to': None,
            'help': False
        }

        # Parse command line arguments
        for arg in sys.argv[1:]:
            if '=' in arg:
                key, value = arg.split('=', 1)
                args[key.strip()] = value.strip()
            elif arg in ['--help', '-h', 'help']:
                args['help'] = True
            else:
                print(f"Warning: Invalid argument format '{arg}'. Use key=value format.")

        # Merge with defaults
        for key, default_value in defaults.items():
            if key not in args:
                args[key] = default_value

        return args

    def show_help():
        """Display help information."""
        help_text = """
HKEX Data Ingestion Pipeline
=============================

Usage:
    python main.py [arguments]

Arguments:
    company=<symbol>       HKEX stock symbol (e.g., 01929, 00700)
    source=<source_name>   Data source name (default: HKEX_NEWS)
    report_type=<type>     Report type: annual, interim, all (default: all)
    year=<year>            Single year to fetch data for (default: current year)
    year_from=<year>       Start year for range (use with year_to)
    year_to=<year>         End year for range (use with year_from)

Examples:
    # Single year
    python main.py company=01929 year=2024
    python main.py company=00700 report_type=annual year=2023

    # Year range (NEW!)
    python main.py company=00288 year_from=2020 year_to=2024
    python main.py company=00700 report_type=interim year_from=2022 year_to=2024
    python main.py company=01929 report_type=annual year_from=2019 year_to=2023

    # Default (current year)
    python main.py company=01929
    python main.py  # Uses defaults

Supported Companies:
    01929 - Chow Tai Fook Jewellery Group Limited
    00700 - Tencent Holdings Limited
    00005 - HSBC Holdings plc
    00941 - China Mobile Limited
    (Add your company to the database first)

Report Types:
    annual   - Annual reports only
    interim  - Interim/quarterly reports only
    all      - All available reports

Year Range Features:
    - Maximum 10 years per run (to prevent overload)
    - Processes years sequentially
    - Shows progress for each year
    - Comprehensive summary at the end
    - Continues processing even if one year fails

Notes:
    - Make sure the company exists in your MongoDB database
    - Ensure the source configuration is set up properly
    - The pipeline will create necessary directories automatically
    - Year ranges are inclusive (year_from and year_to are both included)
"""
        print(help_text)

    def get_date_range(year: str, report_type: str):
        """
        Get start and end dates based on year and report type.

        Args:
            year: Year as string
            report_type: Type of report (annual, interim, all)

        Returns:
            tuple: (start_date, end_date) in YYYYMMDD format
        """
        try:
            year_int = int(year)
        except ValueError:
            raise ValueError(f"Invalid year format: {year}")

        if report_type.lower() == 'annual':
            # For annual reports, typically look at the full year
            start_date = f"{year_int}0101"
            end_date   = f"{year_int}1231"
        elif report_type.lower() == 'interim':
            # For interim reports, focus on mid-year periods
            start_date = f"{year_int}0101"
            end_date   = f"{year_int}1231"
        else:  # 'all' or any other value
            # Get all reports for the year
            start_date = f"{year_int}0101"
            end_date   = f"{year_int}1231"

        return start_date, end_date

    def validate_arguments(args):
        """
        Validate the parsed arguments.

        Args:
            args: Dictionary of parsed arguments

        Raises:
            ValueError: If arguments are invalid
        """
        # Validate company format (should be 5 digits for HKEX)
        company = args['company']
        if not company.isdigit() or len(company) != 5:
            raise ValueError(
                f"Invalid company format: {company}. Expected 5-digit HKEX code (e.g., 01929)"
            )

        # Check if using year range or single year
        using_range  = args.get('year_from') and args.get('year_to')
        current_year = datetime.now().year

        if using_range:
            # Validate year range
            try:
                year_from = int(args['year_from'])
                year_to   = int(args['year_to'])

                if year_from < 2000 or year_from > current_year + 1:
                    raise ValueError(
                        f"Invalid year_from: {year_from}. Must be between 2000 and {current_year + 1}"
                    )

                if year_to < 2000 or year_to > current_year + 1:
                    raise ValueError(
                        f"Invalid year_to: {year_to}. Must be between 2000 and {current_year + 1}"
                    )

                if year_from > year_to:
                    raise ValueError(
                        f"year_from ({year_from}) cannot be greater than year_to ({year_to})"
                    )

                if year_to - year_from > 9:  # 10 years max
                    raise ValueError(
                        f"Year range too large ({year_to - year_from + 1} years). Maximum allowed is 10 years."
                    )

            except ValueError as e:
                if "invalid literal" in str(e):
                    raise ValueError(
                        f"Invalid year format in range: {args.get('year_from', '')} or {args.get('year_to', '')}"
                    )
                raise
        else:
            # Validate single year
            try:
                year_int = int(args['year'])
                if year_int < 2000 or year_int > current_year + 1:
                    raise ValueError(
                        f"Invalid year: {year_int}. Must be between 2000 and {current_year + 1}"
                    )
            except ValueError as e:
                if "invalid literal" in str(e):
                    raise ValueError(f"Invalid year format: {args['year']}")
                raise

        # Validate report type
        valid_report_types = ['annual', 'interim', 'all']
        if args['report_type'].lower() not in valid_report_types:
            raise ValueError(
                f"Invalid report_type: {args['report_type']}. Must be one of: {', '.join(valid_report_types)}"
            )

    def run_pipeline(company: str, source: str, report_type: str, year: str) -> tuple:
        """
        Run HKEX ingestion pipeline with specified parameters.

        Args:
            company: HKEX stock symbol (e.g., "01929")
            source: Source name (e.g., "HKEX_NEWS")
            report_type: Report type (annual, interim, all)
            year: Year to fetch data for

        Returns:
            tuple: (reports_found, processed_count, failed_count)
        """
        logger = get_logger(__name__)

        # Get date range based on parameters
        start_date, end_date = get_date_range(year, report_type)

        logger.info("Starting HKEX pipeline with parameters:")
        logger.info(f"  Company: {company}")
        logger.info(f"  Source: {source}")
        logger.info(f"  Report Type: {report_type}")
        logger.info(f"  Year: {year}")
        logger.info(f"  Date Range: {start_date} to {end_date}")

        db_client = MongoDBClient()
        crawler   = HKEXCrawler()
        parser    = HKEXParser()

        try:
            db_client.connect()

            # Get company document
            company_doc = db_client.get_company_by_symbol(symbol=company, exchange="HKEX")
            if not company_doc:
                raise ValueError(
                    f"Company not found for symbol: {company}. Please ensure the company exists in the database."
                )

            # Get HKEX ticker information
            ticker     = db_client.get_hkex_ticker(company_doc)
            stock_code = ticker["symbol"]
            stock_id   = ticker["stockId"]

            # Get source configuration
            source_doc = db_client.get_source_for_company(
                company_id=company_doc["_id"],
                source_name=source,
            )

            if not source_doc:
                raise ValueError(
                    f"Source '{source}' not found for company: {company_doc['name']}. Please check source configuration."
                )

            # Log company information
            print("Company Information:")
            print(f"  Name: {company_doc.get('name')}")
            print(f"  Stock Code: {stock_code}")
            print(f"  Stock ID: {stock_id}")
            print(f"  Source: {source_doc.get('source')}")
            print(f"  Report Type Filter: {report_type}")
            print(f"  Year: {year}")

            # Fetch data from HKEX
            print("Fetching data from HKEX...")
            html = crawler.fetch_data(
                source_url=source_doc["sourceUrl"],
                source_filters=source_doc.get("filters", {}),
                stock_id=stock_id,
                start_date=start_date,
                end_date=end_date,
            )

            # Parse reports from HTML
            print("Parsing reports from HTML...")
            report_items = parser.extract_reports(html)

            # Filter reports by type if specified
            if report_type.lower() != 'all':
                original_count = len(report_items)
                report_items = [
                    item for item in report_items
                    if report_type.lower() in item.get('reportType', '').lower()
                ]
                print(f"  Filtered from {original_count} to {len(report_items)} reports based on type '{report_type}'")

            print(f"  Total reports found: {len(report_items)}")

            if not report_items:
                print("No reports found for the specified criteria")
                return

            # Create download folder
            folder = f"{DOWNLOAD_DIR}/{stock_code}"
            os.makedirs(folder, exist_ok=True)

            # Process each report
            processed_count = 0
            failed_count    = 0

            print(f"Processing {len(report_items)} reports...")

            for i, report_item in enumerate(report_items, 1):
                report_title = report_item.get('title', 'Unknown')
                print(f"  [{i}/{len(report_items)}] {report_title}")

                ingestion_files = []
                result = {
                    "status": "success",
                    "errorCode": None,
                    "errorMessage": None,
                }

                try:
                    # Download PDF file
                    filename, file_bytes = download_pdf(report_item["url"], folder)

                    # Build GridFS metadata
                    gridfs_metadata = build_gridfs_metadata(
                        company_doc=company_doc,
                        source_doc=source_doc,
                        stock_id=stock_id,
                        stock_code=stock_code,
                        report_item=report_item,
                    )

                    # Save file to GridFS
                    source_file_id = db_client.save_file_to_gridfs(
                        file_bytes=file_bytes,
                        filename=filename,
                        metadata=gridfs_metadata,
                    )

                    # Build file entry
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

                    # Upsert report to database
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
                    print(f"Success: {filename}")

                except Exception as e:
                    failed_count += 1
                    print(f"Failed: {str(e)}")

                    result = {
                        "status": "failed",
                        "errorCode": "DOWNLOAD_ERROR",
                        "errorMessage": str(e),
                    }

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

                # Insert ingestion log
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

            # Final summary
            print("Pipeline completed successfully!")
            print(f"  Company: {company_doc.get('name')} ({stock_code})")
            print(f"  Report Type: {report_type}")
            print(f"  Year: {year}")
            print(f"  Reports Found: {len(report_items)}")
            print(f"  Successfully Processed: {processed_count}")
            print(f"  Failed: {failed_count}")
            print(f"  Download Folder: {folder}")

            logger.info("HKEX pipeline completed successfully")

            return len(report_items), processed_count, failed_count

        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            print(f"Pipeline failed: {e}")
            raise
        finally:
            db_client.close()

    def run_pipeline_for_years(company: str, source: str, report_type: str, years: list) -> None:
        """
        Run HKEX ingestion pipeline for multiple years.

        Args:
            company: HKEX stock symbol (e.g., "01929")
            source: Source name (e.g., "HKEX_NEWS")
            report_type: Report type (annual, interim, all)
            years: List of years to process
        """
        logger = get_logger(__name__)

        total_reports    = 0
        total_processed  = 0
        total_failed     = 0
        successful_years = 0
        failed_years     = 0

        print("HKEX Data Ingestion Pipeline - Multi-Year")
        print("=" * 60)
        print(f"Company: {company}")
        print(f"Source: {source}")
        print(f"Report Type: {report_type}")
        print(f"Years: {', '.join(map(str, years))} ({len(years)} years)")
        print("=" * 60)

        for i, year in enumerate(years, 1):
            try:
                print(f"Processing Year {i}/{len(years)}: {year}")
                print("-" * 40)

                year_reports, year_processed, year_failed = run_pipeline(
                    company, source, report_type, str(year)
                )

                total_reports    += year_reports
                total_processed  += year_processed
                total_failed     += year_failed
                successful_years += 1

                print(f"Year {year} Summary: {year_processed} processed, {year_failed} failed")

            except Exception as e:
                logger.error(f"Error processing year {year}: {e}")
                print(f"  \u274c Year {year} failed: {e}")
                failed_years += 1

        # Final summary
        print("Multi-Year Pipeline Completed!")
        print("=" * 60)
        print(f"Years Requested: {len(years)}")
        print(f"Years Successfully Processed: {successful_years}")
        print(f"Years Failed: {failed_years}")
        print(f"Total Reports Found: {total_reports}")
        print(f"Successfully Downloaded: {total_processed}")
        print(f"Failed Downloads: {total_failed}")
        if total_reports > 0:
            print(f"Success Rate: {(total_processed / total_reports) * 100:.1f}%")
        print("=" * 60)

    def main():
        """Main entry point."""
        try:
            # Parse arguments
            args = parse_arguments()

            # Show help if requested
            if args.get('help'):
                show_help()
                return

            # Validate arguments
            validate_arguments(args)

            # Determine if using year range or single year
            if args.get('year_from') and args.get('year_to'):
                # Year range mode
                year_from = int(args['year_from'])
                year_to   = int(args['year_to'])
                years     = list(range(year_from, year_to + 1))

                run_pipeline_for_years(
                    company=args['company'],
                    source=args['source'],
                    report_type=args['report_type'],
                    years=years
                )
            else:
                # Single year mode
                print("HKEX Data Ingestion Pipeline")
                print("=" * 50)

                reports_found, processed_count, failed_count = run_pipeline(
                    company=args['company'],
                    source=args['source'],
                    report_type=args['report_type'],
                    year=args['year']
                )

                # Single year summary
                print("Pipeline completed successfully!")
                print(f"  Year: {args['year']}")
                print(f"  Reports Found: {reports_found}")
                print(f"  Successfully Processed: {processed_count}")
                print(f"  Failed: {failed_count}")

        except KeyboardInterrupt:
            print("Pipeline interrupted by user")
            sys.exit(1)
        except ValueError as e:
            print(f"Invalid arguments: {e}")
            print("\nUse 'python main.py help' for usage information")
            sys.exit(1)
        except Exception as e:
            print(f"Pipeline failed: {e}")
            sys.exit(1)

    # Execute the HKEX main function
    main()


if __name__ == "__main__":
    main()