import os
import sys
from datetime import datetime, timezone
from pymongo import MongoClient

# Add parent directory to path to import config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set proxy environment variables early (before importing edgar)
os.environ["HTTP_PROXY"] = "http://10.19.27.39:3125"
os.environ["HTTPS_PROXY"] = "http://10.19.27.39:3125"
os.environ["http_proxy"] = "http://10.19.27.39:3125"
os.environ["https_proxy"] = "http://10.19.27.39:3125"

from config.config import Config
from src.crawler.edgar.crawler import EdgarCrawler
from src.parser.edgar.parser import EdgarParser
from src.pipeline.utils import ensure_dir, save_json, convert_json_to_excel


def get_mongo_client(config):
    """Get MongoDB client connection."""
    client = MongoClient(
        host=config.mongo_host,
        port=config.mongo_port,
        username=config.mongo_username,
        password=config.mongo_password,
        authSource=config.mongo_authentication_database
    )
    return client[config.mongo_database]


def run_filings_pipeline(config):
    """
    Fetch SEC filings (10-K, 10-Q, etc.) for configured tickers.
    Original functionality.
    """
    ensure_dir(config.raw_output_dir)
    ensure_dir(config.processed_output_dir)

    crawler = EdgarCrawler(config)

    for ticker in config.tickers:
        for form in config.forms:
            try:
                filings = crawler.fetch_filings(ticker, form)
                parsed_data = EdgarParser.parse_filings(ticker, form, filings)

                output_file = os.path.join(config.processed_output_dir, f"{ticker}_{form.replace('-', '_')}.json")
                save_json(parsed_data, output_file)
                print(f"Saved {ticker} {form} filings to {output_file}")
            except Exception as e:
                print(f"Failed for ticker={ticker}, form={form}, error={e}")


def run_financials_pipeline(config):
    """
    Fetch financial statements (Balance Sheet, Income Statement, Cash Flow)
    from MongoDB requests and store results.
    """
    crawler = EdgarCrawler(config)
    db = get_mongo_client(config)

    ensure_dir(config.raw_output_dir)
    ensure_dir(config.processed_output_dir)

    #get companies from mongoDb
    companies = list(db['companies'].find({
        'tickers.0': {'$exists': True}
    }))
    print(f"found {len(companies)} companies to process")
    if not companies:
        print("No campanies found")
        return
    summary = {'total': len(companies), 'successful': 0, 'failed': 0}

    #process each company
    for company in companies:
        company_name = company.get('name')
        company_id = company.get('_id')
        tickers = company.get('tickers', [])

        if not tickers:
            print(f"No ticker found for company: {company_name}")
            summary['failed'] += 1
            continue
        ticker_info = tickers[0]
        ticker = ticker_info.get('symbol')
        year = getattr(config, "financial_year", 2024)

        if not ticker:
            print(f"Invalid company record: {company}")
            summary['failed'] += 1
            continue
        print(f"\nProcessing: {company_name} - {ticker} - year{year}")
        print("-" * 50)

        print(f"\nProcessing: {ticker} - Year {year}")
        print("-" * 50)

        try:
            # Fetch financials
            financials_data = crawler.fetch_company_financials(ticker, year)

            if not financials_data:
                print(f"No financials available for {ticker}")
                summary['failed'] += 1
                db['company_requests'].update_one(
                    {'_id': request_id},
                    {'$set': {'status': 'failed', 'error': 'No financials available', 'failed_at': datetime.now(timezone.utc)}}
                )
                continue

            # Parse financials
            parsed_data = EdgarParser.parse_financials(ticker, financials_data, year)

            if not parsed_data:
                print(f"Failed to parse financials for {ticker}")
                summary['failed'] += 1
                continue

            # Add metadata
            parsed_data['company_name'] = financials_data.get('company_name', ticker)
            parsed_data['cik'] = str(financials_data.get('cik', ''))

            # Save to file
            output_file = os.path.join(config.processed_output_dir, f"{ticker}_{year}_financials.json")
            save_json(parsed_data, output_file)
            print(f"Saved JSON: {output_file}")

            #conert same JSON into excel
            excel_file = output_file.replace(".json", ".xlsx")
            try:
                convert_json_to_excel(output_file, excel_file)
                print(f"saved Excel: {excel_file}")
            except Exception as e:
                print(f"Excel conversion failed for {ticker}: {e}")

            #get company from existing companies collection
            company_doc = db["companies"].find_one({
                "tickers": {
                    "$elemMatch": {
                        "symbol": ticker,
                        "exchange": "SEC"
                    }
                }
            })
            if not company_doc:
                print(f"Company not found for ticker: {ticker}")
                summary["failed"] += 1
                return

            company_id = company_doc["_id"]
            #Optional: get source doc if you have a sources collection
            source_doc = db["sources"].find_one({"code": "SEC_EDGAR"})
            source_id = source_doc["_id"] if source_doc else "SEC_EDGAR"

            report_doc = {
                "CompanyId": company_id,
                "sourceId": source_id,
                "exchange": "SEC",
                "source": "SEC_EDGAR",
                "sourceFilingId": f"{year}_AR_{ticker}_SEC",
                "reportingType": "Annual",
                "fiscalYear": year,
                "status": "active",
                "files": [],
                "raw": {
                    "balance_sheet": parsed_data.get("financials", {}).get("balance_sheet"),
                    "income_statement": parsed_data.get("financials", {}).get("income_statement"),
                    "cash_flow_statement": parsed_data.get("financials", {}).get("cash_flow_statement"),
                }
            }
            db["reports"].update_one(
                {
                    "companyId": company_id,
                    "source": "SEC_EDGAR",
                    "sourceFilingId": f"{year}_AR_{ticker}_SEC"
                },
                {
                    "$set": report_doc,
                    "$setOnInsert": {
                        "createdAt": datetime.now(timezone.utc)
                    },
                    "$currentDate": {
                        "updatedAt": True
                    }
                },
                upsert=True
            )
            print("Stored in MongoDB: report collection")
            summary["successful"] += 1

            """
            # Store in MongoDB
            db['company_financials'].update_one(
                {'ticker': ticker, 'fiscal_year': year},
                {'$set': parsed_data, '$currentDate': {'updated_at': True}},
                upsert=True
            )
            print(f"Stored in MongoDB: company_financials collection")

            # Mark as completed
            db['company_requests'].update_one(
                {'_id': request_id},
                {'$set': {'status': 'completed', 'completed_at': datetime.now(timezone.utc)}}
            )

            summary['successful'] += 1

            # Print summary
            raw = parsed_data.get('raw', {})
            if raw.get('balance_sheet'):
                print(f"Balance Sheet: {len(raw['balance_sheet'])} line items")
            if raw.get('income_statement'):
                print(f"Income Statement: {len(raw['income_statement'])} line items")
            if raw.get('cash_flow_statement'):
                print(f"Cash Flow Statement: {len(raw['cash_flow_statement'])} line items")
            """

        except Exception as e:
            print(f"Error processing {ticker}: {e}")
            summary['failed'] += 1
            db['company_requests'].update_one(
                {'_id': request_id},
                {'$set': {'status': 'failed', 'error': str(e), 'failed_at': datetime.now(timezone.utc)}}
            )

    # Print summary
    print("\n" + "=" * 60)
    print("FINANCIALS PIPELINE SUMMARY")
    print("=" * 60)
    print(f"Total Requests: {summary['total']}")
    print(f"Successful: {summary['successful']}")
    print(f"Failed: {summary['failed']}")
    print("=" * 60)


def show_help():
    """Show usage help."""
    help_text = """
Usage: python src/main.py [command]

Commands:
  filings      Fetch SEC filings (10-K, 10-Q) for configured tickers
  financials   Fetch financial statements from MongoDB requests
  help         Show this help message

Examples:
  python src/main.py filings
  python src/main.py financials
  python src/main.py help

Default: If no command is provided, runs 'filings' pipeline
    """
    print(help_text)


def main():
    """Main entry point with command support."""
    config = Config()

    # Parse command line arguments
    command = sys.argv[1] if len(sys.argv) > 1 else 'filings'

    if command == 'help':
        show_help()
    elif command == 'filings':
        print("Running Filings Pipeline")
        print("=" * 60)
        run_filings_pipeline(config)
    elif command == 'financials':
        print("Running Financials Pipeline")
        print("=" * 60)
        run_financials_pipeline(config)
    else:
        print(f"Unknown command: {command}")
        print("Use 'python src/main.py help' for usage information")
        sys.exit(1)


if __name__ == "__main__":
    main()