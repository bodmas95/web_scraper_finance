"""
Streamlit UI for Financial Data Ingestion Pipeline

This application provides a beautiful user interface for:
1. Loading company information from MongoDB
2. Triggering OVH API data extraction
3. Viewing LEI and filings information
4. Displaying financial statements (Income Statement, Cash Flow, Assets, Liabilities, etc.)
5. Consolidating and downloading financial data
"""

import streamlit as st
import pandas as pd
import io
import json
from pathlib import Path
from datetime import datetime, timedelta
import sys
import os

# Add the project root to the path
sys.path.insert(0, str(Path(__file__).parent))

from src.pipeline.db import MongoDBClient
# ==============================================================================
# OVH IMPORTS
# ==============================================================================
from src.parser.ovh import parser as ovh_parser

# ==============================================================================
# HKEX IMPORTS
# ==============================================================================
from src.parser.hkexnews.parser import HKEXParser
from src import http_client

from config.config import get_section as _get_section, BASE_URL, SEARCH_URL

# ==============================================================================
# SEC EDGAR IMPORTS
# ==============================================================================
import types as _types
from datetime import timezone as _timezone

# Page configuration
st.set_page_config(
    page_title="Financial Data Ingestion Pipeline",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for beautiful UI
st.markdown("""
    <style>
    .main {
        padding: 0rem 1rem;
    }
    .stButton>button {
        width: 100%;
        background-color: #1A4080;
        color: white;
        border-radius: 5px;
        padding: 0.5rem 1rem;
        font-weight: 600;
        border: none;
        transition: all 0.3s;
    }
    .stButton>button:hover {
        background-color: #0D1B2A;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 10px;
        border-left: 4px solid #1A4080;
    }
    .success-box {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        border-radius: 5px;
        padding: 1rem;
        margin: 1rem 0;
    }
    .error-box {
        background-color: #f8d7da;
        border: 1px solid #f5c6cb;
        border-radius: 5px;
        padding: 1rem;
        margin: 1rem 0;
    }
    .info-box {
        background-color: #d1ecf1;
        border: 1px solid #bee5eb;
        border-radius: 5px;
        padding: 1rem;
        margin: 1rem 0;
    }
    h1 {
        color: #0D1B2A;
        font-weight: 700;
    }
    h2 {
        color: #1A4080;
        font-weight: 600;
    }
    h3 {
        color: #2E4057;
        font-weight: 500;
    }
    .dataframe {
        font-size: 0.9rem;
    }

    /* Elegant Reports Table */
    .reports-table {
        width: 100%;
        border-collapse: collapse;
        margin: 1rem 0;
        background-color: white;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        border-radius: 8px;
        overflow: hidden;
    }
    .reports-table thead {
        background: linear-gradient(135deg, #1A4080 0%, #2E4057 100%);
        color: white;
    }
    .reports-table th {
        padding: 1rem;
        text-align: left;
        font-weight: 600;
        font-size: 0.95rem;
        letter-spacing: 0.5px;
    }
    .reports-table td {
        padding: 1rem;
        border-bottom: 1px solid #e0e0e0;
        font-size: 0.9rem;
    }
    .reports-table tbody tr {
        transition: background-color 0.2s ease;
    }
    .reports-table tbody tr:hover {
        background-color: #f8f9fa;
    }
    .reports-table tbody tr:last-child td {
        border-bottom: none;
    }
    .report-title {
        font-weight: 600;
        color: #0D1B2A;
        margin-bottom: 0.25rem;
    }
    .report-meta {
        font-size: 0.85rem;
        color: #666;
    }
    .action-icon {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 36px;
        height: 36px;
        border-radius: 6px;
        margin: 0 4px;
        text-decoration: none;
        font-size: 18px;
    }
    .view-icon {
        background-color: #1A4080;
        color: white;
    }
    .view-icon:hover {
        background-color: #0D1B2A;
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(26, 64, 128, 0.3);
    }
    .download-icon {
        background-color: #28a745;
        color: white;
    }
    .download-icon:hover {
        background-color: #218838;
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(40, 167, 69, 0.3);
    }
    .actions-cell {
        text-align: center;
        white-space: nowrap;
    }
    </style>
    """, unsafe_allow_html=True)

# Initialize session state
if 'companies' not in st.session_state:
    st.session_state.companies = []
if 'regions' not in st.session_state:
    st.session_state.regions = []
if 'selected_region' not in st.session_state:
    st.session_state.selected_region = None
if 'countries' not in st.session_state:
    st.session_state.countries = []
if 'selected_country' not in st.session_state:
    st.session_state.selected_country = None
if 'filtered_companies' not in st.session_state:
    st.session_state.filtered_companies = []
if 'selected_company' not in st.session_state:
    st.session_state.selected_company = None
if 'selected_country' not in st.session_state:
    st.session_state.selected_country = None
if 'filtered_companies' not in st.session_state:
    st.session_state.filtered_companies = []
if 'selected_company' not in st.session_state:
    st.session_state.selected_company = None
if 'selected_company_name' not in st.session_state:
    st.session_state.selected_company_name = None
if 'is_company_validated' not in st.session_state:
    st.session_state.is_company_validated = False
if 'company_type' not in st.session_state:
    st.session_state.company_type = None
if 'filings' not in st.session_state:
    st.session_state.filings = []
if 'selected_filing' not in st.session_state:
    st.session_state.selected_filing = None
if 'financial_data' not in st.session_state:
    st.session_state.financial_data = {}
if 'consolidated_data' not in st.session_state:
    st.session_state.consolidated_data = None
if 'show_individual_filing' not in st.session_state:
    st.session_state.show_individual_filing = False
if 'lei' not in st.session_state:
    st.session_state.lei = None
if 'api_base' not in st.session_state:
    st.session_state.api_base = None
if 'filing_metadata' not in st.session_state:
    st.session_state.filing_metadata = {}
if 'ovh_sources' not in st.session_state:
    st.session_state.ovh_sources = []
if 'company_sources' not in st.session_state:
    st.session_state.company_sources = []
if 'selected_source' not in st.session_state:
    st.session_state.selected_source = None
if 'show_filings' not in st.session_state:
    st.session_state.show_filings = False
if 'hkex_stock_code' not in st.session_state:
    st.session_state.hkex_stock_code = None
if 'hkex_reports' not in st.session_state:
    st.session_state.hkex_reports = []
if 'hkex_reports_loaded' not in st.session_state:
    st.session_state.hkex_reports_loaded = False
if 'download_confirm' not in st.session_state:
    st.session_state.download_confirm = {}
if 'date_from' not in st.session_state:
    st.session_state.date_from = datetime.now() - timedelta(days=365*5)
if 'date_to' not in st.session_state:
    st.session_state.date_to = datetime.now()
if 'raw_api_data' not in st.session_state:
    st.session_state.raw_api_data = None
if 'all_facts' not in st.session_state:
    st.session_state.all_facts = []  # Flat list of all XBRL facts
if 'concept_map' not in st.session_state:
    st.session_state.concept_map = {}  # {sheet_type: {label: concept}}
if 'parsed_labels' not in st.session_state:
    st.session_state.parsed_labels = set()  # Set of parsed FY labels
# SEC Edgar state
if 'sec_ticker' not in st.session_state:
    st.session_state.sec_ticker = None
if 'edgar_financials' not in st.session_state:
    st.session_state.edgar_financials = None   # parsed financial data dict
if 'edgar_mongo_saved' not in st.session_state:
    st.session_state.edgar_mongo_saved = False
if 'edgar_excel_bytes' not in st.session_state:
    st.session_state.edgar_excel_bytes = None


def load_regions_from_mongodb():
    """Load unique regions from MongoDB companies collection"""
    try:
        with MongoDBClient() as client:
            regions = client.db.companies.distinct("region")
            return sorted([r for r in regions if r])  # Filter out None/empty and sort
    except Exception as e:
        st.error(f"Error loading regions from MongoDB: {str(e)}")
        return []


def load_countries_by_region(region):
    """Load unique countries for a specific region"""
    try:
        with MongoDBClient() as client:
            countries = client.db.companies.distinct("country", {"region": region})
            return sorted([c for c in countries if c])  # Filter out None/empty and sort
    except Exception as e:
        st.error(f"Error loading countries from MongoDB: {str(e)}")
        return []


def load_companies_by_region_country(region, country):
    """Load companies filtered by region and country"""
    try:
        with MongoDBClient() as client:
            companies = list(client.db.companies.find({
                "region": region,
                "country": country
            }))
            return companies
    except Exception as e:
        st.error(f"Error loading companies from MongoDB: {str(e)}")
        return []


def get_company_sources(company_id):
    """Get sources for a specific company"""
    try:
        with MongoDBClient() as client:
            sources = list(client.db.sources.find({"companyId": str(company_id)}))
            return sources
    except Exception as e:
        st.error(f"Error loading sources: {str(e)}")
        return []


def extract_lei_from_company(company):
    """Extract LEI from company document - only from tickers array"""
    tickers = company.get("tickers", [])

    # Handle case where tickers is a list of objects
    if isinstance(tickers, list):
        for ticker in tickers:
            if isinstance(ticker, dict):
                # Check if ticker has nested structure
                if "0" in ticker and isinstance(ticker["0"], dict):
                    ticker_data = ticker["0"]
                else:
                    ticker_data = ticker

                # Only get LEI from tickers
                if ticker_data.get("lei"):
                    return ticker_data["lei"]

    # Handle case where tickers is a dict with numeric keys
    if isinstance(tickers, dict):
        for key, ticker in tickers.items():
            if isinstance(ticker, dict) and ticker.get("lei"):
                return ticker["lei"]

    return None


def extract_hkex_ticker_from_company(company):
    """Extract HKEX ticker/source code from company document"""
    tickers = company.get("tickers", [])

    # Handle case where tickers is a list of objects
    if isinstance(tickers, list):
        for ticker in tickers:
            if isinstance(ticker, dict):
                # Check for nested structure
                if "0" in ticker and isinstance(ticker["0"], dict):
                    ticker_data = ticker["0"]
                else:
                    ticker_data = ticker

                # Look for HKEX exchange
                exchange = ticker_data.get('exchange', '').upper()
                if exchange in ['HKEX', 'HKG', 'SEHK', 'HK']:
                    symbol = ticker_data.get('symbol', '')
                    stock_id = ticker_data.get('stockId', symbol)

                    if symbol:
                        return {
                            'symbol': symbol,
                            'stockId': stock_id,
                            'exchange': exchange
                        }

    # Handle case where tickers is a dict with numeric keys
    if isinstance(tickers, dict):
        for key, ticker in tickers.items():
            if isinstance(ticker, dict):
                exchange = ticker.get('exchange', '').upper()
                if exchange in ['HKEX', 'HKG', 'SEHK', 'HK']:
                    symbol = ticker.get('symbol', '')
                    stock_id = ticker.get('stockId', symbol)

                    if symbol:
                        return {
                            'symbol': symbol,
                            'stockId': stock_id,
                            'exchange': exchange
                        }

    return None


def detect_company_type(company):
    """
    Detect company data source type.
    Returns (type, lei, hkex_ticker) for backwards compatibility.
    SEC companies return ('SEC', None, None) — call extract_sec_ticker_from_company() separately.
    Priority: SEC > OVH > HKEX
    """
    # Check for SEC ticker first
    sec_sym = extract_sec_ticker_from_company(company)
    if sec_sym:
        return 'SEC', None, None

    # Check for LEI (OVH)
    lei = extract_lei_from_company(company)
    if lei:
        return 'OVH', lei, None

    # Check for HKEX ticker
    hkex_ticker = extract_hkex_ticker_from_company(company)
    if hkex_ticker:
        return 'HKEX', None, hkex_ticker

    return None, None, None


# ==============================================================================
# OVH FUNCTIONS
# ==============================================================================

def save_raw_api_data_to_mongodb(lei, api_base, filings_data):
    """Save raw API data to MongoDB reports collection"""
    try:
        with MongoDBClient() as client:
            # Create a document for raw API data
            raw_data_doc = {
                "lei": lei,
                "apiBase": api_base,
                "dataType": "raw_api_filings",
                "filings": filings_data,
            }

            # Check if already exists
            existing = client.db.reports.find_one({
                "lei": lei,
                "dataType": "raw_api_filings"
            })

            if existing:
                # Update existing
                client.db.reports.update_one(
                    {"_id": existing["_id"]},
                    {"$set": {
                        "filings": filings_data,
                        "updatedAt": datetime.utcnow()
                    }}
                )
                return str(existing["_id"])
            else:
                # Insert new
                result = client.db.reports.insert_one(raw_data_doc)
                return str(result.inserted_id)
    except Exception as e:
        st.error(f"Error saving raw API data to MongoDB: {str(e)}")
        return None


def load_raw_api_data_from_mongodb(lei):
    """Load raw API data from MongoDB"""
    try:
        with MongoDBClient() as client:
            doc = client.db.reports.find_one({
                "lei": lei,
                "dataType": "raw_api_filings"
            })
            if doc:
                return doc.get("filings", [])
            return None
    except Exception as e:
        st.error(f"Error loading raw API data from MongoDB: {str(e)}")
        return None


def load_filings_from_api(lei, api_base):
    """Load filings list from API"""
    try:
        filings = ovh_parser.api_discover(lei)
        return filings
    except Exception as e:
        st.error(f"Error loading filings: {str(e)}")
        return []


def save_viewer_data_to_mongodb(lei, filing_id, period_end, viewer_json_path, report_html_path=None):
    """Save viewer_data.json and report_doc.html to MongoDB GridFS"""
    try:
        with MongoDBClient() as client:
            fy_label = f"FY{period_end[:4]}" if period_end else "UNKNOWN"

            # Save report_doc.html to GridFS if provided
            html_file_id = None
            if report_html_path and Path(report_html_path).exists():
                with open(report_html_path, 'rb') as f:
                    html_bytes = f.read()
                html_file_id = client.save_bytes_to_gridfs(
                    html_bytes,
                    filename=f"{filing_id}_{period_end}_report.html",
                    metadata={
                        "lei": lei,
                        "filingId": filing_id,
                        "periodEnd": period_end,
                        "fiscalYear": fy_label,
                        "contentType": "text/html",
                        "fileType": "report_html"
                    }
                )

            # Save viewer_data.json to GridFS
            viewer_file_id = None
            if viewer_json_path and Path(viewer_json_path).exists():
                with open(viewer_json_path, 'rb') as f:
                    viewer_bytes = f.read()
                viewer_file_id = client.save_bytes_to_gridfs(
                    viewer_bytes,
                    filename=f"{filing_id}_{period_end}_viewer_data.json",
                    metadata={
                        "lei": lei,
                        "filingId": filing_id,
                        "periodEnd": period_end,
                        "fiscalYear": fy_label,
                        "contentType": "application/json",
                        "fileType": "viewer_data"
                    }
                )

            # Create or update report document
            report_doc = {
                "lei": lei,
                "filingId": filing_id,
                "periodEnd": period_end,
                "fiscalYear": fy_label,
                "dataType": "xbrl_source_files",
                "reportHtmlFileId": html_file_id,
                "viewerDataFileId": viewer_file_id,
                "createdAt": datetime.utcnow(),
                "updatedAt": datetime.utcnow()
            }

            # Check if already exists
            existing = client.db.reports.find_one({
                "lei": lei,
                "filingId": filing_id,
                "dataType": "xbrl_source_files"
            })

            if existing:
                # Update existing
                client.db.reports.update_one(
                    {"_id": existing["_id"]},
                    {"$set": {
                        "reportHtmlFileId": html_file_id,
                        "viewerDataFileId": viewer_file_id,
                        "updatedAt": datetime.utcnow()
                    }}
                )
                return str(existing["_id"])
            else:
                result = client.db.reports.insert_one(report_doc)
                return str(result.inserted_id)
    except Exception as e:
        st.error(f"Error saving viewer data to MongoDB: {str(e)}")
        import traceback
        st.error(traceback.format_exc())
        return None


def save_parsed_data_to_mongodb(lei, filing_id, period_end, tables, report_path=None):
    """Save parsed financial data to MongoDB reports collection and GridFS"""
    try:
        with MongoDBClient() as client:
            # Save PDF to GridFS if provided
            pdf_file_id = None
            if report_path and Path(report_path).exists():
                with open(report_path, 'rb') as f:
                    pdf_bytes = f.read()
                pdf_file_id = client.save_bytes_to_gridfs(
                    pdf_bytes,
                    filename=f"{filing_id}_{period_end}.pdf",
                    metadata={
                        "lei": lei,
                        "filingId": filing_id,
                        "periodEnd": period_end,
                        "contentType": "application/pdf"
                    }
                )

            # Save parsed tables as JSON to GridFS
            tables_json = json.dumps(tables, ensure_ascii=False, indent=2)
            tables_file_id = client.save_text_to_gridfs(
                tables_json,
                filename=f"{filing_id}_{period_end}_tables.json",
                metadata={
                    "lei": lei,
                    "filingId": filing_id,
                    "periodEnd": period_end,
                    "contentType": "application/json"
                }
            )

            # Create report document
            fy_label = f"FY{period_end[:4]}" if period_end else "UNKNOWN"
            report_doc = {
                "lei": lei,
                "filingId": filing_id,
                "periodEnd": period_end,
                "fiscalYear": fy_label,
                "dataType": "parsed_financial_tables",
                "pdfFileId": pdf_file_id,
                "tablesFileId": tables_file_id,
                "tableNames": list(tables.keys()),
                "createdAt": datetime.utcnow(),
                "updatedAt": datetime.utcnow()
            }

            # Check if already exists
            existing = client.db.reports.find_one({
                "lei": lei,
                "filingId": filing_id,
                "dataType": "parsed_financial_tables"
            })

            if existing:
                # Update existing
                client.db.reports.update_one(
                    {"_id": existing["_id"]},
                    {"$set": {
                        "pdfFileId": pdf_file_id,
                        "tablesFileId": tables_file_id,
                        "tableNames": list(tables.keys()),
                        "updatedAt": datetime.utcnow()
                    }}
                )
                return str(existing["_id"])
            else:
                # Insert new
                result = client.db.reports.insert_one(report_doc)
                return str(result.inserted_id)
    except Exception as e:
        st.error(f"Error saving parsed data to MongoDB: {str(e)}")
        import traceback
        st.error(traceback.format_exc())
        return None


def load_parsed_data_from_mongodb(lei, filing_id):
    """Load parsed financial data from MongoDB"""
    try:
        with MongoDBClient() as client:
            # Find the report document
            report = client.db.reports.find_one({
                "lei": lei,
                "filingId": filing_id,
                "dataType": "parsed_financial_tables"
            })

            if not report:
                return None

            # Get tables from GridFS
            tables_file_id = report.get("tablesFileId")
            if not tables_file_id:
                return None

            # Read from GridFS
            from bson import ObjectId
            tables_data = client.fs.get(ObjectId(tables_file_id)).read()
            tables = json.loads(tables_data.decode('utf-8'))

            return tables
    except Exception as e:
        st.error(f"Error loading parsed data from MongoDB: {str(e)}")
        import traceback
        st.error(traceback.format_exc())
        return None


def parse_filing_data(filing, lei, api_base, silent=False):
    """Parse a specific OVH filing and extract financial tables

    Args:
        filing: Filing dictionary
        lei: LEI identifier
        api_base: API base URL
        silent: If True, don't show info messages (for batch consolidation)

    Returns:
        tuple: (tables, xbrl_facts) or (None, None) on error
    """
    try:
        filing_id = filing.get('_id', 'N/A')
        period_end = filing.get('period_end', '')

        # Set global variables for parser
        ovh_parser.LEI = lei
        ovh_parser.API_BASE = api_base

        # Get OVH config
        _OVH_CFG = _get_section("OVH")
        download_dir = Path(_OVH_CFG.get("download_dir"))

        # Create fiscal year directory
        fy_label = f"FY{period_end[:4]}" if period_end else "UNKNOWN"
        fy_dir = download_dir / fy_label
        fy_dir.mkdir(parents=True, exist_ok=True)

        # Download report
        if not silent:
            st.info(f"Downloading report for {fy_label}...")
        report_path = ovh_parser.download_report(filing, fy_dir)

        if not report_path:
            if not silent:
                st.error(f"No report available for {fy_label}")
            return None, None

        # Extract tables (WITHOUT XBRL concepts yet)
        if not silent:
            st.info(f"Extracting financial tables for {fy_label}...")
        tables = ovh_parser.extract_section_tables(report_path, fy_label)

        if not tables:
            if not silent:
                st.warning(f"No tables found for {fy_label}")
            return None, None

        # Normalize and add English labels (NO XBRL concepts yet)
        for tbl_name in tables:
            tables[tbl_name] = ovh_parser._detect_unit_and_normalize(tables[tbl_name])
            tables[tbl_name] = ovh_parser._add_english_column(tables[tbl_name])

        # Download XBRL facts
        if not silent:
            st.info(f"Downloading XBRL facts for {fy_label}...")
        json_path = ovh_parser.download_xbrl_json(filing, fy_dir)

        xbrl_facts = []
        if json_path:
            xbrl_facts = ovh_parser.parse_xbrl_facts(json_path, fy_label)
            if not silent:
                st.success(f"✅ Extracted {len(xbrl_facts)} XBRL facts")
        else:
            if not silent:
                st.warning("⚠ Could not download XBRL facts")

        if not silent:
            st.success(f"✅ {fy_label}: {len(tables)} statement types parsed")

        return tables, xbrl_facts

    except Exception as e:
        if not silent:
            st.error(f"Error parsing filing: {str(e)}")
            import traceback
            st.error(traceback.format_exc())
        return None, None


# ==============================================================================
# HKEX FUNCTIONS
# ==============================================================================

def search_hkex_annual_reports(stock_id, date_from=None, date_to=None):
    """Search for HKEX annual reports using the API"""
    try:
        # Format stock code with leading zeros (5 digits)
        formatted_stock_code = stock_id.zfill(5) if stock_id.isdigit() else stock_id

        # Format dates to YYYYMMDD
        if date_from:
            start_date = date_from.strftime('%Y%m%d')
        else:
            # Default to 5 years ago
            start_date = (datetime.now() - timedelta(days=365*5)).strftime('%Y%m%d')

        if date_to:
            end_date = date_to.strftime('%Y%m%d')
        else:
            end_date = datetime.now().strftime('%Y%m%d')

        # Use exact payload from working crawler.py - focusing on annual reports
        payload = {
            "lang": "EN",
            "category": "0",
            "market": "SEHK",
            "searchType": "1",
            "documentType": "",
            "t1code": "40000",  # Financial Statements/ESG Information
            "t2Gcode": "-2",
            "t2code": "40100",  # Annual Reports
            "stockId": formatted_stock_code,
            "from": start_date,
            "to": end_date,
            "MB-Daterange": "0",
            "title": ""
        }

        # Make request to HKEX using SEARCH_URL from config
        response = http_client.post(
            SEARCH_URL,
            data=payload,
            timeout=30
        )

        if response.status_code != 200:
            st.error(f"HKEX returned status code: {response.status_code}")
            return []

        # Parse reports using HKEXParser
        parser = HKEXParser()
        all_reports = parser.extract_reports(response.content)

        # Filter for annual reports only
        annual_reports = [
            report for report in all_reports
            if report.get('reportType') == 'annual'
        ]

        return annual_reports

    except Exception as e:
        st.error(f"Error searching HKEX reports: {str(e)}")
        import traceback
        st.error(traceback.format_exc())
        return []


def download_hkex_report(report_url, filename, save_dir):
    """Download a HKEX report PDF"""
    try:
        save_path = Path(save_dir) / filename

        # Create directory if it doesn't exist
        save_path.parent.mkdir(parents=True, exist_ok=True)

        # Check if already downloaded
        if save_path.exists():
            return save_path

        # Download the file
        response = http_client.get(report_url, timeout=180)
        response.raise_for_status()

        # Save the file
        save_path.write_bytes(response.content)

        return save_path
    except Exception as e:
        st.error(f"Error downloading HKEX report: {str(e)}")
        import traceback
        st.error(traceback.format_exc())
        return None


def _rebuild_concept_map():
    """Rebuild concept map from all parsed data (like sample_stream.py)"""
    if not st.session_state.financial_data:
        st.session_state.concept_map = {}
        return

    # Build facts_by_year index
    facts_by_year: dict = {}
    for fact in st.session_state.all_facts:
        year = fact.get("year")
        if year:
            facts_by_year.setdefault(year, []).append(fact)

    # Debug info
    print(f"[DEBUG] Rebuilding concept map...")
    print(f"[DEBUG] Total facts: {len(st.session_state.all_facts)}")
    print(f"[DEBUG] Facts by year: {[(y, len(f)) for y, f in facts_by_year.items()]}")
    print(f"[DEBUG] Financial data keys: {list(st.session_state.financial_data.keys())}")

    # Build concept map using value matching
    st.session_state.concept_map = ovh_parser.build_concept_map(
        st.session_state.financial_data,
        facts_by_year
    )

    # Debug output
    print(f"[DEBUG] Concept map built:")
    for sheet_type, concepts in st.session_state.concept_map.items():
        print(f"[DEBUG]   {sheet_type}: {len(concepts)} concepts matched")
        if concepts:
            # Show first 3 concepts
            for i, (label, concept) in enumerate(list(concepts.items())[:3]):
                print(f"[DEBUG]     - {label[:50]}: {concept}")


def convert_table_to_dataframe(table_rows, filing_label=None, concept_map_for_sheet=None):
    """Convert table rows to pandas DataFrame with XBRL concepts"""
    if not table_rows or len(table_rows) < 2:
        return pd.DataFrame()

    header = table_rows[0]
    data = table_rows[1:]

    # Ensure all rows have the same length
    max_cols = max(len(r) for r in table_rows)
    header = header + [""] * (max_cols - len(header))
    data = [r + [""] * (max_cols - len(r)) for r in data]

    # Clean data
    clean_data = [r for r in data if r[0] and len(r[0]) <= 200 and any(c.strip() for c in r)]

    if not clean_data:
        return pd.DataFrame()

    # Create dataframe
    df = pd.DataFrame(clean_data, columns=header[:max_cols])

    # ALWAYS add XBRL Concept column if concept_map provided and not already present
    if concept_map_for_sheet and "XBRL Concept" not in df.columns:
        concepts = []
        for _, row in df.iterrows():
            # Get French label (first column)
            french_label = str(row.iloc[0]) if len(row) > 0 else ""
            # Look up concept in map
            concept = concept_map_for_sheet.get(french_label, "")
            concepts.append(concept)

        # Insert XBRL Concept column after English label if it exists, otherwise after first column
        if "Label (English)" in df.columns:
            insert_pos = list(df.columns).index("Label (English)") + 1
        else:
            insert_pos = 1

        df.insert(insert_pos, "XBRL Concept", concepts)

    # Add filing source column if provided
    if filing_label:
        df.insert(0, 'Filing Source', filing_label)

    return df


def create_business_friendly_dataframe(all_data, filing_metadata, table_name):
    """Create a business-friendly consolidated dataframe with years in columns - uses fix_parser.py logic"""
    import re

    # Helper function to normalize labels for matching - from fix_parser.py
    def normalize_label(label):
        """Normalize a French label for cross-year matching across filings."""
        if not label:
            return ''
        label = label.strip()
        # Strip leading 4-digit year prefix: "2022 REVENU" -> "REVENU"
        label = re.sub(r'^\d{4}\s+', '', label)
        # Strip trailing formula references: " A", " B = ...", " D = A + B + C"
        label = re.sub(r'\s+[A-G](\s*[=+][A-Z0-9\s+=]*)?$', '', label)
        # Strip trailing note/article refs: " 4.10 - 4.11" or " 4.10"
        label = re.sub(r'\s+\d+\.\d+(\s*[--]\s*\d+\.\d+)*\s*$', '', label)
        # Remove trailing footnote refs: "(1)", "(2)"
        label = re.sub(r'\s*\(\d+\)\s*$', '', label)
        # Normalize apostrophe and quote variants
        label = label.replace('\u2019', "'").replace('\u2018', "'").replace('\u2032', "'")
        # Normalize non-breaking hyphen and en-dash
        label = label.replace('\u2011', '-').replace('\u2013', '-')
        # Normalize typography ligatures
        label = (label
                 .replace('\ufb00', 'ff').replace('\ufb01', 'fi')
                 .replace('\ufb02', 'fl').replace('\ufb03', 'ffi')
                 .replace('\ufb04', 'ffl').replace('\ufb05', 'st')
                 .replace('\ufb06', 'st'))
        # Collapse space-padded hyphens: " - " -> "_"
        label = re.sub(r'\s+-\s+', '-', label)
        # Normalize whitespace
        label = re.sub(r'\s+', ' ', label).strip()
        return label.lower()

    def is_noise_row(label):
        """Return True for rows that are footnotes, document titles, or other garbage."""
        if not label or len(label) > 160:
            return True
        noise_patterns = [
            r'document d.enregistrement universel',
            r'^ovhcloud\s+document',
            r'www\.ovhcloud\.com',
            r'informations financi.res et comptables',
        ]
        noise_re = re.compile('|'.join(noise_patterns), re.IGNORECASE)
        return bool(noise_re.search(label))

    # Collect all data for this table type
    all_rows = {}
    all_years = set()

    # Get reference table (most recent year) for row ordering
    ref_table = None
    for fy_label in sorted(all_data.keys(), reverse=True):
        if table_name in all_data[fy_label]:
            ref_table = all_data[fy_label][table_name]
            if ref_table and len(ref_table) > 1:
                break

    if not ref_table:
        return pd.DataFrame()

    # Build ordered list of labels from reference table
    ordered_labels = []  # list of (display_label, normalized_key)
    seen_norm = set()

    for row in ref_table[1:]:
        if not row or not row[0] or not row[0].strip():
            continue
        raw = row[0].strip()
        if is_noise_row(raw):
            continue
        norm = normalize_label(raw)
        if not norm or norm in seen_norm:
            continue
        ordered_labels.append((raw, norm))
        seen_norm.add(norm)

    # Supplement with labels from older filings not in reference table
    for fy_label in sorted(all_data.keys()):
        if table_name not in all_data[fy_label]:
            continue
        table_rows = all_data[fy_label][table_name]
        if not table_rows:
            continue
        for row in table_rows[1:]:
            if not row or not row[0] or not row[0].strip():
                continue
            raw = row[0].strip()
            if is_noise_row(raw):
                continue
            norm = normalize_label(raw)
            if not norm or norm in seen_norm:
                continue
            ordered_labels.append((raw, norm))
            seen_norm.add(norm)

    # Build English label map (normalized_key -> english_label)
    en_map = {}
    for fy_label in sorted(all_data.keys(), reverse=True):
        if table_name not in all_data[fy_label]:
            continue
        table_rows = all_data[fy_label][table_name]
        if not table_rows or len(table_rows) < 2:
            continue
        for row in table_rows[1:]:
            if not row or not row[0]:
                continue
            norm = normalize_label(row[0])
            if not norm or norm in en_map:
                continue
            en = (row[1].strip() if len(row) > 1 and row[1] else "")
            if en:
                en_map[norm] = en

    # Build year -> normalized_label -> value maps
    year_maps = {}

    # Collect all years first
    for fy_label in sorted(all_data.keys(), reverse=True):
        if table_name not in all_data[fy_label]:
            continue
        table_rows = all_data[fy_label][table_name]
        if not table_rows or len(table_rows) < 2:
            continue
        header = table_rows[0]
        for col_idx, col_header in enumerate(header):
            if col_idx <= 1:
                continue
            col_text = str(col_header).strip()
            year_matches = re.findall(r'\b(\d{4})\b', col_text)
            if year_matches:
                for year in year_matches:
                    if year.startswith('19') or year.startswith('20'):
                        all_years.add(year)

    # For each year, build value map from the best table
    for year in all_years:
        year_int = int(year)
        best_table = None

        # Try FY{year} first, then FY{year+1}
        for fy_candidate in [f"FY{year_int}", f"FY{year_int + 1}"]:
            if fy_candidate in all_data and table_name in all_data[fy_candidate]:
                table_rows = all_data[fy_candidate][table_name]
                if table_rows and len(table_rows) > 1:
                    header = table_rows[0]
                    # Check if this table has the year column
                    for col_idx, col_header in enumerate(header):
                        if year in str(col_header):
                            best_table = (table_rows, col_idx)
                            break
                if best_table:
                    break

        if not best_table:
            year_maps[year] = {}
            continue

        table_rows, year_col = best_table
        value_map = {}

        for row in table_rows[1:]:
            if not row or not row[0]:
                continue
            raw = row[0].strip()
            if is_noise_row(raw):
                continue
            norm = normalize_label(raw)
            if not norm or norm in value_map:
                continue
            value = row[year_col].strip() if year_col < len(row) and row[year_col] is not None else ""
            value_map[norm] = str(value) if value != "" else ""

        year_maps[year] = value_map

    # Get concept map for this table type
    concept_map_for_sheet = st.session_state.concept_map.get(table_name, {})

    # Build final rows
    for display_lbl, norm_key in ordered_labels:
        en = en_map.get(norm_key, "")

        # Get XBRL concept for this label
        xbrl_concept = concept_map_for_sheet.get(display_lbl, "")

        row_data = {
            'Label (French)': display_lbl,
            'Label (English)': en,
            'XBRL Concept': xbrl_concept
        }

        for year in sorted(all_years, reverse=True):
            row_data[year] = year_maps.get(year, {}).get(norm_key, "")

        all_rows[norm_key] = row_data

    if not all_rows:
        return pd.DataFrame()

    # Create dataframe
    df = pd.DataFrame(list(all_rows.values()))

    # Ensure all years are present as columns (even if empty)
    for year in all_years:
        if year not in df.columns:
            df[year] = '-'

    # Sort years in descending order (2025, 2024, 2023, 2022, 2021, ...)
    year_columns = sorted([col for col in df.columns if col not in ['Label (French)', 'Label (English)', 'XBRL Concept']], reverse=True)

    # Reorder columns: Labels first, XBRL Concept, then years in descending order
    column_order = ['Label (French)', 'Label (English)', 'XBRL Concept'] + year_columns
    df = df[column_order]

    # Fill NaN with '-'
    df = df.fillna('-')

    return df


def create_xbrl_facts_excel(all_facts):
    """Create Excel file with all XBRL facts from ALL filings (similar to sample_parser.py)"""
    try:
        import xlsxwriter

        output = io.BytesIO()
        wb = xlsxwriter.Workbook(output, {"nan_inf_to_errors": True})

        def F(**kw):
            d = {"font_name": "Arial", "font_size": 9, "valign": "vcenter"}
            d.update(kw)
            return wb.add_format(d)

        # ---- Sheet 1: All Facts ----
        ws = wb.add_worksheet("All Facts")
        hdr_cols = ["Source FY", "Concept (full)", "Namespace", "Concept (short)",
                    "Period Type", "Period Start", "Period End", "FY Year",
                    "Value (EUR)", "Value (thousands EUR)", "Unit", "Decimals"]
        col_widths = [10, 70, 14, 50, 10, 14, 14, 10, 20, 22, 30, 10]
        ws.set_row(0, 20)
        for ci, (h, w) in enumerate(zip(hdr_cols, col_widths)):
            ws.set_column(ci, ci, w)
            ws.write(0, ci, h, F(bold=True, align="center", border=1))

        for ri, fact in enumerate(all_facts, start=1):
            ws.write(ri, 0,  fact.get("fy_label", ""),       F(border=1))
            ws.write(ri, 1,  fact.get("concept", ""),        F(border=1))
            ws.write(ri, 2,  fact.get("namespace", ""),      F(border=1))
            ws.write(ri, 3,  fact.get("concept_short", ""),  F(border=1))
            ws.write(ri, 4,  fact.get("period_type", ""),    F(border=1, align="center"))
            ws.write(ri, 5,  fact.get("period_start", ""),   F(border=1, align="center"))
            ws.write(ri, 6,  fact.get("period_end", ""),     F(border=1, align="center"))
            ws.write(ri, 7,  fact.get("year", ""),           F(border=1, align="center"))
            val_eur = fact.get("value_eur")
            if val_eur is not None:
                ws.write_number(ri, 8,  val_eur,
                    F(border=1, align="right", num_format="#,##0.##;(#,##0.##)"))
                ws.write_number(ri, 9,  fact.get("value_thousands", 0),
                    F(border=1, align="right", num_format="#,##0;(#,##0)"))
            else:
                ws.write(ri, 8,  "",  F(border=1))
                ws.write(ri, 9,  "",  F(border=1))
            ws.write(ri, 10, fact.get("unit", ""),           F(border=1))
            ws.write(ri, 11, str(fact.get("decimals", "")),  F(border=1, align="center"))

        ws.autofilter(0, 0, len(all_facts), len(hdr_cols) - 1)
        ws.freeze_panes(1, 0)

        # ---- Sheet 2: By Concept (pivoted) ----
        ws2 = wb.add_worksheet("By Concept")
        # Collect unique (concept, period_type) pairs and year columns
        all_years = sorted({f.get("year") for f in all_facts if f.get("year")})
        concept_year_map: dict = {}
        for fact in all_facts:
            if fact.get("value_eur") is None:
                continue
            concept = fact.get("concept", "")
            namespace = fact.get("namespace", "")
            concept_short = fact.get("concept_short", "")
            period_type = fact.get("period_type", "")

            key = (concept, namespace, concept_short, period_type)
            if key not in concept_year_map:
                concept_year_map[key] = {}
            yr = fact.get("year")
            if yr:
                # Keep the most recent value for each concept+year combination
                # (in case same concept appears in multiple filings for same year)
                existing = concept_year_map[key].get(yr)
                if existing is None:
                    concept_year_map[key][yr] = fact.get("value_thousands")

        pivot_hdr = ["Concept (full)", "Namespace", "Concept (short)", "Period Type"] + [str(y) for y in all_years]
        pivot_widths = [70, 14, 50, 10] + [16] * len(all_years)
        ws2.set_row(0, 20)
        for ci, (h, w) in enumerate(zip(pivot_hdr, pivot_widths)):
            ws2.set_column(ci, ci, w)
            ws2.write(0, ci, h, F(bold=True, align="center", border=1))

        for ri, (key, yr_vals) in enumerate(concept_year_map.items(), start=1):
            concept, ns, cs, ptype = key
            ws2.write(ri, 0, concept,  F(border=1))
            ws2.write(ri, 1, ns,       F(border=1))
            ws2.write(ri, 2, cs,       F(border=1))
            ws2.write(ri, 3, ptype,    F(border=1, align="center"))
            for ci, yr in enumerate(all_years, start=4):
                val = yr_vals.get(yr)
                if val is not None:
                    ws2.write_number(ri, ci, val,
                        F(border=1, align="right", num_format="#,##0;(#,##0)"))
                else:
                    ws2.write(ri, ci, None, F(border=1))

        ws2.autofilter(0, 0, len(concept_year_map), len(pivot_hdr) - 1)
        ws2.freeze_panes(1, 4)

        wb.close()
        output.seek(0)
        return output
    except Exception as e:
        st.error(f"Error creating XBRL facts Excel: {str(e)}")
        import traceback
        st.error(traceback.format_exc())
        return None


def create_consolidated_excel(all_data, filing_metadata):
    """Create simple, clean Excel file without color coding - plain format"""
    try:
        output = io.BytesIO()

        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            workbook = writer.book

            # Simple format - no colors, just basic styling
            header_format = workbook.add_format({
                'bold': True,
                'font_name': 'Arial',
                'font_size': 10,
                'align': 'center',
                'valign': 'vcenter',
                'border': 1
            })

            cell_format = workbook.add_format({
                'font_name': 'Arial',
                'font_size': 10,
                'border': 1
            })

            number_format = workbook.add_format({
                'font_name': 'Arial',
                'font_size': 10,
                'border': 1,
                'num_format': '#,##0'
            })

            # Create Overview sheet
            overview_sheet = workbook.add_worksheet('Overview')
            overview_sheet.set_column('A:A', 32)
            overview_sheet.set_column('B:B', 30)

            overview_sheet.write(0, 0, 'Fiscal Year', header_format)
            overview_sheet.write(0, 1, 'Tables Extracted', header_format)

            row = 1
            for fy_label in sorted(all_data.keys(), reverse=True):
                fy_tables = all_data[fy_label]
                overview_sheet.write(row, 0, fy_label, cell_format)
                overview_sheet.write(row, 1, f"{len(fy_tables)} tables", cell_format)
                row += 1

            # Create sheets for each table type
            for table_name in ["Income Statement", "Assets", "Liabilities", "Cash Flow",
                                "Operating Expenses", "Capex Breakdown"]:
                # Create business-friendly dataframe
                combined_df = create_business_friendly_dataframe(all_data, filing_metadata, table_name)

                if combined_df.empty:
                    continue

                # Create worksheet
                ws = workbook.add_worksheet(table_name[:31])

                # Set column widths
                ws.set_column(0, 0, 50)  # Label (French)
                ws.set_column(1, 1, 50)  # Label (English)
                for i in range(2, len(combined_df.columns)):
                    ws.set_column(i, i, 15)  # Year columns

                # Write headers
                for ci, col in enumerate(combined_df.columns):
                    ws.write(0, ci, col, header_format)

                # Write data rows
                for ri, row_data in combined_df.iterrows():
                    actual_row = ri + 1
                    for ci, cell_value in enumerate(row_data):
                        is_label_col = ci <= 1
                        cell_str = str(cell_value)

                        if not is_label_col:
                            # Try to parse as number
                            num_val = ovh_parser._parse_french_number(cell_str)
                            if num_val is not None:
                                ws.write_number(actual_row, ci, num_val, number_format)
                            else:
                                ws.write(actual_row, ci, cell_str, cell_format)
                        else:
                            # Label columns
                            ws.write(actual_row, ci, cell_str, cell_format)

                # Freeze top row
                ws.freeze_panes(1, 0)

        output.seek(0)
        return output
    except Exception as e:
        st.error(f"Error creating Excel file: {str(e)}")
        import traceback
        st.error(traceback.format_exc())
        return None


# ==============================================================================
# SEC EDGAR HELPER FUNCTIONS
# ==============================================================================

def _get_edgar_proxy_urls():
    """
    Derive http/https proxy URL strings for edgartools (which uses env vars)
    based on [PROXY] proxy_use in config.ini.

      proxy_use = none     → direct, no proxy  (return "", "")
      proxy_use = server   → IP-based proxy at system_host:system_port, no auth
      proxy_use = system   → corporate proxy at corporate_host:corporate_port
                             Credentials are fully percent-encoded so special
                             characters in username/password (@ \ : etc.) do not
                             break URL parsing.
    """
    from config.config import load_config
    from urllib.parse import quote as _quote
    cfg = load_config()
    proxy_use = cfg.get("PROXY", "proxy_use", fallback="none").strip().lower()

    if proxy_use == "server":
        host = cfg.get("PROXY", "system_host", fallback="").strip()
        port = cfg.get("PROXY", "system_port", fallback="3125").strip()
        url  = f"http://{host}:{port}" if host else ""
        return url, url

    if proxy_use in ("system", "corporate"):
        host = cfg.get("PROXY", "corporate_host", fallback="").strip()
        port = cfg.get("PROXY", "corporate_port", fallback="8080").strip()
        user = cfg.get("PROXY", "corporate_username", fallback="").strip()
        pwd  = cfg.get("PROXY", "corporate_password", fallback="").strip()
        if host:
            if user and pwd:
                # Percent-encode ALL special chars in user/pass so URL parsing
                # is not tricked by backslash, @, colon, etc.
                # safe="" → encode everything except unreserved chars
                safe_user = _quote(user, safe="")
                safe_pwd  = _quote(pwd,  safe="")
                url = f"http://{safe_user}:{safe_pwd}@{host}:{port}"
            else:
                url = f"http://{host}:{port}"
            return url, url
        return "", ""

    # proxy_use = none
    return "", ""


def extract_sec_ticker_from_company(company):
    """
    Extract the best SEC identifier from the company document.

    Looks for a ticker entry with exchange='SEC'.  Within that entry, prefers
    the 'CIK' field (e.g. 'CIK0000753308') over 'symbol' (e.g. 'NEE') because
    CIK is unambiguous with edgartools.

    Expected MongoDB structure:
        { "tickers": [{ "symbol": "NEE", "exchange": "SEC", "CIK": "CIK0000753308" }] }

    Returns the raw string from MongoDB.  Call normalize_sec_identifier() on
    the result before passing it to edgartools' Company().
    """
    tickers = company.get("tickers", [])
    if isinstance(tickers, list):
        for ticker in tickers:
            if not isinstance(ticker, dict):
                continue
            td = ticker.get("0", ticker) if "0" in ticker else ticker
            if str(td.get("exchange", "")).upper() == "SEC":
                # Prefer CIK field; fall back to symbol
                cik = td.get("CIK", "").strip()
                if cik:
                    return cik
                sym = td.get("symbol", "").strip()
                if sym:
                    return sym
    if isinstance(tickers, dict):
        for td in tickers.values():
            if isinstance(td, dict) and str(td.get("exchange", "")).upper() == "SEC":
                cik = td.get("CIK", "").strip()
                if cik:
                    return cik
                sym = td.get("symbol", "").strip()
                if sym:
                    return sym
    return None


def normalize_sec_identifier(raw: str) -> str:
    """
    Convert the raw symbol stored in MongoDB into the identifier that
    edgartools' Company() accepts.

    'CIK0000753308' → '0000753308'   (strip 'CIK' prefix)
    'NEE'           → 'NEE'           (plain ticker, pass through)
    '753308'        → '0000753308'   (bare numeric CIK, zero-pad to 10 digits)
    """
    if not raw:
        return raw
    s = raw.strip()
    if s.upper().startswith("CIK"):
        return s[3:].lstrip("0").zfill(10)   # drop prefix, normalise padding
    if s.isdigit():
        return s.zfill(10)
    return s  # plain ticker symbol


def _patch_httpx_proxy(proxy_url: str) -> None:
    """
    Force edgar (httpx-based) to route through proxy_url.

    Why env vars alone don't work
    ─────────────────────────────
    • httpx uses HTTP_PROXY only for http:// URLs and HTTPS_PROXY for https://.
      SEC EDGAR is HTTPS, so only HTTPS_PROXY matters.
    • edgar creates its httpx.Client at module-import time (before our env vars
      are set), so the already-cached client never sees them.
    • edgar passes transport=<throttlecache> to httpx.Client(); when transport=
      is given httpx silently ignores proxy= — so patching __init__ with proxy=
      has no effect.

    Three-layer fix
    ───────────────
    1. Set HTTPS_PROXY (+ HTTP_PROXY) for any future bare httpx.Client() calls.
    2. Patch httpx.Client.__init__ using mounts= instead of proxy=.
       httpx always checks mounts BEFORE the default transport, so mounts work
       even when edgar passes transport=throttlecache.
    3. Inject proxy mounts directly into _mounts on edgar's already-created
       module-level client (found by scanning every attribute of
       edgar.httprequests for httpx.Client instances).
    """
    import os
    import httpx

    if proxy_url:
        for v in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
            os.environ[v] = proxy_url
    else:
        for v in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
            os.environ.pop(v, None)
        return  # direct mode — nothing to patch

    _proxy = httpx.Proxy(proxy_url)

    def _make_proxy_transport():
        return httpx.HTTPTransport(proxy=_proxy)

    # ── 2. Patch httpx.Client.__init__ ──────────────────────────────────────
    # Use mounts= so the proxy applies even when transport= is already given.
    # httpx evaluates mounts before _transport in _transport_for_url().
    _sentinel = "_edgar_proxy_patched"
    _orig_init = httpx.Client.__init__

    def _patched_init(self, *args, **kwargs):
        if not any(k in kwargs for k in ("proxy", "proxies", "mounts")):
            kwargs["mounts"] = {
                "http://":  _make_proxy_transport(),
                "https://": _make_proxy_transport(),
            }
        _orig_init(self, *args, **kwargs)

    httpx.Client.__init__ = _patched_init
    setattr(httpx.Client, _sentinel, True)

    # ── 3. Patch edgar's existing module-level client ────────────────────────
    # Scan ALL attributes — edgar's client may have any name.
    # Inject into _mounts so it takes precedence over the throttlecache
    # transport without removing it.
    try:
        import edgar.httprequests as _ehr
        for _attr in dir(_ehr):
            try:
                _obj = getattr(_ehr, _attr, None)
                if not isinstance(_obj, httpx.Client):
                    continue
                _mounts = getattr(_obj, "_mounts", None)
                if isinstance(_mounts, dict):
                    # Replace / add proxy entries; insertion at front gives
                    # highest priority for plain prefix matching.
                    _new = {
                        "http://":  _make_proxy_transport(),
                        "https://": _make_proxy_transport(),
                    }
                    _new.update(_mounts)   # keep existing specific patterns
                    _obj._mounts = _new
                else:
                    # Fallback: replace transport entirely
                    _obj._transport = _make_proxy_transport()
            except Exception:
                pass
    except Exception:
        pass


def _fetch_and_parse_edgar(ticker: str, year: int, identity: str):
    """
    Fetch financial statements from SEC EDGAR for ticker+year.
    Uses proxy settings from config.ini [PROXY] proxy_use.
    Returns parsed dict or None.
    Caches result in session_state to avoid repeated API calls.
    """
    from src.crawler.edgar.crawler import EdgarCrawler
    from src.parser.edgar.parser import EdgarParser

    cache_key = f"_edgar_{ticker}_{year}_{identity}"
    if cache_key in st.session_state:
        return st.session_state[cache_key]

    http_proxy, https_proxy = _get_edgar_proxy_urls()

    # Ensure edgartools' httpx client respects the proxy BEFORE any SEC call
    _patch_httpx_proxy(http_proxy)

    cfg = _types.SimpleNamespace(
        identity=identity,
        http_proxy=http_proxy,
        https_proxy=https_proxy,
        max_filings=1,
    )
    try:
        crawler = EdgarCrawler(cfg)
        raw = crawler.fetch_company_financials(ticker, year)
        if not raw:
            return None
        parsed = EdgarParser.parse_financials(ticker, raw, year)
        if not parsed:
            return None
        parsed["company_name"] = raw.get("company_name", ticker)
        parsed["cik"] = str(raw.get("cik", ""))
        st.session_state[cache_key] = parsed
        return parsed
    except Exception as e:
        st.error(f"Error fetching {ticker} from SEC EDGAR: {e}")
        return None


def _save_edgar_report_to_mongo(company_doc, ticker: str, year: int, parsed_data: dict) -> bool:
    """
    Upsert only the raw financial JSON into MongoDB reports collection.
    Excel is NOT stored here — only raw JSON.
    """
    try:
        with MongoDBClient() as client:
            company_id = company_doc["_id"]

            # Look up source document (try common field names)
            source_doc = client.db.sources.find_one({"code": "SEC_EDGAR"})
            if not source_doc:
                source_doc = client.db.sources.find_one(
                    {"$or": [{"name": "SEC_EDGAR"}, {"source": "SEC_EDGAR"}]}
                )
            source_id = source_doc["_id"] if source_doc else "SEC_EDGAR"

            financials = parsed_data.get("financials", {})
            report_doc = {
                "CompanyId":      company_id,
                "sourceId":       source_id,
                "exchange":       "SEC",
                "source":         "SEC_EDGAR",
                "sourceFilingId": f"{year}_AR_{ticker}_SEC",
                "reportingType":  "Annual",
                "fiscalYear":     year,
                "status":         "active",
                "files":          [],
                # Only raw JSON is stored — no Excel blobs
                "raw": {
                    "balance_sheet":       financials.get("balance_sheet"),
                    "income_statement":    financials.get("income_statement"),
                    "cash_flow_statement": financials.get("cash_flow_statement"),
                },
                "updatedAt": datetime.utcnow(),
            }
            client.db.reports.update_one(
                {
                    "CompanyId":      company_id,
                    "source":         "SEC_EDGAR",
                    "sourceFilingId": f"{year}_AR_{ticker}_SEC",
                },
                {
                    "$set":          report_doc,
                    "$setOnInsert":  {"createdAt": datetime.utcnow()},
                },
                upsert=True,
            )
            return True
    except Exception as e:
        st.error(f"MongoDB save error: {e}")
        return False


def _build_edgar_excel(parsed_data: dict, ticker: str, year: int) -> bytes:
    """
    Build an in-memory Excel workbook with three sheets:
    Balance Sheet | Income Statement | Cash Flow Statement.
    Returns raw bytes.  NOT saved to MongoDB.
    """
    out = io.BytesIO()
    fin = parsed_data.get("financials", {})
    company_name = parsed_data.get("company_name", ticker)
    sheets = [
        ("Balance Sheet",       fin.get("balance_sheet")),
        ("Income Statement",    fin.get("income_statement")),
        ("Cash Flow Statement", fin.get("cash_flow_statement")),
    ]

    try:
        import xlsxwriter
        wb = xlsxwriter.Workbook(out, {"in_memory": True, "nan_inf_to_errors": True})

        def F(**kw):
            d = {"font_name": "Arial", "font_size": 10, "valign": "vcenter"}
            d.update(kw)
            return wb.add_format(d)

        for sheet_name, records in sheets:
            ws = wb.add_worksheet(sheet_name)
            if not records:
                ws.write(0, 0, "No data available", F(italic=True))
                continue
            df = pd.DataFrame(records)
            cols = list(df.columns)
            # Title row
            ws.set_row(0, 22)
            ws.merge_range(0, 0, 0, max(len(cols) - 1, 0),
                f"{company_name} — {sheet_name}  |  FY{year}",
                F(bold=True, font_size=12, align="left", indent=1))
            # Header row
            ws.set_row(1, 18)
            for ci, col in enumerate(cols):
                ws.set_column(ci, ci, 50 if ci == 0 else 20)
                ws.write(1, ci, col, F(bold=True, align="center", border=1))
            # Data rows
            for ri, row in enumerate(df.itertuples(index=False), start=2):
                ws.set_row(ri, 15)
                for ci, val in enumerate(row):
                    if ci == 0:
                        ws.write(ri, ci, str(val) if val is not None else "",
                                 F(border=1, text_wrap=True))
                    elif isinstance(val, (int, float)) and pd.notna(val):
                        ws.write_number(ri, ci, val,
                            F(border=1, align="right",
                              num_format="#,##0.##;(#,##0.##)"))
                    else:
                        ws.write(ri, ci, str(val) if val is not None else "",
                                 F(border=1, align="right"))
            ws.freeze_panes(2, 1)

        wb.close()

    except ImportError:
        from openpyxl import Workbook
        from openpyxl.styles import Font, Alignment
        wb = Workbook()
        wb.remove(wb.active)
        for sheet_name, records in sheets:
            ws = wb.create_sheet(sheet_name)
            if not records:
                ws.cell(1, 1, "No data available")
                continue
            df = pd.DataFrame(records)
            for ci, col in enumerate(df.columns, 1):
                c = ws.cell(1, ci, col)
                c.font = Font(name="Arial", bold=True, size=10)
                c.alignment = Alignment(horizontal="center")
            for ri, row in enumerate(df.itertuples(index=False), 2):
                for ci, val in enumerate(row, 1):
                    ws.cell(ri, ci, val)
        wb.save(out)

    out.seek(0)
    return out.read()


def render_sec_edgar_section(company):
    """
    Full SEC EDGAR UI section rendered when company_type == 'SEC'.
    Reads proxy settings from config.ini [PROXY] to route all requests.
    """
    st.markdown("---")
    st.header("📈 SEC EDGAR — Financial Statements")

    raw_identifier = extract_sec_ticker_from_company(company)
    company_name = company.get("name", raw_identifier)

    if not raw_identifier:
        st.error("No SEC identifier (ticker or CIK) found for this company.")
        return

    ticker = normalize_sec_identifier(raw_identifier)
    # Show raw CIK from MongoDB and the normalized value passed to edgartools
    cik_display = raw_identifier if raw_identifier != ticker else ticker
    st.info(f"Company: **{company_name}**  |  CIK: `{cik_display}`")

    # ── Proxy status pill ─────────────────────────────────────────────────
    from config.config import load_config
    _cfg = load_config()
    _proxy_use = _cfg.get("PROXY", "proxy_use", fallback="none").strip().lower()
    proxy_labels = {"none": "🟢 Direct (no proxy)", "server": "🔵 Server proxy (IP-based)",
                    "system": "🟠 Corporate proxy (NTLM)"}
    st.caption(f"Network: {proxy_labels.get(_proxy_use, _proxy_use)}  "
               f"— controlled by `config.ini [PROXY] proxy_use`")

    st.markdown("---")

    # ── Identity (read silently from config.ini [EDGAR] identity) ─────────
    _cfg_identity = _cfg.get("EDGAR", "identity", fallback="").strip()
    identity = _cfg_identity if "@" in _cfg_identity else f"{_cfg_identity} research@example.com".strip()
    identity_ok = bool(identity and "@" in identity)
    if not identity_ok:
        st.warning("SEC identity not configured. Set `identity` in `config.ini [EDGAR]` as `Name email@domain.com`.", icon="⚠️")

    # ── Fiscal year input ─────────────────────────────────────────────────
    col_yr, _ = st.columns([1, 3])
    with col_yr:
        fiscal_year = st.number_input(
            "Fiscal Year", min_value=2000, max_value=2030,
            value=2024, step=1, key="sec_fiscal_year",
        )

    # ── Fetch button ──────────────────────────────────────────────────────
    col_btn, _ = st.columns([1, 3])
    with col_btn:
        fetch = st.button(
            f"🔄 Fetch {company_name} FY{fiscal_year}",
            disabled=not identity_ok,
            key="sec_fetch_btn",
        )

    if fetch:
        # Clear stale cached results
        for k in list(st.session_state.keys()):
            if k.startswith(f"_edgar_{ticker}_"):
                del st.session_state[k]
        st.session_state.edgar_financials = None
        st.session_state.edgar_mongo_saved = False
        st.session_state.edgar_excel_bytes = None

        with st.spinner(f"Fetching {company_name} (CIK {ticker}) FY{fiscal_year} from SEC EDGAR …"):
            result = _fetch_and_parse_edgar(ticker, int(fiscal_year), identity)

        if result:
            st.session_state.edgar_financials = result
            st.session_state.sec_ticker = ticker

            # Auto-save raw JSON to MongoDB
            saved = _save_edgar_report_to_mongo(company, ticker, int(fiscal_year), result)
            st.session_state.edgar_mongo_saved = saved
        else:
            st.error(f"No financial data returned for {company_name} (CIK {ticker}) FY{fiscal_year}.")

    # ── Display results ───────────────────────────────────────────────────
    result = st.session_state.get("edgar_financials")
    if result and st.session_state.get("sec_ticker") == ticker:
        fin = result.get("financials", {})
        res_year = result.get("fiscal_year", fiscal_year)
        res_company = result.get("company_name", ticker)
        bs_rec = fin.get("balance_sheet") or []
        is_rec = fin.get("income_statement") or []
        cf_rec = fin.get("cash_flow_statement") or []

        # Save status
        if st.session_state.edgar_mongo_saved:
            st.success(
                f"✅ Raw JSON saved to MongoDB `reports` collection  "
                f"(sourceFilingId: `{res_year}_AR_{ticker}_SEC`)",
                icon="💾",
            )
        else:
            st.info("Fetched but not saved to MongoDB (check connection or company doc).", icon="ℹ️")

        # Metrics
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Company", res_company[:22])
        c2.metric("Balance Sheet rows", len(bs_rec))
        c3.metric("Income Statement rows", len(is_rec))
        c4.metric("Cash Flow rows", len(cf_rec))

        # Tabs
        tab_bs, tab_is, tab_cf = st.tabs(
            ["🏦 Balance Sheet", "📊 Income Statement", "💵 Cash Flow"]
        )

        def _show_stmt(tab, records, key_suffix):
            with tab:
                if not records:
                    st.info("No data available.")
                    return
                df = pd.DataFrame(records)
                search = st.text_input(
                    "Search rows",
                    placeholder="Filter …",
                    key=f"sec_search_{key_suffix}",
                    label_visibility="collapsed",
                )
                if search:
                    mask = df.astype(str).apply(
                        lambda c: c.str.contains(search, case=False, na=False)
                    ).any(axis=1)
                    df = df[mask]
                st.dataframe(df, width="stretch", hide_index=True,
                             height=min(600, 40 + 35 * len(df)))

        _show_stmt(tab_bs, bs_rec, "bs")
        _show_stmt(tab_is, is_rec, "is")
        _show_stmt(tab_cf, cf_rec, "cf")

        # ── Excel download ────────────────────────────────────────────────
        st.markdown("---")
        st.subheader("Download Financial Statements")
        st.caption("Excel is generated on demand and **not** stored in MongoDB — only the raw JSON above is persisted.")

        col_gen, col_dl = st.columns([1, 2])
        with col_gen:
            if st.button("⚙️ Generate Excel", key="sec_gen_excel"):
                with st.spinner("Building Excel workbook …"):
                    st.session_state.edgar_excel_bytes = _build_edgar_excel(
                        result, ticker, res_year
                    )
                st.success("Excel ready for download.")

        with col_dl:
            if st.session_state.edgar_excel_bytes:
                st.download_button(
                    label=f"⬇ Download {res_company} FY{res_year} (.xlsx)",
                    data=st.session_state.edgar_excel_bytes,
                    file_name=f"{ticker}_{res_year}_financial_statements.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="sec_dl_excel",
                )


# Main UI
def main():
    # Header
    st.title("Financial Data Ingestion Pipeline")
    st.markdown("---")

    # Sidebar
    with st.sidebar:
        st.header("Configuration")

        if st.button("Refresh Companies"):
            st.session_state.companies = load_companies_from_mongodb()
            st.session_state.hkex_reports_loaded = False
            st.success(f"Loaded {len(st.session_state.companies)} companies")

        st.markdown("---")
        st.caption("Select a company to view its data")

    # Load regions if not already loaded
    if not st.session_state.regions:
        st.session_state.regions = load_regions_from_mongodb()

    if st.session_state.regions:
        # Region selection
        st.header("Select Region, Country, and Company")

        col1, col2, col3 = st.columns(3)

        with col1:
            # Region selector with index to handle changes properly
            region_index = 0
            if st.session_state.selected_region and st.session_state.selected_region in st.session_state.regions:
                region_index = st.session_state.regions.index(st.session_state.selected_region)

            selected_region = st.selectbox(
                "Select Region",
                options=st.session_state.regions,
                index=region_index,
                key="region_selector"
            )

            # Check if region changed
            if selected_region != st.session_state.selected_region:
                # Region changed - reset everything
                st.session_state.selected_region = selected_region
                st.session_state.countries = load_countries_by_region(selected_region)
                st.session_state.selected_country = None
                st.session_state.filtered_companies = []
                st.session_state.selected_company = None
                st.session_state.selected_company_name = None
                st.session_state.is_company_validated = False
                st.session_state.company_sources = []
                st.session_state.company_type = None
                st.session_state.lei = None
                st.session_state.hkex_ticker = None
                st.session_state.filings = []
                st.session_state.show_filings = False
                st.session_state.hkex_reports = []
                st.session_state.hkex_reports_loaded = False
                st.session_state.consolidated_data = None
                st.session_state.financial_data = {}
                st.session_state.all_facts = []
                st.session_state.concept_map = {}
                st.session_state.parsed_labels = set()
                st.session_state.show_individual_filing = False
                st.session_state.edgar_financials = None
                st.session_state.edgar_mongo_saved = False
                st.session_state.edgar_excel_bytes = None
                st.rerun()

        with col2:
            if st.session_state.selected_region:
                # Load countries for the selected region if not already loaded
                if not st.session_state.countries:
                    st.session_state.countries = load_countries_by_region(st.session_state.selected_region)

                if st.session_state.countries:
                    # Country selector with proper index handling
                    country_index = 0
                    if st.session_state.selected_country and st.session_state.selected_country in st.session_state.countries:
                        country_index = st.session_state.countries.index(st.session_state.selected_country)

                    selected_country = st.selectbox(
                        "Select Country",
                        options=st.session_state.countries,
                        index=country_index,
                        key="country_selector"
                    )

                    # Check if country changed
                    if selected_country != st.session_state.selected_country:
                        # Country changed - reset company-related states
                        st.session_state.selected_country = selected_country
                        st.session_state.filtered_companies = load_companies_by_region_country(
                            st.session_state.selected_region,
                            selected_country
                        )
                        st.session_state.selected_company = None
                        st.session_state.selected_company_name = None
                        st.session_state.is_company_validated = False
                        st.session_state.company_sources = []
                        st.session_state.company_type = None
                        st.session_state.lei = None
                        st.session_state.hkex_ticker = None
                        st.session_state.filings = []
                        st.session_state.show_filings = False
                        st.session_state.hkex_reports = []
                        st.session_state.hkex_reports_loaded = False
                        st.session_state.consolidated_data = None
                        st.session_state.financial_data = {}
                        st.session_state.all_facts = []
                        st.session_state.concept_map = {}
                        st.session_state.parsed_labels = set()
                        st.session_state.show_individual_filing = False
                        st.session_state.edgar_financials = None
                        st.session_state.edgar_mongo_saved = False
                        st.session_state.edgar_excel_bytes = None
                        st.rerun()
                else:
                    st.info("No countries found for this region")
            else:
                st.info("Please select a region first")

        with col3:
            if st.session_state.selected_region and st.session_state.selected_country:
                # Always reload companies for the current region and country to ensure fresh data
                current_companies = load_companies_by_region_country(
                    st.session_state.selected_region,
                    st.session_state.selected_country
                )

                # Update filtered companies if they've changed
                if current_companies != st.session_state.filtered_companies:
                    st.session_state.filtered_companies = current_companies
                    # Reset company selection when the list changes
                    st.session_state.selected_company = None
                    st.session_state.selected_company_name = None
                    st.session_state.is_company_validated = False

                if st.session_state.filtered_companies:
                    company_names = [f"{c.get('name', 'Unknown')}"
                                     for c in st.session_state.filtered_companies]

                    # Company selector - only use saved index if the company name is in the current list
                    company_index = 0
                    if st.session_state.selected_company_name and st.session_state.selected_company_name in company_names:
                        company_index = company_names.index(st.session_state.selected_company_name)
                    else:
                        # If saved company is not in the list, reset it
                        st.session_state.selected_company_name = None
                        st.session_state.selected_company = None
                        st.session_state.is_company_validated = False

                    selected_company_name = st.selectbox(
                        "Select Company",
                        options=company_names,
                        index=company_index,
                        key="company_selector"
                    )

                    # Always validate and update when a company is selected from the dropdown
                    if selected_company_name:
                        # Get selected company
                        selected_idx = company_names.index(selected_company_name)
                        company = st.session_state.filtered_companies[selected_idx]

                        # Check if this is a new selection or validation needed
                        if (selected_company_name != st.session_state.selected_company_name or
                                not st.session_state.is_company_validated):
                            # Update session state
                            st.session_state.selected_company = company
                            st.session_state.selected_company_name = selected_company_name
                            st.session_state.company_sources = get_company_sources(company.get('_id'))
                            st.session_state.is_company_validated = True

                            # Reset other states
                            st.session_state.filings = []
                            st.session_state.show_filings = False
                            st.session_state.hkex_reports = []
                            st.session_state.hkex_reports_loaded = False
                            st.session_state.consolidated_data = None
                            st.session_state.financial_data = {}
                            st.session_state.all_facts = []
                            st.session_state.concept_map = {}
                            st.session_state.parsed_labels = set()
                            st.session_state.show_individual_filing = False
                            st.session_state.edgar_financials = None
                            st.session_state.edgar_mongo_saved = False
                            st.session_state.edgar_excel_bytes = None
                            st.rerun()
                else:
                    st.info("No companies found for this region and country")
            else:
                if not st.session_state.selected_region:
                    st.info("Please select a region first")
                elif not st.session_state.selected_country:
                    st.info("Please select a country first")

    st.markdown("---")

    # Validate that all three selections are made and company is validated
    if not st.session_state.selected_region:
        st.info("Please select a region to continue")
        return

    if not st.session_state.selected_country:
        st.info("Please select a country to continue")
        return

    if not st.session_state.selected_company or not st.session_state.is_company_validated:
        st.info("Please select a company to continue")
        return

    # All validations passed - proceed with company information display
    company = st.session_state.selected_company

    # Detect company type automatically
    company_type, lei, hkex_ticker = detect_company_type(company)
    st.session_state.company_type = company_type

    if lei:
        st.session_state.lei = lei
    if hkex_ticker:
        st.session_state.hkex_ticker = hkex_ticker
        st.session_state.hkex_stock_code = hkex_ticker.get('symbol', '')

    # Display company information for all companies after selection
    # Display Company Info and Sources Info side by side
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Company Information")
        st.write(f"**Name:** {company.get('name', 'N/A')}")
        st.write(f"**Company ID:** {company.get('_id', 'N/A')}")
        st.write(f"**Data Source:** {company_type if company_type else 'N/A'}")

        # Display LEI if available
        if lei:
            st.write(f"**LEI:** {lei}")

        # Display HKEX stock info if available
        if hkex_ticker:
            st.write(f"**Stock Code:** {hkex_ticker.get('symbol', 'N/A')}")
            st.write(f"**Stock ID:** {hkex_ticker.get('stockId', 'N/A')}")

        # Display tickers
        st.markdown("**Tickers:**")
        tickers = company.get('tickers', [])
        if isinstance(tickers, list):
            for ticker in tickers:
                if isinstance(ticker, dict):
                    if "0" in ticker and isinstance(ticker["0"], dict):
                        ticker_data = ticker["0"]
                    else:
                        ticker_data = ticker

                    exchange = ticker_data.get('exchange', 'N/A')
                    symbol = ticker_data.get('symbol', 'N/A')
                    ticker_lei = ticker_data.get('lei', '')

                    if exchange != 'N/A' or symbol != 'N/A':
                        st.write(f"- {exchange}: {symbol}")
                        if ticker_lei:
                            st.caption(f"  LEI: {ticker_lei}")
        elif isinstance(tickers, dict):
            for key, ticker in tickers.items():
                if isinstance(ticker, dict):
                    exchange = ticker.get('exchange', 'N/A')
                    symbol = ticker.get('symbol', 'N/A')
                    ticker_lei = ticker.get('lei', '')

                    if exchange != 'N/A' or symbol != 'N/A':
                        st.write(f"- {exchange}: {symbol}")
                        if ticker_lei:
                            st.caption(f"  LEI: {ticker_lei}")

    with col2:
        st.markdown("### Available Data Sources")

        if st.session_state.company_sources:
            # Create dataframe for sources
            sources_data = []
            for idx, source in enumerate(st.session_state.company_sources):
                sources_data.append({
                    '#': idx + 1,
                    'Source': source.get('source', 'N/A'),
                    'Type': source.get('sourceType', 'N/A'),
                    'URL': source.get('sourceUrl', 'N/A'),
                    'Status': source.get('status', 'N/A')
                })

            sources_df = pd.DataFrame(sources_data)
            st.dataframe(
                sources_df,
                width='stretch',
                hide_index=True,
                height=200
            )
        else:
            st.info("No data sources found for this company")

    st.markdown("---")

    # Show date range for HKEX only
    if company_type == 'HKEX':
        st.markdown("### Search Configuration")
        col1, col2 = st.columns(2)

        with col1:
            date_from = st.date_input(
                "From Date",
                value=st.session_state.date_from,
                help="Start date for report search"
            )
            st.session_state.date_from = date_from

        with col2:
            date_to = st.date_input(
                "To Date",
                value=st.session_state.date_to,
                help="End date for report search"
            )
            st.session_state.date_to = date_to

    st.markdown("---")

    # ==============================================================================
    # OVH SECTION
    # ==============================================================================
    if company_type == 'OVH':
        st.header("OVH Data Source")

        # Sources are already loaded automatically when company is selected
        st.session_state.ovh_sources = st.session_state.company_sources

        # Find API source and show button
        api_source = None
        for source in st.session_state.ovh_sources:
            if source.get('sourceType') == 'API':
                api_source = source
                break

        if api_source:
            if st.button("Load Filings from API", type="primary", width="stretch"):
                api_base = api_source.get('sourceUrl', 'https://filings.xbrl.org')
                st.session_state.api_base = api_base
                st.session_state.selected_source = api_source
                st.session_state.show_filings = True

                # First, try to load from MongoDB
                cached_filings = load_raw_api_data_from_mongodb(lei)

                if cached_filings:
                    st.info("Loaded filings from MongoDB cache")
                    st.session_state.filings = cached_filings
                    st.session_state.raw_api_data = cached_filings
                else:
                    # Load from API and save to MongoDB
                    with st.spinner("Loading OVH filings from API..."):
                        ovh_parser.LEI = lei
                        ovh_parser.API_BASE = api_base
                        filings = load_filings_from_api(lei, api_base)

                    if filings:
                        # Save to MongoDB
                        save_raw_api_data_to_mongodb(lei, api_base, filings)
                        st.session_state.filings = filings
                        st.session_state.raw_api_data = filings
                        st.success(f"Loaded and saved {len(filings)} filings to MongoDB")
                    else:
                        st.warning("No filings found")

                if st.session_state.filings:
                    st.rerun()
        else:
            st.info("No API source available for this company")

    # ==============================================================================
    # HKEX SECTION
    # ==============================================================================
    elif company_type == 'HKEX':
        st.header("HKEX Annual Reports")

        hkex_ticker = st.session_state.hkex_ticker
        stock_id = hkex_ticker.get('stockId', '')

        # Search button - only show reports after clicking
        if st.button("Search Annual Reports", type="primary"):
            with st.spinner(f"Searching annual reports for {stock_id}..."):
                reports = search_hkex_annual_reports(
                    stock_id,
                    st.session_state.date_from,
                    st.session_state.date_to
                )
            st.session_state.hkex_reports = reports
            st.session_state.hkex_reports_loaded = True

            if reports:
                st.success(f"Found {len(reports)} annual reports")
                st.rerun()
            else:
                st.warning("No annual reports found for the selected date range")

        # Display reports only after search button is clicked
        if st.session_state.hkex_reports_loaded and st.session_state.hkex_reports:
            st.markdown("---")
            st.header("Available Annual Reports")

            # Create DataFrame for better display
            reports_data = []
            for idx, report in enumerate(st.session_state.hkex_reports):
                reports_data.append({
                    'Index': idx,
                    'Title': report.get('title', 'N/A'),
                    'Fiscal Year': report.get('fiscalYear', 'N/A'),
                    'Report Type': report.get('reportType', 'N/A').upper(),
                    'Filename': report.get('filename', 'N/A'),
                })

            reports_df = pd.DataFrame(reports_data)

            # Display summary
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Reports", len(st.session_state.hkex_reports))
            with col2:
                years = [r.get('fiscalYear') for r in st.session_state.hkex_reports if r.get('fiscalYear')]
                if years:
                    st.metric("Year Range", f"{min(years)} - {max(years)}")
            with col3:
                st.metric("Company", company.get('name', 'N/A')[:20] + "...")

            st.markdown("---")

            # Display reports in dataframe with action icons
            st.subheader("Annual Reports")

            # Create dataframe
            reports_data = []
            for idx, report in enumerate(st.session_state.hkex_reports):
                reports_data.append({
                    '#': idx + 1,
                    'Report Title': report.get('title', 'N/A'),
                    'Fiscal Year': report.get('fiscalYear', 'N/A'),
                    'Type': report.get('reportType', 'N/A').upper(),
                    'Filename': report.get('filename', 'N/A')
                })

            reports_df = pd.DataFrame(reports_data)

            # Display dataframe
            st.dataframe(
                reports_df,
                width="stretch",
                hide_index=True,
                height=400
            )

            # Add action buttons for each report
            st.markdown("### Actions")

            for idx, report in enumerate(st.session_state.hkex_reports):
                report_url = report.get('url', '')

                col1, col2, col3 = st.columns([7, 1.5, 1.5])

                with col1:
                    st.write(f"**{idx + 1}. {report.get('title', 'N/A')}**")

                with col2:
                    # View button - simple link button
                    if report_url:
                        st.link_button("View", report_url, width="stretch")

                with col3:
                    # Download button
                    if st.button("Download", key=f"download_btn_{idx}", width="stretch"):
                        with st.spinner(f"Downloading..."):
                            # Get download directory from config
                            _HKEX_CFG = _get_section("HKEX")
                            download_dir = Path(_HKEX_CFG.get("download_dir", "hkex_pdfs"))

                            # Create company-specific subdirectory
                            company_dir = download_dir / company.get('name', 'unknown').replace(' ', '_')

                            file_path = download_hkex_report(
                                report.get('url'),
                                report.get('filename'),
                                company_dir
                            )

                        if file_path:
                            st.success(f"Downloaded to: {file_path}")

                            # Provide file download through Streamlit
                            with open(file_path, 'rb') as f:
                                st.download_button(
                                    label="Save to Computer",
                                    data=f.read(),
                                    file_name=report.get('filename'),
                                    mime="application/pdf",
                                    key=f"save_{idx}",
                                    width="stretch"
                                )
                        else:
                            st.error("Download failed")

            st.markdown("---")

        elif st.session_state.hkex_reports_loaded:
            st.info("No annual reports found for the selected company and date range.")
            st.markdown("**Suggestions:**")
            st.markdown("- Try expanding the date range in the sidebar")
            st.markdown("- Verify the company has filed annual reports with HKEX")
            st.markdown("- Check if the stock code is correct")

    elif company_type == 'SEC':
        render_sec_edgar_section(company)
    else:
        st.warning("Could not detect company data source. Please ensure the company has LEI (OVH), HKEX ticker, or SEC ticker (exchange='SEC').")

    # Display OVH filings if available
    if st.session_state.filings and st.session_state.company_type == 'OVH' and st.session_state.show_filings:
        st.markdown("---")
        st.header("Available Filings")

        # Create a DataFrame for filings (without Report Available column)
        filings_data = []
        for filing in st.session_state.filings:
            filings_data.append({
                "Period End": filing.get('period_end', 'N/A'),
                "Errors": filing.get('error_count', 0),
                "Filing ID": filing.get('_id', 'N/A')
            })

        filings_df = pd.DataFrame(filings_data)
        st.dataframe(filings_df, width='stretch', hide_index=True)

        # Filing selection
        st.markdown("### Select a Filing to View Details")

        filing_options = [f"Period: {f.get('period_end', 'N/A')} (ID: {f.get('_id', 'N/A')[:8]}...)"
                          for f in st.session_state.filings]

        selected_filing_name = st.selectbox(
            "Choose a filing",
            options=filing_options,
            key="filing_selector"
        )

        selected_filing_idx = filing_options.index(selected_filing_name)
        selected_filing = st.session_state.filings[selected_filing_idx]
        st.session_state.selected_filing = selected_filing

        # Parse filing button
        col1, col2 = st.columns([1, 4])

        with col1:
            if st.button("Parse Filing", type="primary"):
                if st.session_state.lei and st.session_state.api_base:
                    with st.spinner("Parsing filing data..."):
                        tables, xbrl_facts = parse_filing_data(
                            selected_filing,
                            st.session_state.lei,
                            st.session_state.api_base
                        )

                    if tables:
                        filing_id = selected_filing.get('_id')
                        pe = selected_filing.get('period_end', '')
                        fy_label = f"FY{pe[:4]}" if pe else "UNKNOWN"

                        # Get file paths
                        _OVH_CFG = _get_section("OVH")
                        download_dir = Path(_OVH_CFG.get("download_dir"))
                        fy_dir = download_dir / fy_label
                        report_html_path = fy_dir / "report_doc.html"
                        viewer_json_path = fy_dir / "viewer_data.json"

                        # Save viewer data and HTML to MongoDB GridFS
                        if viewer_json_path.exists() or report_html_path.exists():
                            save_viewer_data_to_mongodb(
                                st.session_state.lei,
                                filing_id,
                                pe,
                                viewer_json_path if viewer_json_path.exists() else None,
                                report_html_path if report_html_path.exists() else None
                            )

                        # Check if already parsed to avoid duplicates
                        if fy_label not in st.session_state.parsed_labels:
                            # Store tables
                            st.session_state.financial_data[fy_label] = tables

                            # Store XBRL facts (avoid duplicates)
                            if xbrl_facts:
                                # Remove existing facts for this FY to avoid duplicates
                                st.session_state.all_facts = [
                                    f for f in st.session_state.all_facts
                                    if f.get('fy_label') != fy_label
                                ]
                                st.session_state.all_facts.extend(xbrl_facts)

                            # Mark as parsed
                            st.session_state.parsed_labels.add(fy_label)

                            # Store filing metadata for tracking
                            st.session_state.filing_metadata[fy_label] = f"{fy_label} (from {pe} filing)"

                        # Always rebuild concept map after parsing
                        _rebuild_concept_map()

                        # Set flag to show individual filing data
                        st.session_state.show_individual_filing = True

                        # Show simple success message
                        st.success(f"✅ Successfully parsed {fy_label}")
                        st.rerun()
                    else:
                        st.error("Failed to parse filing")
                else:
                    st.error("LEI or API Base URL not set")

        # Display financial data ONLY if user clicked "Parse Filing" button
        period_end = selected_filing.get('period_end', '')
        fy_label = f"FY{period_end[:4]}" if period_end else "UNKNOWN"

        if st.session_state.show_individual_filing and fy_label in st.session_state.financial_data:
            st.markdown("---")
            st.header("Financial Statements")

            tables = st.session_state.financial_data[fy_label]

            # Create tabs for different statement types
            tab_names = list(tables.keys())
            tabs = st.tabs(tab_names)

            for tab, table_name in zip(tabs, tab_names):
                with tab:
                    st.markdown(f"### {table_name}")

                    # Get concept map for this sheet type
                    concept_map_for_sheet = st.session_state.concept_map.get(table_name, {})

                    # Debug: Check concept map
                    print(f"[DEBUG] Table: {table_name}")
                    print(f"[DEBUG] Concept map has {len(concept_map_for_sheet)} entries")
                    if concept_map_for_sheet:
                        print(f"[DEBUG] Sample concepts: {list(concept_map_for_sheet.items())[:3]}")

                    # Convert to dataframe with XBRL concepts
                    df = convert_table_to_dataframe(
                        tables[table_name],
                        filing_label=None,
                        concept_map_for_sheet=concept_map_for_sheet
                    )

                    # Debug: Check if XBRL Concept column was added
                    print(f"[DEBUG] DataFrame columns: {list(df.columns)}")
                    if "XBRL Concept" in df.columns:
                        n_concepts = df["XBRL Concept"].astype(bool).sum()
                        print(f"[DEBUG] XBRL Concept column present with {n_concepts} non-empty values")
                    else:
                        print(f"[DEBUG] WARNING: XBRL Concept column NOT present!")

                    if not df.empty:
                        # Display dataframe directly without filters
                        st.dataframe(df, width='stretch', height=min(600, 40 + 35 * len(df)))

                        # Download button for individual table
                        csv = df.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label=f"Download {table_name} as CSV",
                            data=csv,
                            file_name=f"{table_name}_{period_end}.csv",
                            mime="text/csv",
                            key=f"download_{table_name}"
                        )
                    else:
                        st.warning("No data available for this table")

    # Consolidate all filings - PARSE AND CONSOLIDATE ALL AVAILABLE FILINGS
    if st.session_state.filings and st.session_state.company_type == 'OVH' and st.session_state.show_filings:
        st.markdown("---")
        st.header("Consolidate All Filings")

        # Show info about available filings
        total_count = len(st.session_state.filings)
        parsed_count = len(st.session_state.financial_data)

        st.info(f"{total_count} filings available. Currently {parsed_count} parsed. Click 'Consolidate Data' to parse and consolidate all filings.")

        col1, col2 = st.columns([1, 4])

        with col1:
            if st.button("Consolidate Data", type="primary", width="stretch"):
                if not st.session_state.lei or not st.session_state.api_base:
                    st.error("LEI or API Base URL not set")
                else:
                    # Hide individual filing display when consolidating
                    st.session_state.show_individual_filing = False

                    # Check which filings need parsing
                    unparsed_filings = []
                    for filing in st.session_state.filings:
                        pe = filing.get('period_end', '')
                        fy_label = f"FY{pe[:4]}" if pe else "UNKNOWN"
                        if fy_label not in st.session_state.parsed_labels:
                            unparsed_filings.append(filing)

                    # Parse only unparsed filings (silently)
                    if unparsed_filings:
                        progress = st.progress(0, text="Starting...")
                        for i, filing in enumerate(unparsed_filings):
                            pe = filing.get('period_end', '')
                            fy_label = f"FY{pe[:4]}" if pe else "UNKNOWN"

                            progress.progress((i) / len(unparsed_filings), text=f"Parsing {fy_label}...")

                            # Parse this filing silently
                            tables, xbrl_facts = parse_filing_data(
                                filing,
                                st.session_state.lei,
                                st.session_state.api_base,
                                silent=True
                            )

                            if tables:
                                # Get file paths for saving to MongoDB
                                _OVH_CFG = _get_section("OVH")
                                download_dir = Path(_OVH_CFG.get("download_dir"))
                                fy_dir = download_dir / fy_label
                                report_html_path = fy_dir / "report_doc.html"
                                viewer_json_path = fy_dir / "viewer_data.json"

                                # Save viewer data and HTML to MongoDB GridFS
                                if viewer_json_path.exists() or report_html_path.exists():
                                    save_viewer_data_to_mongodb(
                                        st.session_state.lei,
                                        filing.get('_id'),
                                        pe,
                                        viewer_json_path if viewer_json_path.exists() else None,
                                        report_html_path if report_html_path.exists() else None
                                    )

                                # Check if already parsed to avoid duplicates
                                if fy_label not in st.session_state.parsed_labels:
                                    st.session_state.financial_data[fy_label] = tables
                                    if xbrl_facts:
                                        # Remove existing facts for this FY to avoid duplicates
                                        st.session_state.all_facts = [
                                            f for f in st.session_state.all_facts
                                            if f.get('fy_label') != fy_label
                                        ]
                                        st.session_state.all_facts.extend(xbrl_facts)
                                    st.session_state.parsed_labels.add(fy_label)

                        progress.progress(1.0, text="Done!")

                    # Rebuild concept map from all parsed data
                    _rebuild_concept_map()

                    # Build consolidated data
                    all_data = st.session_state.financial_data
                    filing_metadata = st.session_state.filing_metadata

                    st.session_state.consolidated_data = all_data

                    st.success(f"✅ Successfully consolidated {len(all_data)} fiscal years")
                    st.rerun()

    if st.session_state.consolidated_data:
        # Download buttons section
        st.markdown("### Download Options")
        col_dl1, col_dl2 = st.columns(2)

        with col_dl1:
            # Create Excel file
            excel_file = create_consolidated_excel(
                st.session_state.consolidated_data,
                st.session_state.filing_metadata
            )

            if excel_file:
                st.download_button(
                    label="📊 Download Consolidated Excel",
                    data=excel_file,
                    file_name=f"consolidated_financials_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type="primary",
                    width="stretch"
                )

        with col_dl2:
            # Create XBRL Facts Excel file
            if st.session_state.all_facts:
                xbrl_excel_file = create_xbrl_facts_excel(st.session_state.all_facts)

                if xbrl_excel_file:
                    st.download_button(
                        label="◇ Download XBRL Facts & Concepts Excel",
                        data=xbrl_excel_file,
                        file_name=f"xbrl_facts_concepts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        type="secondary",
                        width="stretch"
                    )
            else:
                st.info("No XBRL facts available. Parse filings first.")


    # Display consolidated summary — OVH only, and only when data is available
    if (st.session_state.get("company_type") == "OVH"
            and st.session_state.get("consolidated_data")):

        st.markdown("---")
        st.markdown("### Consolidated Data Summary")

        table_types = set()
        for tables in st.session_state.consolidated_data.values():
            table_types.update(tables.keys())

        if table_types:
            tabs = st.tabs(list(table_types))

            for tab, table_name in zip(tabs, table_types):
                with tab:
                    st.markdown(f"#### {table_name} - All Years")

                    # Show debug info about available filings
                    with st.expander("Debug: View Filing Details", expanded=False):
                        for fy_label in sorted(st.session_state.consolidated_data.keys(), reverse=True):
                            tables = st.session_state.consolidated_data[fy_label]
                            if table_name in tables:
                                table_rows = tables[table_name]
                                if table_rows and len(table_rows) > 0:
                                    header = table_rows[0]
                                    st.write(f"**{fy_label}**: {len(table_rows)-1} rows")
                                    st.write(f"Header columns: {header}")
                                    # Show first few data rows for debugging
                                    st.write("First 3 data rows:")
                                    for i, row in enumerate(table_rows[1:4]):
                                        st.write(f"  Row {i+1}: {row[:5]}...")  # Show first 5 columns

                    # Create business-friendly dataframe with years as columns
                    consolidated_df = create_business_friendly_dataframe(
                        st.session_state.consolidated_data,
                        st.session_state.filing_metadata,
                        table_name
                    )

                    if not consolidated_df.empty:
                        # Display the consolidated dataframe with custom styling
                        st.dataframe(
                            consolidated_df,
                            width="stretch",
                            height=500,
                            hide_index=True
                        )

                        # Show summary info
                        years = [col for col in consolidated_df.columns if col not in ['Label (French)', 'Label (English)']]
                        st.caption(f"Showing data for {len(years)} years: {', '.join(sorted(years, reverse=True))}")

                        # Download button for this consolidated table
                        csv = consolidated_df.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label=f"Download {table_name} (All Years) as CSV",
                            data=csv,
                            file_name=f"{table_name}_consolidated_{datetime.now().strftime('%Y%m%d')}.csv",
                            mime="text/csv",
                            key=f"download_consolidated_{table_name}"
                        )
                    else:
                        st.info(f"No data available for {table_name}")

    # Footer
    st.markdown("---")
    st.caption("Financial Data Ingestion Pipeline | Built with Streamlit")


if __name__ == "__main__":
    main()