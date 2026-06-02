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
import sys
from pathlib import Path

# Add the project root to the path
sys.path.insert(0, str(Path(__file__).parent))

from src.components.common import (
    load_regions_from_mongodb,
    load_countries_by_region,
    load_companies_by_region_country,
    get_company_sources,
    detect_company_type,
    initialize_common_session_state,
    reset_company_state,
)
from src.components.xbrl_ui import render_xbrl_section, initialize_xbrl_state
from src.components.hkex_ui import render_hkex_section, initialize_hkex_state
from src.components.edgar_ui import render_sec_edgar_section, initialize_edgar_state

# Page configuration
st.set_page_config(
    page_title="Financial Data Ingestion Pipeline",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
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
    /* PDF Extraction specific styles from bref-populator */
    .centered-subheader {
        text-align: center;
        font-size: 1.15rem;
        font-weight: 600;
        margin-bottom: 0.25rem;
    }
    /* Hide Streamlit's native image fullscreen button */
    [data-testid="StyledFullScreenButton"] { display: none !important; }
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
initialize_common_session_state()
initialize_xbrl_state()
initialize_hkex_state()
initialize_edgar_state()


def main():
    # Header
    st.title("Financial Data Ingestion Pipeline")
    st.markdown("---")

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
                st.session_state.selected_region = selected_region
                st.session_state.countries = load_countries_by_region(selected_region)
                st.session_state.selected_country = None
                st.session_state.filtered_companies = []
                reset_company_state()
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
                        st.session_state.selected_country = selected_country
                        st.session_state.filtered_companies = load_companies_by_region_country(
                            st.session_state.selected_region,
                            selected_country
                        )
                        reset_company_state()
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
                            st.session_state.pdf_extraction_results = {}
                            st.session_state.pdf_extraction_done = False
                            st.session_state.show_pdf_extraction = False
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
        _ds_label = {"XBRL": "XBRL (filings.xbrl.org)", "SEC": "SEC EDGAR", "HKEX": "HKEX"}.get(company_type, company_type or "N/A")
        st.write(f"**Data Source:** {_ds_label}")

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
            # Display sources as simple list instead of dataframe to avoid React errors
            for idx, source in enumerate(st.session_state.company_sources, 1):
                st.write(f"**{idx}. {source.get('source', 'N/A')}**")
                st.caption(f"Type: {source.get('sourceType', 'N/A')} | Status: {source.get('status', 'N/A')}")
                if source.get('sourceUrl'):
                    st.caption(f"URL: {source.get('sourceUrl', 'N/A')}")
                st.markdown("---")
        else:
            st.info("No data sources found for this company")

        st.markdown("---")

    # Show date range for HKEX only
    if company_type == 'HKEX':
        from datetime import timedelta
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

    # ==================================================================
    # ROUTE TO WORKSTREAM COMPONENTS
    # ==================================================================
    if company_type == 'XBRL':
        render_xbrl_section(company, lei)
    elif company_type == 'HKEX':
        render_hkex_section(company, hkex_ticker)
    elif company_type == 'SEC':
        render_sec_edgar_section(company)
    else:
        st.warning("Could not detect company data source. Please ensure the company has LEI (OVH), HKEX ticker, or SEC ticker (exchange='SEC').")

    # Footer
    st.markdown("---")
    st.caption("Financial Data Ingestion Pipeline | Built with Streamlit")


if __name__ == "__main__":
    main()
