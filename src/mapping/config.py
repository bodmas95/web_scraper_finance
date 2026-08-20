# ---------------------------------------------------------------------------
# BREF Populator — central configuration
# ---------------------------------------------------------------------------
# All tuneable constants live here so the rest of the codebase stays generic.

import os

from dotenv import load_dotenv

load_dotenv()


LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o")
LLM_URL = os.getenv("LLM_URL")
LLM_API_KEY = os.getenv("LLM_API_KEY")

# ---------------------------------------------------------------------------
# Exchange / filing format
# ---------------------------------------------------------------------------
# Used to adapt heading detection and company name positioning rules.
# Add new exchanges here as needed.
EXCHANGES = {
    "US-SEC": "United States (SEC)",
    "HKEX": "Hong Kong (HKEX)",
}

# For US-SEC filings the company name appears before the statement heading.
# For other exchanges this may differ — extend this set as patterns are confirmed.
EXCHANGES_WITH_COMPANY_HEADER = {"US-SEC"}

# ---------------------------------------------------------------------------
# Statement heading variants — used for heading detection in the scanner.
# Add variants here when new formats are encountered.
# All comparisons are case-insensitive.
# ---------------------------------------------------------------------------
STATEMENT_HEADINGS = {
    "income_statement": [
        "CONSOLIDATED STATEMENTS OF INCOME",
        "CONSOLIDATED STATEMENT OF INCOME",
        "CONSOLIDATED STATEMENTS OF OPERATIONS",
        "CONSOLIDATED STATEMENT OF OPERATIONS",
        "STATEMENTS OF CONSOLIDATED OPERATIONS",
        "CONSOLIDATED STATEMENTS OF EARNINGS",
        "CONSOLIDATED STATEMENT OF EARNINGS",
        "CONSOLIDATED STATEMENT OF PROFIT OR LOSS",
        "CONSOLIDATED STATEMENT OF PROFIT OR LOSS AND OTHER COMPREHENSIVE INCOME",
        "CONSOLIDATED INCOME STATEMENT",
        "CONSOLIDATED INCOME STATEMENTS",
        "Consolidated Statement of Income",
        "Consolidated Income Statement",
        "Consolidated income statement",
        "Consolidated Profit or Loss Account",
    ],
    "balance_sheet": [
        "CONSOLIDATED BALANCE SHEETS",
        "CONSOLIDATED BALANCE SHEET",
        "CONSOLIDATED STATEMENTS OF FINANCIAL POSITION",
        "CONSOLIDATED STATEMENT OF FINANCIAL POSITION",
        "Consolidated Balance Sheet",
        "Consolidated balance sheet",
    ],
    "cash_flow": [
        "CONSOLIDATED STATEMENTS OF CASH FLOWS",
        "CONSOLIDATED STATEMENT OF CASH FLOWS",
        "CONSOLIDATED CASH FLOW STATEMENTS",
        "CONSOLIDATED CASH FLOW STATEMENT",
        "Consolidated Statement of Cash Flows",
        "Consolidated cash flow statement",
    ],
}

# Number of lines from the top of a page to search for the statement heading.
HEADING_SEARCH_LINES = 4

# Minimum number of table-like rows (label + 2 or more numbers) a page must
# contain to be considered a real financial table.
MIN_TABLE_ROWS = 5

# Maps the statement_type selector value to the corresponding BREF Excel sheet.
# Add entries here when new statement types are supported.
# Note: Sheet names must match exactly with the Excel template
STATEMENT_SHEET_MAP = {
    "income_statement": "Input - Income Statement",
    "balance_sheet": "Input - Assets",  # KLN template uses 'Input - Assets' for balance sheet
    "cash_flow": "Input - Cash flow",  # Note: lowercase 'flow' in KLN template
}

# ---------------------------------------------------------------------------
# BREF template column layout (1-based column indices)
# ---------------------------------------------------------------------------
# UPDATED FOR bref-validator.xlsx structure
# Structure: A=Label, B=Alias, C=2023 Values, D=2024 Output, E=Confidence
# ---------------------------------------------------------------------------

COL_LABEL = 1       # A — BREF field label (e.g. "I30 | Sales (turnover)")
COL_EXTRACT = 2     # B — NOT USED in bref-validator.xlsx (no Extract column)
COL_DESC = 2        # B — Alias/Description for AI mapping (comma-separated terms)
COL_REF_VALUE = 3   # C — Reference year value (2023 for target_year=2024)
COL_OUTPUT = 4      # D — Extracted target year value (2024) - written by this tool
COL_CONFIDENCE = 5  # E — Mapping confidence score - written by this tool

DATA_START_ROW = 4  # Data starts at row 4 (rows 1-3 are headers)

# ---------------------------------------------------------------------------
# PREVIOUS CONFIG (for reference - BREF_Template_NextEra_2024.xlsx)
# ---------------------------------------------------------------------------
# COL_LABEL = 1       # A — BREF field label
# COL_EXTRACT = 2     # B — Extract flag ("Yes"/"No")
# COL_DESC = 3        # C — Field description / mapping hint
# COL_REF_VALUE = 4   # D — Reference year value (target_year - 1)
# COL_OUTPUT = 5      # E — Extracted target year value
# COL_CONFIDENCE = 6  # F — Mapping confidence
# DATA_START_ROW = 6  # Rows 1-5 are header / metadata rows
# ---------------------------------------------------------------------------

# Maximum number of pages after the primary candidate to check for table continuation.
MAX_CONTINUATION_PAGES = 2
