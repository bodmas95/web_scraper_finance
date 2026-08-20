"""
mapping_v1 — Central Configuration

All tuneable constants in one place so the rest of the module stays generic.
"""

# ---------------------------------------------------------------------------
# Excel template layout
# ---------------------------------------------------------------------------

# Column A is always the BREF field label (e.g. "I30 | Sales (turnover)").
COL_LABEL = 1

# Data rows start here; rows 1–3 are headers in standard BREF templates.
DATA_START_ROW = 4

# ---------------------------------------------------------------------------
# Statement type → sheet name(s) mapping
# ---------------------------------------------------------------------------
# Balance sheet is split across two sheets; both are loaded and merged.

STATEMENT_SHEET_MAP: dict[str, list[str]] = {
    "income_statement": ["Input - Income Statement"],
    "balance_sheet":    ["Input - Assets", "Input - Liabilities"],
    "cash_flow":        ["Input - Cash flow"],
}

# ---------------------------------------------------------------------------
# Field-code prefixes that identify real BREF data rows (not section headers)
# ---------------------------------------------------------------------------

VALID_PREFIXES: tuple[str, ...] = (
    "I", "B", "L", "ACF", "CF", "Q", "ITRR", "U",
    "ICF", "DMLTC", "DMLTB", "CAFE", "CAFF",
)

# ---------------------------------------------------------------------------
# Stop-loading sentinels
# ---------------------------------------------------------------------------
# Income-statement loading stops when it hits the OCI section to avoid
# loading I24bis and other OCI fields that cause row misalignment.

OCI_STOP_KEYWORDS: tuple[str, ...] = (
    "Other Comprehensive Income",
    "Comprehensive Income",
)

# ---------------------------------------------------------------------------
# Mapping behaviour
# ---------------------------------------------------------------------------

# Maximum BREF fields per LLM call (Pass 1 and Pass 2).
BATCH_SIZE: int = 30

# Pass 3 sends all-years context per row, producing much larger responses.
# Use a smaller batch to avoid hitting the LLM output token limit.
BATCH_SIZE_PASS3: int = 10

# Numeric tolerance for Pass 1 value-match (1 %).
VALUE_MATCH_TOLERANCE: float = 0.01

# Confidence levels that are accepted in Pass 2 (alias) and Pass 3 (derived).
# "low" confidence results are always rejected.
ACCEPTED_CONFIDENCE_LEVELS: tuple[str, ...] = ("high", "medium")

# ---------------------------------------------------------------------------
# Statement heading variants — used by page scanner
# ---------------------------------------------------------------------------

STATEMENT_HEADINGS: dict[str, list[str]] = {
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
