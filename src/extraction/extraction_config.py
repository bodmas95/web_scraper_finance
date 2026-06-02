"""
Configuration for PDF Extraction
Statement headings, search parameters, etc.
"""

from config.config import load_config

# Load configuration
_cfg = load_config()

# LLM settings - select model based on provider
LLM_PROVIDER = _cfg.get("LLM", "provider", fallback="openai").lower()
if LLM_PROVIDER == "maia":
    LLM_MODEL = _cfg.get("LLM", "maia_model", fallback="gpt-4o-mini-2024-07-18")
else:
    LLM_MODEL = _cfg.get("LLM", "model", fallback="gpt-4o")

# Statement heading variants — used for heading detection in the scanner
# All comparisons are case-insensitive
# Expanded to include Singapore, IFRS, US GAAP, UK, India, and other international variations
STATEMENT_HEADINGS = {
    "income_statement": [
        # Indosat specific - exact text from PDF (bilingual pages)
        "LAPORAN LABA RUGI DAN PENGHASILAN KOMPREHENSIF LAIN KONSOLIDASIAN UNTUK TAHUN-TAHUN YANG BERAKHIR",
        "CONSOLIDATED STATEMENTS OF PROFIT OR LOSS AND OTHER COMPREHENSIVE INCOME FOR THE YEARS ENDED",
        # Indonesian headings - add most specific first
        "LAPORAN LABA RUGI DAN PENGHASILAN KOMPREHENSIF LAIN KONSOLIDASIAN",
        "LAPORAN LABA RUGI DAN PENGHASILAN KOMPREHENSIF LAIN",
        "LAPORAN LABA RUGI KONSOLIDASIAN",
        "LAPORAN LABA RUGI",
        "LAPORAN RUGI LABA",
        # Generic English
        "CONSOLIDATED STATEMENTS OF PROFIT OR LOSS AND OTHER COMPREHENSIVE INCOME",
        "CONSOLIDATED STATEMENT OF PROFIT OR LOSS AND OTHER COMPREHENSIVE INCOME",
        "CONSOLIDATED STATEMENTS OF COMPREHENSIVE INCOME",
        "CONSOLIDATED STATEMENT OF COMPREHENSIVE INCOME",
        # Chinese headings
        "綜合損益表",
        "損益表",
        "合併損益表",
        "綜合收益表",
        # English headings
        "CONSOLIDATED STATEMENTS OF INCOME",
        "CONSOLIDATED STATEMENT OF INCOME",
        "CONSOLIDATED STATEMENTS OF OPERATIONS",
        "CONSOLIDATED STATEMENT OF OPERATIONS",
        "STATEMENTS OF CONSOLIDATED OPERATIONS",
        "CONSOLIDATED STATEMENTS OF EARNINGS",
        "CONSOLIDATED STATEMENT OF EARNINGS",
        "CONSOLIDATED INCOME STATEMENT",
        "CONSOLIDATED INCOME STATEMENTS",
        "CONSOLIDATED STATEMENT OF PROFIT OR LOSS",
        "CONSOLIDATED STATEMENTS OF PROFIT OR LOSS",
        "STATEMENT OF PROFIT OR LOSS",
        "STATEMENTS OF PROFIT OR LOSS",
        "CONSOLIDATED PROFIT OR LOSS",
        "PROFIT OR LOSS STATEMENT",
        "CONSOLIDATED STATEMENT OF PROFIT OR LOSS AND OTHER",
        "CONSOLIDATED PROFIT AND LOSS ACCOUNT",
        "CONSOLIDATED STATEMENT OF TOTAL COMPREHENSIVE INCOME",
        "CONSOLIDATED STATEMENT OF INCOME AND OTHER COMPREHENSIVE INCOME",
        "STATEMENT OF INCOME AND OTHER COMPREHENSIVE INCOME",
        "CONSOLIDATED EARNINGS STATEMENT",
        "EARNINGS STATEMENT",
        "STATEMENT OF NET INCOME",
        "CONSOLIDATED STATEMENT OF NET INCOME",
        "CONSOLIDATED PROFIT AND LOSS ACCOUNT",
        "PROFIT AND LOSS ACCOUNT",
        "CONSOLIDATED PROFIT AND LOSS STATEMENT",
        "PROFIT AND LOSS STATEMENT",
        "PROFIT AND LOSS",
        "CONSOLIDATED PROFIT AND LOSS",
        "GROUP PROFIT AND LOSS ACCOUNT",
        "GROUP INCOME STATEMENT",
        "CONSOLIDATED STATEMENT OF PROFIT AND LOSS",
        "STATEMENT OF PROFIT AND LOSS",
        "CONSOLIDATED STATEMENT OF PROFIT AND LOSS AND OTHER COMPREHENSIVE INCOME",
        "STATEMENT OF PROFIT AND LOSS AND OTHER COMPREHENSIVE INCOME",
        "CONSOLIDATED STATEMENT OF RESULTS",
        "STATEMENT OF RESULTS",
        "CONSOLIDATED RESULTS OF OPERATIONS",
        "RESULTS OF OPERATIONS",
        "CONSOLIDATED STATEMENT OF FINANCIAL PERFORMANCE",
        "STATEMENT OF FINANCIAL PERFORMANCE",
        "CONSOLIDATED STATEMENT OF FINANCIAL PERFORMANCE AND OTHER",
        "STATEMENT OF FINANCIAL PERFORMANCE AND OTHER COMPREHENSIVE INCOME",
        "CONSOLIDATED RESULTS STATEMENT",
        "RESULTS STATEMENT",
        "INCOME STATEMENT (PARENT COMPANY)",
        "INCOME STATEMENT (COMPANY ONLY)",
        "INCOME STATEMENT – GROUP",
        "INCOME STATEMENT – CONSOLIDATED",
        "INCOME STATEMENT – SEPARATE",
        "SEPARATE INCOME STATEMENT",
        "PARENT COMPANY INCOME STATEMENT",
        "GROUP CONSOLIDATED INCOME STATEMENT",
        "Consolidated Statement of Income",
        "Consolidated Income Statement",
        "Consolidated income statement",
        "Consolidated Statement of Profit or Loss",
        "Consolidated Statement of Profit or Loss and Other Comprehensive Income",
        "Statement of Profit or Loss",
        "Statement of Profit or Loss and Other Comprehensive Income",
        "Consolidated Profit and Loss Account",
        "Profit and Loss Account",
        "CONSOLIDATED STATEMENT OF PROFIT OR LOSS FOR THE YEAR ENDED",
        "CONSOLIDATED STATEMENT OF INCOME FOR THE YEAR ENDED",
        "CONSOLIDATED STATEMENT OF INCOME FOR THE YEARS ENDED",
        "STATEMENT OF PROFIT OR LOSS FOR THE YEAR ENDED",
        "STATEMENT OF INCOME FOR THE YEAR ENDED",
        "PROFIT AND LOSS ACCOUNT FOR THE YEAR ENDED",
        "CONSOLIDATED STATEMENT OF PROFIT OR LOSS AND OTHER",
        "STATEMENT OF ACTIVITIES",
        "STATEMENT OF REVENUES, EXPENSES AND CHANGES IN NET POSITION",
        "STATEMENT OF REVENUES AND EXPENDITURES",
        "INCOME AND EXPENDITURE ACCOUNT",
        "STATEMENT OF INCOME AND EXPENDITURE",
        "STATEMENT OF REVENUE AND EXPENDITURE",
        "OPERATING STATEMENT",
        "STATEMENT OF TRADING AND PROFIT AND LOSS ACCOUNT",
        "CONSOLIDATED STATEMENT OF EARNINGS (LOSSES)",
        "STATEMENT OF EARNINGS (LOSSES)",
        "CONSOLIDATED STATEMENT OF INCOME (LOSS)",
        "STATEMENT OF INCOME (LOSS)",
        "STATEMENTS OF VALUE ADDED AND ITS DISTRIBUTION"
    ],
    
    "balance_sheet": [
        # Indosat specific - exact text from PDF
        "LAPORAN POSISI KEUANGAN KONSOLIDASIAN PADA TANGGAL",
        "CONSOLIDATED STATEMENTS OF FINANCIAL POSITION AS AT",
        # Indonesian headings - most specific first
        "LAPORAN POSISI KEUANGAN KONSOLIDASIAN",
        "LAPORAN POSISI KEUANGAN",
        "NERACA KONSOLIDASIAN",
        "NERACA",
        # Generic English
        "CONSOLIDATED STATEMENTS OF FINANCIAL POSITION",
        "CONSOLIDATED STATEMENT OF FINANCIAL POSITION",
        # Chinese headings
        "綜合財務狀況表",
        "資產負債表",
        "合併資產負債表",
        "財務狀況表",
        # English headings
        "CONSOLIDATED BALANCE SHEETS",
        "CONSOLIDATED BALANCE SHEET",
        "BALANCE SHEET",
        "BALANCE SHEETS",
        "GROUP BALANCE SHEET",
        "GROUP CONSOLIDATED BALANCE SHEET",
        "CONSOLIDATED GROUP BALANCE SHEET",
        "PARENT COMPANY BALANCE SHEET",
        "BALANCE SHEET (PARENT COMPANY)",
        "BALANCE SHEET (COMPANY ONLY)",
        "BALANCE SHEET – GROUP",
        "BALANCE SHEET – CONSOLIDATED",
        "BALANCE SHEET – SEPARATE",
        "SEPARATE BALANCE SHEET",
        "COMPANY BALANCE SHEET",
        "CONSOLIDATED STATEMENT OF ASSETS AND LIABILITIES",
        "STATEMENT OF ASSETS AND LIABILITIES",
        "CONSOLIDATED STATEMENT OF ASSETS, LIABILITIES AND EQUITY",
        "STATEMENT OF ASSETS, LIABILITIES AND EQUITY",
        "CONSOLIDATED STATEMENT OF NET ASSETS",
        "STATEMENT OF NET ASSETS",
        "STATEMENTS OF FINANCIAL POSITION",
        "CONSOLIDATED STATEMENT OF FINANCIAL POSITION",
        "Consolidated Balance Sheet",
        "Consolidated balance sheet",
        "Consolidated Statement of Financial Position",
        "Consolidated statement of financial position",
        "Statement of Financial Position",
        "Statement of financial position",
        "Balance Sheet",
        "Group Balance Sheet",
        "CONSOLIDATED BALANCE SHEET AS AT",
        "CONSOLIDATED STATEMENT OF FINANCIAL POSITION AS AT",
        "BALANCE SHEET AS AT",
        "STATEMENT OF FINANCIAL POSITION AS AT",
        "CONSOLIDATED BALANCE SHEET AS OF",
        "BALANCE SHEET AS OF",
        
    ],
    
    "cash_flow": [
        # Indosat specific - exact text from PDF
        "LAPORAN ARUS KAS KONSOLIDASIAN UNTUK TAHUN-TAHUN YANG BERAKHIR",
        "CONSOLIDATED STATEMENTS OF CASH FLOWS FOR THE YEARS ENDED",
        # Indonesian headings - most specific first
        "LAPORAN ARUS KAS KONSOLIDASIAN",
        "LAPORAN ARUS KAS",
        "LAPORAN PERUBAHAN ARUS KAS",
        # Generic English
        "CONSOLIDATED STATEMENTS OF CASH FLOWS",
        "CONSOLIDATED STATEMENT OF CASH FLOWS",
        # Chinese headings
        "綜合現金流量表",
        "現金流量表",
        "合併現金流量表",
        # English headings
        "CONSOLIDATED STATEMENTS OF CASH FLOWS",
        "CONSOLIDATED STATEMENT OF CASH FLOWS",
        "CONSOLIDATED STATEMENT OF CASH FLOWS",
        "STATEMENTS OF CASH FLOWS",
        "STATEMENT OF CASH FLOWS",
        "CONSOLIDATED CASH FLOW STATEMENTS",
        "CONSOLIDATED CASH FLOW STATEMENT",
        "CASH FLOW STATEMENTS",
        "CASH FLOW STATEMENT",
        "CONSOLIDATED STATEMENTS OF CASHFLOWS",
        "CONSOLIDATED STATEMENT OF CASHFLOWS",
        "STATEMENTS OF CASHFLOWS",
        "STATEMENT OF CASHFLOWS",
        "CONSOLIDATED CASHFLOW STATEMENTS",
        "CONSOLIDATED CASHFLOW STATEMENT",
        "CASHFLOW STATEMENTS",
        "CASHFLOW STATEMENT",
        "GROUP CASH FLOW STATEMENT",
        "GROUP CONSOLIDATED CASH FLOW STATEMENT",
        "CONSOLIDATED GROUP CASH FLOW STATEMENT",
        "PARENT COMPANY CASH FLOW STATEMENT",
        "CASH FLOW STATEMENT (PARENT COMPANY)",
        "CASH FLOW STATEMENT (COMPANY ONLY)",
        "CASH FLOW STATEMENT – GROUP",
        "CASH FLOW STATEMENT – CONSOLIDATED",
        "CASH FLOW STATEMENT – SEPARATE",
        "SEPARATE CASH FLOW STATEMENT",
        "COMPANY CASH FLOW STATEMENT",
        "Consolidated Statement of Cash Flows",
        "Consolidated statement of cash flows",
        "Consolidated Cash Flow Statement",
        "Consolidated cash flow statement",
        "Statement of Cash Flows",
        "Statement of cash flows",
        "Cash Flow Statement",
        "Group Cash Flow Statement",
        "Group Statement of Cash Flows",
        "CONSOLIDATED STATEMENT OF CASH FLOWS FOR THE YEAR ENDED",
        "CONSOLIDATED STATEMENT OF CASH FLOWS FOR THE YEARS ENDED",
        "STATEMENT OF CASH FLOWS FOR THE YEAR ENDED",
        "CASH FLOW STATEMENT FOR THE YEAR ENDED",
        "CONSOLIDATED CASH FLOW STATEMENT FOR THE YEAR ENDED",
        "CONSOLIDATED STATEMENT OF CASH FLOW",
        "STATEMENT OF CASH FLOW",
        "CONSOLIDATED STATEMENTS OF CHANGES IN CASH",
        "STATEMENTS OF CHANGES IN CASH",
        "CONSOLIDATED STATEMENT OF CHANGES IN CASH AND CASH EQUIVALENTS",
        "STATEMENT OF CHANGES IN CASH AND CASH EQUIVALENTS",
        "GROUP STATEMENT OF CASH FLOWS",
        "CONSOLIDATED GROUP STATEMENT OF CASH FLOWS",
        "STATEMENT OF CASH RECEIPTS AND DISBURSEMENTS",
        "CONSOLIDATED STATEMENT OF CASH RECEIPTS AND DISBURSEMENTS",
        "CONSOLIDATED FUNDS FLOW STATEMENT",
        "FUNDS FLOW STATEMENT",
        "STATEMENT OF SOURCES AND USES OF FUNDS",
        "STATEMENT OF SOURCES AND APPLICATIONS OF FUNDS",
    ],
}


# Number of lines from the top of a page to search for the statement heading
# Increased to 30 for complex PDFs like Indosat where headings appear after section headers
HEADING_SEARCH_LINES = _cfg.getint("PDF_EXTRACTION", "heading_search_lines", fallback=30)

# Minimum number of table-like rows (label + 2 or more numbers) a page must
# contain to be considered a real financial table
MIN_TABLE_ROWS = _cfg.getint("PDF_EXTRACTION", "min_table_rows", fallback=5)

# Statement labels for display
STATEMENT_LABELS = {
    "income_statement": "Income Statement",
    "balance_sheet": "Balance Sheet",
    "cash_flow": "Cash Flow Statement",
}

# Maximum number of pages after the primary candidate to check for table continuation.
MAX_CONTINUATION_PAGES = _cfg.getint("PDF_EXTRACTION", "max_continuation_pages", fallback=2)
