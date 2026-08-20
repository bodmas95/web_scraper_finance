import json
import re
import logging

import pdfplumber

from .extraction_config import HEADING_SEARCH_LINES, LLM_MODEL, MAX_CONTINUATION_PAGES, MIN_TABLE_ROWS, STATEMENT_HEADINGS
from .llm_client import get_client, track_usage
from config.config import load_config

# Load config to check for Maia credentials
_cfg = load_config()
MAIA_CREDENTIALS = _cfg.get("LLM", "maia_credentials", fallback="")
MAIA_MODEL = _cfg.get("LLM", "maia_model", fallback="gpt-5.1-2025-11-13")

_STATEMENT_TYPE_LABELS = {
    "income_statement": "Income Statement (e.g. Statements of Income / Operations / Earnings / Profit or Loss)",
    "balance_sheet":    "Balance Sheet (e.g. Balance Sheet / Statements of Financial Position)",
    "cash_flow":        "Cash Flow Statement (e.g. Statements of Cash Flows)",
}

_LLM_SCAN_PROMPT = """You are helping locate a specific financial statement in an annual report PDF.

Target: {statement_label}

Below are the first 3 lines from pages that contain financial tables (page numbers are 1-based):

{pages_text}

Return the 1-based page number(s) where the target statement most likely starts.
Only respond with valid JSON:
{{"page_numbers": [list of integers, or empty array if none found]}}
"""

# Pattern for a table-like row: some text followed by at least 2 numbers.
# Handles comma-formatted numbers, negatives in parentheses, dollar signs.
_TABLE_ROW_RE = re.compile(
    r".{2,}\s+"                          # label (at least 2 chars)
    r"[\(\$]?\d[\d,]*(?:\.\d+)?\)?"     # first number
    r"(?:\s+[\(\$]?\d[\d,]*(?:\.\d+)?\)?){1,}"  # one or more additional numbers
)

_CID_RE = re.compile(r'\(cid:\d+\)')


def _is_garbled_text(text: str, threshold: int = 10) -> bool:
    """Detect CID-encoded or otherwise unreadable PDF text."""
    if not text or len(text.strip()) < 50:
        return False
    sample = text[:2000]
    if len(_CID_RE.findall(sample)) > threshold:
        return True
    non_ws = re.sub(r'\s', '', sample)
    if not non_ws:
        return False
    alpha_count = sum(1 for c in non_ws if c.isalpha() and ord(c) < 128)
    if alpha_count / len(non_ws) < 0.3:
        return True
    return False


def pdf_has_garbled_text(pdf_path: str) -> bool:
    """Check if a PDF produces garbled text by sampling the first few content pages."""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            pages_to_check = min(10, len(pdf.pages))
            garbled_count = 0
            checked = 0
            for i in range(pages_to_check):
                text = pdf.pages[i].extract_text() or ""
                if len(text.strip()) < 50:
                    continue
                checked += 1
                if _is_garbled_text(text):
                    garbled_count += 1
            return checked > 0 and garbled_count >= max(1, checked // 2)
    except Exception:
        return False


def _extract_text(page) -> str:
    return page.extract_text() or ""


def _is_landscape(page) -> bool:
    return page.width > page.height


def _check_which_statement_on_page(text: str, statement_type: str) -> bool:
    """
    For bilingual pages with multiple statements, check which statement appears FIRST.
    Returns True if the target statement appears before other statement types.
    
    This handles pages like Indosat page 323 which has:
    - Balance Sheet on left
    - Income Statement on right
    
    We check which heading appears first in the text to determine the primary statement.
    """
    lines = text.splitlines()
    
    # Find line number where target statement heading appears
    target_line = 999
    target_heading = _heading_in_first_lines(text, statement_type)
    if target_heading:
        for i, line in enumerate(lines[:50]):
            if _normalize(target_heading) in _normalize(line):
                target_line = i
                break
    
    # Find line number where OTHER statement headings appear
    other_statement_types = ["income_statement", "balance_sheet", "cash_flow"]
    earliest_other_line = 999
    earliest_other_type = None
    
    for stype in other_statement_types:
        if stype != statement_type:
            other_heading = _heading_in_first_lines(text, stype)
            if other_heading:
                for i, line in enumerate(lines[:50]):
                    if _normalize(other_heading) in _normalize(line):
                        if i < earliest_other_line:
                            earliest_other_line = i
                            earliest_other_type = stype
                        break
    
        # If target statement appears BEFORE other statements, accept it
    if target_line < earliest_other_line:
        return True
    
    # If other statement appears first, reject
    if earliest_other_line < target_line:
        print(f"  DEBUG: Page has {earliest_other_type} at line {earliest_other_line}, {statement_type} at line {target_line} - rejecting")
        return False
    
    # CRITICAL: If both headings are on the SAME line (bilingual side-by-side), check CONTENT
    # This handles pages like True Corporation page 223 which has both BS and IS headings on same line
    if target_line == earliest_other_line and target_line < 999:
        print(f"  DEBUG: Both {statement_type} and {earliest_other_type} headings on same line {target_line} - checking content")
        
        # Check content to determine which statement this actually is
        text_upper = text.upper()
        
        if statement_type == "income_statement":
            # Income statement should have REVENUE/SALES at the top, not ASSETS
            has_revenue = any(keyword in text_upper[:2000] for keyword in ["REVENUE", "SALES", "PENDAPATAN"])
            has_assets = any(keyword in text_upper[:2000] for keyword in ["TOTAL ASSETS", "CURRENT ASSETS", "ASET LANCAR"])
            if has_assets and not has_revenue:
                print(f"  DEBUG: Content check: Page has ASSETS (not REVENUE) - this is balance sheet, not income statement")
                return False
        elif statement_type == "balance_sheet":
            # Balance sheet should have ASSETS, not REVENUE
            has_revenue = any(keyword in text_upper[:2000] for keyword in ["TOTAL REVENUE", "PENDAPATAN"])
            has_assets = any(keyword in text_upper[:2000] for keyword in ["TOTAL ASSETS", "CURRENT ASSETS"])
            if has_revenue and not has_assets:
                print(f"  DEBUG: Content check: Page has REVENUE (not ASSETS) - this is income statement, not balance sheet")
                return False
    
    # If only target statement found (no other statements), accept
    if target_heading and not earliest_other_type:
        return True
    
    # No clear winner - accept by default
    return True


def _find_heading_side(page, statement_type: str) -> tuple:
    """
    For a landscape page that may contain two side-by-side tables, determine
    which horizontal half holds the target statement heading.

    Returns (cropped_page, side, bbox) where side is "left", "right", or None.
    None means the heading wasn't found in either half (or spans both — fall back to full page).
    
    NOTE: This is ONLY used for TRUE landscape pages (width > height).
    For portrait pages with side-by-side layout, we use the full page.
    """
    mid_x = page.width / 2

    left_bbox  = (0,     0, mid_x,      page.height)
    right_bbox = (mid_x, 0, page.width, page.height)

    left_text  = page.crop(left_bbox).extract_text()  or ""
    right_text = page.crop(right_bbox).extract_text() or ""

    left_heading  = _heading_in_first_lines(left_text,  statement_type)
    right_heading = _heading_in_first_lines(right_text, statement_type)

    if left_heading and not right_heading:
        return page.crop(left_bbox), "left", left_bbox
    if right_heading and not left_heading:
        return page.crop(right_bbox), "right", right_bbox
    
    # Heading not found in either half — use full page
    return page, None, None


def _normalize(text: str) -> str:
    """Normalize text for comparison — collapse whitespace, expand abbreviations."""
    text = text.upper()
    text = re.sub(r"\s+", " ", text)
    text = text.replace("&", "AND")
    text = re.sub(r"[.,\-]", "", text)
    return text.strip()


def _is_summary_or_highlights_page(text: str) -> bool:
    """
    Detect if this is a summary/highlights page rather than a detailed financial statement.
    Summary pages should be EXCLUDED from extraction.
    
    Indicators:
    - Contains "SUMMARY", "RINGKASAN", "HIGHLIGHTS", "IKHTISAR" in first few lines
    - Has very few rows (< 20 rows) - summaries are typically condensed
    - Contains "FINANCIAL HIGHLIGHTS" or similar
    - Contains only high-level totals without detailed line items
    - Contains "RASIO" (ratios) which are summary metrics
    - Has "(Rp miliar)" or "(Rp billion)" with very few line items (summary format)
    """
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return False
    
    # Check first 15 lines for summary indicators
    top_text = _normalize(" ".join(lines[:15]))
    
    # Also check first 2000 characters (for bilingual PDFs)
    first_chars = _normalize(text[:2000])
    
    # Check entire text for ratio indicators
    full_text = _normalize(text)
    
    # Summary/highlights keywords (Indonesian and English)
    summary_keywords = [
        "RINGKASAN",  # Indonesian: Summary
        "IKHTISAR",   # Indonesian: Summary/Overview
        "SUMMARY",
        "HIGHLIGHTS",
        "FINANCIAL HIGHLIGHTS",
        "RINGKASAN LAPORAN",
        "IKHTISAR KEUANGAN",
        "SUMMARY OF CONSOLIDATED",
        "RINGKASAN LAPORAN POSISI KEUANGAN",
        "RINGKASAN LAPORAN LABA",
        "RINGKASAN LAPORAN ARUS KAS",
        "2025 HIGHLIGHTS",
        "IKHTISAR 2025",
    ]
    
    # Check for summary keywords
    has_summary_keyword = False
    for keyword in summary_keywords:
        if _normalize(keyword) in top_text or _normalize(keyword) in first_chars:
            has_summary_keyword = True
            break
    
    # Count table-like rows
    table_row_count = sum(1 for line in lines if _TABLE_ROW_RE.search(line))
    
    # Count ratio/percentage lines (strong indicator of summary page)
    ratio_count = 0
    for line in lines:
        line_norm = _normalize(line)
        if any(keyword in line_norm for keyword in [
            "RASIO", "RATIO", "MARGIN", "MARJIN", "RETURN", "PENGEMBALIAN"
        ]):
            ratio_count += 1
    
    # CRITICAL: If page has many ratios (> 5) and few rows (< 30), it's a summary
    if ratio_count > 5 and table_row_count < 30:
        return True
    
        # If has summary keyword AND few rows, it's a summary
    # BUT: Don't reject if it has a proper financial statement heading
    # (Some PDFs use "IKHTISAR" in the title but are still full statements)
    if has_summary_keyword and table_row_count < 20:
        return True
    
    # Check for summary format: "(Rp miliar)" or "(Rp billion)" with very few line items
    # Detailed statements have 50+ line items, summaries have < 20
    if ("RP MILIAR" in full_text or "RP BILLION" in full_text) and table_row_count < 20:
        return True
    
        # Don't check row count here - it's checked in the scanner (Filter 3)
    # This function only checks for summary/highlights KEYWORDS
    
    return False


def _fuzzy_heading_match(text_normalized: str, heading_normalized: str) -> bool:
    """
    Fuzzy matching for statement headings using keyword-based approach.
    
    Instead of exact string matching, check if key words from the heading appear in the text.
    This handles variations like:
    - "Income statements" vs "Income Statement" vs "INCOME STATEMENT"
    - "Consolidated statements of income" vs "Consolidated income statement"
    - "Balance sheets" vs "Balance Sheet"
    
    Returns True if the heading is likely present based on keyword matching.
    """
    # Extract key words from heading (ignore common filler words)
    filler_words = {"THE", "OF", "AND", "FOR", "AS", "AT", "TO", "A", "AN", "IN", "ON", "BY"}
    
    heading_words = [w for w in heading_normalized.split() if w not in filler_words and len(w) > 2]
    
    # If heading has no meaningful words, fall back to exact match
    if not heading_words:
        return heading_normalized in text_normalized
    
    # Check if most key words (at least 70%) appear in the text
    # This allows for minor variations while still being specific
    matches = sum(1 for word in heading_words if word in text_normalized)
    match_ratio = matches / len(heading_words)
    
    # Require at least 70% of key words to match
    # For short headings (1-2 words), require 100% match
    if len(heading_words) <= 2:
        return match_ratio >= 1.0
    else:
        return match_ratio >= 0.7


def _is_notes_page(text: str) -> bool:
    """
    Detect if this is a notes/footnotes page rather than a primary financial statement.
    Notes pages should be EXCLUDED from extraction.
    
    Indicators:
    - Contains "NOTES TO THE FINANCIAL STATEMENTS" or "CATATAN ATAS LAPORAN KEUANGAN"
    - Contains "Notes to the consolidated financial statements"
    - Contains note numbers like "Note 11", "Note 35", etc. in the heading
    - Contains "(continued)" indicating it's a continuation of notes
    - Contains "Notes to the financial statements" in first 15 lines
    """
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return False
    
    # Check first 15 lines for notes indicators
    top_text = _normalize(" ".join(lines[:15]))
    
    # Notes keywords (English and Indonesian)
    notes_keywords = [
        "NOTES TO THE FINANCIAL STATEMENTS",
        "NOTES TO FINANCIAL STATEMENTS",
        "NOTES TO THE CONSOLIDATED FINANCIAL STATEMENTS",
        "NOTES TO CONSOLIDATED FINANCIAL STATEMENTS",
        "NOTES TO AND FORMING PART OF THE FINANCIAL STATEMENTS",
        "CATATAN ATAS LAPORAN KEUANGAN",
        "CATATAN ATAS LAPORAN KEUANGAN KONSOLIDASIAN",
        "CONTINUED",
    ]
    
    for keyword in notes_keywords:
        if _normalize(keyword) in top_text:
            return True
    
    # Check for note numbers in first few lines (e.g., "11. GOODWILL AND OTHER INTANGIBLE ASSETS")
    for line in lines[:5]:
        line_stripped = line.strip()
        # Match patterns like "11. GOODWILL" or "35. PROPERTY" or "Note 1."
        if re.match(r'^(Note\s+)?\d{1,2}\.\s+[A-Z]', line_stripped, re.IGNORECASE):
            return True
    
    return False


def _is_comprehensive_income_only(text: str) -> bool:
    """
    Check if this is a comprehensive income statement (not a regular income statement).
    
    Returns True if:
    - Contains "COMPREHENSIVE INCOME" but NOT "PROFIT OR LOSS" or "INCOME STATEMENT"
    - Contains "OTHER COMPREHENSIVE INCOME" as main heading
    - Title is "STATEMENT OF COMPREHENSIVE INCOME" (not "PROFIT OR LOSS AND OTHER COMPREHENSIVE INCOME")
    - Title is "STATEMENTS OF COMPREHENSIVE INCOME" (not combined with profit/loss)
    - First line item is "Other comprehensive income" or "Profit after income tax" (not revenue/income)
    
    CRITICAL: Macquarie-style reports have "Statements of comprehensive income" as the heading
    and start with "Profit after income tax" (not revenue). This is OCI, not income statement.
    """
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return False
    
    # Check first 20 lines (increased from 15 to catch more context)
    top_text = _normalize(" ".join(lines[:20]))
    
        # CRITICAL: Check if first line item is "Other comprehensive income" or "Profit after income tax"
    # "Profit after income tax" as FIRST line item indicates this is OCI (not full income statement)
    # Full income statements start with Revenue/Sales/Interest Income
    found_first_item = False
    for i, line in enumerate(lines[:20]):
        line_norm = _normalize(line)
        # Skip header lines (more comprehensive list)
        if any(skip in line_norm for skip in [
            "CONSOLIDATED", "COMPANY", "STATEMENT", "STATEMENTS", "COMPREHENSIVE INCOME",
            "YEAR ENDED", "MARCH", "NOTES", "2024", "2025", "$M", "FOR THE FINANCIAL"
        ]):
            continue
        
        # Skip if line is just numbers (year headers)
        if line_norm.replace(" ", "").replace("$", "").replace("M", "").isdigit():
            continue
        
        # This is the first actual line item
        if not found_first_item:
            found_first_item = True
            
            # If first line is "Other comprehensive income", this is OCI
            if "OTHER COMPREHENSIVE" in line_norm:
                return True
            
            # CRITICAL: If first line is "Profit after income tax", this is OCI (not full income statement)
            # Full income statements start with revenue/interest income, not with profit
            if "PROFIT AFTER INCOME TAX" in line_norm or "PROFIT AFTER TAX" in line_norm or "PROFIT FOR THE" in line_norm:
                return True
            
            # If first line is a normal income statement item, it's NOT comprehensive-only
            if any(item in line_norm for item in [
                "REVENUE", "INTEREST INCOME", "NET INTEREST", "OPERATING INCOME", 
                "SALES", "NET INTEREST INCOME", "INTEREST AND SIMILAR INCOME",
                "INTEREST REVENUE", "NET SALES", "TOTAL REVENUE"
            ]):
                return False
            
            # If we found the first item but it doesn't match any pattern, continue checking
            # (might be a label without numbers)
    
                # Check the heading to determine if it's comprehensive income only
    if "COMPREHENSIVE INCOME" in top_text:
        # CRITICAL: Check if it's a COMBINED statement (profit/loss AND comprehensive income)
        # Combined statements like "PROFIT OR LOSS AND OTHER COMPREHENSIVE INCOME" are VALID income statements
        if "PROFIT OR LOSS" in top_text or "PROFIT AND LOSS" in top_text:
            return False  # Combined statement - this is a valid income statement
        
        # CRITICAL: If heading contains "SUMMARY OF" or "RINGKASAN" it's likely a valid summary income statement
        # Example: "SUMMARY OF CONSOLIDATED STATEMENTS OF PROFIT OR LOSS AND OTHER COMPREHENSIVE INCOME"
        if "SUMMARY OF" in top_text or "RINGKASAN" in top_text or "IKHTISAR" in top_text:
            return False  # Summary statements are valid income statements
        
        # If heading is "STATEMENTS OF COMPREHENSIVE INCOME" (not "INCOME STATEMENTS")
        # and there's NO "PROFIT OR LOSS" in the heading, it's OCI only
        if "STATEMENTS OF COMPREHENSIVE INCOME" in top_text and "PROFIT OR LOSS" not in top_text:
            return True
        
        # Check if it's "STATEMENT OF COMPREHENSIVE INCOME" (singular) without "PROFIT OR LOSS"
        if "STATEMENT OF COMPREHENSIVE INCOME" in top_text and "PROFIT OR LOSS" not in top_text:
            return True
    
    return False


def _is_notes_page(text: str) -> bool:
    """
    Detect if this is a notes/footnotes page rather than a primary financial statement.
    Notes pages should be EXCLUDED from extraction.
    
    Indicators:
    - Contains "NOTES TO THE FINANCIAL STATEMENTS" or "CATATAN ATAS LAPORAN KEUANGAN"
    - Contains "Notes to the consolidated financial statements"
    - Contains note numbers like "Note 11", "Note 35", etc. in the heading
    - Contains "(continued)" indicating it's a continuation of notes
    - Contains "Notes to the financial statements" in first 15 lines
    """
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return False
    
    # Check first 15 lines for notes indicators (increased from 10)
    top_text = _normalize(" ".join(lines[:15]))
    
    # Notes keywords (English and Indonesian) - EXPANDED
    notes_keywords = [
        "NOTES TO THE FINANCIAL STATEMENTS",
        "NOTES TO FINANCIAL STATEMENTS",
        "NOTES TO THE CONSOLIDATED FINANCIAL STATEMENTS",
        "NOTES TO CONSOLIDATED FINANCIAL STATEMENTS",
        "NOTES TO AND FORMING PART OF THE FINANCIAL STATEMENTS",  # Macquarie format
        "NOTES TO THE CONSOLIDATED FINANCIAL STATEMENTS",
        "CATATAN ATAS LAPORAN KEUANGAN",  # Indonesian: Notes to financial statements
        "CATATAN ATAS LAPORAN KEUANGAN KONSOLIDASIAN",  # Indonesian: Notes to consolidated financial statements
        "CONTINUED",  # Notes pages often have "continued" in header
    ]
    
    for keyword in notes_keywords:
        if _normalize(keyword) in top_text:
            return True
    
    # Check for note numbers in first few lines (e.g., "11. GOODWILL AND OTHER INTANGIBLE ASSETS")
    # Pattern: number followed by period at start of line
    for line in lines[:5]:
        line_stripped = line.strip()
        # Match patterns like "11. GOODWILL" or "35. PROPERTY" or "Note 1."
        if re.match(r'^(Note\s+)?\d{1,2}\.\s+[A-Z]', line_stripped, re.IGNORECASE):
            # This looks like a note number - check if it's in the notes section
            # Notes typically have titles like "11. GOODWILL AND OTHER INTANGIBLE ASSETS"
            # But income statements don't start with numbers
            return True
    
    return False


def _fuzzy_heading_match(text_normalized: str, heading_normalized: str) -> bool:
    """
    Fuzzy matching for statement headings using keyword-based approach.
    
    Instead of exact string matching, check if key words from the heading appear in the text.
    This handles variations like:
    - "Income statements" vs "Income Statement" vs "INCOME STATEMENT"
    - "Consolidated statements of income" vs "Consolidated income statement"
    - "Balance sheets" vs "Balance Sheet"
    
    Returns True if the heading is likely present based on keyword matching.
    """
    # Extract key words from heading (ignore common filler words)
    filler_words = {"THE", "OF", "AND", "FOR", "AS", "AT", "TO", "A", "AN", "IN", "ON", "BY"}
    
    heading_words = [w for w in heading_normalized.split() if w not in filler_words and len(w) > 2]
    
    # If heading has no meaningful words, fall back to exact match
    if not heading_words:
        return heading_normalized in text_normalized
    
    # Check if most key words (at least 70%) appear in the text
    # This allows for minor variations while still being specific
    matches = sum(1 for word in heading_words if word in text_normalized)
    match_ratio = matches / len(heading_words)
    
    # Require at least 70% of key words to match
    # For short headings (1-2 words), require 100% match
    if len(heading_words) <= 2:
        return match_ratio >= 1.0
    else:
        return match_ratio >= 0.7


def _heading_in_first_lines(text: str, statement_type: str) -> str | None:
    """
    Check whether a known statement heading appears in the first
    HEADING_SEARCH_LINES lines of the page.

    Uses FUZZY MATCHING instead of exact string matching to handle variations:
    - "Income statements" vs "Income Statement"
    - "Consolidated statements of income" vs "Consolidated income statement"
    - Different word orders and pluralization
    
    Handles two common PDF extraction artefacts:
      - Extra/double spaces  → normalized comparison
      - No spaces at all     → space-stripped comparison
      - Text extraction order issues (bilingual PDFs) → check first N characters too
    
    IMPORTANT: Excludes summary/highlights pages and notes pages even if they match headings.
    """
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    
    # CRITICAL: Check if this is a notes page first (before any heading checks)
    if _is_notes_page(text):
        # This is a notes page - skip it
        return None
    
    # Method 1: Check first HEADING_SEARCH_LINES lines (sequential)
    top_block         = _normalize(" ".join(lines[:HEADING_SEARCH_LINES]))
    top_block_nospace = top_block.replace(" ", "")
    
    # Method 2: Check first 3000 characters (for bilingual PDFs with text order issues)
    # This catches headings that appear visually at the top but are extracted out of order
    first_chars = _normalize(text[:3000])
    first_chars_nospace = first_chars.replace(" ", "")

    for heading in STATEMENT_HEADINGS.get(statement_type, []):
        normalized = _normalize(heading)
        normalized_nospace = normalized.replace(" ", "")
        
        # Debug: Print what we're looking for vs what we have (for pages 226-227)
        if "COMPREHENSIVE" in top_block and statement_type == "income_statement":
            print(f"    DEBUG: Looking for '{normalized_nospace[:50]}...' in '{top_block_nospace[:100]}...'")
        
        # EXACT MATCH ONLY (no fuzzy matching)
        # Check in first HEADING_SEARCH_LINES lines
        if normalized in top_block or normalized_nospace in top_block_nospace:
            # Found exact match - check if it's a summary page
            if _is_summary_or_highlights_page(text):
                return None
            return heading
        
        # Fallback: Check in first 3000 characters (for bilingual PDFs with text order issues)
        if normalized in first_chars or normalized_nospace in first_chars_nospace:
            # Found exact match - check if it's a summary page
            if _is_summary_or_highlights_page(text):
                return None
            return heading

    return None


def _has_table_structure(text: str) -> bool:
    """
    Return True if the page contains at least MIN_TABLE_ROWS lines that look
    like financial table rows (label + 2 or more numeric values).
    """
    count = sum(1 for line in text.splitlines() if _TABLE_ROW_RE.search(line))
    return count >= MIN_TABLE_ROWS


def _is_continuation_page(text: str, parent_heading: str, statement_type: str) -> bool:
    """
    OPTIMIZED CONTINUATION LOGIC (as per user requirements):
    
    1. Check if page has SAME statement type heading as parent
       - If YES → Continue immediately (don't check other statement types!)
    2. If NO same heading → Check if page has DIFFERENT statement type heading
       - If YES → STOP (different statement)
    3. If no heading at all → Check if page has table rows
       - If YES → Continue (continuation without heading)
    """
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return False
    
    # STEP 1: Check if page has SAME statement type heading (PRIORITY CHECK)
    # This is the most important check - if same heading found, accept immediately!
    current_heading = _heading_in_first_lines(text, statement_type)
    if current_heading:
        # Page has same statement type heading → continuation
        # Don't check other statement types - we already found the right one!
        print(f"  DEBUG: Same statement heading found ('{current_heading}') → continuing")
        return True
    
        # STEP 2: No same heading found - check if page has DIFFERENT statement type heading
    # Only check other statement types if we didn't find the target statement heading
    all_statement_types = ["income_statement", "balance_sheet", "cash_flow"]
    for stype in all_statement_types:
        if stype != statement_type:
            other_heading = _heading_in_first_lines(text, stype)
            if other_heading:
                # Page has different statement type heading → STOP
                print(f"  DEBUG: Different statement heading found ('{other_heading}') → stopping")
                return False
    
    # CRITICAL: Also check for Statement of Changes in Equity (not in our main statement types)
    # This statement often appears between income statement and cash flow
    top_text = _normalize(" ".join(lines[:15]))
    
    equity_change_keywords = [
        "CHANGES IN EQUITY",
        "CHANGES IN UNITHOLDERS",
        "CHANGES IN SHAREHOLDERS",
        "STATEMENT OF CHANGES IN EQUITY",
        "STATEMENTS OF CHANGES IN EQUITY",
        "STATEMENT OF CHANGES IN UNITHOLDERS",
        "STATEMENTS OF CHANGES IN UNITHOLDERS",
        "STATEMENT OF CHANGES IN SHAREHOLDERS",
        "STATEMENTS OF CHANGES IN SHAREHOLDERS",
        "PERUBAHAN EKUITAS",
        "LAPORAN PERUBAHAN EKUITAS",
    ]
    
    for keyword in equity_change_keywords:
        if _normalize(keyword) in top_text:
            print(f"  DEBUG: Statement of Changes in Equity found ('{keyword}') → stopping")
            return False
    
    # STEP 3: No heading found at all - check if page has table rows
    table_row_count = sum(1 for line in lines if _TABLE_ROW_RE.search(line))
    if table_row_count < 1:
        print(f"  DEBUG: No heading and no table rows → not a continuation")
        return False
    
    # Has table rows but no heading → likely a continuation
    if table_row_count >= 5:
        print(f"  DEBUG: No heading but has {table_row_count} table rows → continuing")
        return True
    
    # Too few table rows → not a continuation
    print(f"  DEBUG: Only {table_row_count} table rows (< 5 required) → not a continuation")
    return False


def _find_balance_sheet_anchor(pdf) -> int | None:
    """
    Find the first ACTUAL balance sheet page in the PDF (not summary/highlights).
    This serves as an anchor point for finding other statements nearby.
    
    Uses the SAME strict criteria as balance sheet extraction to avoid false positives.
    
    Returns the 0-based page number of the first balance sheet, or None if not found.
    """
    print("  Looking for Balance Sheet anchor page...")
    
    for page_num, page in enumerate(pdf.pages):
        text = _extract_text(page)
        if not text.strip():
            continue
        
        # CRITICAL: Skip summary/highlights pages
        if _is_summary_or_highlights_page(text):
            continue
        
        # CRITICAL: Skip notes pages
        if _is_notes_page(text):
            continue
        
        # Check if this page has a balance sheet heading
        matched_heading = _heading_in_first_lines(text, "balance_sheet")
        if not matched_heading:
            continue
        
        # CRITICAL: Check if page has multiple statement headings (bilingual pages)
        # If so, check which statement appears FIRST
        if not _check_which_statement_on_page(text, "balance_sheet"):
            continue
        
        # Check if it has table structure
        if not _has_table_structure(text):
            continue
        
        # Check if it has enough rows (same as balance sheet extraction)
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        table_row_count = sum(1 for line in lines if _TABLE_ROW_RE.search(line))
        if table_row_count < 15:
            continue
        
        # CRITICAL: Validate content - must have Assets, Liabilities, Equity
        text_upper = text.upper()
        has_assets = "ASSETS" in text_upper or "ASET" in text_upper
        has_liabilities = "LIABILITIES" in text_upper or "LIABILITAS" in text_upper or "KEWAJIBAN" in text_upper
        has_equity = "EQUITY" in text_upper or "EKUITAS" in text_upper
        
        if not (has_assets and has_liabilities and has_equity):
            # Not a real balance sheet - probably a summary
            continue
        
        # Found a valid balance sheet page!
        print(f"  ✓ Found Balance Sheet anchor at page {page_num + 1}")
        print(f"    Heading: '{matched_heading}'")
        print(f"    Table rows: {table_row_count}")
        return page_num
    
    print("  ✗ No Balance Sheet anchor found")
    return None


def scan_for_candidates(
    pdf_path: str,
    statement_type: str,
    balance_sheet_anchor: int = None,
) -> list[dict]:
    """
    OPTIMIZED SCANNING STRATEGY:
    
    1. First, find the Balance Sheet page (anchor point)
    2. For Income Statement: Search only in range [balance_sheet_page, balance_sheet_page + 10]
    3. For Cash Flow: Search only in range [balance_sheet_page, balance_sheet_page + 10]
    4. For Balance Sheet: Search normally (it's the anchor)
    
    This is MUCH faster than scanning the entire PDF because financial statements
    are almost always grouped together in annual reports.
    
    If no balance sheet anchor is found, fall back to full PDF scan.
    
    Args:
        pdf_path: Path to PDF file
        statement_type: Type of statement to find
        balance_sheet_anchor: 0-based page number of balance sheet (if already known)
    """
    candidates = []

    print(f"Scanning PDF for '{statement_type}'...")

    with pdfplumber.open(pdf_path) as pdf:
        total_pages = len(pdf.pages)
        print(f"  Total pages: {total_pages}\n")

                # Early check: detect garbled/CID-encoded text
        _sample_idx = min(total_pages - 1, max(5, total_pages // 4))
        _sample_text = pdf.pages[_sample_idx].extract_text() or ""
        if _is_garbled_text(_sample_text):
            print(f"  WARNING: PDF has garbled/CID-encoded text -- text-based heading scan will not work.")
            print(f"  Use manual page entry with vision extraction.\n")
            return []

                # OPTIMIZATION: Find balance sheet anchor for targeted scanning
        search_range = None
        
        if statement_type in ["income_statement", "cash_flow"]:
            # Use provided anchor if available, otherwise find it
            if balance_sheet_anchor is None:
                balance_sheet_anchor = _find_balance_sheet_anchor(pdf)
            else:
                print(f"  Using provided Balance Sheet anchor: page {balance_sheet_anchor + 1}")
            
            if balance_sheet_anchor is not None:
                # CRITICAL: Start search from the page AFTER balance sheet ends
                # This prevents matching balance sheet content as income statement
                start_page = balance_sheet_anchor + 1
                end_page = min(start_page + 10, total_pages)
                search_range = (start_page, end_page)
                print(f"  Using optimized search range: pages {search_range[0] + 1} to {search_range[1]}")
                print(f"  (Starting from page after balance sheet ends)")
                print(f"  (Skipping {search_range[0]} pages before and {total_pages - search_range[1]} pages after)\n")
            else:
                print(f"  No balance sheet anchor found - falling back to full PDF scan\n")
        
        skip_pages = set()

        # Determine which pages to scan
        if search_range:
            pages_to_scan = range(search_range[0], search_range[1])
        else:
            pages_to_scan = range(total_pages)
        
        for page_num in pages_to_scan:
            page = pdf.pages[page_num]
            if page_num in skip_pages:
                # Debug: show which pages are being skipped
                if page_num >= 320 and page_num <= 330:
                    print(f"  Page {page_num + 1}: SKIPPED (already merged as continuation)")
                continue
            
            # Debug: Show which pages are being scanned in optimized range
            if search_range:
                print(f"  Scanning page {page_num + 1}...")

            # Debug: Show page dimensions for pages around 321
            if page_num >= 320 and page_num <= 327:
                print(f"  DEBUG: Page {page_num + 1} dimensions: width={page.width:.0f}, height={page.height:.0f}, is_landscape={_is_landscape(page)}")

            text = _extract_text(page)
            if not text.strip():
                continue

            landscape_side = None
            landscape_crop_bbox = None

                        # CRITICAL: For bilingual PDFs with side-by-side layout, DON'T crop
            # Cropping breaks table structure detection (labels on one side, numbers on other)
            # Instead, use full page and rely on content validation
            # Only crop for true landscape pages (width > height)
            if _is_landscape(page):
                cropped_page, side, bbox = _find_heading_side(page, statement_type)
                if side is not None:
                    landscape_side = side
                    landscape_crop_bbox = bbox
                    text = _extract_text(cropped_page)
                    print(f"  Page {page_num + 1}: landscape — heading on {side} half, cropping")
                else:
                    print(f"  DEBUG: Landscape page but heading not isolated to one side - using full page")

                                                # OPTIMIZED FILTERING (as per user requirements):
            # 1. Check heading FIRST (fastest check)
            # 2. Then check table structure
            # 3. Then check row count
            # This avoids unnecessary table checks for pages without the target heading
            
                                    # Filter 1 — heading must appear in first HEADING_SEARCH_LINES lines
            matched_heading = _heading_in_first_lines(text, statement_type)
            if not matched_heading:
                # DEBUG: Print first few lines to help diagnose heading match failures
                if search_range or page_num < 10 or (page_num >= 299 and page_num <= 330):  # Debug pages in optimized range
                    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
                    debug_text = " ".join(lines[:HEADING_SEARCH_LINES])
                    print(f"  Page {page_num + 1}: NO MATCH. First {HEADING_SEARCH_LINES} lines: {debug_text[:300]}...")
                continue
            
                                                # Debug: Show which heading was matched for pages around 321-327
            if page_num >= 320 and page_num <= 327:
                print(f"  DEBUG: Page {page_num + 1} matched heading: '{matched_heading}'")
            
                        # CRITICAL: Check if page has multiple statement headings (bilingual pages)
            # If so, check which statement appears FIRST to determine if this is the right page
            if not _check_which_statement_on_page(text, statement_type):
                # Target statement is NOT the primary statement on this page
                print(f"  Page {page_num + 1}: Skipped (has multiple statement headings, target not primary)")
                continue

            # Filter 2 — page must have table-like structure (only check if heading matched)
            if not _has_table_structure(text):
                print(f"  Page {page_num + 1}: heading found but no table structure — skipped")
                continue
            
                        # Filter 3 — page must have at least 15 table rows (only check if heading + table matched)
            # EXCEPTION: Don't apply this filter during continuation detection - continuations can have < 15 rows
            lines_for_count = [ln.strip() for ln in text.splitlines() if ln.strip()]
            table_row_count = sum(1 for line in lines_for_count if _TABLE_ROW_RE.search(line))
            
            # Debug: Show row count for pages around 321-327
            if page_num >= 320 and page_num <= 327:
                print(f"  DEBUG: Page {page_num + 1} has {table_row_count} table rows")
            
            if table_row_count < 15:
                print(f"  Page {page_num + 1}: heading found but only {table_row_count} rows (< 15 required) — skipped")
                continue

            # Walk forward to collect continuation pages
            all_page_nums = [page_num]
            merged_text   = text

            for offset in range(1, MAX_CONTINUATION_PAGES + 1):
                next_idx = page_num + offset
                if next_idx >= total_pages:
                    break
                next_page = pdf.pages[next_idx]

                # Apply same landscape crop if the continuation page is also landscape
                if landscape_crop_bbox and _is_landscape(next_page):
                    next_text = next_page.crop(landscape_crop_bbox).extract_text() or ""
                else:
                    next_text = _extract_text(next_page)

                # Debug: show page number being checked
                print(f"  Checking page {next_idx + 1} for continuation...")
                
                # Check if this is a continuation page
                is_continuation = _is_continuation_page(next_text, matched_heading, statement_type)
                if not is_continuation:
                    break

                all_page_nums.append(next_idx)
                skip_pages.add(next_idx)
                merged_text += f"\n{next_text}"
                print(f"  Page {next_idx + 1}: continuation detected, merged")

            # Add candidate AFTER continuation detection (not inside the loop!)
            candidates.append({
                "page_num":            page_num,
                "page_display":        page_num + 1,
                "heading_found":       matched_heading,
                "text_snippet":        text[:500],
                "full_text":           merged_text,
                "validation_text":     text,           # first page only — used by validator
                "all_page_nums":       all_page_nums,
                "landscape_side":      landscape_side,
                "landscape_crop_bbox": landscape_crop_bbox,
            })
            print(f"  Page {page_num + 1}: candidate accepted (heading: '{matched_heading}')")
            print(f"    Page numbers: {[p + 1 for p in all_page_nums]}")

        print(f"\n  {len(candidates)} candidate(s) found by heading scan")
        
        if len(candidates) == 0 and search_range and balance_sheet_anchor is not None:
            print(f"\n  ⚠️  No {statement_type} found after balance sheet (pages {search_range[0] + 1}-{search_range[1]})")
            print(f"  🔍 Trying BACKWARD search (4 pages before balance sheet)...\n")
            
            # Search 4 pages before balance sheet anchor
            backward_start = max(0, balance_sheet_anchor - 4)
            backward_end = balance_sheet_anchor
            backward_range = (backward_start, backward_end)
            
            print(f"  Backward search range: pages {backward_range[0] + 1} to {backward_range[1]}")
            print(f"  (Searching 4 pages before balance sheet at page {balance_sheet_anchor + 1})\n")
            
            # Scan backward range
            for page_num in range(backward_range[0], backward_range[1]):
                page = pdf.pages[page_num]
                if page_num in skip_pages:
                    continue
                
                print(f"  Scanning page {page_num + 1} (backward search)...")
                
                text = _extract_text(page)
                if not text.strip():
                    continue
                
                landscape_side = None
                landscape_crop_bbox = None
                
                if _is_landscape(page):
                    cropped_page, side, bbox = _find_heading_side(page, statement_type)
                    if side is not None:
                        landscape_side = side
                        landscape_crop_bbox = bbox
                        text = _extract_text(cropped_page)
                        print(f"  Page {page_num + 1}: landscape — heading on {side} half, cropping")
                
                # Filter 1 — heading must appear in first HEADING_SEARCH_LINES lines
                matched_heading = _heading_in_first_lines(text, statement_type)
                if not matched_heading:
                    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
                    debug_text = " ".join(lines[:HEADING_SEARCH_LINES])
                    print(f"  Page {page_num + 1}: NO MATCH. First {HEADING_SEARCH_LINES} lines: {debug_text[:300]}...")
                    continue
                
                print(f"  DEBUG: Page {page_num + 1} matched heading: '{matched_heading}'")
                
                # Check if page has multiple statement headings
                if not _check_which_statement_on_page(text, statement_type):
                    print(f"  Page {page_num + 1}: Skipped (has multiple statement headings, target not primary)")
                    continue
                
                # Filter 2 — page must have table-like structure
                if not _has_table_structure(text):
                    print(f"  Page {page_num + 1}: heading found but no table structure — skipped")
                    continue
                
                # Filter 3 — page must have at least 15 table rows
                lines_for_count = [ln.strip() for ln in text.splitlines() if ln.strip()]
                table_row_count = sum(1 for line in lines_for_count if _TABLE_ROW_RE.search(line))
                
                print(f"  DEBUG: Page {page_num + 1} has {table_row_count} table rows")
                
                if table_row_count < 15:
                    print(f"  Page {page_num + 1}: heading found but only {table_row_count} rows (< 15 required) — skipped")
                    continue
                
                # Walk forward to collect continuation pages
                all_page_nums = [page_num]
                merged_text   = text
                
                for offset in range(1, MAX_CONTINUATION_PAGES + 1):
                    next_idx = page_num + offset
                    if next_idx >= total_pages:
                        break
                    next_page = pdf.pages[next_idx]
                    
                    # Apply same landscape crop if the continuation page is also landscape
                    if landscape_crop_bbox and _is_landscape(next_page):
                        next_text = next_page.crop(landscape_crop_bbox).extract_text() or ""
                    else:
                        next_text = _extract_text(next_page)
                    
                    print(f"  Checking page {next_idx + 1} for continuation...")
                    
                    # Check if this is a continuation page
                    is_continuation = _is_continuation_page(next_text, matched_heading, statement_type)
                    if not is_continuation:
                        break
                    
                    all_page_nums.append(next_idx)
                    skip_pages.add(next_idx)
                    merged_text += f"\n{next_text}"
                    print(f"  Page {next_idx + 1}: continuation detected, merged")
                
                # Add candidate from backward search
                candidates.append({
                    "page_num":            page_num,
                    "page_display":        page_num + 1,
                    "heading_found":       matched_heading,
                    "text_snippet":        text[:500],
                    "full_text":           merged_text,
                    "validation_text":     text,
                    "all_page_nums":       all_page_nums,
                    "landscape_side":      landscape_side,
                    "landscape_crop_bbox": landscape_crop_bbox,
                })
                print(f"  ✅ Page {page_num + 1}: candidate accepted from BACKWARD search (heading: '{matched_heading}')")
                print(f"    Page numbers: {[p + 1 for p in all_page_nums]}")
            
            if len(candidates) > 0:
                print(f"\n  ✅ Found {len(candidates)} candidate(s) in BACKWARD search (before balance sheet)")
            else:
                print(f"\n  ❌ No candidates found in backward search either")
    
    # CRITICAL: Prefer candidates with longer/more specific headings
    # This ensures we pick "CONSOLIDATED STATEMENT OF PROFIT OR LOSS AND OTHER COMPREHENSIVE INCOME"
    # over "STATEMENT OF PROFIT OR LOSS" (which is often a summary)
    if len(candidates) > 1:
        # Sort by heading length (longer = more specific)
        candidates_with_length = [(c, len(c['heading_found'])) for c in candidates]
        candidates_with_length.sort(key=lambda x: x[1], reverse=True)
        
        # Keep only candidates with headings >= 80% of the longest heading length
        longest_length = candidates_with_length[0][1]
        threshold_length = longest_length * 0.8
        candidates = [c for c, length in candidates_with_length if length >= threshold_length]
        
        print(f"\n  Filtered by heading specificity: {len(candidates)} candidate(s) remaining")
        for c in candidates:
            print(f"    Page {c['page_display']}: '{c['heading_found']}' (length: {len(c['heading_found'])})")
    
        # SPECIAL HANDLING FOR INCOME STATEMENT: Prefer regular income statement over comprehensive income
    if statement_type == "income_statement" and len(candidates) > 1:
        print(f"\n  Multiple income statement candidates found - checking for comprehensive income...")
        
        # Separate candidates into regular income and comprehensive income
        regular_income = []
        comprehensive_income = []
        
        for candidate in candidates:
            heading = candidate.get('heading_found', '').upper()
            # Check if heading contains "COMPREHENSIVE INCOME"
            if "COMPREHENSIVE INCOME" in heading or "COMPREHENSIVE" in heading:
                # Check if it's ONLY comprehensive (no profit/loss) or combined
                if "PROFIT" in heading or "LOSS" in heading:
                    # Combined statement (e.g., "PROFIT OR LOSS AND OTHER COMPREHENSIVE INCOME")
                    regular_income.append(candidate)
                    print(f"    Page {candidate['page_display']}: Combined income + comprehensive (heading: '{candidate['heading_found']}')")
                else:
                    # Comprehensive income only
                    comprehensive_income.append(candidate)
                    print(f"    Page {candidate['page_display']}: Comprehensive income only (heading: '{candidate['heading_found']}')")
            else:
                # Regular income statement
                regular_income.append(candidate)
                print(f"    Page {candidate['page_display']}: Regular income statement (heading: '{candidate['heading_found']}')")
        
        # RULE: If both exist, prefer regular/combined income statement
        # EXCEPTION: If using optimized search (balance sheet anchor found), accept comprehensive income too
        if regular_income:
            print(f"\n  Using regular/combined income statement(s), ignoring comprehensive-only")
            candidates = regular_income
        elif comprehensive_income:
            print(f"\n  Only comprehensive income found - using it")
            candidates = comprehensive_income
    
                    # OPTIMIZATION: If using optimized search range, don't filter by row count
    # Accept any income statement found near the balance sheet, regardless of row count
    # This handles cases where the statement is split across multiple pages
    
    # Row count filtering is now done during scanning (Filter 3)
    # No need to filter again here
    
        # DEBUG: Print why candidates were rejected
    if len(candidates) == 0:
        print(f"\n  WARNING: All candidates were rejected during filtering!")
        print(f"     This usually means:")
        print(f"     1. All pages were classified as 'comprehensive income only' (for income_statement)")
        print(f"     2. All pages had < 25 table rows (summary pages)")
        print(f"     3. All pages were classified as notes/summary pages")
        print(f"     4. No statements found in the optimized search range (if used)")
    
        print(f"\n  Final: {len(candidates)} candidate(s) to validate")
    
    # Show performance improvement if optimized search was used
        # Show performance improvement if optimized search was used
    if search_range:
        # Calculate total pages scanned (forward + backward if applicable)
        forward_pages = search_range[1] - search_range[0]
        backward_pages = 0
        if len(candidates) > 0 and balance_sheet_anchor is not None:
            # Check if any candidate came from backward search
            for candidate in candidates:
                if candidate['page_num'] < balance_sheet_anchor:
                    # This candidate is from backward search
                    backward_pages = min(4, balance_sheet_anchor)  # We searched up to 4 pages back
                    break
        
        total_scanned = forward_pages + backward_pages
        pages_skipped = total_pages - total_scanned
        
        if backward_pages > 0:
            print(f"\n  ⚡ OPTIMIZATION: Scanned {total_scanned} pages ({forward_pages} forward + {backward_pages} backward, skipped {pages_skipped} pages)")
        else:
            print(f"\n  ⚡ OPTIMIZATION: Scanned only {total_scanned} pages (skipped {pages_skipped} pages)")
        print(f"  ⚡ Speed improvement: ~{int((pages_skipped / total_pages) * 100)}% faster\n")
    else:
        print()
    
    return candidates


def build_manual_candidate(pdf_path: str, page_num: int, statement_type: str, allow_continuation: bool = False) -> dict | None:
    """
    Build a candidate dict from a user-supplied 0-based page number, skipping heading check.
    
    Args:
        pdf_path: Path to PDF file
        page_num: 0-based page number
        statement_type: Type of statement (income_statement, balance_sheet, cash_flow)
        allow_continuation: If True, apply continuation detection. If False, extract ONLY the specified page.
                           Default is False - when user manually specifies a page, extract ONLY that page!
    
    Returns:
        Candidate dictionary or None if page is invalid
    
    Flags ``text_garbled=True`` when the PDF uses CID-encoded or unreadable fonts.
    """
    with pdfplumber.open(pdf_path) as pdf:
        total_pages = len(pdf.pages)
        if page_num < 0 or page_num >= total_pages:
            print(f"  Page {page_num + 1} is out of range (PDF has {total_pages} pages).")
            return None

        page = pdf.pages[page_num]
        text = _extract_text(page)
        garbled = _is_garbled_text(text)

        if not text.strip() and not garbled:
            print(f"  Page {page_num + 1} has no extractable text.")
            return None

        if garbled:
            print(f"  Page {page_num + 1}: garbled/CID-encoded text detected — will use vision extraction.")

        landscape_side = None
        landscape_crop_bbox = None

        if not garbled and _is_landscape(page):
            cropped_page, side, bbox = _find_heading_side(page, statement_type)
            if side is not None:
                landscape_side = side
                landscape_crop_bbox = bbox
                text = _extract_text(cropped_page)
                print(f"  Page {page_num + 1}: landscape — heading on {side} half, cropping")

                        # CRITICAL: Only do continuation detection if explicitly allowed
        # When user manually specifies a page, extract ONLY that page (no continuation)
        all_page_nums = [page_num]
        merged_text   = text

        if allow_continuation and not garbled:
            # Continuation detection enabled (used by LLM scan, not manual input)
            print(f"  Continuation detection enabled for page {page_num + 1}")
            for offset in range(1, MAX_CONTINUATION_PAGES + 1):
                next_idx = page_num + offset
                if next_idx >= total_pages:
                    break
                next_page = pdf.pages[next_idx]

                if landscape_crop_bbox and _is_landscape(next_page):
                    next_text = next_page.crop(landscape_crop_bbox).extract_text() or ""
                else:
                    next_text = _extract_text(next_page)

                if not _is_continuation_page(next_text, "", statement_type):
                    break

                all_page_nums.append(next_idx)
                merged_text += f"\n{next_text}"
                print(f"  Page {next_idx + 1}: continuation detected, merged")
        else:
            if not allow_continuation:
                print(f"  Continuation detection DISABLED - extracting ONLY page {page_num + 1}")
        
        print(f"  Manual candidate built: page {page_num + 1}, all_pages: {[p + 1 for p in all_page_nums]}")
        
        return {
            "page_num":            page_num,
            "page_display":        page_num + 1,
            "heading_found":       "manual",
            "text_snippet":        text[:500],
            "full_text":           merged_text,
            "validation_text":     text,
            "all_page_nums":       all_page_nums,
            "landscape_side":      landscape_side,
            "landscape_crop_bbox": landscape_crop_bbox,
            "is_confirmed":        True,
            "text_garbled":        garbled,
        }


def llm_scan_for_candidates(pdf_path: str, statement_type: str) -> list[dict]:
    """
    Tier 2: LLM-based page identification when the static heading scan finds nothing.
    Collects the first 3 lines of all table-structure pages and asks the LLM in one call.
    """
    print(f"  Tier 2: LLM scan for '{statement_type}'...")

    table_pages = []
    with pdfplumber.open(pdf_path) as pdf:
        # Skip Tier 2 when text is garbled — LLM will get gibberish
        _sample_idx = min(len(pdf.pages) - 1, max(5, len(pdf.pages) // 4))
        _sample_text = pdf.pages[_sample_idx].extract_text() or ""
        if _is_garbled_text(_sample_text):
            print("  Skipping LLM scan — PDF has garbled/CID-encoded text.")
            return []

        for page_num, page in enumerate(pdf.pages):
            text = _extract_text(page)
            if not text.strip() or not _has_table_structure(text):
                continue
            lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
            table_pages.append((page_num, "\n".join(lines[:3])))

    if not table_pages:
        print("  No table pages found for LLM scan.")
        return []

        pages_text      = "\n\n".join(f"Page {p + 1}:\n{lines}" for p, lines in table_pages)
    statement_label = _STATEMENT_TYPE_LABELS.get(statement_type, statement_type)

    # Try primary LLM first, with fallback to MAIA
    try:
        client   = get_client()
        response = client.chat.completions.create(
            model=LLM_MODEL,
            messages=[{"role": "user", "content": _LLM_SCAN_PROMPT.format(
                statement_label=statement_label,
                pages_text=pages_text,
            )}],
            temperature=0,
            response_format={"type": "json_object"},
        )
        track_usage(response)
        result = json.loads(response.choices[0].message.content)
        
    except Exception as e:
        logging.warning(f"Primary LLM failed during LLM scan: {e}")
        
                # Fallback to MAIA if configured
        if MAIA_CREDENTIALS:
            logging.info("Falling back to MAIA API for LLM scan...")
            print(f"  Primary LLM failed, retrying with MAIA API...")
            try:
                import os
                # CRITICAL: Clear ALL proxy environment variables for MAIA (internal .intranet domain)
                old_proxy_env = {}
                for var in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 
                           'ALL_PROXY', 'all_proxy', 'NO_PROXY', 'no_proxy']:
                    old_proxy_env[var] = os.environ.pop(var, None)
                
                try:
                    maia_client = get_client(provider="maia", model=MAIA_MODEL)
                    # MAIA doesn't support response_format parameter
                    response = maia_client.chat.completions.create(
                        model=MAIA_MODEL,
                        messages=[{"role": "user", "content": _LLM_SCAN_PROMPT.format(
                            statement_label=statement_label,
                            pages_text=pages_text,
                        )}],
                        temperature=0,
                    )
                    track_usage(response)
                    # Parse JSON from response (MAIA returns JSON in content)
                    content = response.choices[0].message.content
                    # Extract JSON from markdown code blocks if present
                    if "```json" in content:
                        content = content.split("```json")[1].split("```")[0].strip()
                    elif "```" in content:
                        content = content.split("```")[1].split("```")[0].strip()
                    result = json.loads(content)
                    logging.info("MAIA API fallback successful for LLM scan")
                finally:
                    # Restore proxy environment variables
                    for var, val in old_proxy_env.items():
                        if val is not None:
                            os.environ[var] = val
            except Exception as maia_error:
                logging.error(f"MAIA API fallback also failed during LLM scan: {maia_error}")
                print(f"  Both primary LLM and MAIA fallback failed during LLM scan.")
                return []
        else:
            logging.error("No MAIA credentials configured for fallback")
            print(f"  Primary LLM failed and no MAIA fallback configured.")
            return []
    
    found_pages = result.get("page_numbers", [])

    if not found_pages:
        print("  LLM found no matching pages.")
        return []

    print(f"  LLM identified pages: {found_pages}")

    candidates = []
    for page_num_1based in found_pages:
        # LLM scan should enable continuation detection
        candidate = build_manual_candidate(pdf_path, page_num_1based - 1, statement_type, allow_continuation=True)
        if candidate:
            candidate["heading_found"] = "LLM-identified"
            candidates.append(candidate)

    return candidates
