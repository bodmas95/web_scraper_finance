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


def _find_heading_side(page, statement_type: str) -> tuple:
    """
    For a landscape page that may contain two side-by-side tables, determine
    which horizontal half holds the target statement heading.

    Returns (cropped_page, side, bbox) where side is "left", "right", or None.
    None means the heading wasn't found in either half (or spans both — fall back to full page).
    """
    mid_x = page.width / 2

    left_bbox  = (0,     0, mid_x,      page.height)
    right_bbox = (mid_x, 0, page.width, page.height)

    left_text  = page.crop(left_bbox).extract_text()  or ""
    right_text = page.crop(right_bbox).extract_text() or ""

    in_left  = bool(_heading_in_first_lines(left_text,  statement_type))
    in_right = bool(_heading_in_first_lines(right_text, statement_type))

    if in_left and not in_right:
        return page.crop(left_bbox), "left", left_bbox
    if in_right and not in_left:
        return page.crop(right_bbox), "right", right_bbox

    # Heading found in both halves or neither — use full page
    return page, None, None


def _normalize(text: str) -> str:
    """Normalize text for comparison — collapse whitespace, expand abbreviations."""
    text = text.upper()
    text = re.sub(r"\s+", " ", text)
    text = text.replace("&", "AND")
    text = re.sub(r"[.,\-]", "", text)
    return text.strip()


def _heading_in_first_lines(text: str, statement_type: str) -> str | None:
    """
    Check whether a known statement heading appears in the first
    HEADING_SEARCH_LINES lines of the page.

    Handles two common PDF extraction artefacts:
      - Extra/double spaces  → normalized comparison
      - No spaces at all     → space-stripped comparison
    """
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    top_block         = _normalize(" ".join(lines[:HEADING_SEARCH_LINES]))
    top_block_nospace = top_block.replace(" ", "")

    for heading in STATEMENT_HEADINGS.get(statement_type, []):
        normalized = _normalize(heading)
        if normalized in top_block:
            return heading
        if normalized.replace(" ", "") in top_block_nospace:
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
    Return True if the page should be merged as a continuation.

    Case 3 — first line is a table row (no heading at all)  → continue
    Case 2 — first line is a heading matching the parent    → continue
    Case 1 — first line is any other heading                → stop
    """
    if not _has_table_structure(text):
        return False

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return False

    first_line = lines[0]

    # Case 3: page starts directly with a table row — no heading
    if _TABLE_ROW_RE.search(first_line):
        return True

    # Has some heading as first line — check if it matches the parent
    # Case 2: same heading repeated
    if _heading_in_first_lines(text, statement_type):
        return True

    # Case 1: different heading — stop
    return False


def scan_for_candidates(
    pdf_path: str,
    statement_type: str,
) -> list[dict]:
    """
    Scan the PDF and return pages that pass both filters:
      1. A known statement heading in the first HEADING_SEARCH_LINES lines
      2. Sufficient table-like rows (label + 2+ numbers)
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

        skip_pages = set()

        for page_num, page in enumerate(pdf.pages):
            if page_num in skip_pages:
                continue

            text = _extract_text(page)
            if not text.strip():
                continue

            landscape_side = None
            landscape_crop_bbox = None

            # For landscape pages, check if the heading is isolated to one half
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
                # DEBUG: Print first few lines to help diagnose heading match failures
                if page_num < 10 or (page_num >= 299 and page_num <= 305):  # Debug pages around expected range
                    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
                    debug_text = " ".join(lines[:HEADING_SEARCH_LINES])
                    print(f"  Page {page_num + 1}: NO MATCH. First {HEADING_SEARCH_LINES} lines: {debug_text[:300]}...")
                continue

            # Filter 2 — page must have table-like structure
            if not _has_table_structure(text):
                print(f"  Page {page_num + 1}: heading found but no table structure — skipped")
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

                if not _is_continuation_page(next_text, matched_heading, statement_type):
                    break

                all_page_nums.append(next_idx)
                skip_pages.add(next_idx)
                merged_text += f"\n{next_text}"
                print(f"  Page {next_idx + 1}: continuation detected, merged")

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

    print(f"\n  {len(candidates)} candidate(s) after filtering\n")
    return candidates


def build_manual_candidate(pdf_path: str, page_num: int, statement_type: str) -> dict | None:
    """
    Build a candidate dict from a user-supplied 0-based page number, skipping heading check.
    Applies landscape detection and continuation walk exactly like the normal scan.
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

        # Walk continuation pages (skip when text is garbled — detection won't work)
        all_page_nums = [page_num]
        merged_text   = text

        if not garbled:
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
        candidate = build_manual_candidate(pdf_path, page_num_1based - 1, statement_type)
        if candidate:
            candidate["heading_found"] = "LLM-identified"
            candidates.append(candidate)

    return candidates
