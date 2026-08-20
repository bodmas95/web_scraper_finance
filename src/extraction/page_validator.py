import json
import logging

from .extraction_config import LLM_MODEL
from .llm_client import get_client, track_usage
from config.config import load_config

# Load config to check for Maia credentials
_cfg = load_config()
MAIA_CREDENTIALS = _cfg.get("LLM", "maia_credentials", fallback="")
MAIA_MODEL = _cfg.get("LLM", "maia_model", fallback="gpt-5.1-2025-11-13")

VALIDATION_PROMPT = """
You are analysing a page from an annual report PDF.

TARGET STATEMENT TYPE: {statement_type}

A genuine {statement_type} page has all of the following:
  1. A statement heading such as "{heading_found}" near the top of the page.
  2. A financial table with labelled line items and numeric values for multiple years.
  3. The content matches the target statement type (not a different financial statement).

CRITICAL VALIDATION RULES:
- If looking for INCOME STATEMENT: Must contain revenue/sales/income at the top and expenses below (NOT just assets/liabilities, NOT just cash flows, NOT just comprehensive income items)
- If looking for BALANCE SHEET: Must contain assets, liabilities, and equity sections (NOT revenue/expenses, NOT cash flows)
- If looking for CASH FLOW: Must contain operating/investing/financing cash flow sections (NOT revenue/expenses, NOT assets/liabilities)
- "Statement of Changes in Equity" is NOT a cash flow statement - REJECT it for cash_flow
- "Comprehensive Income" that starts with "Profit after tax" (not revenue) is NOT a regular income statement - REJECT it for income_statement
- "Notes to the financial statements" are NOT primary statements - REJECT them
- "Summary" or "Highlights" pages are NOT detailed statements - REJECT them

Page text:
{page_text}

Is this the actual {statement_type} page (not a different statement type, not a summary, not notes)?

Respond in this exact JSON format:
{{
  "is_actual_statement":  true or false,
  "confidence":           "high" or "medium" or "low",
  "reason":               "one sentence explanation",
  "has_numeric_table":    true or false
}}

Only respond with valid JSON — no other text.
"""


def validate_candidate_page(
    candidate: dict,
    statement_type: str = "financial statement",
) -> dict:
    """Use the LLM to confirm whether a candidate page is the real statement.
    
    Automatically falls back to MAIA API if primary LLM fails.
    
    Args:
        candidate: Candidate page dict
        statement_type: Type of statement (income_statement, balance_sheet, cash_flow)
    """
    # Convert statement_type to readable label
    statement_labels = {
        "income_statement": "income statement",
        "balance_sheet": "balance sheet",
        "cash_flow": "cash flow statement",
    }
    statement_label = statement_labels.get(statement_type, statement_type)
    
    prompt = VALIDATION_PROMPT.format(
        statement_type=statement_label,
        heading_found=candidate.get("heading_found", "financial statement"),
        page_text=candidate.get("validation_text", candidate["full_text"])[:3000],
    )

    # Try primary LLM first
    try:
        client = get_client()
        response = client.chat.completions.create(
            model=LLM_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            response_format={"type": "json_object"},
        )
        track_usage(response)
        result = json.loads(response.choices[0].message.content)
        
    except Exception as e:
        logging.warning(f"Primary LLM failed: {e}")
        
        # Fallback to MAIA if configured
        if MAIA_CREDENTIALS:
            logging.info("Falling back to MAIA API...")
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
                        messages=[{"role": "user", "content": prompt}],
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
                    logging.info("MAIA API fallback successful")
                finally:
                    # Restore proxy environment variables
                    for var, val in old_proxy_env.items():
                        if val is not None:
                            os.environ[var] = val
            except Exception as maia_error:
                logging.error(f"MAIA API fallback also failed: {maia_error}")
                raise Exception(f"Both primary LLM and MAIA fallback failed. Primary: {e}, MAIA: {maia_error}")
        else:
            logging.error("No MAIA credentials configured for fallback")
            raise Exception(f"Primary LLM failed and no MAIA fallback configured: {e}")
    
    candidate["validation"] = result
    candidate["is_confirmed"] = (
        result.get("is_actual_statement", False)
        and result.get("confidence") in ("high", "medium")
    )

    status = "confirmed" if candidate["is_confirmed"] else "rejected"
    print(f"  Page {candidate['page_display']}: {status}")
    print(f"  Reason: {result.get('reason', '')}")
    print(f"  Table: {result.get('has_numeric_table')}\n")

    return candidate


def find_correct_page(
    pdf_path: str,
    statement_type: str,
    balance_sheet_anchor: int = None,
) -> dict | None:
    """
    Locate the correct financial statement page in the PDF.

    Steps:
      1. Heading + table-structure scan.
      2. If a single candidate, return it directly.
      3. If multiple candidates, use the LLM to pick the right one.
    
    Args:
        pdf_path: Path to PDF file
        statement_type: Type of statement to find
        balance_sheet_anchor: 0-based page number of balance sheet (for optimized search)
    """
    from .scanner import llm_scan_for_candidates, pdf_has_garbled_text, scan_for_candidates

    # Early exit for PDFs with garbled/CID-encoded text
    if pdf_has_garbled_text(pdf_path):
        print(f"  WARNING: PDF has garbled/CID-encoded text -- text-based scanning will not work.")
        print(f"  Please enter page numbers manually; vision extraction will be used automatically.\n")
        return None

    # Tier 1 — static heading scan
    # CRITICAL: Pass balance sheet anchor for optimized search
    candidates = scan_for_candidates(pdf_path, statement_type, balance_sheet_anchor=balance_sheet_anchor)

    # Tier 2 — LLM scan (only when Tier 1 finds nothing)
    if not candidates:
        print(f"  Tier 1 found nothing for '{statement_type}' — trying LLM scan...")
        candidates = llm_scan_for_candidates(pdf_path, statement_type)

    if not candidates:
        print(f"  No candidates found for '{statement_type}' — manual input required.")
        return None

    # CRITICAL: Filter candidates by row count BEFORE LLM validation
    # This prevents selecting summary pages (< 25 rows) over detailed statements (50+ rows)
    if len(candidates) > 1:
        print(f"  ⚠️ Multiple candidates found - applying preference rules before validation:")
        
        # Count table rows for each candidate
        import re
        _TABLE_ROW_RE = re.compile(
            r".{2,}\s+"
            r"[\(\$]?\d[\d,]*(?:\.\d+)?\)?"
            r"(?:\s+[\(\$]?\d[\d,]*(?:\.\d+)?\)?){1,}"
        )
        
        for candidate in candidates:
            text = candidate.get('full_text', '')
            lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
            table_row_count = sum(1 for line in lines if _TABLE_ROW_RE.search(line))
            candidate['_table_row_count'] = table_row_count
            print(f"    Page {candidate['page_display']}: {table_row_count} table rows")
        
                # Filter out summary pages (< 25 rows)
        detailed_candidates = [c for c in candidates if c.get('_table_row_count', 0) >= 25]
        
        if detailed_candidates:
            print(f"\n  ✅ Filtered to {len(detailed_candidates)} detailed statement(s) (>= 25 rows)")
            candidates = detailed_candidates
        else:
            # If all candidates are summaries, prefer the one with most rows
            print(f"\n  ⚠️ All candidates appear to be summaries - selecting the one with most rows")
            candidates = [max(candidates, key=lambda c: c.get('_table_row_count', 0))]
        
        print(f"\n  Final: {len(candidates)} candidate(s) to validate\n")
    
    # HYBRID VALIDATION APPROACH:
    # 1. Single candidate → Accept without validation (strong filters already applied)
    # 2. Multiple candidates → Use FAST content validation (check keywords in text)
    # 3. If content validation fails for all → Fall back to LLM validation
    
    if len(candidates) == 1:
        print(f"Single candidate found — accepting without validation (strong filters applied).\n")
        candidates[0]["is_confirmed"] = True
        return candidates[0]
    
    # Multiple candidates - use FAST content validation (no LLM calls)
    print(f"Multiple candidates found — using fast content validation...\n")
    
    for candidate in candidates:
        page_num = candidate['page_display']
        text = candidate.get('full_text', '').upper()
        
        # Fast content validation based on keywords
        is_valid = False
        
        if statement_type == "income_statement":
            # Income statement should have REVENUE/SALES, not ASSETS/LIABILITIES
            # Check more text (first 3000 chars) to handle headers/company names
            has_revenue = any(keyword in text[:3000] for keyword in ["REVENUE", "SALES", "PENDAPATAN", "TOTAL REVENUE", "JUMLAH PENDAPATAN"])
            has_expenses = any(keyword in text[:3000] for keyword in ["EXPENSES", "BEBAN", "COST OF", "BIAYA"])
            has_assets = any(keyword in text[:3000] for keyword in ["TOTAL ASSETS", "JUMLAH ASET", "ASET LANCAR", "CURRENT ASSETS"])
            has_cash_flows = any(keyword in text[:3000] for keyword in ["CASH FLOWS FROM", "ARUS KAS DARI"])
            is_valid = (has_revenue or has_expenses) and not has_assets and not has_cash_flows
            
        elif statement_type == "balance_sheet":
            # Balance sheet should have ASSETS and LIABILITIES
            has_assets = any(keyword in text for keyword in ["ASSETS", "ASET"])
            has_liabilities = any(keyword in text for keyword in ["LIABILITIES", "LIABILITAS"])
            is_valid = has_assets and has_liabilities
            
        elif statement_type == "cash_flow":
            # Cash flow should have OPERATING/INVESTING/FINANCING activities
            has_operating = any(keyword in text for keyword in ["OPERATING ACTIVITIES", "AKTIVITAS OPERASI"])
            has_investing = any(keyword in text for keyword in ["INVESTING ACTIVITIES", "AKTIVITAS INVESTASI"])
            is_valid = has_operating or has_investing
        
        if is_valid:
            print(f"  Page {page_num}: ✅ Content validation passed (fast check)")
            candidate["is_confirmed"] = True
            return candidate
        else:
            print(f"  Page {page_num}: ❌ Content validation failed (fast check)")
    
    # If all candidates failed fast validation, fall back to LLM validation
    print(f"\n  All candidates failed fast validation — falling back to LLM validation...\n")
    for candidate in candidates:
        validated = validate_candidate_page(candidate, statement_type)
        if validated["is_confirmed"]:
            return validated
    
    # If all candidates failed LLM validation too, try LLM scan as last resort
    print(f"\n  All candidates failed LLM validation — trying LLM scan as fallback...")
    candidates = llm_scan_for_candidates(pdf_path, statement_type)
    if candidates:
        print(f"  LLM scan found {len(candidates)} candidate(s), validating...\n")
        for candidate in candidates:
            validated = validate_candidate_page(candidate, statement_type)
            if validated["is_confirmed"]:
                return validated

    return None
