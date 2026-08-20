from agents.state import BREFState
from services.llm_service import get_llm
from extraction_config import STATEMENT_HEADINGS
import asyncio
import json
import re
import logging

logger = logging.getLogger(__name__)

SECTION_WINDOW = 20


def _find_statements_section(pdf_text: dict[int, str]) -> list[int]:
    """Find the ~20-page window containing the financial statements.

    Strategy: locate the section header (e.g. 'Statement of Accounts') or the
    auditor's report (statements always follow it), then return that window.
    """
    if not pdf_text:
        return []

    total_pages = max(pdf_text.keys())

    exclude = [
        "financial summary", "financial highlights",
        "five-year", "five year", "ten-year", "ten year",
        "at a glance",
    ]

    # 1) Section header like "Statement of Accounts" or "Financial Statements"
    section_headers = [
        "statement of accounts",
        "consolidated financial statements",
        "financial statements and notes",
        "financial statements",
    ]
    for page_num, text in sorted(pdf_text.items()):
        first_5 = " ".join(text.split("\n")[:5]).lower()
        if any(e in first_5 for e in exclude):
            continue
        if any(h in first_5 for h in section_headers):
            end = min(page_num + SECTION_WINDOW, total_pages)
            logger.info("  Section found via header on page %d → window %d-%d",
                        page_num, page_num, end)
            return [p for p in range(page_num, end + 1) if p in pdf_text]

    # 2) Auditor's report — financial statements follow after ~2 pages
    auditor = ["independent auditor", "report of the auditor", "auditor's report"]
    for page_num, text in sorted(pdf_text.items()):
        first_10 = " ".join(text.split("\n")[:10]).lower()
        if any(h in first_10 for h in auditor):
            start = page_num + 2
            end = min(start + SECTION_WINDOW, total_pages)
            logger.info("  Section found via auditor report on page %d → window %d-%d",
                        page_num, start, end)
            return [p for p in range(start, end + 1) if p in pdf_text]

    # 3) First income statement heading that isn't a summary/notes page
    is_headings = [h.lower() for h in STATEMENT_HEADINGS["income_statement"][:30]]
    notes_exclude = exclude + ["notes to", "accounting polic", "summary of"]
    for page_num, text in sorted(pdf_text.items()):
        first_10 = " ".join(text.split("\n")[:10]).lower()
        if any(e in first_10 for e in notes_exclude):
            continue
        if any(h in first_10 for h in is_headings):
            end = min(page_num + SECTION_WINDOW, total_pages)
            logger.info("  Section found via IS heading on page %d → window %d-%d",
                        page_num, page_num, end)
            return [p for p in range(page_num, end + 1) if p in pdf_text]

    return []


def _find_pages_by_values(
    rows: list[dict], pdf_text: dict[int, str], report_year: int,
    restrict_to: set[int] | None = None,
) -> list[int]:
    """Find PDF pages where extracted values actually appear."""
    if not rows or not pdf_text:
        return []

    search_values: set[int] = set()
    year_keys = [str(report_year), str(report_year - 1)]
    for row in rows:
        for yk in year_keys:
            val = row.get(yk)
            if isinstance(val, (int, float)) and abs(val) >= 1000:
                search_values.add(abs(int(val)))
        if len(search_values) >= 20:
            break

    if len(search_values) < 3:
        return []

    page_hits: dict[int, int] = {}
    for page_num, text in pdf_text.items():
        if restrict_to and page_num not in restrict_to:
            continue
        hits = 0
        for val in search_values:
            formatted = f"{val:,}"
            plain = str(val)
            if formatted in text or plain in text:
                hits += 1
        if hits >= 3:
            page_hits[page_num] = hits

    if not page_hits:
        return []

    sorted_pages = sorted(page_hits.items(), key=lambda x: -x[1])
    max_hits = sorted_pages[0][1]
    threshold = max(max_hits // 2, 3)
    return sorted(p for p, h in sorted_pages if h >= threshold)[:4]


async def _extract_single_statement(
    llm, stmt_type: str, pages_text: str, report_year: int
) -> dict:
    descriptions = {
        "income_statement": (
            "Consolidated Income Statement (also known as Consolidated Statement of "
            "Profit or Loss, Consolidated Statement of Comprehensive Income)"
        ),
        "balance_sheet": (
            "Consolidated Balance Sheet (also known as Consolidated Statement of "
            "Financial Position). This includes BOTH the assets section AND the "
            "liabilities/equity section."
        ),
        "cash_flow": (
            "Consolidated Statement of Cash Flows (also known as Consolidated "
            "Cash Flow Statement)"
        ),
    }

    prompt = f"""You are a financial data extraction expert. Extract the {descriptions[stmt_type]} from the following annual report pages.

IMPORTANT:
- Extract ONLY the CONSOLIDATED version, NOT the company/parent-only version
- If the page shows multiple statements side by side, extract ONLY the {descriptions[stmt_type]}
- Do NOT extract from the Financial Summary section or Notes — only the actual financial statement
- Extract EVERY line item with its label and numerical values for ALL years present
- Values MUST be numbers (integers or decimals), not strings
- Negative values should use negative numbers (e.g., -50000), not parentheses
- Include note references in labels if present (e.g., "Revenue (Note 5)")
- For sub-items, include a "parent" key (e.g., {{"label": "Interest income", "parent": "Finance income", "{report_year}": 1200, "{report_year - 1}": 1100}})
- Include ALL rows: line items, subtotals, and totals

LANGUAGE HANDLING (CRITICAL):
- ALL labels MUST be in ENGLISH ONLY
- If the annual report is in a non-English language (French, Chinese, Japanese, Korean, Indonesian, German, etc.), translate ALL labels to English
- If a row has BOTH local language and English labels (e.g., "Chiffre d'affaires / Revenue"), extract ONLY the English part ("Revenue")
- If a row has ONLY non-English labels, translate to English
- Common translations: "Chiffre d'affaires" = "Revenue", "Resultat net" = "Net income", "Produits" = "Income", "Charges" = "Expenses", "Actif" = "Assets", "Passif" = "Liabilities", "Capitaux propres" = "Equity", "Tresorerie" = "Cash", "Emprunts" = "Borrowings"
- Do NOT create separate rows for local language and English versions of the same line item

Return a JSON object:
{{
  "rows": [
    {{"label": "Revenue", "{report_year}": 85000, "{report_year - 1}": 82000}},
    {{"label": "Cost of sales", "{report_year}": -50000, "{report_year - 1}": -48000}}
  ],
  "year_headers": ["{report_year}", "{report_year - 1}"],
  "unit_scale": "Thousands"
}}

"unit_scale" should be "Thousands", "Millions", or null based on what the report states.

Pages:
{pages_text}"""

    response = await llm.ainvoke(prompt)
    content = response.content

    logger.info("  %s: LLM response length=%d, first 200 chars: %s",
                stmt_type, len(content), content[:200].replace("\n", " "))

    try:
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0]
        elif "```" in content:
            content = content.split("```")[1].split("```")[0]
        result = json.loads(content)
        logger.info("  %s: parsed %d rows", stmt_type, len(result.get("rows", [])))
        return result
    except json.JSONDecodeError as e:
        logger.error("  %s: JSON parse failed: %s — raw: %s", stmt_type, e, content[:500])
        return {
            "rows": [],
            "year_headers": [str(report_year), str(report_year - 1)],
            "unit_scale": None,
        }


async def finance_agent_node(state: BREFState) -> dict:
    logger.info("=" * 60)
    logger.info("FINANCE AGENT: Starting financial statement extraction")
    logger.info("=" * 60)
    llm = get_llm()
    pdf_text = state["pdf_text"]
    report_year = state.get("report_year", 2024)

    # Find the financial statements section (~20 pages)
    section = _find_statements_section(pdf_text)
    section_set = set(section) if section else None

    if section:
        section_text = "\n\n".join(
            f"--- PAGE {p} ---\n{pdf_text[p]}" for p in section if p in pdf_text
        )
        logger.info("  Using section pages %d-%d (%d pages)",
                     section[0], section[-1], len(section))
    else:
        section_text = "\n\n".join(
            f"--- PAGE {p} ---\n{text}" for p, text in sorted(pdf_text.items())
        )
        logger.warning("  No section found, using all %d pages", len(pdf_text))

    all_pages_text = None  # Lazy — built only if fallback needed

    async def extract_one(stmt_type: str):
        nonlocal all_pages_text

        result = await _extract_single_statement(llm, stmt_type, section_text, report_year)

        if not result.get("rows"):
            logger.warning("  %s: 0 rows from section, retrying with all pages", stmt_type)
            if all_pages_text is None:
                all_pages_text = "\n\n".join(
                    f"--- PAGE {p} ---\n{text}" for p, text in sorted(pdf_text.items())
                )
            result = await _extract_single_statement(llm, stmt_type, all_pages_text, report_year)

        rows = result.get("rows", [])
        verified = _find_pages_by_values(rows, pdf_text, report_year, section_set)
        if not verified:
            verified = _find_pages_by_values(rows, pdf_text, report_year)
        result["pages"] = verified
        logger.info("  %s: %d rows, verified pages %s", stmt_type, len(rows), verified)

        return stmt_type, result

    results = await asyncio.gather(
        extract_one("income_statement"),
        extract_one("balance_sheet"),
        extract_one("cash_flow"),
    )

    financial_statements = {stmt_type: data for stmt_type, data in results}

    logger.info("FINANCE AGENT: Extraction complete")
    logger.info("=" * 60)
    return {"financial_statements": financial_statements, "status": "extraction_complete"}
