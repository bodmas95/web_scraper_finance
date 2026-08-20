from agents.state import BREFState
from services.llm_service import get_llm
import json
import re
import logging

logger = logging.getLogger(__name__)

MAX_NOTES_PER_BATCH = 10


def _find_all_note_refs(pdf_text: dict[int, str], financial_statements: dict) -> set[str]:
    """Find all note references from both extracted rows and raw PDF text."""
    note_refs = set()

    for stmt_type in ["income_statement", "balance_sheet", "cash_flow"]:
        stmt = financial_statements.get(stmt_type, {})
        for row in stmt.get("rows", []):
            label = row.get("label", "")
            matches = re.findall(r'[Nn]ote\s*(\d+)', label)
            note_refs.update(matches)
        for field_name in stmt.get("fields", {}):
            matches = re.findall(r'[Nn]ote\s*(\d+)', field_name)
            note_refs.update(matches)

    statement_pages = set()
    for stmt_type in ["income_statement", "balance_sheet", "cash_flow"]:
        stmt = financial_statements.get(stmt_type, {})
        statement_pages.update(stmt.get("pages", []))

    for page_num, text in pdf_text.items():
        if page_num not in statement_pages:
            continue

        for page_num2, text2 in pdf_text.items():
            if page_num2 in statement_pages:
                note_col_matches = re.findall(
                    r'(?:Note|Notes?)\s*\n?((?:\d{1,2}(?:\([a-z]\))?\s*\n?)+)',
                    text2, re.IGNORECASE,
                )
                for block in note_col_matches:
                    nums = re.findall(r'(\d{1,2})', block)
                    note_refs.update(nums)

    for page_num, text in pdf_text.items():
        if page_num not in statement_pages:
            continue
        for line in text.split('\n'):
            ref_matches = re.findall(
                r'(?:Note|Notes?)\s+(\d{1,2}(?:\([a-z]\))?)', line, re.IGNORECASE,
            )
            note_refs.update(m.split('(')[0] for m in ref_matches)

            col_matches = re.findall(
                r'^\s*(.+?)\s+(\d{1,2}(?:\([a-z]\))?)\s+[\d,()\-]+', line,
            )
            for _, num in col_matches:
                clean_num = num.split('(')[0]
                if clean_num.isdigit() and 1 <= int(clean_num) <= 50:
                    note_refs.add(clean_num)

    note_refs.discard('0')
    return {n.split('(')[0] for n in note_refs
            if n.split('(')[0].isdigit() and 1 <= int(n.split('(')[0]) <= 50}


def _find_notes_pages(pdf_text: dict[int, str], financial_statements: dict) -> list[int]:
    """Find pages that contain the notes to the financial statements.

    Notes typically start right after the last financial statement page and
    continue to the end of the report (minus a few appendix pages).
    """
    stmt_pages: list[int] = []
    for stmt_type in ["income_statement", "balance_sheet", "cash_flow"]:
        stmt = financial_statements.get(stmt_type, {})
        stmt_pages.extend(stmt.get("pages", []))

    if stmt_pages:
        notes_start = max(stmt_pages) + 1
    else:
        notes_start = 1

    all_pages = sorted(pdf_text.keys())
    if not all_pages:
        return []

    notes_pages = [p for p in all_pages if p >= notes_start]

    if not notes_pages and all_pages:
        notes_pages = all_pages

    return notes_pages


async def _extract_notes_batch(
    llm, note_refs: list[str], pages_text: str,
) -> dict:
    """Extract a batch of notes from the given pages."""
    prompt = f"""You are a financial notes extraction expert. From the following annual report pages, extract the content of these specific notes: {note_refs}

For each note, extract:
- The note number
- The title/subject
- All numerical breakdowns and sub-items with their values for each year
- Any relevant explanations about how values are composed

Return a JSON object:
{{
  "note_5": {{
    "title": "Revenue",
    "pages": [120, 121],
    "breakdown": {{
      "Logistics services": {{"2024": 50000, "2023": 48000}},
      "Integrated logistics": {{"2024": 8000, "2023": 7500}}
    }},
    "summary": "Revenue is broken down by business segment"
  }}
}}

Important:
- Extract ALL the notes listed: {note_refs}
- Include every sub-item with numerical values for each year
- If a note has sub-notes (e.g., 17(a), 17(b)), include them under the main note
- Include the page numbers where each note appears
- Return ONLY valid JSON — no other text

Pages:
{pages_text}"""

    response = await llm.ainvoke(prompt)
    content = response.content

    logger.info("  Batch %s: LLM response length=%d", note_refs[:3], len(content))

    try:
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0]
        elif "```" in content:
            content = content.split("```")[1].split("```")[0]
        return json.loads(content)
    except json.JSONDecodeError as e:
        logger.error("  Batch %s: JSON parse failed: %s", note_refs[:3], e)
        logger.error("  First 500 chars: %s", content[:500])
        logger.error("  Last 200 chars: %s", content[-200:] if len(content) > 200 else content)
        return {}


async def notes_agent_node(state: BREFState) -> dict:
    logger.info("=" * 60)
    logger.info("NOTES AGENT: Extracting financial statement notes")
    logger.info("=" * 60)
    llm = get_llm()
    pdf_text = state["pdf_text"]
    financial_statements = state.get("financial_statements", {})

    note_refs = _find_all_note_refs(pdf_text, financial_statements)

    if not note_refs:
        logger.info("  No note references found, using default notes 5-15")
        note_refs = {str(i) for i in range(5, 16)}
    else:
        logger.info("  Found %d note references: %s", len(note_refs), sorted(note_refs, key=int))

    # Only send notes pages (after financial statements), not the whole report
    notes_pages = _find_notes_pages(pdf_text, financial_statements)
    logger.info("  Notes pages: %d-%d (%d pages)",
                notes_pages[0] if notes_pages else 0,
                notes_pages[-1] if notes_pages else 0,
                len(notes_pages))

    pages_text = "\n\n".join(
        f"--- PAGE {page} ---\n{pdf_text[page]}"
        for page in notes_pages if page in pdf_text
    )

    sorted_refs = sorted(note_refs, key=int)
    all_notes: dict = {}

    # Batch notes extraction if there are many
    if len(sorted_refs) <= MAX_NOTES_PER_BATCH:
        batches = [sorted_refs]
    else:
        batches = [
            sorted_refs[i:i + MAX_NOTES_PER_BATCH]
            for i in range(0, len(sorted_refs), MAX_NOTES_PER_BATCH)
        ]

    logger.info("  Extracting %d notes in %d batch(es)...", len(sorted_refs), len(batches))

    for batch_idx, batch in enumerate(batches):
        logger.info("  Batch %d/%d: notes %s", batch_idx + 1, len(batches), batch)
        result = await _extract_notes_batch(llm, batch, pages_text)
        all_notes.update(result)

    logger.info("  Extracted %d notes:", len(all_notes))
    for note_key, note_data in sorted(all_notes.items()):
        title = note_data.get("title", "Unknown") if isinstance(note_data, dict) else "?"
        breakdown_count = len(note_data.get("breakdown", {})) if isinstance(note_data, dict) else 0
        logger.info("    %-12s %-40s (%d items)", note_key, title[:40], breakdown_count)

    logger.info("NOTES AGENT: Complete")
    logger.info("=" * 60)
    return {"notes": all_notes}
