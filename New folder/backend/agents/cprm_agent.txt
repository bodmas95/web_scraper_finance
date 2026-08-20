"""
CPRM (Credit Portfolio Risk Management) Agent
==============================================
Specialist agent that deeply understands financial field relationships.
When the mapper can't value-match a BREF field, this agent analyses the
extracted rows AND notes breakdowns to find the arithmetic combination
(sum, difference, sub-items from notes) that produces the reference value.

It returns, for each field, the formula/logic it discovered so the mapper
can apply the same logic to the current year.
"""

from services.llm_service import get_llm
import json
import logging

logger = logging.getLogger(__name__)


async def resolve_unmatched_fields(
    unmatched_fields: list[dict],
    extracted_text: str,
    notes: dict,
    previous_year: str,
    current_year: str,
    stmt_type: str,
    already_used: set[str],
) -> list[dict]:
    """
    For each unmatched BREF field, ask the CPRM agent to decompose
    the reference value into a combination of extracted rows / notes items,
    then compute the current-year value using the same logic.

    Parameters
    ----------
    unmatched_fields : list of dicts, each with:
        - label: BREF field key (e.g. "Q5 | Cost of sales")
        - description: aliases joined
        - reference_value: previous-year BREF value to match
    extracted_text : formatted string of all extracted rows with year values
    notes : dict of extracted notes with breakdowns
    previous_year / current_year : year strings
    stmt_type : "income_statement" / "balance_sheet" / "cash_flow"
    already_used : set of extraction labels already claimed

    Returns
    -------
    list of dicts, one per input field, each with:
        - matched_labels: list of extraction labels used
        - formula_description: human-readable formula explanation
        - target_value: computed current-year value
        - reference_value: the prev-year value (should match reference)
        - confidence: "high" / "medium" / "low"
        - reason: detailed explanation
    """
    if not unmatched_fields:
        return []

    llm = get_llm()

    notes_text = _format_notes(notes) if notes else "No notes available."
    used_str = ", ".join(sorted(already_used)) if already_used else "None"
    fields_json = json.dumps(unmatched_fields, indent=2, default=str)

    prompt = f"""You are a CPRM (Credit Portfolio Risk Management) expert who deeply understands how financial statement line items compose into BREF template fields.

TASK: For each BREF field below, find which combination of extracted rows and/or note breakdowns produces the "reference_value" (the {previous_year} value from the BREF template). Then apply the SAME arithmetic to compute the {current_year} value.

STATEMENT TYPE: {stmt_type.replace("_", " ").title()}

EXTRACTED ROWS from the annual report (each row has values for {previous_year} and {current_year}):
{extracted_text}

NOTES from the annual report (contain sub-breakdowns of line items):
{notes_text}

ALREADY MATCHED labels (do NOT reuse these):
{used_str}

BREF FIELDS TO RESOLVE:
{fields_json}

INSTRUCTIONS — follow this exact process for EACH field:

STEP 1 — ARITHMETIC DECOMPOSITION (most important):
  Look at the reference_value for {previous_year}. Search through ALL extracted rows AND notes breakdowns to find a combination that equals this value:
  - Single row whose {previous_year} value equals reference_value (within 1% tolerance)
  - SUM of 2-4 rows/note items whose {previous_year} values add up to reference_value
  - DIFFERENCE: row A minus row B (or minus a note sub-item like depreciation)
  - Example: If reference_value = 40,715,097 and you see:
      "Direct operating expenses" {previous_year} = 42,506,403
      Note 7 "Depreciation and amortisation" {previous_year} = 2,100,000
      Note X "Reversal of provision" {previous_year} = 308,694
    Then: 42,506,403 - 2,100,000 + 308,694 = 40,715,097 ← this is the decomposition!
  - Check notes breakdowns for sub-items that need to be added or subtracted
  - Try different combinations — the answer may require 2, 3, or even 4 components

STEP 2 — COMPUTE CURRENT YEAR (CRITICAL — DO NOT SKIP):
  Once you find which rows/notes produce the reference_value for {previous_year}, you MUST look up the {current_year} values from those SAME rows/notes and apply the SAME arithmetic.

  EXAMPLE (the two years will have DIFFERENT values):
    {previous_year}: 42,506,403 - 2,100,000 + 308,694 = 40,715,097 ← matches reference
    {current_year}:  45,200,000 - 2,300,000 + 350,000 = 43,250,000 ← this is target_value

  The target_value MUST be computed from {current_year} data. It will almost NEVER equal the reference_value.
  If target_value == reference_value, you are doing it WRONG — you copied instead of computing.

STEP 3 — VERIFY:
  Double-check your arithmetic for BOTH years. Show the full calculation in the reason field.
  Confirm that target_value ≠ reference_value (unless the business genuinely had no change).

RESPOND in this exact JSON format:
{{
  "results": [
    {{
      "label": "exact BREF field label from input",
      "matched_labels": ["list of extraction row labels and/or note items used"],
      "formula_description": "DirectOperatingExpenses - Note7_Depreciation + Note_Reversal",
      "target_value": <computed from {current_year} data — NOT the same as reference_value>,
      "reference_value": <computed from {previous_year} data — should match the input reference_value>,
      "confidence": "high | medium | low",
      "reason": "Step-by-step: {previous_year}: A(42,506,403) - B(2,100,000) + C(308,694) = 40,715,097. {current_year}: A(45,200,000) - B(2,300,000) + C(350,000) = 43,250,000"
    }}
  ]
}}

RULES:
- You MUST attempt arithmetic decomposition for every field — do NOT just match by name
- target_value MUST be computed using {current_year} values — NEVER copy the reference_value
- Show BOTH {previous_year} and {current_year} calculations in "reason"
- If you cannot find any combination that works, set confidence to "low" and explain what you tried
- Use null for values you genuinely cannot compute, NEVER use 0 as a placeholder
- Return numbers as-is (integers or max 2 decimal places), no rounding of large numbers
- One result per input field, in the same order as input

Only valid JSON — no other text."""

    logger.info("  CPRM Agent: Resolving %d unmatched fields...", len(unmatched_fields))

    try:
        resp = await llm.ainvoke(prompt)
        content = resp.content

        if "```json" in content:
            content = content.split("```json")[1].split("```")[0]
        elif "```" in content:
            content = content.split("```")[1].split("```")[0]

        response_data = json.loads(content)
        results = response_data.get("results", [])

        for r in results:
            tv = r.get("target_value")
            rv = r.get("reference_value")
            if tv is not None and rv is not None and tv == rv:
                logger.warning("  CPRM: target_value == reference_value for '%s' — likely a copy error, marking low confidence", r.get("label", "?")[:40])
                r["confidence"] = "low"
                r["reason"] = (r.get("reason", "") + " [WARNING: target_value equals reference_value — current year was likely not computed correctly]")

        logger.info("  CPRM Agent: Got %d results", len(results))
        for r in results:
            label = r.get("label", "?")[:40]
            conf = r.get("confidence", "?")
            tv = r.get("target_value")
            rv = r.get("reference_value")
            logger.info("    %-40s  conf=%s  prev=%s  curr=%s", label, conf, rv, tv)
            if r.get("reason"):
                for line in r["reason"].split(". ")[:2]:
                    logger.info("      %s", line.strip()[:80])

        return results

    except Exception as e:
        logger.error("  CPRM Agent FAILED: %s", e)
        return []


def _format_notes(notes: dict) -> str:
    """Format notes dict into readable text with all breakdowns."""
    lines = []
    for note_key in sorted(notes.keys(), key=lambda k: int(k.split("_")[-1]) if k.split("_")[-1].isdigit() else 999):
        note = notes[note_key]
        if not isinstance(note, dict):
            continue
        title = note.get("title", "")
        lines.append(f"\n=== {note_key}: {title} ===")

        breakdown = note.get("breakdown", {})
        if breakdown:
            for item_name, year_vals in breakdown.items():
                if isinstance(year_vals, dict):
                    vals_str = " | ".join(f"{y}: {v}" for y, v in year_vals.items())
                    lines.append(f"  {item_name}: {vals_str}")
                else:
                    lines.append(f"  {item_name}: {year_vals}")

        summary = note.get("summary", "")
        if summary:
            lines.append(f"  Summary: {summary}")

    return "\n".join(lines) if lines else "No notes available."
