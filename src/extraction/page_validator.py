import json

from .extraction_config import LLM_MODEL
from .llm_client import get_client, track_usage

VALIDATION_PROMPT = """
You are analysing a page from an annual report PDF.

A genuine financial statement page has all of the following:
  1. A statement heading such as "{heading_found}" near the top of the page.
  2. A financial table with labelled line items and numeric values for multiple years.

Page text:
{page_text}

Is this the actual primary financial statement page, or merely a reference, summary, or supplementary section?

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
) -> dict:
    """Use the LLM to confirm whether a candidate page is the real statement."""
    client = get_client()

    prompt = VALIDATION_PROMPT.format(
        heading_found=candidate.get("heading_found", "financial statement"),
        page_text=candidate.get("validation_text", candidate["full_text"])[:3000],
    )

    response = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        response_format={"type": "json_object"},
    )

    track_usage(response)
    result = json.loads(response.choices[0].message.content)
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
) -> dict | None:
    """
    Locate the correct financial statement page in the PDF.

    Steps:
      1. Heading + table-structure scan.
      2. If a single candidate, return it directly.
      3. If multiple candidates, use the LLM to pick the right one.
    """
    from .scanner import llm_scan_for_candidates, pdf_has_garbled_text, scan_for_candidates

    # Early exit for PDFs with garbled/CID-encoded text
    if pdf_has_garbled_text(pdf_path):
        print(f"  WARNING: PDF has garbled/CID-encoded text -- text-based scanning will not work.")
        print(f"  Please enter page numbers manually; vision extraction will be used automatically.\n")
        return None

    # Tier 1 — static heading scan
    candidates = scan_for_candidates(pdf_path, statement_type)

    # Tier 2 — LLM scan (only when Tier 1 finds nothing)
    if not candidates:
        print(f"  Tier 1 found nothing for '{statement_type}' — trying LLM scan...")
        candidates = llm_scan_for_candidates(pdf_path, statement_type)

    if not candidates:
        print(f"  No candidates found for '{statement_type}' — manual input required.")
        return None

    if len(candidates) == 1:
        # Always validate landscape pages to ensure we cropped the correct half
        if candidates[0].get("landscape_side"):
            print("Single landscape candidate found — validating to confirm correct crop...\n")
            validated = validate_candidate_page(candidates[0])
            if validated["is_confirmed"]:
                return validated
            # If validation failed, wrong half was cropped - try LLM scan
            print(f"  Landscape crop validation failed (wrong half) — trying LLM scan...")
            candidates = llm_scan_for_candidates(pdf_path, statement_type)
            if candidates:
                # LLM scan found pages, validate them
                if len(candidates) == 1:
                    candidates[0]["is_confirmed"] = True
                    return candidates[0]
                # Multiple candidates from LLM scan, validate each
                for candidate in candidates:
                    validated = validate_candidate_page(candidate)
                    if validated["is_confirmed"]:
                        return validated
            return None
        else:
            print("Single candidate found — skipping LLM validation.\n")
            candidates[0]["is_confirmed"] = True
            return candidates[0]

    print(f"LLM validating {len(candidates)} candidates...\n")
    for candidate in candidates:
        validated = validate_candidate_page(candidate)
        if validated["is_confirmed"]:
            return validated

    return None
