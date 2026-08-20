"""
mapping_v1 - Post-Mapping Validator

Cross-validates every mapped field by comparing:
  - reference_value     : the previous-year value from the BREF Excel template
  - extracted_ref_value : the previous-year value the LLM found in the annual report

Validation rules (aligned with pass semantics):
  Pass 1 "mapped"         -> LLM matched by value, ref values should agree -> "validated"
  Pass 2 "mapped_alias"   -> matched by name, ref values not guaranteed -> "unverified"
  Pass 3 "mapped_derived" -> derived/combination, may differ -> "unverified" or "validated"
  "blank_reference"       -> nothing to validate -> "unverified"
  "no_match"/"ambiguous"  -> no value written -> "unverified"

final_confidence:
  "high"   -> validation_status == "validated"  (values agree within tolerance)
  "medium" -> mapped_alias / mapped_derived with medium LLM confidence
  "low"    -> mismatch, unverified, or low LLM confidence
"""

from typing import Any

TOLERANCE = 0.01   # 1% - same as Pass 1 matching tolerance


def validate_results(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Add validation_status and final_confidence to every result dict.

    Args:
        results: Output list from mapper.map_fields() or pipeline.run() results.

    Returns:
        Same list with validation_status and final_confidence added in-place.
    """
    print("\\n  Validating mapped fields...")

    high_count = low_count = 0

    for field in results:
        status   = field.get("status", "")
        llm_conf = field.get("confidence")       # set by Pass 2 / Pass 3 LLM

        bref_ref      = _clean(field.get("reference_value"))
        extracted_ref = _clean(field.get("extracted_ref_value"))  # set by mapper

        # ------------------------------------------------------------------
        # Determine validation_status
        # ------------------------------------------------------------------
        if status == "mapped":
            # Pass 1: matched by value - implicit validation.
            # Cross-check extracted_ref_value if available.
            if extracted_ref and bref_ref and _match(bref_ref, extracted_ref):
                validation_status = "validated"
            elif extracted_ref and bref_ref:
                validation_status = "mismatch"
            else:
                # No extracted_ref to compare against - trust the value match
                validation_status = "validated"

        elif status in ("mapped_alias", "mapped_derived"):
            # Passed by name/alias/reasoning - validate by value if possible.
            if extracted_ref and bref_ref and _match(bref_ref, extracted_ref):
                validation_status = "validated"
            else:
                validation_status = "unverified"

        else:
            # blank_reference / no_match / ambiguous
            validation_status = "unverified"

        # ------------------------------------------------------------------
        # Determine final_confidence
        # ------------------------------------------------------------------
        if validation_status == "validated":
            if status == "mapped":
                final_confidence = "high"
            elif llm_conf == "high":
                final_confidence = "high"
            else:
                final_confidence = "medium"

        elif validation_status == "mismatch":
            final_confidence = "low"

        else:
            # unverified
            if llm_conf in ("high", "medium"):
                final_confidence = "medium"
            else:
                final_confidence = "low"

        field["validation_status"] = validation_status
        field["final_confidence"]  = final_confidence

        icon = {"validated": "[ok]", "mismatch": "[!!]", "unverified": "[?]"}.get(
            validation_status, "[?]"
        )
        bref_str = bref_ref if bref_ref else "-"
        ext_str  = extracted_ref if extracted_ref else "-"
        print(
            f"    {icon} {field.get('label', '')[:50]:<50}  "
            f"bref_ref={bref_str:>12}  "
            f"ext_ref={ext_str:>12}  "
            f"-> {validation_status} / {final_confidence}"
        )

        if final_confidence == "high":
            high_count += 1
        else:
            low_count += 1

    total = len(results)
    print(f"\\n  Validation done: {high_count}/{total} high confidence, {low_count}/{total} low/medium\\n")
    return results


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clean(value: Any) -> str:
    """Normalise a value to a plain numeric string for comparison."""
    if value is None:
        return ""
    return str(value).replace(",", "").replace("$", "").replace(" ", "").strip()


def _match(a: str, b: str, tolerance: float = TOLERANCE) -> bool:
    """Return True if two string-encoded numbers agree within tolerance."""
    try:
        fa, fb = float(a), float(b)
        if fa == 0 and fb == 0:
            return True
        return abs(fa - fb) / max(abs(fa), abs(fb)) <= tolerance
    except (ValueError, ZeroDivisionError):
        return a == b
