"""
BREF Mapping Validator
Cross-validates mapped fields by comparing reference values from template with extracted values
"""


def validate_mappings(mapped_fields: list) -> list:
    """
    Cross-validate each mapped field by comparing:
      - The reference value from the BREF template (Column D)
      - The reference value extracted by the LLM from the annual report

    Sets final_confidence based on the match and the mapping confidence.
    """
    print("Validating mapped fields...\n")
    validated = []

    for field in mapped_fields:
        bref_ref   = _clean_value(field.get("reference_value", ""))
        llm_ref    = _clean_value(field.get("extracted_reference_value", ""))
        map_conf   = field.get("mapping_confidence", "low")

        if not llm_ref or not bref_ref:
            validation_status = "unverified"
            final_confidence  = "low"
        elif _values_match(bref_ref, llm_ref):
            validation_status = "validated"
            final_confidence  = "high"
        else:
            validation_status = "mismatch"
            final_confidence  = "low"

        if map_conf == "low":
            final_confidence = "low"

        enriched = {
            **field,
            "validation_status": validation_status,
            "final_confidence":  final_confidence,
        }

        status = {"validated": "[ok]", "mismatch": "[!!]", "unverified": "[?]"}.get(
            validation_status, "[?]"
        )
        print(f"  {status} {field.get('label', '')[:45]}")
        print(
            f"       ref (bref): {bref_ref}  "
            f"ref (extracted): {llm_ref}  "
            f"status: {validation_status}"
        )

        validated.append(enriched)

    total = len(validated)
    high  = sum(1 for f in validated if f["final_confidence"] == "high")
    low   = sum(1 for f in validated if f["final_confidence"] == "low")
    print(f"\nValidation summary: {high}/{total} high confidence, {low}/{total} low\n")

    return validated


def _clean_value(value: str) -> str:
    if not value:
        return ""
    return str(value).replace(",", "").replace("$", "").strip()


def _values_match(val1: str, val2: str, tolerance: float = 0.01) -> bool:
    """Compare two financial values within a fractional tolerance."""
    try:
        v1, v2 = float(val1), float(val2)
        if v1 == 0 and v2 == 0:
            return True
        return abs(v1 - v2) / max(abs(v1), abs(v2)) <= tolerance
    except (ValueError, ZeroDivisionError):
        return val1 == val2
