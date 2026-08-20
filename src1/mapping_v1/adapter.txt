"""
mapping_v1 — Compatibility Adapter
====================================
Exposes the same function signatures as src.mapping so that UI components
(brefmap_ui.py, brefmap_multi_ui.py, pdf_helpers.py) can switch to
mapping_v1 by changing only their import block — no other code changes needed.

OLD import block (src.mapping):
    from src.mapping import (
        map_all_fields,
        validate_mappings,
        FIELD_MAPPINGS,
        get_field_mappings,
        load_bref_fields,
        create_clean_output_excel,
        STATEMENT_SHEET_MAP,
    )
    from src.mapping.fast_mapper import fast_map_fields
    from src.mapping.region_adjustments import apply_sign_corrections

NEW import block (mapping_v1 adapter — drop-in replacement):
    from src.mapping_v1.adapter import (
        map_all_fields,
        validate_mappings,
        FIELD_MAPPINGS,
        get_field_mappings,
        load_bref_fields,
        create_clean_output_excel,
        STATEMENT_SHEET_MAP,
        fast_map_fields,
        apply_sign_corrections,
    )

Adapter contract
----------------
Every function here matches the OLD call signature exactly.
Internally it routes to mapping_v1 logic and converts the result
back to the old result-dict shape so downstream UI code is unaffected.

Old result dict shape (what the UI reads):
    {
      "label":                  str,
      "reference_value":        float | None,
      "target_value":           float | None,
      "extracted_reference_value": float | None,   (= extracted_ref_value in v1)
      "year_values":            {year_str: float},
      "mapping_confidence":     "high" | "medium" | "low",
      "final_confidence":       "high" | "medium" | "low",
      "validation_status":      "validated" | "mismatch" | "unverified",
      "mapping_method":         str,
      "matched_label":          str | None,
      "reason":                 str,
      "sign_corrected":         bool,
    }
"""

from __future__ import annotations

import re
from typing import Any

# ---------------------------------------------------------------------------
# Re-export constants and pass-through functions unchanged
# ---------------------------------------------------------------------------

from src.mapping_v1.excel             import (
    create_clean_output_excel,
    STATEMENT_SHEET_MAP,
)
from src.mapping_v1.region_adjustments import apply_sign_corrections
from src.mapping_v1.validator          import validate_results as _validate_results
from src.mapping_v1.mapper             import map_fields      as _map_fields
from src.mapping_v1.excel             import load_bref_fields as _load_bref_fields_v1
from src.mapping_v1.config            import VALID_PREFIXES

# ---------------------------------------------------------------------------
# FIELD_MAPPINGS / get_field_mappings stubs
# ---------------------------------------------------------------------------
# The old API exposed a static registry of BREF fields keyed by region and
# statement type.  mapping_v1 has no such registry — it reads fields directly
# from the Excel template.  We provide lightweight stubs so that UI code that
# checks `if FIELD_MAPPINGS` or calls `get_field_mappings(region)` doesn't
# crash; the returned dict is empty, which causes the UI to fall back to the
# Excel-template path (which is exactly what mapping_v1 uses).

FIELD_MAPPINGS: dict = {}


def get_field_mappings(region: str = "US") -> dict:
    """
    Stub — mapping_v1 does not use a static field registry.
    Returns an empty dict, which causes callers to fall back to the
    Excel-template loading path (load_bref_fields).
    """
    return {}


# ---------------------------------------------------------------------------
# Result shape conversion helpers
# ---------------------------------------------------------------------------

_STATUS_TO_METHOD = {
    "mapped":          "rule1_direct_match",
    "mapped_alias":    "rule3_alias_match",
    "mapped_derived":  "rule3_alias_match",   # closest old equivalent
    "no_match":        "no_match",
    "ambiguous":       "no_match",
    "blank_reference": "blank_reference",
}

_STATUS_TO_CONFIDENCE = {
    "mapped":          "high",
    "mapped_alias":    "medium",
    "mapped_derived":  "medium",
    "no_match":        "low",
    "ambiguous":       "low",
    "blank_reference": "low",
}


def _to_old_shape(result: dict) -> dict:
    """
    Convert a mapping_v1 result dict to the old src.mapping result shape
    that the UI components expect.
    """
    status      = result.get("status", "no_match")
    v1_conf     = result.get("confidence")                    # LLM confidence (Pass 2/3)
    final_conf  = result.get("final_confidence")              # set by validator
    map_conf    = v1_conf or _STATUS_TO_CONFIDENCE.get(status, "low")
    fin_conf    = final_conf or map_conf

    val_status  = result.get("validation_status", "unverified")

    return {
        # Core identity
        "label":                     result.get("label", ""),
        "description":               result.get("description", ""),
        # Values
        "reference_value":           result.get("reference_value"),
        "target_value":              result.get("target_value"),
        "extracted_reference_value": result.get("extracted_ref_value"),   # key rename
        "year_values":               result.get("year_values", {}),
        # Confidence / validation
        "mapping_confidence":        map_conf,
        "final_confidence":          fin_conf,
        "validation_status":         val_status,
        # Method
        "mapping_method":            _STATUS_TO_METHOD.get("status", "no_match"),
        "status":                    status,
        # Match info
        "matched_label":             result.get("matched_label"),
        "reason":                    result.get("reason", ""),
        # Pass-3 extras
        "formula":                   result.get("formula"),
        "components":                result.get("components"),
        "sign_flipped":              result.get("sign_flipped"),
        "years_verified":            result.get("years_verified"),
        "sign_corrected":            result.get("sign_corrected", False),
        # Calculated field flag (for bref_direct mode)
        "is_calculated":             result.get("is_calculated", False),
        # Pass number for debugging
        "pass":                      result.get("pass", 0),
    }

# ---------------------------------------------------------------------------
# load_bref_fields — adapter
# ---------------------------------------------------------------------------

def load_bref_fields(
    excel_path: str,
    sheet_name: str,
    target_year: int,
    field_mappings: dict | None = None,
    ignore_extract_column: bool = False,
) -> list[dict]:
    """
    Adapter for old load_bref_fields signature.

    Old callers pass:
        sheet_name  — a raw sheet name ("Input - Income Statement") or
                      a statement-type key ("income_statement")
        field_mappings — ignored in v1 (fields are loaded from the template)
        ignore_extract_column — ignored in v1 (always loads all valid rows)

    Internally resolves to a statement_type and calls mapping_v1's load.
    Returns fields in the old dict shape (adds "description" key).
    """
    # Reverse-map sheet name → statement_type
    stmt_type = _sheet_to_statement_type(sheet_name)

    fields = _load_bref_fields_v1(excel_path, stmt_type, target_year)

    # Add "description" key expected by old callers (empty — v1 has no aliases)
    for f in fields:
        f.setdefault("description", "")

    return fields


def _sheet_to_statement_type(sheet_name_or_type: str) -> str:
    """
    Convert either a statement-type key or a raw sheet name to the
    mapping_v1 statement_type key.
    """
    # Already a statement_type key
    if sheet_name_or_type in STATEMENT_SHEET_MAP:
        return sheet_name_or_type

    # Reverse lookup from sheet name
    for stmt_type, sheets in STATEMENT_SHEET_MAP.items():
        if sheet_name_or_type in sheets:
            return stmt_type

    # Fallback: try to infer from string content
    lower = sheet_name_or_type.lower()
    if "income" in lower or "profit" in lower or "earnings" in lower:
        return "income_statement"
    if "asset" in lower or "liabilit" in lower or "balance" in lower:
        return "balance_sheet"
    if "cash" in lower or "flow" in lower:
        return "cash_flow"

    # Last resort — return as-is and let v1 raise a clear error
    return sheet_name_or_type


# ---------------------------------------------------------------------------
# map_all_fields — adapter (replaces old mapper.map_all_fields)
# ---------------------------------------------------------------------------

def map_all_fields(
    fields: list[dict],
    extracted_rows: list[dict],
    company_name: str = "",
    target_year: int | None = None,
    provider: str | None = None,
    model: str | None = None,
    already_matched_labels: set | None = None,
) -> list[dict]:
    """
    Adapter for old map_all_fields signature.

    Routes to mapping_v1's three-pass map_fields(), converts results to the
    old dict shape, and returns them in the same order as `fields`.

    Notes:
    - `company_name` is accepted but not used (v1 prompts don't need it).
    - `already_matched_labels` seeds the duplicate guard in v1.
    - If `target_year` is None, auto-detects from extraction row column names.
    """
    if not fields or not extracted_rows:
        return fields

    # Auto-detect target_year from extraction data if not provided
    if target_year is None:
        target_year = _detect_target_year(extracted_rows)

    if target_year is None:
        print("  ⚠️  map_all_fields: could not detect target_year — returning fields unchanged.")
        return fields

    results = _map_fields(
        fields          = fields,
        extraction_rows = extracted_rows,
        target_year     = target_year,
        provider        = provider,
        model           = model,
    )

    # Validate (adds validation_status + final_confidence)
    results = _validate_results(results)

    # Convert to old shape
    return [_to_old_shape(r) for r in results]


def _detect_target_year(rows: list[dict]) -> int | None:
    """Infer target year from column names — take the most recent 4-digit year."""
    years = set()
    if rows:
        for col in rows[0]:
            m = re.search(r"(20\d{2})", str(col))
            if m:
                years.add(int(m.group(1)))
    return max(years) if years else None


# ---------------------------------------------------------------------------
# fast_map_fields — adapter
# ---------------------------------------------------------------------------

def fast_map_fields(
    fields: list[dict],
    extraction_rows: list[dict],
    available_years: list[int] | None = None,
    field_mappings: dict | None = None,
    target_year: int | None = None,
) -> tuple[list[dict], list[dict]]:
    """
    Adapter for old fast_map_fields signature.

    In mapping_v1, Pass 1 (value-match) subsumes the fast-mapper role.
    This stub returns ([], fields) so that the caller proceeds directly
    to LLM mapping (map_all_fields), which runs the full three-pass pipeline.

    Returning all fields as "unmatched" is safe because map_all_fields will
    handle everything — the fast-mapper pre-pass is no longer needed.
    """
    print(
        f"  [adapter] fast_map_fields: skipping pre-pass — "
        f"all {len(fields)} fields forwarded to map_all_fields (three-pass pipeline)."
    )
    return [], fields


# ---------------------------------------------------------------------------
# validate_mappings — adapter (replaces old validator.validate_mappings)
# ---------------------------------------------------------------------------

def validate_mappings(mapped_fields: list[dict]) -> list[dict]:
    """
    Adapter for old validate_mappings signature.

    If fields are already in the old shape (output of map_all_fields adapter),
    validation_status and final_confidence are already set — this is a no-op
    that simply returns the list unchanged.

    If called on raw v1 result dicts (unusual), runs _validate_results first.
    """
    if not mapped_fields:
        return mapped_fields

    # Detect whether validation already ran (old shape has "mapping_method")
    if "mapping_method" in mapped_fields[0]:
        # Already in old shape — validation already happened inside map_all_fields
        return mapped_fields

    # Raw v1 dicts — validate then convert
    validated = _validate_results(mapped_fields)
    return [_to_old_shape(r) for r in validated]


# ---------------------------------------------------------------------------
# __all__
# ---------------------------------------------------------------------------

__all__ = [
    "map_all_fields",
    "validate_mappings",
    "FIELD_MAPPINGS",
    "get_field_mappings",
    "load_bref_fields",
    "create_clean_output_excel",
    "STATEMENT_SHEET_MAP",
    "fast_map_fields",
    "apply_sign_corrections",
]
