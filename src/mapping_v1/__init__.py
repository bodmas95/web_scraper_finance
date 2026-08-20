"""
BREF Mapping v1 — Three-Pass LLM Mapping

  Pass 1 — Value Match (VLookup with LLM):
    Match BREF fields to extraction rows by comparing previous-year values
    within 1% tolerance. Exactly 1 match → "mapped". 0 or 2+ → Pass 2.

  Pass 2 — Alias Match:
    LLM matches by field name / known financial aliases against extraction
    row labels. Confident unique match → "mapped_alias". Still no → Pass 3.

  Pass 3 — Self-Reasoning (Derived Match):
    LLM figures it out from scratch using five rules:
      1. Identify which source row(s) produce the correct value.
      2. Verify derivation holds across ALL available previous years.
      3. Handle sign conventions (positive expense rows that need negating).
      4. List all components + arithmetic if it is a combination.
      5. State the final formula.
    Confident result → "mapped_derived". Nothing defensible → "no_match".

  Post-processing:
    - validate_results()     → adds validation_status + final_confidence
    - apply_sign_corrections() → enforces region-specific sign conventions

Entry point:
    from src.mapping_v1.pipeline import run
"""

from src.mapping_v1.pipeline          import run
from src.mapping_v1.mapper            import map_fields
from src.mapping_v1.excel             import load_bref_fields, write_results, create_clean_output_excel, STATEMENT_SHEET_MAP
from src.mapping_v1.validator         import validate_results
from src.mapping_v1.region_adjustments import apply_sign_corrections

__all__ = [
    "run",
    "map_fields",
    "load_bref_fields",
    "write_results",
    "create_clean_output_excel",
    "STATEMENT_SHEET_MAP",
    "validate_results",
    "apply_sign_corrections",
]
