"""
BREF Sign Correction Module
============================

Implements sign correction logic for BREF mapping by comparing reference year values
between BREF template and extraction results.

Logic:
------
For each field in BREF mapping:
1. Compare reference year value from BREF template with extraction results
2. If absolute values match but signs differ → sign inversion detected
3. For target year: invert the sign of extraction value to match BREF convention
4. For reference year display: always show BREF template value (as-is)

Example:
--------
BREF Template (2023): 35
Extraction (2023): -35
→ Sign inversion detected (absolute values match: |35| = |-35|)
→ For 2024 extraction value: invert sign (if extraction shows -50, use +50)
→ For 2023 display: show 35 (from BREF template, not from extraction)

Usage:
------
from src.mapping_v1.sign_correction import apply_sign_correction_logic

corrected_results = apply_sign_correction_logic(
    mapping_results=results,
    ref_year=2023,
    target_year=2024,
    tolerance=0.01  # 1% tolerance for value matching
)
"""

from typing import Any, Dict, List, Optional
import math


def _to_float(value: Any) -> Optional[float]:
    """Convert a value to float, return None if not numeric."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.replace(",", "").strip())
        except ValueError:
            return None
    return None


def _values_match_absolute(a: Any, b: Any, tolerance: float = 0.01) -> bool:
    """
    Check if two values match in absolute value within tolerance.
    
    Args:
        a: First value
        b: Second value
        tolerance: Relative tolerance (default 1%)
    
    Returns:
        True if |a| ≈ |b| within tolerance
    """
    try:
        fa, fb = abs(float(a)), abs(float(b))
        if fa == 0 and fb == 0:
            return True
        if fa == 0 or fb == 0:
            return False
        return abs(fa - fb) / max(fa, fb) <= tolerance
    except (TypeError, ValueError, ZeroDivisionError):
        return False


def _signs_differ(a: Any, b: Any) -> bool:
    """
    Check if two values have different signs.
    
    Args:
        a: First value
        b: Second value
    
    Returns:
        True if signs differ (one positive, one negative)
    """
    try:
        fa, fb = float(a), float(b)
        # Both zero → same sign
        if fa == 0 and fb == 0:
            return False
        # One zero, one non-zero → different signs
        if fa == 0 or fb == 0:
            return True
        # Check if signs differ
        return (fa > 0) != (fb > 0)
    except (TypeError, ValueError):
        return False


def detect_sign_inversion(
    bref_ref_value: Any,
    extraction_ref_value: Any,
    tolerance: float = 0.01
) -> bool:
    """
    Detect if sign inversion exists between BREF template and extraction.
    
    Sign inversion is detected when:
    1. Absolute values match within tolerance
    2. Signs differ (one positive, one negative)
    
    Args:
        bref_ref_value: Reference year value from BREF template
        extraction_ref_value: Reference year value from extraction results
        tolerance: Relative tolerance for value matching (default 1%)
    
    Returns:
        True if sign inversion detected, False otherwise
    
    Example:
        >>> detect_sign_inversion(35, -35)
        True
        >>> detect_sign_inversion(35, 35)
        False
        >>> detect_sign_inversion(35, -36, tolerance=0.03)
        True  # Within 3% tolerance
    """
    # Both values must be non-None
    if bref_ref_value is None or extraction_ref_value is None:
        return False
    
    # Convert to float
    bref_val = _to_float(bref_ref_value)
    extr_val = _to_float(extraction_ref_value)
    
    if bref_val is None or extr_val is None:
        return False
    
    # Check if absolute values match AND signs differ
    return _values_match_absolute(bref_val, extr_val, tolerance) and _signs_differ(bref_val, extr_val)


def apply_sign_correction(
    target_value: Any,
    should_invert: bool
) -> Optional[float]:
    """
    Apply sign correction to target year value.
    
    Args:
        target_value: Target year value from extraction
        should_invert: Whether to invert the sign
    
    Returns:
        Corrected value (inverted if should_invert=True)
    
    Example:
        >>> apply_sign_correction(-50, True)
        50.0
        >>> apply_sign_correction(-50, False)
        -50.0
    """
    if target_value is None:
        return None
    
    val = _to_float(target_value)
    if val is None:
        return None
    
    return -val if should_invert else val


def apply_sign_correction_logic(
    mapping_results: List[Dict[str, Any]],
    ref_year: int,
    target_year: int,
    tolerance: float = 0.01,
    verbose: bool = True
) -> List[Dict[str, Any]]:
    """
    Apply sign correction logic to BREF mapping results.
    
    For each field:
    1. Compare reference year values (BREF template vs extraction)
    2. Detect sign inversion (absolute values match but signs differ)
    3. If sign inversion detected:
       - Invert target year value from extraction
       - Mark field with sign_correction_applied=True
       - Store original extraction values for reference
    4. Always preserve BREF template reference year values for display
    
    Args:
        mapping_results: List of mapping result dicts from mapper
        ref_year: Reference year (e.g., 2023)
        target_year: Target year (e.g., 2024)
        tolerance: Relative tolerance for value matching (default 1%)
        verbose: Print correction details (default True)
    
    Returns:
        Updated mapping results with sign corrections applied
    
    Example:
        results = [
            {
                "label": "I30 | Sales",
                "reference_value": 35,  # From BREF template
                "extracted_ref_value": -35,  # From extraction
                "target_value": -50,  # From extraction
                "year_values": {"2023": -35, "2024": -50}
            }
        ]
        
        corrected = apply_sign_correction_logic(results, 2023, 2024)
        
        # Result:
        # {
        #     "label": "I30 | Sales",
        #     "reference_value": 35,  # Unchanged (from BREF template)
        #     "extracted_ref_value": -35,  # Original extraction value
        #     "target_value": 50,  # CORRECTED (inverted from -50)
        #     "year_values": {"2023": -35, "2024": -50},  # Original values
        #     "sign_correction_applied": True,
        #     "sign_correction_reason": "Sign inversion detected: BREF=35, Extraction=-35"
        # }
    """
    if verbose:
        print(f"\n{'='*80}")
        print(f"SIGN CORRECTION LOGIC")
        print(f"Reference Year: {ref_year} | Target Year: {target_year} | Tolerance: {tolerance*100}%")
        print(f"{'='*80}\n")
    
    corrections_applied = 0
    
    for field in mapping_results:
        label = field.get("label", "")
        
        # Get reference year values
        bref_ref_value = field.get("reference_value")  # From BREF template
        extracted_ref_value = field.get("extracted_ref_value")  # From extraction
        
        # Get target year value
        target_value = field.get("target_value")
        
        # Skip if missing required values
        if bref_ref_value is None or extracted_ref_value is None or target_value is None:
            continue
        
        # Detect sign inversion
        sign_inverted = detect_sign_inversion(bref_ref_value, extracted_ref_value, tolerance)
        
        if sign_inverted:
            # Apply sign correction to target year value
            original_target = target_value
            corrected_target = apply_sign_correction(target_value, should_invert=True)
            
            # Update field
            field["target_value"] = corrected_target
            field["sign_correction_applied"] = True
            field["sign_correction_reason"] = (
                f"Sign inversion detected: BREF={bref_ref_value}, Extraction={extracted_ref_value}"
            )
            field["original_target_value"] = original_target
            
            corrections_applied += 1
            
            if verbose:
                print(f"✓ {label}")
                print(f"  BREF Ref ({ref_year}): {bref_ref_value}")
                print(f"  Extraction Ref ({ref_year}): {extracted_ref_value}")
                print(f"  Target ({target_year}): {original_target} → {corrected_target} (INVERTED)")
                print()
    
    if verbose:
        print(f"{'='*80}")
        print(f"Sign corrections applied: {corrections_applied}/{len(mapping_results)} fields")
        print(f"{'='*80}\n")
    
    return mapping_results


def apply_sign_correction_to_all_years(
    mapping_results: List[Dict[str, Any]],
    ref_year: int,
    tolerance: float = 0.01,
    verbose: bool = True
) -> List[Dict[str, Any]]:
    """
    Apply sign correction to ALL year values in year_values dict.
    
    This is useful when you want to correct all historical values, not just target year.
    
    Args:
        mapping_results: List of mapping result dicts
        ref_year: Reference year for sign detection
        tolerance: Relative tolerance for value matching
        verbose: Print correction details
    
    Returns:
        Updated mapping results with all year values corrected
    """
    if verbose:
        print(f"\n{'='*80}")
        print(f"SIGN CORRECTION - ALL YEARS")
        print(f"Reference Year: {ref_year} | Tolerance: {tolerance*100}%")
        print(f"{'='*80}\n")
    
    corrections_applied = 0
    
    for field in mapping_results:
        label = field.get("label", "")
        
        # Get reference year values
        bref_ref_value = field.get("reference_value")
        extracted_ref_value = field.get("extracted_ref_value")
        
        # Get year_values dict
        year_values = field.get("year_values", {})
        
        # Skip if missing required values
        if bref_ref_value is None or extracted_ref_value is None or not year_values:
            continue
        
        # Detect sign inversion
        sign_inverted = detect_sign_inversion(bref_ref_value, extracted_ref_value, tolerance)
        
        if sign_inverted:
            # Invert ALL year values
            original_year_values = year_values.copy()
            corrected_year_values = {}
            
            for year, value in year_values.items():
                corrected_value = apply_sign_correction(value, should_invert=True)
                corrected_year_values[year] = corrected_value
            
            # Update field
            field["year_values"] = corrected_year_values
            field["sign_correction_applied"] = True
            field["sign_correction_reason"] = (
                f"Sign inversion detected: BREF={bref_ref_value}, Extraction={extracted_ref_value}"
            )
            field["original_year_values"] = original_year_values
            
            # Also update target_value if it exists
            if "target_value" in field and field["target_value"] is not None:
                field["original_target_value"] = field["target_value"]
                field["target_value"] = apply_sign_correction(field["target_value"], should_invert=True)
            
            corrections_applied += 1
            
            if verbose:
                print(f"✓ {label}")
                print(f"  BREF Ref ({ref_year}): {bref_ref_value}")
                print(f"  Extraction Ref ({ref_year}): {extracted_ref_value}")
                print(f"  Year values inverted:")
                for year in sorted(corrected_year_values.keys()):
                    orig = original_year_values.get(year)
                    corr = corrected_year_values.get(year)
                    print(f"    {year}: {orig} → {corr}")
                print()
    
    if verbose:
        print(f"{'='*80}")
        print(f"Sign corrections applied: {corrections_applied}/{len(mapping_results)} fields")
        print(f"{'='*80}\n")
    
    return mapping_results


def get_display_values(
    field: Dict[str, Any],
    ref_year: int,
    target_year: int,
    use_bref_for_ref: bool = True
) -> Dict[str, Any]:
    """
    Get display values for a field, respecting sign correction logic.
    
    Display rules:
    1. Reference year: Always show BREF template value (not extraction)
    2. Target year: Show corrected value (after sign inversion if applicable)
    
    Args:
        field: Mapping result dict
        ref_year: Reference year
        target_year: Target year
        use_bref_for_ref: Use BREF template value for reference year (default True)
    
    Returns:
        Dict with display values:
        {
            "ref_year_value": <value to display for reference year>,
            "target_year_value": <value to display for target year>,
            "sign_corrected": <bool>,
            "correction_reason": <str or None>
        }
    """
    # Reference year: use BREF template value
    ref_value = field.get("reference_value") if use_bref_for_ref else field.get("extracted_ref_value")
    
    # Target year: use corrected value
    target_value = field.get("target_value")
    
    # Sign correction info
    sign_corrected = field.get("sign_correction_applied", False)
    correction_reason = field.get("sign_correction_reason")
    
    return {
        "ref_year_value": ref_value,
        "target_year_value": target_value,
        "sign_corrected": sign_corrected,
        "correction_reason": correction_reason
    }


# ==============================================================================
# INTEGRATION WITH EXISTING PIPELINE
# ==============================================================================

def integrate_with_mapper_results(
    results: List[Dict[str, Any]],
    ref_year: int,
    target_year: int,
    apply_to_all_years: bool = False,
    tolerance: float = 0.01,
    verbose: bool = True
) -> List[Dict[str, Any]]:
    """
    Main integration function to apply sign correction to mapper results.
    
    This function should be called AFTER mapping (Passes 1-3) but BEFORE
    region-specific sign corrections (region_adjustments.py).
    
    Args:
        results: Mapping results from mapper.map_fields()
        ref_year: Reference year
        target_year: Target year
        apply_to_all_years: Apply correction to all year values (default False)
        tolerance: Relative tolerance for value matching
        verbose: Print correction details
    
    Returns:
        Updated results with sign corrections applied
    
    Usage in pipeline:
        # After mapping
        results = mapper.map_fields(...)
        
        # Apply sign correction logic
        results = integrate_with_mapper_results(
            results, ref_year=2023, target_year=2024
        )
        
        # Then apply region-specific corrections
        results = apply_sign_corrections(results, region="EMEA", ...)
    """
    if apply_to_all_years:
        return apply_sign_correction_to_all_years(
            results, ref_year, tolerance, verbose
        )
    else:
        return apply_sign_correction_logic(
            results, ref_year, target_year, tolerance, verbose
        )
