"""
Rule-Based BREF Mapper for Existing Clients

This module implements a rule-based approach for mapping BREF templates with existing client data.
Instead of removing blank rows first, it applies intelligent matching rules based on reference year values.

RULES:
1. If reference year value exists → match with extraction results → populate target year
2. If reference year value is blank → keep target year blank (no matching)
3. If no direct match → try aliases from field_mappings.py
4. If alias doesn't match → try field combinations to derive values
5. Prevent duplicate mappings (each field mapped only once)
6. Perform calculations AFTER all mapping is complete
"""

import logging
from typing import Dict, List, Optional, Set, Tuple
import re

logger = logging.getLogger(__name__)


def apply_rule_based_mapping(
    fields: List[Dict],
    extraction_rows: List[Dict],
    field_mappings: Dict,
    target_year: int,
    available_years: List[int],
    company_name: str = "",
    region: str = "US",
    statement_type: str = "income_statement",
) -> Tuple[List[Dict], List[Dict]]:
    """
    Apply rule-based mapping for existing client BREF templates.
    
    This function implements the 6 rules for intelligent field mapping:
    1. Match reference year value with extraction → populate target year
    2. Blank reference year → keep target year blank
    3. No direct match → use aliases
    4. No alias match → try field combinations
    5. Prevent duplicate mappings
    6. Calculations done separately (after this function)
    
    Args:
        fields: List of BREF fields from template (with reference_value)
        extraction_rows: Extracted data from annual report
        field_mappings: Field mappings dictionary with aliases
        target_year: Target fiscal year
        available_years: List of all available years in extraction
        company_name: Company name (for logging)
    
    Returns:
        Tuple of (matched_fields, unmatched_fields)
        - matched_fields: Fields successfully mapped with values
        - unmatched_fields: Fields that need LLM mapping or calculation
    """
    
    logger.info(f"\n{'='*80}")
    logger.info(f"RULE-BASED MAPPING FOR EXISTING CLIENT")
    logger.info(f"{'='*80}")
    logger.info(f"  Company: {company_name}")
    logger.info(f"  Target year: {target_year}")
    logger.info(f"  Reference year: {target_year - 1}")
    logger.info(f"  Region: {region}")
    logger.info(f"  Total fields: {len(fields)}")
    logger.info(f"  Total extraction rows: {len(extraction_rows)}")
    logger.info(f"  Available years: {available_years}")
    
    # Get list of calculated fields that can fallback to direct matching
    from src.mapping.field_mappings import get_calculated_with_fallback
    calculated_with_fallback = get_calculated_with_fallback(region, statement_type)
    logger.info(f"  Statement type: {statement_type}")
    logger.info(f"  Calculated fields with fallback: {len(calculated_with_fallback)}")
    if calculated_with_fallback:
        logger.info(f"  Fallback fields: {calculated_with_fallback}")
    
    reference_year = target_year - 1
    
    # Build year-to-column mapping for extraction rows
    year_to_column = _build_year_to_column_mapping(extraction_rows, available_years)
    logger.info(f"  Year-to-column mapping: {year_to_column}")
    
    # Track matched labels to prevent duplicates (Rule 5)
    # Store lowercase versions for case-insensitive comparison
    matched_labels: Set[str] = set()
    
    # Results
    matched_fields = []
    unmatched_fields = []
    
    # Statistics
    stats = {
        "rule1_matched": 0,      # Direct match with reference value
        "rule2_blank": 0,        # Blank reference value → blank target
        "rule3_alias": 0,        # Matched via alias
        "rule4_combination": 0,  # Matched via field combination
        "rule5_duplicate": 0,    # Prevented duplicate
        "unmatched": 0,          # Needs LLM or calculation
    }
    
    logger.info(f"\n{'='*80}")
    logger.info(f"APPLYING MAPPING RULES")
    logger.info(f"{'='*80}\n")
    
    for idx, field in enumerate(fields, 1):
        field_label = field.get("label", "")
        reference_value = field.get("reference_value")
        is_calculated = field.get("is_calculated", False)
        
        # DEBUG for I23 and I24
        if "I23" in field_label or "I24" in field_label:
            print(f"\n{'='*80}")
            print(f"PROCESSING FIELD: {field_label}")
            print(f"  Reference value: {reference_value}")
            print(f"  Is calculated: {is_calculated}")
            print(f"  Template year values: {field.get('template_year_values', {})}")
            print(f"{'='*80}")
        
        logger.info(f"[{idx}/{len(fields)}] Processing: {field_label}")
        logger.info(f"  Reference value: {reference_value}")
        logger.info(f"  Is calculated: {is_calculated}")
        
        # Skip calculated fields UNLESS they're in the fallback list
        # (fields that can be matched directly if calculation fails)
        if is_calculated and field_label not in calculated_with_fallback:
            logger.info(f"  ⏭️ SKIP: Calculated field (will be computed later)")
            unmatched_fields.append(field)
            continue
        elif is_calculated and field_label in calculated_with_fallback:
            logger.info(f"  🔄 SPECIAL: Calculated field with fallback - will try direct matching")
            # Continue to try matching (don't skip)
        
        # RULE 2: If reference year is blank → keep target year blank
        if reference_value is None or (isinstance(reference_value, str) and reference_value.strip() == ""):
            logger.info(f"  📭 RULE 2: Reference value is blank → target will be blank")
            stats["rule2_blank"] += 1
            
            matched_fields.append({
                **field,
                "target_value": None,
                "matched_label": "—",
                "mapping_method": "blank_reference",
                "mapping_confidence": "low",  # CHANGED: Blank fields should be low confidence
                "final_confidence": "low",
                "validation_status": "unverified",  # CHANGED: Not validated, just blank
                "reason": "Reference year value is blank in template",
                "year_values": {},
            })
            continue
        
        # RULE 1: Try to match reference year value with extraction results
        match_result = _try_direct_match(
            field=field,
            extraction_rows=extraction_rows,
            reference_year=reference_year,
            target_year=target_year,
            year_to_column=year_to_column,
            matched_labels=matched_labels,
        )
        
        if match_result:
            # DEBUG for I23 and I24
            if "I23" in field_label or "I24" in field_label:
                print(f"\n[RULE 1 MATCHED]: {field_label}")
                print(f"  Matched to: {match_result.get('matched_label')}")
                print(f"  Target value: {match_result.get('target_value')}")
                print(f"  Reference value: {match_result.get('reference_value')}")
                print(f"  Year values: {match_result.get('year_values')}\n")
            logger.info(f"  ✅ RULE 1: Direct match found!")
            logger.info(f"     Matched label: {match_result['matched_label']}")
            logger.info(f"     Target value: {match_result.get('target_value')}")
            stats["rule1_matched"] += 1
            matched_fields.append(match_result)
            # Add lowercase version to prevent duplicates
            matched_labels.add(match_result["matched_label"].lower().strip())
            continue
        
        # RULE 3: Try to match using aliases from field_mappings.py
        alias_result = _try_alias_match(
            field=field,
            extraction_rows=extraction_rows,
            field_mappings=field_mappings,
            reference_year=reference_year,
            target_year=target_year,
            year_to_column=year_to_column,
            matched_labels=matched_labels,
        )
        
        if alias_result:
            # DEBUG for I23 and I24
            if "I23" in field_label or "I24" in field_label:
                print(f"\n[RULE 3 ALIAS MATCHED]: {field_label}")
                print(f"  Matched to: {alias_result.get('matched_label')}")
                print(f"  Target value: {alias_result.get('target_value')}")
                print(f"  Reference value: {alias_result.get('reference_value')}")
                print(f"  Year values: {alias_result.get('year_values')}\n")
            logger.info(f"  ✅ RULE 3: Alias match found!")
            logger.info(f"     Matched label: {alias_result['matched_label']}")
            logger.info(f"     Target value: {alias_result.get('target_value')}")
            stats["rule3_alias"] += 1
            matched_fields.append(alias_result)
            # Add lowercase version to prevent duplicates
            matched_labels.add(alias_result["matched_label"].lower().strip())
            continue
        
        # RULE 4: Try field combinations (if applicable)
        # This is a placeholder for future enhancement
        # For now, we'll mark it as unmatched and let LLM handle it
        
        logger.info(f"  ❌ NO MATCH: Will need LLM mapping")
        stats["unmatched"] += 1
        unmatched_fields.append(field)
    
    # Print statistics
    logger.info(f"\n{'='*80}")
    logger.info(f"RULE-BASED MAPPING STATISTICS")
    logger.info(f"{'='*80}")
    logger.info(f"  Rule 1 (Direct match):     {stats['rule1_matched']}")
    logger.info(f"  Rule 2 (Blank reference):  {stats['rule2_blank']}")
    logger.info(f"  Rule 3 (Alias match):      {stats['rule3_alias']}")
    logger.info(f"  Rule 4 (Combination):      {stats['rule4_combination']}")
    logger.info(f"  Rule 5 (Duplicate prevented): {stats['rule5_duplicate']}")
    logger.info(f"  Unmatched (needs LLM):     {stats['unmatched']}")
    logger.info(f"  Total matched:             {len(matched_fields)}")
    logger.info(f"  Total unmatched:           {len(unmatched_fields)}")
    logger.info(f"{'='*80}\n")
    
    return matched_fields, unmatched_fields


def _build_year_to_column_mapping(extraction_rows: List[Dict], available_years: List[int]) -> Dict[int, str]:
    """Build mapping from year to column name in extraction rows."""
    year_to_column = {}
    
    if not extraction_rows:
        return year_to_column
    
    first_row = extraction_rows[0]
    
    for col_name in first_row.keys():
        if col_name in ['label', 'parent', 'parent_abstract_concept', 'Currency', 'Unit']:
            continue
        
        # Extract year from column name (e.g., "2024 RMB (thousands)" -> 2024)
        year_match = re.search(r'(\d{4})', str(col_name))
        if year_match:
            year = int(year_match.group(1))
            if year in available_years:
                year_to_column[year] = col_name
    
    return year_to_column


def _try_direct_match(
    field: Dict,
    extraction_rows: List[Dict],
    reference_year: int,
    target_year: int,
    year_to_column: Dict[int, str],
    matched_labels: Set[str],
) -> Optional[Dict]:
    """
    RULE 1: Try to match field by comparing reference year value with extraction results.
    
    Returns matched field dict if successful, None otherwise.
    """
    
    field_label = field.get("label", "")
    reference_value = field.get("reference_value")
    
    if reference_value is None:
        return None
    
    # Convert reference value to float for comparison
    try:
        ref_val_float = float(reference_value)
    except (ValueError, TypeError):
        logger.warning(f"  Cannot convert reference value to float: {reference_value}")
        return None
    
    # Get reference year column name
    ref_year_col = year_to_column.get(reference_year)
    if not ref_year_col:
        logger.warning(f"  Reference year {reference_year} not found in extraction columns")
        return None
    
    # Search for matching row
    for row in extraction_rows:
        row_label = row.get("label", "")
        
        # Skip if already matched (Rule 5)
        if row_label.lower().strip() in matched_labels:
            continue
        
        # Get reference year value from extraction row
        row_ref_value = row.get(ref_year_col)
        
        if row_ref_value is None:
            continue
        
        # Convert to float for comparison
        try:
            row_ref_float = float(str(row_ref_value).replace(',', '').strip())
        except (ValueError, TypeError):
            continue
        
        # Check if values match (with 1% tolerance for rounding)
        # CRITICAL: Compare absolute values because expenses may be negative in extraction
        # but positive in template (or vice versa)
        tolerance = abs(ref_val_float) * 0.01
        
        # Try exact match first
        if abs(row_ref_float - ref_val_float) <= tolerance:
            # Direct match (same sign)
            pass
        elif abs(abs(row_ref_float) - abs(ref_val_float)) <= tolerance:
            # Match by absolute value (different signs)
            logger.info(f"     Note: Matched by absolute value (signs differ)")
            logger.info(f"     Template: {ref_val_float}, Extraction: {row_ref_float}")
        else:
            # No match
            continue
        
        # If we reach here, we have a match
        if True:
            logger.info(f"     Found match: {row_label}")
            logger.info(f"     Template ref value: {ref_val_float}")
            logger.info(f"     Extraction ref value: {row_ref_float}")
            
                        # Extract all year values
            year_values = _extract_year_values(row, year_to_column)
            
            # Get target year value
            target_value = year_values.get(str(target_year))
            
            # CRITICAL FIX: For existing clients, reference year should NOT be in year_values
            # Remove reference year from year_values to prevent it from overriding template value
            # The reference year value should ONLY come from the template's reference_value field
            year_values_without_ref = {k: v for k, v in year_values.items() if k != str(reference_year)}
            
            # DEBUG for I23 and I24
            if "I23" in field_label or "I24" in field_label:
                print(f"\n=== RULE1 MATCH: {field_label} ===")
                print(f"  Matched to row: {row_label}")
                print(f"  Reference year value: {ref_val_float}")
                print(f"  Target year value: {target_value}")
                print(f"  Year values (without ref): {year_values_without_ref}")
                print(f"================================\n")
            
            logger.info(f"     Removed reference year from year_values to preserve template value")
            
            return {
                **field,
                "target_value": target_value,
                "matched_label": row_label,
                "mapping_method": "rule1_direct_match",
                "mapping_confidence": "high",
                "final_confidence": "high",  # Add final_confidence for UI
                "validation_status": "validated",  # Mark as validated since matched by reference
                "reason": f"Matched by reference year value ({reference_year}: {ref_val_float})",
                "year_values": year_values_without_ref,  # Exclude reference year
            }
    
    return None


def _try_alias_match(
    field: Dict,
    extraction_rows: List[Dict],
    field_mappings: Dict,
    reference_year: int,
    target_year: int,
    year_to_column: Dict[int, str],
    matched_labels: Set[str],
) -> Optional[Dict]:
    """
    RULE 3: Try to match field using aliases from field_mappings.py.
    
    IMPORTANT: This only matches TARGET YEAR values, not reference year.
    Reference year values always come from the template and are never modified.
    
    Returns matched field dict if successful, None otherwise.
    """
    
    field_label = field.get("label", "")
    reference_value = field.get("reference_value")  # Keep from template
    
    # Get aliases for this field
    field_def = field_mappings.get(field_label, {})
    
    if isinstance(field_def, dict):
        aliases = field_def.get("aliases", [])
    elif isinstance(field_def, list):
        aliases = field_def
    else:
        aliases = []
    
    if not aliases:
        logger.info(f"     No aliases defined for {field_label}")
        return None
    
    logger.info(f"     Trying {len(aliases)} aliases: {aliases[:3]}...")
    
    # Try each alias
    for alias in aliases:
        alias_lower = alias.lower().strip()
        
        for row in extraction_rows:
            row_label = row.get("label", "")
            row_label_lower = row_label.lower().strip()
            
            # Skip if already matched (Rule 5)
            if row_label_lower in matched_labels:
                continue
            
            # Check if alias matches row label (fuzzy match)
            if alias_lower in row_label_lower or row_label_lower in alias_lower:
                logger.info(f"     Alias match: '{alias}' → '{row_label}'")
                
                                # Extract all year values
                year_values = _extract_year_values(row, year_to_column)
                
                # Get target year value (this is what we're matching)
                target_value = year_values.get(str(target_year))
                
                # CRITICAL FIX: For existing clients, reference year should NOT be in year_values
                # Remove reference year from year_values to prevent it from overriding template value
                year_values_without_ref = {k: v for k, v in year_values.items() if k != str(reference_year)}
                
                # CRITICAL: Reference year value stays from template, NOT from extraction
                # We only populate target_value, reference_value remains unchanged
                logger.info(f"     Target year ({target_year}) value: {target_value}")
                logger.info(f"     Reference year ({reference_year}) value: {reference_value} (from template, unchanged)")
                logger.info(f"     Removed reference year from year_values to preserve template value")
                
                return {
                    **field,
                    "target_value": target_value,
                    "reference_value": reference_value,  # Keep original from template
                    "matched_label": row_label,
                    "mapping_method": "rule3_alias_match",
                    "mapping_confidence": "low",  # CHANGED: Alias matches should be low confidence (need human review)
                    "final_confidence": "low",
                    "validation_status": "unverified",  # CHANGED: Alias matches need verification
                    "reason": f"Matched via alias: '{alias}' for target year {target_year}",
                    "year_values": year_values_without_ref,  # Exclude reference year
                }
    
    return None


def _extract_year_values(row: Dict, year_to_column: Dict[int, str]) -> Dict[str, float]:
    """Extract all year values from a row."""
    year_values = {}
    
    for year, col_name in year_to_column.items():
        if col_name in row:
            value = row[col_name]
            if value is not None and str(value).strip() != "":
                try:
                    clean_value = str(value).replace(',', '').strip()
                    year_values[str(year)] = float(clean_value)
                except (ValueError, TypeError):
                    pass
    
    return year_values


def integrate_rule_based_mapping_with_llm(
    fields: List[Dict],
    extraction_rows: List[Dict],
    field_mappings: Dict,
    target_year: int,
    available_years: List[int],
    company_name: str,
    provider: str,
    model: str,
) -> List[Dict]:
    """
    Integrate rule-based mapping with LLM mapping.
    
    This function:
    1. Applies rule-based mapping first (fast, deterministic)
    2. Uses LLM only for unmatched fields (slow, but necessary)
    3. Combines results and returns complete field list
    
    Args:
        fields: BREF fields from template
        extraction_rows: Extracted data
        field_mappings: Field mappings with aliases
        target_year: Target year
        available_years: Available years in extraction
        company_name: Company name
        provider: LLM provider
        model: LLM model
    
    Returns:
        Complete list of mapped fields
    """
    
    # Step 1: Apply rule-based mapping
    matched_fields, unmatched_fields = apply_rule_based_mapping(
        fields=fields,
        extraction_rows=extraction_rows,
        field_mappings=field_mappings,
        target_year=target_year,
        available_years=available_years,
        company_name=company_name,
    )
    
    # Step 2: Use LLM for unmatched fields (if any)
    if unmatched_fields:
        logger.info(f"\n{'='*80}")
        logger.info(f"LLM MAPPING FOR UNMATCHED FIELDS")
        logger.info(f"{'='*80}")
        logger.info(f"  {len(unmatched_fields)} fields need LLM mapping")
        
        from src.mapping.mapper import map_all_fields
        
        # Build set of already matched labels to prevent duplicates
        already_matched_labels = {f.get("matched_label") for f in matched_fields if f.get("matched_label") and f.get("matched_label") != "—"}
        
        llm_mapped_fields = map_all_fields(
            fields=unmatched_fields,
            extracted_rows=extraction_rows,
            company_name=company_name,
            target_year=target_year,
            provider=provider,
            model=model,
            already_matched_labels=already_matched_labels,
        )
        
        # Combine results
        all_fields = matched_fields + llm_mapped_fields
    else:
        logger.info(f"\n✅ All fields matched via rules - no LLM needed!")
        all_fields = matched_fields
    
    return all_fields
