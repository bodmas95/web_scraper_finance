"""
BREF Field Mapper - Maps extracted financial data to BREF template fields
Integrated from bref-populator-latest/core/mapping/mapper.py
"""

import json
import re
from decimal import Decimal, InvalidOperation
from src.extraction.extraction_config import LLM_MODEL
from src.extraction.llm_client import get_client, track_usage


def sanitize_number(value):
    """Sanitize numeric values to prevent excessive decimal places.
    
    Args:
        value: Numeric value (int, float, str, or None)
    
    Returns:
        Sanitized number (rounded to 4 decimal places) or None
    """
    if value is None:
        return None
    
    try:
        # Convert to float
        if isinstance(value, str):
            # Remove commas and whitespace
            value = value.replace(',', '').strip()
        
        num = float(value)
        
        # Round to 4 decimal places to prevent excessive precision
        # This prevents issues like 28987.000000000...000
        rounded = round(num, 4)
        
        # If it's effectively an integer, return as int
        if rounded == int(rounded):
            return int(rounded)
        
        return rounded
    
    except (ValueError, TypeError, InvalidOperation):
        return None


def clean_excessive_decimals(json_str: str) -> str:
    """Remove excessive trailing zeros from decimal numbers in JSON string.
    
    Args:
        json_str: JSON string that may contain numbers with excessive decimal places
    
    Returns:
        Cleaned JSON string with normalized decimal numbers
    """
    def replace_long_decimal(match):
        """Replace long decimal numbers with clean versions."""
        integer_part = match.group(1)
        decimal_part = match.group(2)
        
        # If decimal part is very long (more than 10 digits)
        if len(decimal_part) > 10:
            # Check if it's all zeros
            if decimal_part.strip('0') == '':
                return integer_part
            else:
                # Keep only first 4 decimal digits
                return f"{integer_part}.{decimal_part[:4]}"
        return match.group(0)
    
    # Match: integer_part . decimal_part (where decimal_part is 10+ digits)
    pattern = r'(\d+)\.(\d{10,})'
    cleaned = re.sub(pattern, replace_long_decimal, json_str)
    
    return cleaned


def rows_to_text(rows: list) -> str:
    """Convert extracted rows to text format for LLM"""
    if not rows:
        return "No data"
    
    text_lines = []
    for row in rows:
        if isinstance(row, dict):
            # Handle dict format (from PDF extraction)
            label = row.get('label', '')
            parent = row.get('parent', '')
                        # Get year columns (all keys except 'label', 'parent', and metadata)
            year_cols = {k: v for k, v in row.items() 
                        if k not in ['label', 'parent', 'parent_abstract_concept', 'Currency', 'Unit']}
            
            # CRITICAL: Format year columns clearly for LLM
            # Extract year from column name and format as "2024: value, 2025: value"
            import re
            year_values = {}
            for col_name, col_value in year_cols.items():
                # Extract year from column name (e.g., "2024 RMB (thousands)" -> "2024")
                year_match = re.search(r'(\d{4})', str(col_name))
                if year_match:
                    year = year_match.group(1)
                    year_values[year] = col_value
            
            # Sort years and format as "2024: value, 2025: value"
            if year_values:
                sorted_years = sorted(year_values.keys())
                values_str = ', '.join([f"{year}: {year_values[year]}" for year in sorted_years])
            else:
                # Fallback to original format if no years found
                values_str = ' | '.join([f"{k}: {v}" for k, v in year_cols.items()])
            
            if parent:
                text_lines.append(f"{parent} > {label}: {values_str}")
            else:
                text_lines.append(f"{label}: {values_str}")
        else:
            # Handle list format (legacy)
            text_lines.append(' | '.join(str(cell) for cell in row))
    
    return '\n'.join(text_lines)

MAPPING_PROMPT = """
You are a financial data analyst mapping BREF template fields to annual report data.

Company: {company_name}
Target year: {target_year}
Reference year: {reference_year}

Extracted rows from the annual report:
{extracted_rows}

{already_matched_note}

BREF fields to map (as a JSON array):
{fields_json}

Instructions:
1. For each field, find the SINGLE BEST MATCHING row from the extracted data.
2. CRITICAL: Do 1:1 mapping ONLY - match each BREF field to exactly ONE extraction row.
3. DO NOT sum multiple rows together - this is handled separately by calculated fields.
4. If the description mentions multiple items (e.g., "A + B"), ignore the "+" and find the best single match.
5. Prefer EXACT label matches over partial matches.
6. CRITICAL - EXTRACT TARGET YEAR VALUE: For each matched row, you MUST extract:
   - target_value: The value for year {target_year}
   - reference_value: The value for year {reference_year} (if available)
   Values are shown in the format "2024: value, 2025: value" in the extracted data.
   If a row shows "2024: 774367835, 2025: 812134650", then:
   - reference_value = 774367835 (year {reference_year})
   - target_value = 812134650 (year {target_year})
   NOTE: We will extract ALL other years programmatically after mapping, so focus on getting the correct matched_label.
7. Return one result object per field, in the same order as the input fields array.
6. CRITICAL NUMBER FORMATTING:
   - Return numbers as simple integers or decimals with MAXIMUM 2 decimal places
   - Examples of CORRECT formatting:
     * 28987 (integer)
     * 1234.56 (two decimals)
     * -500 (negative integer)
     * null (for missing values)
   - Examples of INCORRECT formatting (DO NOT USE):
     * 28987.0 (unnecessary decimal)
     * 28987.00 (unnecessary zeros)
     * 28987.0000000000...000 (NEVER use excessive zeros)
   - If a number is a whole number (like 28987), write it WITHOUT a decimal point
   - Use null for missing values, NEVER use 0 unless the actual value is zero

Respond in this exact JSON format:
{{
  "mappings": [
    {{
      "field_label":     "BREF field label (e.g., 'Q35 | Income tax')",
      "matched_label":   "exact label(s) from extracted rows, comma-separated if multiple",
      "target_value":    <number or null>,
      "reference_value": <number or null>,
      "confidence":      "high | medium | low",
      "reason":          "one sentence explaining the match or sum"
    }}
  ]
}}

Only respond with valid JSON — no other text.
"""


def map_all_fields(
    fields: list,
    extracted_rows: list,
    company_name: str,
    target_year: int,
    provider: str = None,
    model: str = None,
    already_matched_labels: set = None,
) -> list:
    """
    Map all BREF fields to extracted rows, using batching if needed.
    Returns the enriched fields list.
    
    NEW BEHAVIOR (Improved Sequence):
    1. Filter out fields with empty reference_value (will be calculated later)
    2. Filter out calculated fields (marked with * or is_calculated=True)
    3. Map only non-empty, non-calculated fields via LLM
    4. Return results preserving original field order
    
    Args:
        fields: List of BREF fields to map
        extracted_rows: Extracted data from PDF
        company_name: Company name
        target_year: Target year for extraction
        provider: LLM provider ("maia" or "openai"/"gemma")
        model: Model ID to use
        already_matched_labels: Set of labels already matched (to avoid duplicates)
    """
    reference_year = target_year - 1
    
    # STEP 1: Separate fields into categories
    non_empty_fields = []      # Fields with reference_value (to be mapped)
    empty_fields = []          # Fields without reference_value (to be calculated)
    calculated_fields = []     # Fields with formulas (to be calculated)
    
    for field in fields:
        field_label = field.get('label', '')
        is_calculated = field.get('is_calculated', False) or field_label.startswith('*')
        reference_value = field.get('reference_value')
        
        if is_calculated:
            calculated_fields.append(field)
            print(f"  ⏭️ Calculated field (will calculate later): {field_label}")
        elif reference_value is None:
            # Truly None - empty field
            empty_fields.append(field)
            print(f"  📭 Empty field (None): {field_label}")
        elif isinstance(reference_value, str) and reference_value.strip() == '':
            # Empty string - empty field
            empty_fields.append(field)
            print(f"  📭 Empty field (empty string): {field_label}")
        elif isinstance(reference_value, (int, float)) and reference_value == 0:
            # Zero is a VALID value, not empty!
            non_empty_fields.append(field)
            print(f"  ✅ Non-empty field (value=0): {field_label}")
        else:
            # Has a value
            non_empty_fields.append(field)
    
    print(f"\n📊 Field categorization:")
    print(f"  • {len(non_empty_fields)} non-empty fields (will map via LLM)")
    print(f"  • {len(empty_fields)} empty fields (will calculate from formulas)")
    print(f"  • {len(calculated_fields)} calculated fields (will calculate from formulas)")
    print(f"  • Total: {len(fields)} fields\n")
    
        # DEBUG: Show first few fields of each category with their reference values
    if non_empty_fields:
        print(f"  First 3 non-empty fields:")
        for f in non_empty_fields[:3]:
            ref_val = f.get('reference_value')
            print(f"    - {f.get('label')}: {ref_val} (type: {type(ref_val).__name__})")
    if empty_fields:
        print(f"  First 3 empty fields:")
        for f in empty_fields[:3]:
            ref_val = f.get('reference_value')
            print(f"    - {f.get('label')}: {ref_val} (type: {type(ref_val).__name__})")
    if calculated_fields:
        print(f"  First 3 calculated fields: {[f.get('label') for f in calculated_fields[:3]]}")
    print()
    
    # STEP 2: Map only non-empty fields
    if not non_empty_fields:
        print("⚠️ No non-empty fields to map - all fields are empty or calculated")
        print(f"  DEBUG: Total fields received: {len(fields)}")
        print(f"  DEBUG: Empty fields: {len(empty_fields)}")
        print(f"  DEBUG: Calculated fields: {len(calculated_fields)}")
        if fields:
            print(f"  DEBUG: First field: {fields[0]}")
                # Return all fields as unmapped
        return fields
    
    # Map non-empty fields using existing logic
    mapped_non_empty = _map_fields_batch(
        fields=non_empty_fields,
        extracted_rows=extracted_rows,
        company_name=company_name,
        target_year=target_year,
        provider=provider,
        model=model,
        already_matched_labels=already_matched_labels
    )
    
    # STEP 3: Add empty and calculated fields back (will be processed by bref_calculated.py)
    all_results = []
    
    # Create a dict for quick lookup of mapped results
    mapped_dict = {f.get('label'): f for f in mapped_non_empty}
    
    # Preserve original order from input fields
    for field in fields:
        label = field.get('label')
        if label in mapped_dict:
            # Use mapped result
            all_results.append(mapped_dict[label])
        else:
            # Empty or calculated field - add as unmapped
            all_results.append({
                **field,
                "target_value":              None,
                "matched_label":             None,
                "mapping_confidence":        "pending_calculation" if field.get('is_calculated') or label.startswith('*') else "empty",
                "reason":                    "Will be calculated from formula" if field.get('is_calculated') or label.startswith('*') else "Empty field in template",
                "extracted_reference_value": None,
            })
    
    return all_results


def _map_fields_batch_with_batching(
    fields: list,
    extracted_rows: list,
    company_name: str,
    target_year: int,
    provider: str = None,
    model: str = None,
    already_matched_labels: set = None,
) -> list:
    """
    Helper function to handle batching of large field lists.
    Splits into batches of 40 fields if needed.
    """
    # CRITICAL: If more than 40 fields, split into batches to avoid truncation
    BATCH_SIZE = 40  # Safe batch size that won't cause truncation
    
    if len(fields) > BATCH_SIZE:
        print(f"\n⚠️ {len(fields)} fields is too many for one LLM call")
        print(f"   Splitting into batches of {BATCH_SIZE} fields...\n")
        
        all_results = []
        num_batches = (len(fields) + BATCH_SIZE - 1) // BATCH_SIZE
        
        for batch_idx in range(num_batches):
            start_idx = batch_idx * BATCH_SIZE
            end_idx = min((batch_idx + 1) * BATCH_SIZE, len(fields))
            batch_fields = fields[start_idx:end_idx]
            
            print(f"\n📦 Batch {batch_idx + 1}/{num_batches}: Mapping fields {start_idx + 1}-{end_idx}...\n")
            
            batch_results = _map_fields_batch(
                fields=batch_fields,
                extracted_rows=extracted_rows,
                company_name=company_name,
                target_year=target_year,
                provider=provider,
                model=model,
                already_matched_labels=already_matched_labels
            )
            
            all_results.extend(batch_results)
            
            # Update already_matched_labels with new matches from this batch
            if already_matched_labels is not None:
                for result in batch_results:
                    matched_label = result.get('matched_label')
                    if matched_label and matched_label != '—':
                        already_matched_labels.add(matched_label)
        
        return all_results
    else:
        # Small enough to map in one call
        return _map_fields_batch(
            fields=fields,
            extracted_rows=extracted_rows,
            company_name=company_name,
            target_year=target_year,
            provider=provider,
            model=model,
            already_matched_labels=already_matched_labels
        )


def _map_fields_batch(
    fields: list,
    extracted_rows: list,
    company_name: str,
    target_year: int,
    provider: str = None,
    model: str = None,
    already_matched_labels: set = None,
) -> list:
    """
    Map a batch of BREF fields to extracted rows in a single LLM call.
    Internal function - use map_all_fields() instead.
    
    NOTE: This function assumes fields have already been filtered
    (no calculated fields, no empty fields) by map_all_fields().
    
    Args:
        fields: List of BREF fields to map (should be <= 40 fields, non-calculated, non-empty)
        extracted_rows: Extracted data from PDF
        company_name: Company name
        target_year: Target year for extraction
        provider: LLM provider
        model: Model ID to use
        already_matched_labels: Set of labels already matched
    """
    reference_year = target_year - 1
    
    if not fields:
        print("⚠️ No fields to map")
        return []
    
    client = get_client(provider=provider, model=model)

    # Prepare fields for LLM
    fields_json = json.dumps(
        [{"label": f.get("label", ""), "description": f.get("description", ""),
          "reference_value": f.get("reference_value")} for f in fields],
        indent=2,
    )

    # Build note about already-matched labels
    already_matched_note = ""
    if already_matched_labels:
        matched_list = list(already_matched_labels)[:10]  # Show first 10
        already_matched_note = f"""IMPORTANT: The following extraction labels have ALREADY been matched to other fields.
DO NOT use these labels again (find different matches):
{', '.join(matched_list)}
"""
    
    prompt = MAPPING_PROMPT.format(
        company_name=company_name,
        target_year=target_year,
        reference_year=reference_year,
        extracted_rows=rows_to_text(extracted_rows),
        already_matched_note=already_matched_note,
        fields_json=fields_json,
    )

    _model = model or LLM_MODEL
    print(f"Mapping {len(fields)} fields in a single pass  (model: {_model})\n")
    print(f"⏳ Waiting for LLM response... (this may take 30-60 seconds)\n")

    stream = client.chat.completions.create(
        model=_model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        response_format={"type": "json_object"},
        stream=True,
        stream_options={"include_usage": True},
    )

    full_content = ""
    chunk_count = 0
    last_progress_time = 0
    import time
    import sys
    
    for chunk in stream:
        delta = chunk.choices[0].delta.content if chunk.choices else ""
        if delta:
            full_content += delta
            print(delta, end="", flush=True)
            chunk_count += 1
            
            # CRITICAL: Send progress updates every 2 seconds to keep WebSocket alive
            # Reduced from 5 to 2 seconds for more aggressive keep-alive
            # This prevents "tornado.websocket.WebSocketClosedError" during long operations
            current_time = time.time()
            if current_time - last_progress_time >= 2:
                # Force flush to ensure message reaches Streamlit immediately
                progress_msg = f"\n[⏳ Mapping in progress... {len(full_content)} chars received, {chunk_count} chunks processed]\n"
                print(progress_msg, flush=True)
                sys.stdout.flush()  # Extra flush to ensure it reaches Streamlit
                last_progress_time = current_time
        if chunk.usage:
            track_usage(chunk)

    print(f"\n\n Received complete response ({len(full_content)} chars, {chunk_count} chunks)\n")
    sys.stdout.flush()
    
    # Clean up excessive decimal places in the JSON string before parsing
    # This prevents issues with numbers like 28987.000000...000
    full_content = clean_excessive_decimals(full_content)
    
    # Debug: Print response info
    print("\n" + "="*80)
    print(f"RAW LLM RESPONSE (Length: {len(full_content)} chars):")
    print("="*80)
    print(full_content[:1000])  # Print first 1000 chars
    if len(full_content) > 1000:
        print("\n... [truncated] ...\n")
        print(full_content[-500:])  # Print last 500 chars to see the end
    print("="*80 + "\n")
    
    # Try to parse JSON with better error handling
    try:
        response_data = json.loads(full_content)
        mappings = response_data.get("mappings", [])
        
        # CRITICAL: Sanitize all numeric values in mappings immediately after parsing
        # This is a safety net in case the regex cleanup missed something
        for mapping in mappings:
            if 'target_value' in mapping:
                mapping['target_value'] = sanitize_number(mapping['target_value'])
            if 'reference_value' in mapping:
                mapping['reference_value'] = sanitize_number(mapping['reference_value'])
    except json.JSONDecodeError as e:
        print(f"\n JSON PARSING ERROR: {e}")
        print(f"Error at position {e.pos}")
        
        # Show context around the error
        error_start = max(0, e.pos - 200)
        error_end = min(len(full_content), e.pos + 200)
        print(f"\nContext around error position {e.pos}:")
        print("-" * 80)
        print(full_content[error_start:error_end])
        print("-" * 80)
        
        # Try to extract JSON from markdown code blocks
        if "```json" in full_content:
            print("\nAttempting to extract JSON from markdown code block...")
            import re
            json_match = re.search(r'```json\s*(.+?)\s*```', full_content, re.DOTALL)
            if json_match:
                try:
                    response_data = json.loads(json_match.group(1))
                    mappings = response_data.get("mappings", [])
                    print(" Successfully extracted JSON from code block")
                except json.JSONDecodeError as e2:
                    print(f" Failed to parse JSON even from code block: {e2}")
                    raise
            else:
                print(" No JSON code block found")
                raise
        else:
            # Try to fix common JSON issues
            print("\nAttempting to fix common JSON issues...")
            
            # Check if response is truncated (missing closing braces)
            if not full_content.rstrip().endswith('}'):
                print(" Response appears truncated (missing closing brace)")
                print("This usually means the LLM response was cut off.")
                print("Try reducing the number of fields or using a model with larger context.")
            
            # Try to salvage partial JSON
            try:
                # Find the last complete mapping entry
                import re
                # Find all complete mapping objects
                pattern = r'\{\s*"matched_label".*?"reason":\s*"[^"]*"\s*\}'
                matches = list(re.finditer(pattern, full_content, re.DOTALL))
                
                if matches:
                    print(f"Found {len(matches)} complete mapping entries")
                    # Reconstruct JSON with complete entries only
                    complete_mappings = [full_content[m.start():m.end()] for m in matches]
                    reconstructed = '{"mappings": [' + ','.join(complete_mappings) + ']}'
                    
                    try:
                        response_data = json.loads(reconstructed)
                        mappings = response_data.get("mappings", [])
                        print(f" Successfully reconstructed JSON with {len(mappings)} mappings")
                        print(f" Warning: {len(fields) - len(mappings)} fields were lost due to truncation")
                    except json.JSONDecodeError:
                        print(" Failed to reconstruct JSON")
                        raise
                else:
                    print(" No complete mapping entries found")
                    raise
            except Exception as fix_error:
                print(f" Failed to fix JSON: {fix_error}")
                raise e  # Raise original error

    # CRITICAL FIX: Match mappings to fields by label, not by position
    # This prevents Q35's value from going to Q102 when LLM reorders results
    mapping_dict = {}
    for result in mappings:
        field_label = result.get("field_label", "")
        if field_label:
            mapping_dict[field_label] = result
    
    # If LLM didn't include field_label, fall back to position-based matching
    # but warn the user
    if not mapping_dict:
        print("\n⚠️ WARNING: LLM did not include field_label in response")
        print("   Falling back to position-based matching (may cause misalignment)\n")
        mapping_dict = {fields[i].get('label'): mappings[i] for i in range(min(len(fields), len(mappings)))}
    
    # Process fields with LLM results
    # CRITICAL: Track matched labels to prevent duplicates
    llm_matched_labels = set()
    if already_matched_labels:
        llm_matched_labels = already_matched_labels.copy()
    
    results = []
    for field in fields:
        field_label = field.get('label', '')
        result = mapping_dict.get(field_label, {})
        
        matched_label = result.get("matched_label")
        
        # CRITICAL: Check for duplicate matches
        if matched_label and matched_label in llm_matched_labels:
            print(f"  ⚠️ DUPLICATE DETECTED: {field_label} -> {matched_label} (already matched)")
            print(f"      Marking as unmapped to prevent duplicate values\n")
            # Mark as unmapped instead of using duplicate
            results.append({
                **field,
                "target_value":              None,
                "matched_label":             "—",
                "mapping_confidence":        "low",
                "reason":                    f"Duplicate match prevented: {matched_label} already matched to another field",
                "extracted_reference_value": None,
            })
            continue
        
        # Add to matched labels set
        if matched_label:
            llm_matched_labels.add(matched_label)
        
        # Sanitize numeric values to prevent excessive decimal places
        target_value = sanitize_number(result.get("target_value"))
        reference_value = sanitize_number(result.get("reference_value"))
        
        status = {"high": "[ok]", "medium": "[~]", "low": "[!]"}. get(
            result.get("confidence"), "[?]"
        )
        print(
            f"  {status} {field_label}\n"
            f"      matched: {result.get('matched_label')} "
            f"| {target_year}: {target_value} "
            f"| {result.get('confidence')}\n"
        )
                # CRITICAL FIX: Extract ALL year values from the matched row
        # This ensures 2024 and all other years are populated correctly
        year_values = {}
        if matched_label and matched_label != "—":
            # Find the matching row in extraction_rows
            for row in extracted_rows:
                if row.get("label", "").lower().strip() == matched_label.lower().strip():
                    # Extract all year columns from this row
                    import re
                    for col_name, col_value in row.items():
                        if col_name not in ['label', 'parent', 'parent_abstract_concept', 'Currency', 'Unit']:
                            # Extract year from column name
                            year_match = re.search(r'(\d{4})', str(col_name))
                            if year_match:
                                year = year_match.group(1)
                                if col_value is not None and str(col_value).strip() != "":
                                    try:
                                        clean_value = str(col_value).replace(',', '').strip()
                                        year_values[year] = float(clean_value)
                                    except (ValueError, TypeError):
                                        pass
                    break
        
        # If we extracted year_values, use them; otherwise use LLM values
        if year_values:
            # Use extracted year_values for all years
            final_target_value = year_values.get(str(target_year), target_value)
            final_reference_value = year_values.get(str(reference_year), reference_value)
        else:
            # Fallback to LLM values
            final_target_value = target_value
            final_reference_value = reference_value
            # Create year_values from LLM values
            if final_target_value is not None:
                year_values[str(target_year)] = final_target_value
            if final_reference_value is not None:
                year_values[str(reference_year)] = final_reference_value
        
                # CRITICAL FIX: For existing clients, preserve template's reference_value
        # Only set extracted_reference_value, don't overwrite reference_value from template
        result_dict = {
            **field,  # This preserves reference_value from template
            "target_value":              final_target_value,
            "matched_label":             matched_label,
            "mapping_confidence":        result.get("confidence", "low"),
            "reason":                    result.get("reason", "No mapping found"),
            "extracted_reference_value": final_reference_value,  # Store extracted value separately
            "year_values":               year_values,  # CRITICAL: Store all year values
        }
        
        # ONLY overwrite reference_value if template didn't have one (new client / raw mode)
        # For existing clients, template's reference_value should be preserved
        if field.get("reference_value") is None:
            result_dict["reference_value"] = final_reference_value
        
        results.append(result_dict)

    return results
