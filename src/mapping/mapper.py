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
            # Get year columns (all keys except 'label' and 'parent')
            year_cols = {k: v for k, v in row.items() if k not in ['label', 'parent']}
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

BREF fields to map (as a JSON array):
{fields_json}

Instructions:
1. For each field, use its Description as the primary guide to find the matching row(s).
2. "under SECTION" in the description means the item appears within that section.
3. Multiple items separated by " + " must be summed to produce the final value.
4. Extract the {target_year} value and, for validation, the {reference_year} value.
5. Return one result object per field, in the same order as the input fields array.
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
) -> list:
    """
    Map all BREF fields to extracted rows in a single LLM call.
    Returns the enriched fields list.
    
    Args:
        fields: List of BREF fields to map
        extracted_rows: Extracted data from PDF
        company_name: Company name
        target_year: Target year for extraction
        provider: LLM provider ("maia" or "openai"/"gemma")
        model: Model ID to use
    """
    reference_year = target_year - 1
    client = get_client(provider=provider, model=model)

    fields_json = json.dumps(
        [{"label": f.get("label", ""), "description": f.get("description", ""),
          "reference_value": f.get("reference_value")} for f in fields],
        indent=2,
    )

    prompt = MAPPING_PROMPT.format(
        company_name=company_name,
        target_year=target_year,
        reference_year=reference_year,
        extracted_rows=rows_to_text(extracted_rows),
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

    print(f"\n\n✅ Received complete response ({len(full_content)} chars, {chunk_count} chunks)\n")
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
        print(f"\n❌ JSON PARSING ERROR: {e}")
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
                    print("✅ Successfully extracted JSON from code block")
                except json.JSONDecodeError as e2:
                    print(f"❌ Failed to parse JSON even from code block: {e2}")
                    raise
            else:
                print("❌ No JSON code block found")
                raise
        else:
            # Try to fix common JSON issues
            print("\nAttempting to fix common JSON issues...")
            
            # Check if response is truncated (missing closing braces)
            if not full_content.rstrip().endswith('}'):
                print("⚠️ Response appears truncated (missing closing brace)")
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
                        print(f"✅ Successfully reconstructed JSON with {len(mappings)} mappings")
                        print(f"⚠️ Warning: {len(fields) - len(mappings)} fields were lost due to truncation")
                    except json.JSONDecodeError:
                        print("❌ Failed to reconstruct JSON")
                        raise
                else:
                    print("❌ No complete mapping entries found")
                    raise
            except Exception as fix_error:
                print(f"❌ Failed to fix JSON: {fix_error}")
                raise e  # Raise original error

    results = []
    for field, result in zip(fields, mappings):
        # Sanitize numeric values to prevent excessive decimal places
        target_value = sanitize_number(result.get("target_value"))
        reference_value = sanitize_number(result.get("reference_value"))
        
        status = {"high": "[ok]", "medium": "[~]", "low": "[!]"}.get(
            result.get("confidence"), "[?]"
        )
        print(
            f"  {status} {field.get('label', '')}\n"
            f"      matched: {result.get('matched_label')} "
            f"| {target_year}: {target_value} "
            f"| {result.get('confidence')}\n"
        )
        results.append({
            **field,
            "target_value":              target_value,
            "matched_label":             result.get("matched_label"),
            "mapping_confidence":        result.get("confidence"),
            "reason":                    result.get("reason"),
            "extracted_reference_value": reference_value,
        })

    return results
