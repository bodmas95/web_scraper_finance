"""
BREF Field Mapper - Maps extracted financial data to BREF template fields
Integrated from bref-populator-latest/core/mapping/mapper.py
"""

import json
from src.extraction.extraction_config import LLM_MODEL
from src.extraction.llm_client import get_client, track_usage


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
) -> list:
    """
    Map all BREF fields to extracted rows in a single LLM call.
    Returns the enriched fields list.
    """
    reference_year = target_year - 1
    client = get_client()

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

    print(f"Mapping {len(fields)} fields in a single pass  (model: {LLM_MODEL})\n")

    stream = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        response_format={"type": "json_object"},
        stream=True,
        stream_options={"include_usage": True},
    )

    full_content = ""
    for chunk in stream:
        delta = chunk.choices[0].delta.content if chunk.choices else ""
        if delta:
            full_content += delta
            print(delta, end="", flush=True)
        if chunk.usage:
            track_usage(chunk)

    print("\n")
    mappings = json.loads(full_content).get("mappings", [])

    results = []
    for field, result in zip(fields, mappings):
        status = {"high": "[ok]", "medium": "[~]", "low": "[!]"}.get(
            result.get("confidence"), "[?]"
        )
        print(
            f"  {status} {field.get('label', '')}\n"
            f"      matched: {result.get('matched_label')} "
            f"| {target_year}: {result.get('target_value')} "
            f"| {result.get('confidence')}\n"
        )
        results.append({
            **field,
            "target_value":              result.get("target_value"),
            "matched_label":             result.get("matched_label"),
            "mapping_confidence":        result.get("confidence"),
            "reason":                    result.get("reason"),
            "extracted_reference_value": result.get("reference_value"),
        })

    return results
