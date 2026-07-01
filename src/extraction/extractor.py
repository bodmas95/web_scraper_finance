import base64
import json
import logging

from .extraction_config import LLM_MODEL
from .llm_client import get_client, track_usage
from config.config import load_config

# Load config to check for Maia credentials
_cfg = load_config()
MAIA_CREDENTIALS = _cfg.get("LLM", "maia_credentials", fallback="")
MAIA_MODEL = _cfg.get("LLM", "maia_model", fallback="gpt-5.1-2025-11-13")

EXTRACTION_PROMPT = """
You are a financial data extraction specialist.

Below is raw text from a financial statement page in an annual report.
Extract every line item from the financial table as structured data.

Rules:
- Include all line items: revenues, expenses, subtotals, totals, per-share figures.
- Do NOT include page headers, footnotes, page numbers, or unit notes.
- Do NOT output heading rows — instead, track headings as context and attach them to data rows.
- Each data row must have a "label" and one value per year column found.
- If a data row has one or more ancestor headings, include a "parent" field containing all ancestor headings joined by " > " (outermost to innermost). Omit "parent" if the row has no ancestors.
- First identify the year column headers (e.g. 2024, 2023, 2022) from the table header row.
- The numbers in each data row appear LEFT TO RIGHT in the SAME ORDER as the year headers. The first number belongs to the first year, the second to the second year, and so on. Do not reorder them.
- Some labels are long and wrap across two lines in the PDF. In these cases the numeric values may be split: some appear beside the first line of the label and the rest beside the continuation line. Collect ALL numbers for that logical row and assign them in left-to-right order to the year columns. Do not treat the continuation line as a separate row.
- For negative values shown in parentheses like (1,234), return as -1234.
- Strip commas and currency symbols from numbers — return plain numbers.
- If a value is missing or blank for a year, use null.

Page text:
{page_text}

Respond in this exact JSON format:
{{
  "year_headers": ["2024", "2023", "2022"],
  "year_currencies": {{"2024": "USD", "2023": "USD", "2022": "USD"}},
  "unit_scale": "millions",
  "year_end_date": "December 31",
  "rows": [
    {{"label": "Net income", "2024": 1234, "2023": 1100, "2022": 980}},
    {{"parent": "CASH FLOWS FROM OPERATING ACTIVITIES > Adjustments to reconcile net income > Changes in operating assets and liabilities", "label": "Current assets", "2024": -382, "2023": 58, "2022": -1340}}
  ]
}}

IMPORTANT - Currency Extraction:
For "year_currencies": Look CAREFULLY for currency information in these places (in order of priority):
1. Table column headers (e.g., "2024 RMB", "US$ 2025", "IDR millions")
2. Table title or heading (e.g., "CONSOLIDATED STATEMENTS OF INCOME (in millions of US dollars)")
3. Unit notes above or below the table (e.g., "(in millions, except per share data)", "Amounts in RMB millions")
4. Page headers or footers

Examples of what to look for:
- "2024 RMB" or "RMB 2024" -> {{"2024": "RMB"}}
- "2025 US$" or "US$ 2025" or "USD" -> {{"2025": "USD"}}
- "IDR millions" in header -> {{"2024": "IDR", "2025": "IDR"}}
- "(in millions of dollars)" or "(in millions, except per share data)" -> {{"2024": "USD", "2023": "USD", "2022": "USD"}}
- "Amounts in RMB millions" -> {{"2024": "RMB", "2023": "RMB", "2022": "RMB"}}
- Look for currency codes: USD, US$, $, RMB, CNY, IDR, EUR, GBP, JPY, SGD, HKD, INR, THB, MYR, PHP, VND, etc.

If you find the currency ANYWHERE on the page, you MUST include it in year_currencies for all years. If truly no currency is mentioned anywhere, return empty dict {{}}.

For "unit_scale": extract ONLY the unit of measurement (e.g. "millions", "billions", "thousands"), NOT the currency. Return null if not stated.
For "year_end_date": extract the fiscal year-end month and day (e.g. "December 31", "March 31"). Return null if not stated.
Only respond with valid JSON — no other text.
"""

IMAGE_EXTRACTION_PROMPT = """
You are a financial data extraction specialist.

This is an image of a financial statement page from an annual report.
Extract every line item from the financial table as structured data.

Rules:
- Include all line items: revenues, expenses, subtotals, totals, per-share figures.
- Do NOT include page headers, footnotes, page numbers, or unit notes.
- Do NOT output heading rows — instead, track headings as context and attach them to data rows.
- Each data row must have a "label" and one value per year column found.
- If a data row has one or more ancestor headings, include a "parent" field containing all ancestor headings joined by " > " (outermost to innermost). Omit "parent" if the row has no ancestors.
- Year columns are identified from the column headers in the table.
- For negative values shown in parentheses like (1,234), return as -1234.
- Strip commas and currency symbols from numbers — return plain numbers.
- If a value is missing or blank for a year, use null.

Respond in this exact JSON format:
{
  "year_headers": ["2024", "2023", "2022"],
  "year_currencies": {"2024": "USD", "2023": "USD", "2022": "USD"},
  "unit_scale": "millions",
  "year_end_date": "December 31",
  "rows": [
    {"label": "Net income", "2024": 1234, "2023": 1100, "2022": 980},
    {"parent": "CASH FLOWS FROM OPERATING ACTIVITIES > Changes in operating assets and liabilities", "label": "Current assets", "2024": -382, "2023": 58, "2022": -1340}
  ]
}

IMPORTANT - Currency Extraction:
For "year_currencies": Look CAREFULLY for currency information in these places (in order of priority):
1. Table column headers (e.g., "2024 RMB", "US$ 2025", "IDR millions")
2. Table title or heading (e.g., "CONSOLIDATED STATEMENTS OF INCOME (in millions of US dollars)")
3. Unit notes above or below the table (e.g., "(in millions, except per share data)", "Amounts in RMB millions")
4. Page headers or footers

Examples of what to look for:
- "2024 RMB" or "RMB 2024" -> {"2024": "RMB"}
- "2025 US$" or "US$ 2025" or "USD" -> {"2025": "USD"}
- "IDR millions" in header -> {"2024": "IDR", "2025": "IDR"}
- "(in millions of dollars)" or "(in millions, except per share data)" -> {"2024": "USD", "2023": "USD", "2022": "USD"}
- "Amounts in RMB millions" -> {"2024": "RMB", "2023": "RMB", "2022": "RMB"}
- Look for currency codes: USD, US$, $, RMB, CNY, IDR, EUR, GBP, JPY, SGD, HKD, INR, THB, MYR, PHP, VND, etc.

If you find the currency ANYWHERE in the image, you MUST include it in year_currencies for all years. If truly no currency is mentioned anywhere, return empty dict {}.

For "unit_scale": extract ONLY the unit of measurement (e.g. "millions", "billions", "thousands"), NOT the currency. Return null if not stated.
For "year_end_date": extract the fiscal year-end month and day (e.g. "December 31", "March 31"). Return null if not stated.
Only respond with valid JSON — no other text.
"""


def _clean_extraction_result(result: dict) -> dict:
    """Clean extraction result by replacing None values with empty strings."""
    rows = result.get("rows", [])
    cleaned_rows = []
    for row in rows:
        if isinstance(row, dict):
            cleaned_row = {k: (v if v is not None else "") for k, v in row.items()}
            cleaned_rows.append(cleaned_row)
        else:
            cleaned_rows.append(row)
    
    result["rows"] = cleaned_rows
    return result


def extract_table(
    page_text: str,
    provider: str = None,
    model: str = None,
) -> dict:
    print("\n" + "#"*80)
    print("EXTRACT_TABLE CALLED - TEXT EXTRACTION")
    print("#"*80)
    
    if not page_text.strip():
        print("No text to extract from.")
        return {"rows": [], "year_headers": [], "total_rows": 0}

    _model = model or LLM_MODEL
    print(f"Calling LLM with model: {_model}")
    print(f"Page text length: {len(page_text)} characters")
    print("\nFull page text being sent to LLM:")
    print("-" * 80)
    print(page_text)
    print("-" * 80)
    print("#"*80)
    
    # Try primary LLM first, with fallback to MAIA
    try:
        client = get_client(provider=provider, model=model)
        response = client.chat.completions.create(
            model=_model,
            messages=[{"role": "user", "content": EXTRACTION_PROMPT.format(page_text=page_text)}],
            temperature=0,
            response_format={"type": "json_object"},
        )
        print("LLM response received!")
        track_usage(response)
        
    except Exception as e:
        logging.warning(f"Primary LLM failed during table extraction: {e}")
        
        # Fallback to MAIA if configured
        if MAIA_CREDENTIALS:
            logging.info("Falling back to MAIA API for table extraction...")
            print(f"  Primary LLM failed, retrying with MAIA API...")
            try:
                import os
                # CRITICAL: Clear ALL proxy environment variables for MAIA
                old_proxy_env = {}
                for var in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 
                           'ALL_PROXY', 'all_proxy', 'NO_PROXY', 'no_proxy']:
                    old_proxy_env[var] = os.environ.pop(var, None)
                
                try:
                    maia_client = get_client(provider="maia", model=MAIA_MODEL)
                    response = maia_client.chat.completions.create(
                        model=MAIA_MODEL,
                        messages=[{"role": "user", "content": EXTRACTION_PROMPT.format(page_text=page_text)}],
                        temperature=0,
                    )
                    print("MAIA LLM response received!")
                    track_usage(response)
                    logging.info("MAIA API fallback successful for table extraction")
                finally:
                    # Restore proxy environment variables
                    for var, val in old_proxy_env.items():
                        if val is not None:
                            os.environ[var] = val
            except Exception as maia_error:
                logging.error(f"MAIA API fallback also failed during table extraction: {maia_error}")
                raise Exception(f"Both primary LLM and MAIA fallback failed. Primary: {e}, MAIA: {maia_error}")
        else:
            logging.error("No MAIA credentials configured for fallback")
            raise Exception(f"Primary LLM failed and no MAIA fallback configured: {e}")
    
    # Parse LLM response
    raw_response = response.choices[0].message.content
    print("\n" + "="*80)
    print("LLM RAW RESPONSE:")
    print("="*80)
    print(raw_response)
    print("="*80 + "\n")
    
    # Try to parse JSON with error handling
    try:
        result = json.loads(raw_response)
    except json.JSONDecodeError as e:
        print(f"\n⚠️ JSON parsing error at line {e.lineno}, column {e.colno}: {e.msg}")
        print(f"Error position (char {e.pos}): ...{raw_response[max(0, e.pos-50):e.pos+50]}...")
        
        # Try to repair common JSON issues
        print("\nAttempting to repair JSON...")
        repaired = raw_response
        
        # Fix common issues:
        # 1. Remove trailing commas before closing braces/brackets
        import re
        repaired = re.sub(r',\s*}', '}', repaired)
        repaired = re.sub(r',\s*]', ']', repaired)
        
        # 2. Fix unescaped quotes in strings (basic attempt)
        # This is tricky - we'll try to escape quotes that appear to be in the middle of values
        
        # 3. Try parsing again
        try:
            result = json.loads(repaired)
            print("✅ JSON repaired successfully!")
        except json.JSONDecodeError as e2:
            print(f"❌ JSON repair failed. Error: {e2}")
            print("\nReturning empty result. Please check the LLM response above.")
            return {
                "rows": [],
                "year_headers": [],
                "year_currencies": {},
                "unit_scale": None,
                "year_end_date": None,
                "total_rows": 0,
                "error": f"JSON parsing failed: {str(e)}"
            }
    year_headers = result.get("year_headers", [])
    # Fix: Handle None values in label field
    rows         = [r for r in result.get("rows", []) if r.get("label") and str(r.get("label", "")).strip()]
    year_currencies = result.get("year_currencies", {})
    unit_scale   = result.get("unit_scale")
    year_end_date = result.get("year_end_date")

    print(f"\n[EXTRACTION SUMMARY]")
    print(f"  + Extracted {len(rows)} rows via LLM")
    print(f"  + Year columns: {year_headers}")
    print(f"  + Year currencies: {year_currencies if year_currencies else 'NOT EXTRACTED'}")
    print(f"  + Unit scale: {unit_scale if unit_scale else 'NOT EXTRACTED'}")
    print(f"  + Year-end date: {year_end_date if year_end_date else 'NOT EXTRACTED'}")
    print()

    result = {
        "rows":          rows,
        "year_headers":  year_headers,
        "year_currencies": year_currencies,
        "unit_scale":    unit_scale,
        "year_end_date": year_end_date,
        "total_rows":    len(rows),
    }
    
    # Clean None values (especially for GPT-5.1 which sometimes returns None)
    return _clean_extraction_result(result)


def extract_table_from_image(image_bytes: bytes, provider: str = None, model: str = None) -> dict:
    """Extract a financial table from a page image using LLM vision."""
    print("\n" + "#"*80)
    print("EXTRACT_TABLE_FROM_IMAGE CALLED - VISION EXTRACTION")
    print("#"*80)
    _model = model or LLM_MODEL
    print(f"Calling LLM with model: {_model}")
    print(f"Image size: {len(image_bytes)} bytes")
    print("#"*80)
    
    b64 = base64.b64encode(image_bytes).decode("utf-8")
    print(f"Image encoded to base64, length: {len(b64)} chars")

    # Try primary LLM first, with fallback to MAIA
    try:
        client = get_client(provider=provider, model=model)
        response = client.chat.completions.create(
            model=_model,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "text",      "text": IMAGE_EXTRACTION_PROMPT},
                    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64}"}},
                ],
            }],
            temperature=0,
            response_format={"type": "json_object"},
        )
        print("LLM response received (image)!")
        track_usage(response)
        
    except Exception as e:
        logging.warning(f"Primary LLM failed during image extraction: {e}")
        
        # Fallback to MAIA if configured
        if MAIA_CREDENTIALS:
            logging.info("Falling back to MAIA API for image extraction...")
            print(f"  Primary LLM failed, retrying with MAIA API...")
            try:
                import os
                # CRITICAL: Clear ALL proxy environment variables for MAIA
                old_proxy_env = {}
                for var in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 
                           'ALL_PROXY', 'all_proxy', 'NO_PROXY', 'no_proxy']:
                    old_proxy_env[var] = os.environ.pop(var, None)
                
                try:
                    maia_client = get_client(provider="maia", model=MAIA_MODEL)
                    response = maia_client.chat.completions.create(
                        model=MAIA_MODEL,
                        messages=[{
                            "role": "user",
                            "content": [
                                {"type": "text",      "text": IMAGE_EXTRACTION_PROMPT},
                                {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64}"}},
                            ],
                        }],
                        temperature=0,
                    )
                    print("MAIA LLM response received (image)!")
                    track_usage(response)
                    logging.info("MAIA API fallback successful for image extraction")
                finally:
                    # Restore proxy environment variables
                    for var, val in old_proxy_env.items():
                        if val is not None:
                            os.environ[var] = val
            except Exception as maia_error:
                logging.error(f"MAIA API fallback also failed during image extraction: {maia_error}")
                raise Exception(f"Both primary LLM and MAIA fallback failed. Primary: {e}, MAIA: {maia_error}")
        else:
            logging.error("No MAIA credentials configured for fallback")
            raise Exception(f"Primary LLM failed and no MAIA fallback configured: {e}")
    
    # Parse LLM response
    raw_response = response.choices[0].message.content
    print("\n" + "="*80)
    print("LLM RAW RESPONSE (IMAGE):")
    print("="*80)
    print(raw_response)
    print("="*80 + "\n")
    
    # Try to parse JSON with error handling
    try:
        result = json.loads(raw_response)
    except json.JSONDecodeError as e:
        print(f"\n⚠️ JSON parsing error at line {e.lineno}, column {e.colno}: {e.msg}")
        print(f"Error position (char {e.pos}): ...{raw_response[max(0, e.pos-50):e.pos+50]}...")
        
        # Try to repair common JSON issues
        print("\nAttempting to repair JSON...")
        repaired = raw_response
        
        # Fix common issues:
        # 1. Remove trailing commas before closing braces/brackets
        import re
        repaired = re.sub(r',\s*}', '}', repaired)
        repaired = re.sub(r',\s*]', ']', repaired)
        
        # 2. Try parsing again
        try:
            result = json.loads(repaired)
            print("✅ JSON repaired successfully!")
        except json.JSONDecodeError as e2:
            print(f"❌ JSON repair failed. Error: {e2}")
            print("\nReturning empty result. Please check the LLM response above.")
            return {
                "rows": [],
                "year_headers": [],
                "year_currencies": {},
                "unit_scale": None,
                "year_end_date": None,
                "total_rows": 0,
                "error": f"JSON parsing failed: {str(e)}"
            }
    year_headers  = result.get("year_headers", [])
        # Fix: Handle None values in label field
    rows         = [r for r in result.get("rows", []) if r.get("label") and str(r.get("label", "")).strip()]
    year_currencies = result.get("year_currencies", {})
    unit_scale    = result.get("unit_scale")
    year_end_date = result.get("year_end_date")
    
    print(f"\n[EXTRACTION SUMMARY (IMAGE)]")
    print(f"  + Extracted {len(rows)} rows via LLM")
    print(f"  + Year columns: {year_headers}")
    print(f"  + Year currencies: {year_currencies if year_currencies else 'NOT EXTRACTED'}")
    print(f"  + Unit scale: {unit_scale if unit_scale else 'NOT EXTRACTED'}")
    print(f"  + Year-end date: {year_end_date if year_end_date else 'NOT EXTRACTED'}")
    print()

    result = {
        "rows":          rows,
        "year_headers":  year_headers,
        "year_currencies": year_currencies,
        "unit_scale":    unit_scale,
        "year_end_date": year_end_date,
        "total_rows":    len(rows),
    }
    
    # Clean None values (especially for GPT-5.1 which sometimes returns None)
    return _clean_extraction_result(result)


def extract_table_with_vision_fallback(
    page_candidate: dict,
    pdf_path: str,
    stitch_fn=None,
    provider: str = None,
    model: str = None,
) -> dict:
    """Extract a financial table, auto-falling back to vision when text is garbled."""
    if page_candidate.get("text_garbled"):
        import fitz
        all_pnums = page_candidate.get("all_page_nums", [page_candidate["page_num"]])
        crop_bbox = page_candidate.get("landscape_crop_bbox")
        doc = fitz.open(pdf_path)
        page_imgs = []
        for pnum in all_pnums:
            fp = doc[pnum]
            if crop_bbox and fp.rect.width > fp.rect.height:
                px = fp.get_pixmap(dpi=150, clip=fitz.Rect(*crop_bbox))
            else:
                px = fp.get_pixmap(dpi=150)
            page_imgs.append(px.tobytes("png"))
        doc.close()
        if len(page_imgs) > 1 and stitch_fn:
            img_bytes = stitch_fn(page_imgs)
        else:
            img_bytes = page_imgs[0]
        print("  Auto-falling back to vision extraction (garbled/CID-encoded text detected)")
        return extract_table_from_image(img_bytes, provider=provider, model=model)
    else:
        return extract_table(page_candidate["full_text"], provider=provider, model=model)


def rows_to_text(rows: list) -> str:
    """Convert rows to readable text for the field mapper LLM prompt."""
    lines = []
    for row in rows:
        label     = row.get("label", "")
        parent    = row.get("parent", "")
        full_label = f"{parent} > {label}" if parent else label
        values    = {k: v for k, v in row.items() if k not in ("label", "parent") and v is not None}
        value_str = " | ".join(f"{k}: {v}" for k, v in values.items())
        if label:
            lines.append(f"{full_label}: {value_str}")
    return "\n".join(lines)
