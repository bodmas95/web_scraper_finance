import io

import openpyxl
from openpyxl.styles import Font

from src.mapping.config import (
    COL_CONFIDENCE,
    COL_DESC,
    COL_EXTRACT,
    COL_LABEL,
    COL_OUTPUT,
    COL_REF_VALUE,
    DATA_START_ROW,
    STATEMENT_SHEET_MAP,
)

__all__ = [
    'load_bref_fields',
    'write_output_values',
    'write_output_values_to_bytes',
    'create_clean_output_excel',
]


def _resolve_sheet(sheet_name_or_type: str) -> str:
    """Accept either a raw sheet name or a statement type key."""
    return STATEMENT_SHEET_MAP.get(sheet_name_or_type, sheet_name_or_type)


def _find_year_column(ws, year: int) -> int:
    """Find column containing specific year by searching row 1 headers"""
    for col in range(1, 30):  # Search first 30 columns
        header = ws.cell(1, col).value
        if header and str(year) in str(header):
            return col
    return None


def load_bref_fields(excel_path: str, sheet_name: str, target_year: int, field_mappings: dict = None, ignore_extract_column: bool = False) -> list:
    """
    Read a BREF Excel template and return all rows to process.

    For templates with Extract column (COL_EXTRACT): only processes rows with 'Yes'
    For templates without Extract column (like bref-validator.xlsx): processes all rows with labels

    Uses smart year detection: searches for target_year and reference_year in headers.
    Raises ValueError on sheet-not-found or missing reference values.

    Returns a list of dicts with keys:
        label, description, reference_value, row_num
    """
    reference_year = target_year - 1
    sheet_name = _resolve_sheet(sheet_name)

    wb = openpyxl.load_workbook(excel_path)
    if sheet_name not in wb.sheetnames:
        raise ValueError(
            f"Sheet '{sheet_name}' not found in workbook. "
            f"Available sheets: {wb.sheetnames}"
        )
    ws = wb[sheet_name]
    
    # Auto-detect template structure by checking column B header
    col_b_header = ws.cell(1, 2).value or ws.cell(2, 2).value
    has_extract_column = False
    if col_b_header and str(col_b_header).strip().lower() in ['extract', 'yes/no', 'y/n']:
        has_extract_column = True
        if ignore_extract_column:
            print(f"Detected Extract column in column B - but IGNORING it (loading all fields)")
        else:
            print(f"Detected Extract column in column B - using NEXTERA template structure")
        # Override config for NEXTERA template
        global COL_EXTRACT, COL_DESC
        COL_EXTRACT = 2  # B - Extract flag
        COL_DESC = 3     # C - Description
    else:
        print(f"No Extract column detected - using bref-validator template structure")
        # Use config as-is (both COL_EXTRACT and COL_DESC = 2)

    # Smart year column detection
    detected_ref_col = _find_year_column(ws, reference_year)
    detected_target_col = _find_year_column(ws, target_year)
    
    if detected_ref_col:
        print(f"Smart detection: Found reference year {reference_year} in column {detected_ref_col} ({chr(64+detected_ref_col)})")
        # Override config with detected column
        global COL_REF_VALUE
        COL_REF_VALUE = detected_ref_col
    else:
        print(f"Using config column {COL_REF_VALUE} for reference year {reference_year}")
    
    if detected_target_col:
        print(f"Smart detection: Found target year {target_year} in column {detected_target_col} ({chr(64+detected_target_col)})")
        global COL_OUTPUT
        COL_OUTPUT = detected_target_col
    else:
        print(f"Using config column {COL_OUTPUT} for target year {target_year}")

    # Detect alias column (Column O in NEXTERA 4)
    alias_col = None
    for col in range(1, 30):
        header = ws.cell(2, col).value  # Row 2 has sub-headers
        if header and str(header).strip().lower() == "alias":
            alias_col = col
            print(f"Smart detection: Found Alias column at {col} ({chr(64+col)})")
            break
    
    fields = []
    has_reference_values = False

    for row in ws.iter_rows(min_row=DATA_START_ROW):
        label = row[COL_LABEL - 1].value
        
        # Skip rows without labels
        if not label:
            continue
        
        # Skip header/section rows (those that don't start with field codes)
        label_str = str(label).strip()
        if not any(label_str.startswith(prefix) for prefix in ["I", "B", "L", "ACF", "CF"]):
            continue
        
        # Check Extract column if COL_EXTRACT and COL_DESC are different
        # (meaning template has separate Extract column)
        if COL_EXTRACT != COL_DESC and not ignore_extract_column:
            extract_val = row[COL_EXTRACT - 1].value
            if not (extract_val and str(extract_val).strip().lower() == "yes"):
                # print(f"  Skipping {label_str}: Extract column = '{extract_val}' (not 'Yes')")
                continue
            # else:
            #     print(f"  Including {label_str}: Extract column = 'Yes'")

        ref_value = row[COL_REF_VALUE - 1].value
        if ref_value is not None and isinstance(ref_value, (int, float)):
            has_reference_values = True

        # Use alias column if detected, otherwise use COL_DESC, otherwise use field_mappings
        if alias_col:
            description = row[alias_col - 1].value or ""
        elif COL_DESC and row[COL_DESC - 1].value:
            description = row[COL_DESC - 1].value or ""
        elif field_mappings:
            # Fallback to field_mappings.py if no alias/description in template
            description = field_mappings.get(label_str, "")
            if isinstance(description, list):
                description = ", ".join(description)
            if description:
                print(f"  Using alias from field_mappings.py for {label_str}: {description[:50]}...")
        else:
            description = ""
        
        fields.append({
            "label":           label_str,
            "description":     description,
            "reference_value": ref_value,
            "row_num":         row[COL_LABEL - 1].row,
        })

    if not fields:
        print(f"\n⚠️ WARNING: No valid fields found!")
        print(f"  Sheet: '{sheet_name}'")
        print(f"  COL_EXTRACT: {COL_EXTRACT}, COL_DESC: {COL_DESC}")
        print(f"  Has Extract column: {COL_EXTRACT != COL_DESC}")
        print(f"  Total rows scanned: {ws.max_row}")
        raise ValueError(
            f"No valid fields found in sheet '{sheet_name}'. "
            f"Make sure rows have field labels starting with I, B, L, ACF, or CF."
        )
    
    print(f"Loaded {len(fields)} fields from '{sheet_name}'")
    if has_reference_values:
        ref_count = sum(1 for f in fields if f['reference_value'] is not None)
        print(f"  {ref_count} fields have reference year ({reference_year}) values for validation")
    else:
        print(f"  Warning: No reference values found for validation")
    
    # Summary of alias sources
    alias_from_template = sum(1 for f in fields if f['description'] and not field_mappings)
    alias_from_mappings = sum(1 for f in fields if f['description'] and field_mappings)
    if alias_col:
        print(f"  Aliases loaded from template Alias column (column {alias_col})")
    elif field_mappings:
        print(f"  Aliases loaded from field_mappings.py (fallback)")
    else:
        print(f"  No aliases available (template has no Alias column and no field_mappings provided)")

    return fields


def _apply_output_to_sheet(ws, mapped_fields: list, target_year: int) -> None:
    """Write target year values and confidence to the worksheet with bold headers."""
    bold = Font(bold=True)

    header_output = ws.cell(row=1, column=COL_OUTPUT, value=f"12/31/{target_year}")
    header_output.font = bold

    header_conf = ws.cell(row=1, column=COL_CONFIDENCE, value="Confidence")
    header_conf.font = bold

    for field in mapped_fields:
        row_num = field.get("row_num")
        target_value = field.get("target_value")
        confidence = field.get("final_confidence") or field.get("mapping_confidence")
        if not row_num:
            continue
        if target_value is not None:
            try:
                ws.cell(row=row_num, column=COL_OUTPUT).value = float(target_value)
            except (TypeError, ValueError):
                ws.cell(row=row_num, column=COL_OUTPUT).value = target_value
        if confidence:
            ws.cell(row=row_num, column=COL_CONFIDENCE).value = confidence


def write_output_values(excel_path: str, mapped_fields: list, target_year: int) -> None:
    """Write mapped values back to the Excel file in-place."""
    wb = openpyxl.load_workbook(excel_path)
    sheet_name = next(iter(wb.sheetnames))  # write to first/active sheet
    _apply_output_to_sheet(wb[sheet_name], mapped_fields, target_year)
    wb.save(excel_path)


def write_output_values_to_bytes(
    excel_path: str, mapped_fields: list, target_year: int
) -> bytes:
    """Write mapped values and return the workbook as an in-memory bytes object."""
    wb = openpyxl.load_workbook(excel_path)
    sheet_name = next(iter(wb.sheetnames))
    _apply_output_to_sheet(wb[sheet_name], mapped_fields, target_year)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def create_clean_output_excel(mapped_fields: list, target_year: int, statement_type: str) -> bytes:
    """
    Create a clean output Excel with only:
    - Column A: Field labels
    - Column B: Reference year values (from template)
    - Column C: Target year values (extracted from annual report)
    - Column D: Confidence scores
    """
    from openpyxl.styles import PatternFill, Alignment
    
    reference_year = target_year - 1
    
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = f"BREF Output - {statement_type.replace('_', ' ').title()}"
    
    # Styling
    header_font = Font(bold=True, size=11, color="FFFFFF")
    header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
    bold_font = Font(bold=True)
    
    # Headers (Row 1)
    ws['A1'] = 'Field'
    ws['B1'] = f'{reference_year}\nReference'
    ws['C1'] = f'{target_year}\nExtracted'
    ws['D1'] = 'Confidence'
    
    for cell in ['A1', 'B1', 'C1', 'D1']:
        ws[cell].font = header_font
        ws[cell].fill = header_fill
        ws[cell].alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
    
    # Data rows (starting from row 2)
    row_num = 2
    for field in mapped_fields:
        # Column A: Field label
        ws.cell(row_num, 1, field.get('label', ''))
        ws.cell(row_num, 1).font = Font(size=10)
        
        # Column B: Reference year value (from template)
        ref_value = field.get('reference_value')
        if ref_value is not None:
            try:
                ws.cell(row_num, 2, float(ref_value))
                ws.cell(row_num, 2).number_format = '#,##0'
            except (TypeError, ValueError):
                ws.cell(row_num, 2, ref_value)
        
        # Column C: Target year value (extracted)
        target_value = field.get('target_value')
        if target_value is not None:
            try:
                ws.cell(row_num, 3, float(target_value))
                ws.cell(row_num, 3).number_format = '#,##0'
            except (TypeError, ValueError):
                ws.cell(row_num, 3, target_value)
        
        # Column D: Confidence
        confidence = field.get('final_confidence') or field.get('mapping_confidence', '')
        ws.cell(row_num, 4, confidence)
        
        row_num += 1
    
    # Set column widths
    ws.column_dimensions['A'].width = 50
    ws.column_dimensions['B'].width = 18
    ws.column_dimensions['C'].width = 18
    ws.column_dimensions['D'].width = 15
    
    # Save to bytes
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()
