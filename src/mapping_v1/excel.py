"""
BREF Excel I/O — mapping_v1

Responsibilities:
  - Load BREF fields from the Excel template (all statement sheets).
  - Write mapping results back into the template and return as bytes.

Auto-detects year columns by scanning row 1 for 4-digit year values.
Balance-sheet loads BOTH "Input - Assets" and "Input - Liabilities".
"""

import io
import re

import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment

# Imported here to filter calculated fields at load time.
# Lazy-imported at function level to avoid circular import at module load.
_is_calculated_fn = None

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

STATEMENT_SHEET_MAP = {
    "income_statement": ["Input - Income Statement"],
    "balance_sheet":    ["Input - Assets", "Input - Liabilities"],
    "cash_flow":        ["Input - Cash flow"],
}

# Column A is always the field label.
COL_LABEL = 1
# Data rows start here (rows 1-3 are headers in standard BREF templates).
DATA_START_ROW = 4

# Field-code prefixes that identify real BREF data rows (not section headers).
VALID_PREFIXES = (
    "I", "B", "L", "ACF", "CF", "Q", "ITRR", "U",
    "ICF", "DMLTC", "DMLTB", "CAFE", "CAFF",
)

# Stop loading income-statement rows when we hit OCI section.
OCI_STOP_KEYWORDS = ("Other Comprehensive Income", "Comprehensive Income")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_float(value) -> float | None:
    """Convert a cell value to float; return None if not numeric."""
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


def _detect_year_columns(ws) -> dict[int, int]:
    """
    Scan row 1 (and row 2 as fallback) for 4-digit year values.
    Returns {year: col_index (1-based)}.
    """
    year_cols: dict[int, int] = {}
    for col in range(1, 40):
        for scan_row in (1, 2):
            header = ws.cell(scan_row, col).value
            if header:
                m = re.search(r"(20\d{2})", str(header))
                if m:
                    year = int(m.group(1))
                    if year not in year_cols:
                        year_cols[year] = col
    return year_cols


def _is_valid_field(label_str: str) -> bool:
    """Return True if the row label looks like a real BREF field (not a header)."""
    if not any(label_str.startswith(p) for p in VALID_PREFIXES):
        return False
    if " | " not in label_str and "|" not in label_str:
        return False
    return True


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_bref_fields(
    excel_path: str,
    statement_type: str,
    target_year: int,
) -> list[dict]:
    """
    Load all BREF field rows for a given statement type from the Excel template.

    Returns a list of dicts:
        {
          "label":           str,   e.g. "I30 | Sales (turnover)"
          "reference_value": float | None,   previous-year value
          "sheet_name":      str,   source sheet
          "row_num":         int,   1-based row number in that sheet
        }
    """
    reference_year = target_year - 1
    sheets = STATEMENT_SHEET_MAP.get(statement_type, [])
    if not sheets:
        raise ValueError(f"Unknown statement_type: {statement_type!r}")

    wb = openpyxl.load_workbook(excel_path, data_only=True)
    all_fields: list[dict] = []
    seen_labels: set[str] = set()

    for sheet_name in sheets:
        if sheet_name not in wb.sheetnames:
            print(f"  ⚠️  Sheet '{sheet_name}' not found — skipping.")
            continue

        ws = wb[sheet_name]
        year_cols = _detect_year_columns(ws)

        # Pick the most recently populated prior year as the reference year.
        # Don't blindly assume target_year-1 — the template may have been
        # pre-populated only through 2022 while the target is 2024.
        prior_years = sorted(
            [yr for yr in year_cols if yr < target_year], reverse=True
        )
        reference_year = target_year - 1  # default fallback
        for candidate in prior_years:
            col = year_cols[candidate]
            # Check if this column has at least one non-empty data value
            has_data = any(
                _to_float(ws.cell(r, col).value) is not None
                for r in range(DATA_START_ROW, min(DATA_START_ROW + 30, ws.max_row + 1))
            )
            if has_data:
                reference_year = candidate
                break

        ref_col = year_cols.get(reference_year)

        if ref_col is None:
            print(
                f"  ⚠️  Reference year {reference_year} column not found in "
                f"'{sheet_name}'. Available years: {sorted(year_cols)}"
            )

        print(f"  📄 '{sheet_name}': ref_year={reference_year} col={ref_col}, target_year={target_year}, year_cols={year_cols}")

        for row in ws.iter_rows(min_row=DATA_START_ROW):
            label_cell = row[COL_LABEL - 1].value
            if not label_cell:
                continue

            label_str = str(label_cell).strip()

            # Stop at OCI section for income statement
            if any(kw in label_str for kw in OCI_STOP_KEYWORDS):
                print(f"  🛑 Stopping at OCI section: '{label_str}'")
                break

            if not _is_valid_field(label_str):
                continue

            if label_str in seen_labels:
                continue  # deduplicate across sheets
            seen_labels.add(label_str)

            # Skip calculated fields entirely — they are handled by Pass 4 in the
            # Calculations section and must never enter the mapping pipeline.
            # Including them distorts accuracy metrics and wastes LLM tokens.
            global _is_calculated_fn
            if _is_calculated_fn is None:
                from src.mapping_v1.mapper import _is_calculated as _fn
                _is_calculated_fn = _fn
            if _is_calculated_fn(label_str)[0]:
                continue

            ref_value = _to_float(row[ref_col - 1].value) if ref_col else None
            row_num   = row[COL_LABEL - 1].row

            # Collect ALL previous-year values for multi-year verification in mapper
            all_ref_values = {}
            for yr, yc in year_cols.items():
                if yr < target_year:  # only previous years
                    val = _to_float(ws.cell(row_num, yc).value)
                    if val is not None:
                        all_ref_values[str(yr)] = val

            # Skip fields with no prior-year data at all — nothing to match against.
            # The mapper would be guessing blindly; exclude them entirely so they
            # don't consume LLM tokens or pollute results.
            if ref_value is None and not all_ref_values:
                continue

            field_dict = {
                "label":           label_str,
                "reference_value": ref_value,
                "all_ref_values":  all_ref_values,
                "sheet_name":      sheet_name,
                "row_num":         row_num,
            }
            all_fields.append(field_dict)

            # Debug: print L22 and any field whose ref value looks suspicious
            if "L22" in label_str or "L27" in label_str:
                print(f"  [TEMPLATE DEBUG] {label_str!r}")
                print(f"    ref_col={ref_col}  ref_value={ref_value}  row={row_num}")
                # Print all year values in this row
                for yr, yc in year_cols.items():
                    raw = ws.cell(row_num, yc).value
                    print(f"    year={yr} col={yc} raw_value={raw}")

    print(f"  ✅ Loaded {len(all_fields)} fields for '{statement_type}' (ref_year={reference_year})")
    return all_fields, reference_year


def write_results(
    excel_path: str,
    results_by_statement: dict[str, list[dict]],
    target_year: int,
    region: str = "US",
) -> bytes:
    """
    Write mapping results back into the BREF Excel template.

    Only rows with status == "mapped" or "mapped_alias" receive a value.
    Returns the modified workbook as bytes (for Streamlit download).
    
    Special highlighting for EMEA region cashflow:
      - ICF38 and ICF49: Green background when mapped by direct value match
      - Fields with opposite signs: Orange background (sign mismatch indicator)

    results_by_statement: {
        "income_statement": [result_dict, ...],
        "balance_sheet":    [result_dict, ...],
        "cash_flow":        [result_dict, ...],
    }
    """
    wb = openpyxl.load_workbook(excel_path, data_only=True)
    bold = Font(bold=True)

    for statement_type, results in results_by_statement.items():
        sheets = STATEMENT_SHEET_MAP.get(statement_type, [])

        for sheet_name in sheets:
            if sheet_name not in wb.sheetnames:
                continue

            ws = wb[sheet_name]
            year_cols = _detect_year_columns(ws)
            target_col = year_cols.get(target_year)

            if target_col is None:
                # Try to use the column right after the reference-year column
                ref_col = year_cols.get(target_year - 1)
                target_col = (ref_col + 1) if ref_col else None

            if target_col is None:
                print(f"  ⚠️  Cannot determine target year column in '{sheet_name}' — skipping write.")
                continue

            # Write header
            ws.cell(row=1, column=target_col, value=str(target_year)).font = bold

            # Build row_num → result lookup
            row_map = {r["row_num"]: r for r in results if r.get("sheet_name") == sheet_name}

            for row_num, result in row_map.items():
                status = result.get("status", "")
                if status not in ("mapped", "mapped_alias", "mapped_derived"):
                    continue
                target_value = result.get("target_value")
                if target_value is None:
                    continue
                
                # Write the value
                cell = ws.cell(row=row_num, column=target_col)
                try:
                    cell.value = float(target_value)
                except (TypeError, ValueError):
                    cell.value = target_value
                
                                # Apply special highlighting for EMEA cashflow
                if region in ("EMEA", "APAC") and statement_type == "cash_flow":
                    label = result.get("label", "")
                    field_code = label.split(" |")[0].strip() if " |" in label else label.strip()
                    
                    # Rule 1: ICF38 and ICF49 always get green background when mapped
                    if field_code in ("ICF38", "ICF49") and status == "mapped":
                        cell.fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
                    # Rule 2: Sign mismatch detection - check the flag set by mapper
                    elif result.get("sign_mismatch_detected"):
                        cell.fill = PatternFill(start_color="FFA500", end_color="FFA500", fill_type="solid")
                    # Rule 3: Fallback - check signs manually if flag not set
                    elif status in ("mapped", "mapped_alias", "mapped_derived"):
                        ref_val = result.get("reference_value")
                        ext_ref_val = result.get("extracted_ref_value")
                        
                        if ref_val is not None and ext_ref_val is not None:
                            try:
                                ref_f = float(ref_val)
                                ext_f = float(ext_ref_val)
                                # Both non-zero and signs differ
                                if ref_f != 0 and ext_f != 0 and (ref_f > 0) != (ext_f > 0):
                                    # Orange background - except for ICF38/ICF49
                                    if field_code not in ("ICF38", "ICF49"):
                                        cell.fill = PatternFill(start_color="FFA500", end_color="FFA500", fill_type="solid")
                            except (ValueError, TypeError):
                                pass

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def create_clean_output_excel(
    results: list[dict],
    target_year: int,
    statement_type: str,
    region: str = "US",
) -> bytes:
    """
    Create a clean standalone output Excel (does NOT write into the BREF template).

    Columns:
      A — BREF field label
      B — Reference year value (from template)
      C — Target year value (mapped)
      D — Status
      E — Confidence
      F — Formula / derivation (Pass 3) or reason

    Rows are colour-coded by status:
      green  = mapped       | yellow = mapped_alias  | blue = mapped_derived
      red    = no_match     | orange = ambiguous      | grey = blank_reference
      
    Special highlighting for EMEA region cashflow:
      - ICF38 and ICF49: Always green background when mapped by direct value match
      - Fields with opposite signs: Orange background (sign mismatch indicator)

    Returns workbook as bytes (ready for st.download_button).
    """
    from openpyxl import Workbook
    from openpyxl.styles import PatternFill, Alignment

    reference_year = target_year - 1

    wb = Workbook()
    ws = wb.active
    ws.title = statement_type.replace("_", " ").title()[:31]

    hdr_font  = Font(bold=True, color="FFFFFF", size=11)
    hdr_fill  = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
    hdr_align = Alignment(horizontal="center", vertical="center", wrap_text=True)

    STATUS_COLORS = {
        "mapped":          "C6EFCE",
        "mapped_alias":    "FFEB9C",
        "mapped_derived":  "BDD7EE",
        "no_match":        "FFC7CE",
        "ambiguous":       "FFCC99",
        "blank_reference": "F2F2F2",
    }

    headers = [
        "BREF Field",
        f"{reference_year} Reference",
        f"{target_year} Mapped",
        "Status",
        "Confidence",
        "Formula / Reason",
    ]
    for col, hdr in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=hdr)
        cell.font      = hdr_font
        cell.fill      = hdr_fill
        cell.alignment = hdr_align

    for row_num, result in enumerate(results, start=2):
        status   = result.get("status", "")
        label    = result.get("label", "")
        
        # Extract field code (e.g., "ICF38" from "ICF38 | Dividends paid...")
        field_code = label.split(" |")[0].strip() if " |" in label else label.strip()
        
        # Determine row color based on status and special EMEA cashflow rules
        row_fill_color = STATUS_COLORS.get(status, "FFFFFF")
        
                        # EMEA region cashflow special highlighting
        if region == "EMEA" and statement_type == "cash_flow":
            # Rule 1: ICF38 and ICF49 always get green background when mapped
            if field_code in ("ICF38", "ICF49") and status == "mapped":
                row_fill_color = "C6EFCE"  # Green - indicates sign was changed
            # Rule 2: Sign mismatch detection - check the flag set by mapper
            elif result.get("sign_mismatch_detected"):
                row_fill_color = "FFA500"  # Orange - sign mismatch (analyst should review)
            # Rule 3: Fallback - check signs manually if flag not set
            elif status in ("mapped", "mapped_alias", "mapped_derived"):
                ref_val = result.get("reference_value")
                ext_ref_val = result.get("extracted_ref_value")
                
                # Check if signs differ (one positive, one negative)
                if ref_val is not None and ext_ref_val is not None:
                    try:
                        ref_f = float(ref_val)
                        ext_f = float(ext_ref_val)
                        # Both non-zero and signs differ
                        if ref_f != 0 and ext_f != 0 and (ref_f > 0) != (ext_f > 0):
                            # Orange background - sign mismatch (analyst should review)
                            # Exception: ICF38 and ICF49 already handled above
                            if field_code not in ("ICF38", "ICF49"):
                                row_fill_color = "FFA500"  # Orange
                    except (ValueError, TypeError):
                        pass
        
        row_fill = PatternFill(
            start_color=row_fill_color,
            end_color=row_fill_color,
            fill_type="solid",
        )

        ws.cell(row_num, 1, label)

        rv = result.get("reference_value")
        if rv is not None:
            try:
                c = ws.cell(row_num, 2, float(rv))
                c.number_format = "#,##0.##"
            except (TypeError, ValueError):
                ws.cell(row_num, 2, rv)

        tv = result.get("target_value")
        if tv is not None:
            try:
                c = ws.cell(row_num, 3, float(tv))
                c.number_format = "#,##0.##"
            except (TypeError, ValueError):
                ws.cell(row_num, 3, tv)

        ws.cell(row_num, 4, status)
        ws.cell(row_num, 5, result.get("final_confidence") or result.get("confidence") or "")
        ws.cell(row_num, 6, result.get("formula") or result.get("reason") or "")

        for col in range(1, 7):
            ws.cell(row_num, col).fill = row_fill

    ws.column_dimensions["A"].width = 52
    ws.column_dimensions["B"].width = 18
    ws.column_dimensions["C"].width = 18
    ws.column_dimensions["D"].width = 18
    ws.column_dimensions["E"].width = 14
    ws.column_dimensions["F"].width = 50
    ws.freeze_panes = "A2"

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()
