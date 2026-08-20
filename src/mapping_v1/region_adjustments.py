"""
mapping_v1 — Region-Specific Sign Corrections

After mapping, certain BREF fields must ALWAYS be positive regardless of how
the value appears in the annual report (e.g. expense rows stored as negatives).

Regions:
  "US"          → AMER field lists  (I-series income, B/L-series balance, ACF cash flow)
  "APAC"/"EMEA" → APAC field lists  (Q-series income, U-series balance, ICF cash flow)

Usage:
    from src.mapping_v1.region_adjustments import apply_sign_corrections

    results = apply_sign_corrections(results, region="US", statement_type="income_statement")
"""

from typing import Any

# ---------------------------------------------------------------------------
# AMER (US) — Force-positive field lists
# ---------------------------------------------------------------------------

AMER_INCOME_FORCE_POSITIVE = [
    "I2 | Costs and expenses (or COGS)",
    "I4 | SG&A Expense",
    "I48 | o/w administratives expenses",
    "I49 | o/w distribution costs, advertising & promotion",
    "I53 | o/w salaries & related costs",
    "I54 | Research and Development costs",
    "I46 | Taxes (other than income tax) & Insurance",
    "I55 | Bad debt expenses",
    "I6 | Other expenses from current operations",
    "I8 | Depreciation, depletion and amortization",
    "I10 | Other amortization (net)",
    "I14 | Interest Expenses",
    "I17 | Net total Interest",
    "I35 | Provision for income taxes (benefit)",
]

AMER_BALANCE_FORCE_POSITIVE = [
    # Assets
    "B150 | Cash and bank deposits",
    "B17 | Short Term investments (marketable securities)",
    "B149 | Restricted cash",
    "B18 | Net Cash and cash equivalents (incl. marketable securities & excl. overdrafts)",
    "B15 | Trade Receivables / debtors",
    "B33 | Other debtors",
    "B14 | Inventories (net)",
    "B4 | Fixed assets",
    "B6 | Net fixed assets (property, plants and equipment, Net)",
    "B2 | Goodwill (net)",
    "B3 | Other assets and Intangible - Net",
    "B19 | Total current assets",
    "B13 | Total Long term assets",
    "B1 | TOTAL ASSETS",
    # Liabilities
    "L22 | Short-term debt < 1 yr , including current maturities of LT debt",
    "L75 | Accrued and other current liabilities",
    "L21 | Trade Accounts payable / Creditors",
    "L15 | Long-term debt, less current maturities",
    "L27 | Total Current liabilities",
]

AMER_CASHFLOW_FORCE_POSITIVE = [
    "ACF22 | +/-Depreciation Amortization and Depletion",
    "ACF23 | +/-Amortization of Intangible Assets",
    "ACF26 | +/-Tax (inc. Deffered tax, tax credit)",
    "ACF27 | +/-Stock-based compensation expense",
]

# ---------------------------------------------------------------------------
# APAC / EMEA — Force-positive field lists
# ---------------------------------------------------------------------------

APAC_INCOME_FORCE_POSITIVE = [
    "Q5 | Cost of sales",
    "Q6 | SG&A Expense",
    "Q48 | o/w administratives expenses",
    "Q49 | o/w distribution costs, advertising & promotion",
    "Q50 | o/w Salaries & related costs",
    "Q7 | External operating costs (incl. services, R&D)",
    "Q8 | Taxes other than income tax",
    "Q14 | Net depreciation and amortization expense",
    "Q105 | D&A of right of use Assets IFRS16",
    "Q97 | Net charges to provisions and impairment losses",
    "Q22 | Goodwill Impairment (non recurring)",
    "Q28 | Interest costs (gross)",
    "Q29 | o/w from hybrid instruments, convertible bonds",
    "Q29ifrs16 | o/w lease interest IFRS16",
    "Q28init | o/w interest costs (gross) pre-IFRS16",
    "Q30 | Cost of net financial debt",
    "Q35 | Income tax",
]

APAC_BALANCE_FORCE_POSITIVE = [
    # Assets
    "U16 | Goodwill (net)",
    "U10 | Other Intangible assets Net",
    "U2 | Tangible assets: Property, plant and equipment",
    "U6 | Investment properties",
    "U24 | Inventories (net)",
    "U200 | Trade receivables",
    "U31 | Other receivables",
    "U39 | Net Cash and bank deposits (excluding overdrafts)",
    "U1 | NON CURRENT ASSETS",
    "U22 | CURRENT ASSETS",
    "U41 | TOTAL ASSET IFRS16",
    "U41init | TOTAL ASSET pre-IFRS16",
    # Liabilities
    "U53 | Borrowings / debt >1 yr (excluding current maturities of LT debt)",
    "U53init | o/w Borrowings / debt >1 yr (excluding current maturities of LT debt) pre-IFRS16",
    "U63 | Borrowings / debt < 1 yr (including current maturities of LT debt)",
    "U63init | o/w Borrowings / debt < 1 yr (including current maturities of LT debt) pre-IFRS16",
    "U75 | Accrued and other current liabilities",
    "U71 | Trade Accounts payable",
    "U52 | NON CURRENT LIABILITIES",
    "U52init | NON CURRENT LIABILITIES pre-IFRS16",
    "U62ifrs16 | CURRENT LIABILITIES",
    "U62init | CURRENT LIABILITIES pre-IFRS16",
]

APAC_CASHFLOW_FORCE_POSITIVE = [
    "ICF23 | +/-Depreciation Amortization and Depletion",
    "ICF24 | +/-Amortization of Intangible Assets",
    "ICF27 | +/-Tax (inc. Deffered tax, tax credit)",
    "ICF28 | +/-Stock-based compensation expense",
    "ICF53 | Gross interests (cash)",
    "ICF53ifrs16 | lease interest IFRS16)",
    "ICF54 | Gross interests (cash)",
    "ICF54ifrs16 | Gross interests (cash) IFRS16",
    "ICF53bis | - Gross interests (cash)",
    "ICF54ter | Lease interest IFRS16",
]

# ---------------------------------------------------------------------------
# Internal lookup helper
# ---------------------------------------------------------------------------

def _force_positive_list(region: str, statement_type: str) -> list[str]:
    """Return the correct force-positive list for a region + statement_type combo."""
    if region == "US":
        return {
            "income_statement": AMER_INCOME_FORCE_POSITIVE,
            "balance_sheet":    AMER_BALANCE_FORCE_POSITIVE,
            "cash_flow":        AMER_CASHFLOW_FORCE_POSITIVE,
        }.get(statement_type, [])
    elif region in ("APAC", "EMEA"):
        return {
            "income_statement": APAC_INCOME_FORCE_POSITIVE,
            "balance_sheet":    APAC_BALANCE_FORCE_POSITIVE,
            "cash_flow":        APAC_CASHFLOW_FORCE_POSITIVE,
        }.get(statement_type, [])
    return []


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def apply_sign_corrections(
    results: list[dict[str, Any]],
    region: str = "US",
    statement_type: str = "income_statement",
) -> list[dict[str, Any]]:
    """
    Enforce sign conventions on a list of mapping result dicts.

    For every field in the force-positive list whose target_value is negative,
    the value is flipped to positive and `sign_corrected=True` is set.
    Also corrects `year_values` entries and `reference_value` for consistency.

    Args:
        results:        Output of mapper.map_fields() or pipeline.run() results list.
        region:         "US", "APAC", or "EMEA".
        statement_type: "income_statement", "balance_sheet", or "cash_flow".

    Returns:
        Updated results list (mutates in-place and returns same list).
    """
    force_positive = _force_positive_list(region, statement_type)
    if not force_positive:
        return results

    print(f"\n  Sign corrections [{region} / {statement_type}] ...")
    corrections = 0

    for field in results:
        label       = field.get("label", "").lstrip("*")
        if label not in force_positive:
            continue

        # --- target_value ---
        tv = field.get("target_value")
        if tv is not None:
            try:
                tv_f = float(tv)
                if tv_f < 0:
                    field["target_value"]   = abs(tv_f)
                    field["sign_corrected"] = True
                    print(f"    ✏️  {label}: target {tv_f} → {abs(tv_f)}")
                    corrections += 1
            except (ValueError, TypeError):
                pass

        # --- reference_value ---
        rv = field.get("reference_value")
        if rv is not None:
            try:
                rv_f = float(rv)
                if rv_f < 0:
                    field["reference_value"] = abs(rv_f)
            except (ValueError, TypeError):
                pass

        # --- year_values dict ---
        yv = field.get("year_values", {})
        if yv:
            field["year_values"] = {
                yr: (abs(float(v)) if v is not None and float(v) < 0 else v)
                for yr, v in yv.items()
            }
            # year_values are now all positive — reset sign_flipped so UI doesn't double-negate
            field["sign_flipped"] = False

    print(f"  {'✅' if corrections else '—'}  {corrections} correction(s) applied.")
    return results


def should_force_positive(
    field_label: str,
    region: str = "US",
    statement_type: str = "income_statement",
) -> bool:
    """Return True if this field must always be stored as a positive value."""
    clean = field_label.lstrip("*")
    return clean in _force_positive_list(region, statement_type)
