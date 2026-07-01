"""
Region-Specific Sign Corrections Module
========================================
Handles sign corrections for mapped and calculated values based on region.

AMER (US) Region:
- Certain expense fields should ALWAYS be positive (even if extracted/calculated as negative)
- This ensures consistency with BREF template expectations

APAC/EMEA Regions:
- Different accounting conventions may require different sign corrections
- Currently configured to match AMER, but can be customized per region

Usage:
------
from src.mapping.region_adjustments import apply_sign_corrections

# After mapping/calculation
corrected_fields = apply_sign_corrections(
    fields=mapped_fields,
    region="US",
    statement_type="income_statement"
)
"""

from typing import Dict, List, Any


# ============================================================================
# AMER (US) REGION - FORCE POSITIVE FIELDS
# ============================================================================

# Income Statement - Fields that should ALWAYS be positive
AMER_INCOME_FORCE_POSITIVE = [
    "I2 | Costs and expenses (or \"COGS\")",
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
    "I17 | Net total Interest",  # Net interest expense (when expenses > income)
    "I35 | Provision for income taxes (benefit)",
]

# Balance Sheet - Fields that should ALWAYS be positive
AMER_BALANCE_FORCE_POSITIVE = [
    # Assets (all should be positive)
    "B150 | Cash and bank deposits",
    "B17 | Short Term investments (marketable securities)",
    "B149 | Restricted cash",
    "B18 | Net Cash and cash equivalents (incl. marketable securities & excl. overdrafts)",
    "B15 | Trade Receivables / debtors",
    "B33 | Other debtors",
    "B14 | Inventories (net)",
    "B4 | Fixed assets",
    "B6 | Net fixed assets (property, plants and equipment, Net)",
    "B2 | Goodwill (\"net\")",
    "B3 | Other assets and Intangible - Net",
    "B19 | Total current assets",
    "B13 | Total Long term assets",
    "B1 | TOTAL ASSETS",
    
    # Liabilities (all should be positive)
    "L22 | Short-term debt < 1 yr , including current maturities of LT debt",
    "L75 | Accrued and other current liabilities",
    "L21 | Trade Accounts payable / Creditors",
    "L15 | Long-term debt, less current maturities",
    "L27 | Total Current liabilities",
    
    # Equity (can be negative in rare cases, so not included)
]

# Cash Flow - Fields that should ALWAYS be positive
AMER_CASHFLOW_FORCE_POSITIVE = [
    "ACF22 | +/-Depreciation Amortization and Depletion",
    "ACF23 | +/-Amortization of Intangible Assets",
    "ACF26 | +/-Tax (inc. Deffered tax, tax credit)",
    "ACF27 | +/-Stock-based compensation expense",
]


# ============================================================================
# APAC/EMEA REGIONS - FORCE POSITIVE FIELDS
# ============================================================================

# Income Statement - Fields that should ALWAYS be positive
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
    "Q30 | Cost of net financial debt",  # Net interest expense
    "Q35 | Income tax",
]

# Balance Sheet - Fields that should ALWAYS be positive
APAC_BALANCE_FORCE_POSITIVE = [
    # Assets (all should be positive)
    "U16 | Goodwill (\"net\")",
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
    
    # Liabilities (all should be positive)
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

# Cash Flow - Fields that should ALWAYS be positive
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


# ============================================================================
# MAIN CORRECTION FUNCTION
# ============================================================================

def apply_sign_corrections(
    fields: List[Dict[str, Any]], 
    region: str = "US",
    statement_type: str = "income_statement"
) -> List[Dict[str, Any]]:
    """
    Apply sign corrections to mapped fields based on region and statement type.
    
    Args:
        fields: List of mapped field dictionaries
        region: "US" (AMER), "APAC", or "EMEA"
        statement_type: "income_statement", "balance_sheet", or "cash_flow"
    
    Returns:
        List of fields with sign corrections applied
    
    Example:
        Input:  I4 | SG&A Expense = -1000 (extracted as negative)
        Output: I4 | SG&A Expense = 1000 (corrected to positive)
    """
    # Select the appropriate force-positive list
    force_positive_fields = []
    
    if region == "US":
        if statement_type == "income_statement":
            force_positive_fields = AMER_INCOME_FORCE_POSITIVE
        elif statement_type == "balance_sheet":
            force_positive_fields = AMER_BALANCE_FORCE_POSITIVE
        elif statement_type == "cash_flow":
            force_positive_fields = AMER_CASHFLOW_FORCE_POSITIVE
    elif region in ["APAC", "EMEA"]:
        if statement_type == "income_statement":
            force_positive_fields = APAC_INCOME_FORCE_POSITIVE
        elif statement_type == "balance_sheet":
            force_positive_fields = APAC_BALANCE_FORCE_POSITIVE
        elif statement_type == "cash_flow":
            force_positive_fields = APAC_CASHFLOW_FORCE_POSITIVE
    
    if not force_positive_fields:
        # No corrections needed
        return fields
    
    print(f"\n🔧 Applying {region} sign corrections for {statement_type}...")
    corrections_made = 0
    
    corrected_fields = []
    for field in fields:
        field_label = field.get('label', '')
        
        # Remove calculated field marker (*) for comparison
        clean_label = field_label.lstrip('*')
        
        # Check if this field needs sign correction
        if clean_label in force_positive_fields:
            # Correct target value
            target_value = field.get('target_value')
            if target_value is not None:
                try:
                    target_float = float(target_value)
                    if target_float < 0:
                        field['target_value'] = abs(target_float)
                        field['sign_corrected'] = True
                        print(f"  ✓ {clean_label}: {target_float} → {abs(target_float)}")
                        corrections_made += 1
                except (ValueError, TypeError):
                    pass
            
            # Correct reference value
            reference_value = field.get('reference_value')
            if reference_value is not None:
                try:
                    ref_float = float(reference_value)
                    if ref_float < 0:
                        field['reference_value'] = abs(ref_float)
                        if 'sign_corrected' not in field:
                            field['sign_corrected'] = True
                except (ValueError, TypeError):
                    pass
            
            # CRITICAL FIX: Also correct year_values dictionary
            year_values = field.get('year_values', {})
            if year_values:
                corrected_year_values = {}
                for year, value in year_values.items():
                    if value is not None:
                        try:
                            value_float = float(value)
                            if value_float < 0:
                                corrected_year_values[year] = abs(value_float)
                                if 'sign_corrected' not in field:
                                    field['sign_corrected'] = True
                            else:
                                corrected_year_values[year] = value_float
                        except (ValueError, TypeError):
                            corrected_year_values[year] = value
                    else:
                        corrected_year_values[year] = value
                
                field['year_values'] = corrected_year_values
        
        corrected_fields.append(field)
    
    if corrections_made > 0:
        print(f"✅ Applied {corrections_made} sign correction(s)")
    else:
        print(f"✅ No sign corrections needed")
    
    return corrected_fields


def apply_calculated_sign_corrections(
    calculated_values: Dict[str, Any],
    region: str = "US",
    statement_type: str = "income_statement"
) -> Dict[str, Any]:
    """
    Apply sign corrections to calculated field values.
    
    Args:
        calculated_values: Dictionary of calculated field values (with * prefix)
        region: "US" (AMER), "APAC", or "EMEA"
        statement_type: "income_statement", "balance_sheet", or "cash_flow"
    
    Returns:
        Dictionary with corrected calculated values
    
    Example:
        Input:  {"*I17 | Net total Interest": -500}
        Output: {"*I17 | Net total Interest": 500}
    """
    # Select the appropriate force-positive list
    force_positive_fields = []
    
    if region == "US":
        if statement_type == "income_statement":
            force_positive_fields = AMER_INCOME_FORCE_POSITIVE
        elif statement_type == "balance_sheet":
            force_positive_fields = AMER_BALANCE_FORCE_POSITIVE
        elif statement_type == "cash_flow":
            force_positive_fields = AMER_CASHFLOW_FORCE_POSITIVE
    elif region in ["APAC", "EMEA"]:
        if statement_type == "income_statement":
            force_positive_fields = APAC_INCOME_FORCE_POSITIVE
        elif statement_type == "balance_sheet":
            force_positive_fields = APAC_BALANCE_FORCE_POSITIVE
        elif statement_type == "cash_flow":
            force_positive_fields = APAC_CASHFLOW_FORCE_POSITIVE
    
    if not force_positive_fields:
        return calculated_values
    
    corrected_values = calculated_values.copy()
    corrections_made = 0
    
    for field_label, value in calculated_values.items():
        # Remove * prefix for comparison
        clean_label = field_label.lstrip('*')
        
        # Check if this calculated field needs sign correction
        if clean_label in force_positive_fields:
            if value is not None:
                try:
                    value_float = float(value)
                    if value_float < 0:
                        corrected_values[field_label] = abs(value_float)
                        print(f"  ✓ Calculated {clean_label}: {value_float} → {abs(value_float)}")
                        corrections_made += 1
                except (ValueError, TypeError):
                    pass
    
    if corrections_made > 0:
        print(f"✅ Applied {corrections_made} calculated field sign correction(s)")
    
    return corrected_values


# ============================================================================
# USAGE EXAMPLES
# ============================================================================

"""
Example 1: Apply sign corrections to mapped fields (AMER Income Statement)
--------------------------------------------------------------------------
from src.mapping.region_adjustments import apply_sign_corrections

mapped_fields = [
    {"label": "I4 | SG&A Expense", "target_value": -1000, "reference_value": -950},
    {"label": "I14 | Interest Expenses", "target_value": -500, "reference_value": -480},
    {"label": "I30 | Sales (turnover)", "target_value": 10000, "reference_value": 9500},
]

corrected_fields = apply_sign_corrections(
    fields=mapped_fields,
    region="US",
    statement_type="income_statement"
)

# Result:
# I4 | SG&A Expense: -1000 → 1000 ✓
# I14 | Interest Expenses: -500 → 500 ✓
# I30 | Sales: 10000 → 10000 (unchanged)


Example 2: Apply sign corrections to calculated fields (AMER)
-------------------------------------------------------------
from src.mapping.region_adjustments import apply_calculated_sign_corrections

calculated_values = {
    "*I17 | Net total Interest": -300,
    "*I81 | EBITDA": 2000,
}

corrected_values = apply_calculated_sign_corrections(
    calculated_values=calculated_values,
    region="US",
    statement_type="income_statement"
)

# Result:
# *I17 | Net total Interest: -300 → 300 ✓
# *I81 | EBITDA: 2000 → 2000 (unchanged)


Example 3: APAC Income Statement
--------------------------------
corrected_fields = apply_sign_corrections(
    fields=mapped_fields,
    region="APAC",
    statement_type="income_statement"
)

# Uses APAC_INCOME_FORCE_POSITIVE list
# Q5, Q6, Q28, Q30, Q35, etc. will be forced positive


Example 4: Balance Sheet (AMER)
-------------------------------
balance_fields = [
    {"label": "B1 | TOTAL ASSETS", "target_value": -50000},  # Should be positive
    {"label": "L7 | Equity attributable to owners", "target_value": -5000},  # Can be negative
]

corrected_fields = apply_sign_corrections(
    fields=balance_fields,
    region="US",
    statement_type="balance_sheet"
)

# Result:
# B1 | TOTAL ASSETS: -50000 → 50000 ✓
# L7 | Equity: -5000 → -5000 (unchanged, equity can be negative)
"""
