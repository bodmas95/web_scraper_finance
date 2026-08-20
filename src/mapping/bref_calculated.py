"""
BREF Calculated Fields Module
Handles calculation of derived fields based on mapped values from annual reports.

This module:
1. Identifies calculated fields (marked with is_calculated=True)
2. Evaluates formulas (e.g., "I1-I2", "I30+I31+I79+I47")
3. Appends calculated values to the mapped JSON
4. Marks calculated fields with * prefix in the final output
"""

import re
from typing import Dict, List, Any, Optional
from src.mapping.field_mappings import get_field_mappings
from src.mapping.region_adjustments import apply_calculated_sign_corrections


def parse_calculation_formula(formula: str) -> List[tuple]:
    """
    Parse a calculation formula into operations.
    
    Args:
        formula: String like "I1-I2" or "I30+I31+I79+I47" or "(I9)+I15-I14"
    
    Returns:
        List of tuples: [(field_code, operator), ...]
        Example: "I1-I2" -> [('I1', '+'), ('I2', '-')]
                 "I30+I31-I47" -> [('I30', '+'), ('I31', '+'), ('I47', '-')]
    
    Note:
        - First field always has '+' operator
        - Handles parentheses by removing them
        - Supports +, -, *, / operators
    """
    # Remove spaces and parentheses
    formula = formula.replace(" ", "").replace("(", "").replace(")", "")
    
    # Split by operators while keeping them
    # Pattern: split on +, -, *, / but keep the operators
    tokens = re.split(r'([+\-*/])', formula)
    
    # Remove empty strings
    tokens = [t for t in tokens if t]
    
    operations = []
    
    # First token is always positive (implicit +)
    if tokens:
        operations.append((tokens[0], '+'))
    
    # Process remaining tokens in pairs (operator, field)
    for i in range(1, len(tokens), 2):
        if i + 1 < len(tokens):
            operator = tokens[i]
            field_code = tokens[i + 1]
            operations.append((field_code, operator))
    
    return operations


def calculate_field_value(
    formula: str,
    mapped_values: Dict[str, Any],
    field_mappings: Dict[str, Dict],
    treat_missing_as_zero: bool = True
) -> Optional[float]:
    """
    Calculate the value of a field based on its formula and mapped values.
    
    Args:
        formula: Calculation formula (e.g., "I1-I2")
        mapped_values: Dictionary of {field_code: value} from mapping
        field_mappings: Field definitions with metadata
        treat_missing_as_zero: If True, treat missing fields as 0 (default: True)
                               If False, return None if any field is missing
    
    Returns:
        Calculated value as float, or None if calculation fails
    
    Example:
        formula = "I30+I31+I79+I47"
        mapped_values = {"I30 | Sales": 1000}  # Only I30 found
        treat_missing_as_zero = True
        Returns: 1000.0  # I31, I79, I47 treated as 0
    """
    try:
        operations = parse_calculation_formula(formula)
        result = 0.0
        found_any_value = False  # Track if we found at least one value
        
        for field_code, operator in operations:
            # Extract just the code part (e.g., "I1" from "I1 | Sales")
            field_code_clean = field_code.strip()
            
            # Find the full field key in mapped_values
            # mapped_values keys are like "I30 | Sales (turnover)" or "*I30 | Sales (turnover)" (calculated)
            value = None
            for key in mapped_values.keys():
                # Remove * prefix if present for comparison
                key_clean = key.lstrip("*")
                if key_clean.startswith(field_code_clean + " |") or key_clean == field_code_clean:
                    value = mapped_values[key]
                    break
            
            # Convert value to float
            numeric_value = 0.0  # Default to 0 if missing
            
            if value is not None:
                try:
                    numeric_value = float(value)
                    found_any_value = True  # Found at least one value
                except (ValueError, TypeError):
                    # Value exists but can't convert to float
                    if not treat_missing_as_zero:
                        return None
                    numeric_value = 0.0
            else:
                # Value not found
                if not treat_missing_as_zero:
                    # Check if field exists in template
                    field_exists = False
                    for full_key in field_mappings.keys():
                        if full_key.startswith(field_code_clean + " |") or full_key == field_code_clean:
                            field_exists = True
                            break
                    
                    if field_exists:
                        # Field exists in template but not mapped/calculated yet
                        return None
                # else: treat as 0 (default behavior)
            
            # Apply operation
            if operator == '+':
                result += numeric_value
            elif operator == '-':
                result -= numeric_value
            elif operator == '*':
                result *= numeric_value
            elif operator == '/':
                if numeric_value == 0:
                    return None  # Division by zero
                result /= numeric_value
        
        # Only return result if we found at least one value
        # This prevents calculating 0+0+0 = 0 when all fields are missing
        if not found_any_value:
            return None
        
        return result
    
    except Exception as e:
        print(f"Error calculating formula '{formula}': {e}")
        return None


def get_calculated_fields(statement_type: str, region: str = "US") -> Dict[str, Dict]:
    """
    Get all calculated fields for a statement type.
    
    Args:
        statement_type: "income_statement", "balance_sheet", or "cash_flow"
        region: "US", "APAC", or "EMEA"
    
    Returns:
        Dictionary of {field_code: field_definition} for calculated fields only
    """
    field_mappings = get_field_mappings(region)
    statement_fields = field_mappings.get(statement_type, {})
    
    calculated = {}
    for field_key, field_def in statement_fields.items():
        if field_def.get("is_calculated", False):
            calculated[field_key] = field_def
    
    return calculated


def calculate_all_fields(
    mapped_values: Dict[str, Any],
    statement_type: str,
    region: str = "US",
    tolerance_percent: float = 5.0
) -> Dict[str, Any]:
    """
    Calculate all derived fields and add them to mapped values.
    
    Handles the case where a calculated field is also directly mapped from the annual report:
    - Uses the extracted value as primary
    - Calculates the value as validation
    - Flags discrepancies if they differ by more than tolerance_percent
    
    Args:
        mapped_values: Dictionary of {field_code: value} from mapping
        statement_type: "income_statement", "balance_sheet", or "cash_flow"
        region: "US", "APAC", or "EMEA"
        tolerance_percent: Acceptable difference percentage (default 5%)
    
    Returns:
        Updated dictionary with calculated fields added (marked with * prefix)
        Also adds validation flags for discrepancies
    
    Example:
        Input mapped_values: {"I30 | Sales": 1000, "I2 | COGS": 300, "I3 | Gross profit": 695}
        Output: {
            "I30 | Sales": 1000,
            "I2 | COGS": 300,
            "I3 | Gross profit": 695,  # Extracted value (primary)
            "*I3 | Gross profit": 700,  # Calculated value (validation)
            "I3_validation_flag": "MISMATCH"  # Flag for discrepancy
        }
    """
    field_mappings = get_field_mappings(region)
    statement_fields = field_mappings.get(statement_type, {})
    
    calculated_fields = get_calculated_fields(statement_type, region)
    result = mapped_values.copy()
    
    # ITERATIVE CALCULATION: Keep looping until no more fields can be calculated
    # This handles dependencies between calculated fields (e.g., Q47 depends on Q93)
    max_iterations = 10  # Prevent infinite loops
    iteration = 0
    fields_calculated_this_iteration = 1  # Start with 1 to enter loop
    
    while fields_calculated_this_iteration > 0 and iteration < max_iterations:
        fields_calculated_this_iteration = 0
        iteration += 1
        
        # Try to calculate each field
        for field_key, field_def in calculated_fields.items():
            formula = field_def.get("calculation")
            if not formula:
                continue
            
            # Skip if already calculated in a previous iteration
            if ("*" + field_key) in result:
                continue
        
            # Check if this field was already mapped from the annual report
            extracted_value = None
            for key in mapped_values.keys():
                if key == field_key or key.startswith(field_key + " |"):
                    extracted_value = mapped_values[key]
                    break
            
            # Calculate value using formula (treat missing fields as 0)
            calculated_value = calculate_field_value(formula, result, statement_fields, treat_missing_as_zero=True)
            
            if calculated_value is not None:
                fields_calculated_this_iteration += 1
                
                if extracted_value is not None:
                    # CASE 1: Field exists in both extracted and calculated
                    try:
                        extracted_float = float(extracted_value)
                        
                        # Compare values
                        diff_percent = abs((calculated_value - extracted_float) / extracted_float * 100) if extracted_float != 0 else 0
                        
                        if diff_percent <= tolerance_percent:
                            # Values match within tolerance - use extracted value, mark as validated
                            print(f" {field_key}: Extracted={extracted_float}, Calculated={calculated_value} (diff: {diff_percent:.1f}%) - MATCH")
                            result["*" + field_key] = extracted_float  # Use extracted value
                            result[field_key + "_validation"] = "VALIDATED"
                        else:
                            # Values don't match - flag for review
                            print(f" {field_key}: Extracted={extracted_float}, Calculated={calculated_value} (diff: {diff_percent:.1f}%) - MISMATCH")
                            result["*" + field_key] = extracted_float  # Still use extracted value as primary
                            result[field_key + "_calculated"] = calculated_value  # Store calculated value for reference
                            result[field_key + "_validation"] = "MISMATCH"
                            result[field_key + "_diff_percent"] = diff_percent
                    
                    except (ValueError, TypeError):
                        # Extracted value is not numeric - use calculated value
                        print(f" {field_key}: Extracted value invalid, using calculated={calculated_value}")
                        result["*" + field_key] = calculated_value
                        result[field_key + "_validation"] = "CALCULATED_ONLY"
                        # CRITICAL: Also store without * prefix for lookups
                        result[field_key] = calculated_value
                else:
                    # CASE 2: Field not extracted - use calculated value
                    result["*" + field_key] = calculated_value
                    result[field_key + "_validation"] = "CALCULATED_ONLY"
                    print(f" Calculated {field_key}: {calculated_value} (formula: {formula})")
                    
                    # CRITICAL: Also store without * prefix for lookups
                    result[field_key] = calculated_value
            else:
                # Could not calculate in this iteration - will try again in next iteration
                pass
    
    # After all iterations, mark fields that couldn't be calculated
    for field_key, field_def in calculated_fields.items():
        if ("*" + field_key) not in result:
            # Check if extracted value exists
            extracted_value = None
            for key in mapped_values.keys():
                if key == field_key or key.startswith(field_key + " |"):
                    extracted_value = mapped_values[key]
                    break
            
            if extracted_value is not None:
                # Use extracted value even though we couldn't validate it
                print(f" {field_key}: Using extracted value (could not calculate for validation)")
                result["*" + field_key] = extracted_value
                result[field_key + "_validation"] = "EXTRACTED_ONLY"
            else:
                formula = field_def.get("calculation", "")
                print(f" Could not calculate {field_key} (formula: {formula}) - missing dependencies")
    
    # Apply region-specific sign corrections to calculated fields
    result = apply_calculated_sign_corrections(
        calculated_values=result,
        region=region,
        statement_type=statement_type
    )
    
    return result


def get_ordered_fields(statement_type: str, region: str = "US") -> List[str]:
    """
    Get all fields in the order they appear in NEXTERA 4.xlsx (US) or KLN.xlsx (APAC/EMEA).
    
    Args:
        statement_type: "income_statement", "balance_sheet", or "cash_flow"
        region: "US", "APAC", or "EMEA"
    
    Returns:
        List of field keys in the correct order
    
    Note:
        The order is preserved from field_mappings.py which was generated
        from the NEXTERA 4.xlsx (US) or KLN.xlsx (APAC/EMEA) files.
    """
    field_mappings = get_field_mappings(region)
    statement_fields = field_mappings.get(statement_type, {})
    
    # Return keys in order (Python 3.7+ dicts maintain insertion order)
    return list(statement_fields.keys())


def get_dependent_fields(field_code: str, statement_type: str, region: str = "US") -> List[str]:
    """
    Get all calculated fields that depend on a given field.
    
    Args:
        field_code: Field code (e.g., "Q5", "I2")
        statement_type: "income_statement", "balance_sheet", or "cash_flow"
        region: "US", "APAC", or "EMEA"
    
    Returns:
        List of field keys that depend on this field
    
    Example:
        get_dependent_fields("Q5", "income_statement", "APAC")
        Returns: ["Q47 | GROSS PROFIT - TRR31"]  # Because Q47 = Q93-Q5
    """
    field_mappings = get_field_mappings(region)
    statement_fields = field_mappings.get(statement_type, {})
    
    # Extract just the code part (e.g., "Q5" from "Q5 | Cost of sales")
    field_code_clean = field_code.split(" |")[0].strip()
    
    dependent = []
    for field_key, field_def in statement_fields.items():
        if not field_def.get("is_calculated", False):
            continue
        
        formula = field_def.get("calculation", "")
        if not formula:
            continue
        
        # Check if field_code appears in the formula
        # Parse formula to get all field codes
        operations = parse_calculation_formula(formula)
        formula_fields = [op[0] for op in operations]
        
        if field_code_clean in formula_fields:
            dependent.append(field_key)
    
    return dependent


def recalculate_dependent_fields(
    updated_field: str,
    updated_value: Any,
    current_values: Dict[str, Any],
    statement_type: str,
    region: str = "US"
) -> Dict[str, Any]:
    """
    Recalculate all fields that depend on an updated field.
    
    Args:
        updated_field: Field code that was updated (e.g., "Q5 | Cost of sales")
        updated_value: New value for the field
        current_values: Current values of all fields
        statement_type: "income_statement", "balance_sheet", or "cash_flow"
        region: "US", "APAC", or "EMEA"
    
    Returns:
        Dictionary of {field_key: new_value} for all recalculated fields
    
    Example:
        User updates Q5 from None to 500
        System recalculates Q47 = Q93 - Q5 = 1000 - 500 = 500
        Returns: {"Q47 | GROSS PROFIT": 500}
    """
    field_mappings = get_field_mappings(region)
    statement_fields = field_mappings.get(statement_type, {})
    
    # Update current values with the new value
    updated_values = current_values.copy()
    updated_values[updated_field] = updated_value
    
    # Get all fields that depend on this field
    dependent_fields = get_dependent_fields(updated_field, statement_type, region)
    
    recalculated = {}
    
    for field_key in dependent_fields:
        field_def = statement_fields.get(field_key, {})
        formula = field_def.get("calculation")
        
        if not formula:
            continue
        
        # Recalculate the value (treat missing fields as 0)
        new_value = calculate_field_value(formula, updated_values, statement_fields, treat_missing_as_zero=True)
        
        if new_value is not None:
            recalculated[field_key] = new_value
            # Update for cascading calculations
            updated_values[field_key] = new_value
            print(f" Recalculated {field_key}: {new_value} (due to {updated_field} change)")
    
    return recalculated


def create_ordered_output(
    mapped_values: Dict[str, Any],
    statement_type: str,
    region: str = "US"
) -> Dict[str, Any]:
    """
    Create final output with all fields in NEXTERA order.
    Includes mapped fields, calculated fields (with *), and blank unmapped fields.
    
    Args:
        mapped_values: Dictionary with calculated fields already added
        statement_type: "income_statement", "balance_sheet", or "cash_flow"
        region: "US", "APAC", or "EMEA"
    
    Returns:
        Ordered dictionary with all fields in NEXTERA order
    
    Example:
        {
            "I30 | Sales": 1000,
            "I31 | Other revenue": "",  # Unmapped
            "*I1 | Total Net sales": 1000,  # Calculated
            "I2 | COGS": 300,
            "*I3 | Gross profit": 700,  # Calculated
            ...
        }
    """
    ordered_fields = get_ordered_fields(statement_type, region)
    result = {}
    
    for field_key in ordered_fields:
        # Check if field is calculated
        field_mappings = get_field_mappings(region)
        statement_fields = field_mappings.get(statement_type, {})
        field_def = statement_fields.get(field_key, {})
        is_calculated = field_def.get("is_calculated", False)
        
        if is_calculated:
            # Look for calculated value with * prefix
            marked_key = "*" + field_key
            if marked_key in mapped_values:
                result[marked_key] = mapped_values[marked_key]
            else:
                # Calculated field but couldn't be calculated - leave blank
                result["*" + field_key] = ""
        else:
            # Regular field - use mapped value or blank
            if field_key in mapped_values:
                result[field_key] = mapped_values[field_key]
            else:
                result[field_key] = ""  # Unmapped field - blank
    
    return result


# ==============================================================================
# CALCULATED FIELDS REFERENCE - COMPLETE LIST (Extracted from field_mappings.py)
# ==============================================================================
"""

                    US INCOME STATEMENT (I-prefix) - 9 fields                

*I1 | Total Net sales (turnover) = I30+I31+I79+I47
*I3 | Gross profit - TRR31 = I1-I2
*I81 | EBITDA = I3-I4-I54-I46-I55+I42+I5-I6
*I12 | EBIT = I81-I8-I10
*I17 | Net total Interest = (I9)+I15-I14
*I34 | Financial Income (loss) = I17+I52+I64+I13+I32+I33
*I21 | Income (loss) before income taxes = I34+I44+I56+I57+I19+I60+I20
*I37 | Income from continuing operations = I21-I35+I36
*I24 | Net profit for the year = I37+I38


                 APAC/EMEA INCOME STATEMENT (Q-prefix) - 12 fields            

*Q93 | Total Revenue - TRR = Q1+Q3
*Q47 | GROSS PROFIT - TRR31 = Q93-Q5
*Q20 | EBIT = Q47-Q6-Q7-Q8-Q14-Q97+Q9+Q10
*Q104 | Current EBITDA = Q20+Q14+Q97
*ITRR225 | EBITDA IFRS16 = Q20+Q14+Q97
*Q52 | Exceptional items = Q22+Q23+Q56+Q57+Q58+Q60+Q74+Q24
*Q26 | EBIT (including exceptional items) = Q20+Q52
*Q30 | Cost of net financial debt = Q28-Q27
*Q34 | Financial income = Q30+Q100+Q101+Q31+Q33+Q94
*Q70 | Profit before income tax (PBT) = Q26+Q34
*Q37 | Profit for the year from continuing operations = Q70+Q36+Q102+Q35
*Q39 | Net profit (loss) for the year = Q37+Q38


                   US BALANCE SHEET (B/L-prefix) - 10 fields                 

ASSETS (B-prefix):
*B18 | Net Cash and cash equivalents = B150+B148+B17+B149
*B32 | Customers & other debtors = B46+B15+B33+B35
*B19 | Total current assets = B18+B32+B14+B98+B41+B40
*B6 | Net fixed assets = B4+B5
*B13 | Total Long term assets = B6+B7+B22+B2+B3+B48+B47+B8+B9+B10+B45
*B1 | TOTAL ASSETS = B19+B13

LIABILITIES (L-prefix):
*L27 | Total Current liabilities = L22+L75+L72+L21+L116+L176+L114+L70+L113+L115+L26+L69
*L7 | Equity attributable to owners = L2+L33+L37+L4+L34+L36+L47+L3+L35
*L111 | TOTAL EQUITY = L7+L8+L177
*L28 | TOTAL LIABILITIES AND EQUITIES = L27+L15+L16+L59+L17+L18+L111


                APAC/EMEA BALANCE SHEET (U/L-prefix) - 18 fields              

ASSETS (U-prefix):
*U10 | Other Intangible assets Net = U11+U12+U13+U14
*U2 | Tangible assets: Property, plant and equipment = U4+U3+U5+U115+U116+U201
*U6 | Investment properties = U7+U8+U9
*U1 | NON CURRENT ASSETS = U16+U10+U2+U6+U18+U17+U20+U88+U21
*U24 | Inventories (net) = U25+U26+U27+U28
*U29 | Trade receivables & other debtors Net = U20+U31-U34
*U36 | Current financial assets = U103+U104+U37+U106+U107+U108
*U22 | CURRENT ASSETS = U24+U114+U29+U98+U36+U35+U38+U39
*U41init | TOTAL ASSET pre-IFRS16 = U1+U22-U201
*U41 | TOTAL ASSET IFRS16 = U1+U22

LIABILITIES (L/U-prefix):
*U43 | Equity attributable to owners = U44+U45+U181+U174+U172+U47+U48+U49
*U161 | TOTAL EQUITY = U43+U51
*U53 | Borrowings / debt >1 yr = U53+U182
*U53init | o/w Borrowings / debt >1 yr pre-IFRS16 = U54+U55+U56+U57
*U52init | NON CURRENT LIABILITIES pre-IFRS16 = U53+U58+U59+U175+U60-U182
*U52 | NON CURRENT LIABILITIES = U53+U58+U59+U175+U60
*U63 | Borrowings / debt < 1 yr = U63+U183
*U63init | o/w Borrowings / debt < 1 yr pre-IFRS16 = U64+U65+U66+U67+U68+U166+U162
*U62init | CURRENT LIABILITIES pre-IFRS16 = U63+U176+U75+U72+U71+U180+U70+U177-U183
*U62ifrs16 | CURRENT LIABILITIES = U63+U176+U75+U72+U71+U180+U70+U177
*U78init | TOTAL LIABILITIES and EQUITIES IFRS16 = U52INIT+U62INIT+U161
*U78 | TOTAL LIABILITIES and EQUITIES = U52IFRS+U62IFRS+U161


                     US CASH FLOW (ACF-prefix) - 10 fields                   

*ACF01 | Cash-flow before change in WC (FFO) = ACF36-ACF35
*ACF02 | +/- Change in WC = ACF39+ACF40+ACF41+ACF42+ACF43
*ACF03 | Operating cash flow = ACF01+ACF02
*ACF04 | CAPEX = ACF44+ACF45+ACF46
*ACF05 | Recurring Free Cash-Flow = ACF03+ACF04
*ACF06 | +/- Acquisitions net of disposals = ACF15+ACF16
*ACF07 | Dividend paid = ACF47+ACF48
*ACF08 | +/- Change in Capital = ACF50+ACF51
*ACF11 | +/- Change in Cash = ACF05+ACF06+ACF07+ACF49+ACF08+ACF09+ACF52+ACF53+ACF54+ACF10
*ACF14 | CASH AT BEGINNING OF PERIOD = ACF11+ACF12


                  APAC/EMEA CASH FLOW (ICF-prefix) - 11 fields                

*ICF01 | Cash-flow before change in WCR (FFO) = ICF52-ICF53-ICF54
*ICF02 | +/- Change in WCR = ICF45+ICF46+ICF47+ICF48+ICF49
*ICF03 | Operating cash flow = ICF01+ICF02
*ICF04 | CAPEX = ICF34+ICF35+ICF36
*ICF05 | Recurring Free Cash-Flow = ICF03+ICF04
*ICF06 | +/- Acquisitions net of disposals = ICF17+ICF18
*ICF07 | Dividend paid = ICF37+ICF38+ICF55
*ICF08 | +/- Change in Capital = ICF39+ICF40
*ICF09 | +/- Change in Debt = ICF19+ICF20+ICF41+ICF42
*ICF11 | +/- Change in Cash = ICF05+ICF06+ICF07+ICF08+ICF09+ICF33+ICF10
*ICF13 | TRESORERIE A LA CLOTURE = ICF11+ICF12+ICF16


                              SUMMARY                                         

Total Calculated Fields by Statement:
  - US Income Statement: 9 fields
  - APAC Income Statement: 12 fields
  - US Balance Sheet: 10 fields
  - APAC Balance Sheet: 18 fields
  - US Cash Flow: 10 fields
  - APAC Cash Flow: 11 fields

GRAND TOTAL: 70 calculated fields across all statements and regions

Note: EMEA uses the same field codes as APAC (Q/U/L/ICF prefixes)
"""
