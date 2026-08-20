"""
BREF Mapping v1 — Pipeline Orchestrator

Entry point: run()

Runs three-pass LLM mapping for each statement category
(income_statement, balance_sheet, cash_flow) and writes
results back into the BREF Excel template.

Usage:
    from src.mapping_v1.pipeline import run

    result = run(
        excel_path      = "BREF_Template.xlsx",
        extraction_data = {
            "income_statement": [...],   # rows from PDF extraction
            "balance_sheet":    [...],
            "cash_flow":        [...],
        },
        target_year = 2024,
        provider    = "maia",            # or "gemma" / None for config default
        model       = "gpt-5.5-...",    # or None for config default
    )

    result = {
        "income_statement": {
            "results": [...],
            "summary": {
                "mapped": 18, "mapped_alias": 3, "mapped_derived": 2,
                "no_match": 4, "ambiguous": 1, "blank_reference": 10, "total": 38
            }
        },
        "balance_sheet": { ... },
        "cash_flow":     { ... },
        "excel_bytes":   b"...",   # modified workbook ready for download
        "target_year":   2024,
    }
"""

from src.mapping_v1.excel import load_bref_fields, write_results, STATEMENT_SHEET_MAP
from src.mapping_v1.mapper import map_fields, _pass4_direct_copy, _pass4_calculate
from src.mapping_v1.validator import validate_results
from src.mapping_v1.region_adjustments import apply_sign_corrections
from src.mapping_v1.sign_correction import integrate_with_mapper_results

STATEMENT_TYPES = ["income_statement", "balance_sheet", "cash_flow"]


def generate_summary(
    results_by_statement: dict,
    target_year: int,
    company_name: str = "Company",
    region: str = "APAC",
    currency: str = "HK$m",
) -> dict:
    """
    Generate financial summary from mapping results.
    
    This function integrates with the summary generator to create a 7-sheet Excel file:
    - 4 input sheets (Income Statement, Assets, Liabilities, Cash Flow)
    - 3 summary sheets (Income Statement Summary, Balance Sheet Summary, Cash Flow Summary)
    
    Args:
        results_by_statement: Dict of {statement_type: [result_dicts]}
        target_year: Target fiscal year
        company_name: Company name for the summary
        region: "US", "APAC", or "EMEA"
        currency: Currency format (e.g., "HK$m", "USD Millions")
    
    Returns:
        Dict with 'complete_file' (7-sheet Excel bytes) or 'error'
    
    Example:
        result = generate_summary(
            results_by_statement={
                "income_statement": income_results,
                "balance_sheet": balance_results,
                "cash_flow": cashflow_results,
            },
            target_year=2024,
            company_name="ABC Corp",
            region="EMEA",
            currency="EUR Millions"
        )
        
        if 'complete_file' in result:
            # Download the 7-sheet summary file
            excel_bytes = result['complete_file']
    """
    try:
        from src.integration.summary_integration import generate_summary_from_fields
        
        print("\n" + "="*70)
        print("GENERATING FINANCIAL SUMMARY")
        print(f"Company: {company_name}")
        print(f"Region: {region}")
        print(f"Target Year: {target_year}")
        print("="*70)
        
        # Extract field lists from results
        income_fields = results_by_statement.get("income_statement", [])
        balance_fields = results_by_statement.get("balance_sheet", [])
        cashflow_fields = results_by_statement.get("cash_flow", [])
        
        # Generate summary using the integration module
        result = generate_summary_from_fields(
            income_fields=income_fields,
            balance_fields=balance_fields,
            cashflow_fields=cashflow_fields,
            target_year=target_year,
            company_name=company_name,
            region=region,
            currency=currency
        )
        
        if result.get('error'):
            print(f"  ❌ Summary generation failed: {result['error']}")
            return result
        
        print("  ✅ Summary generated successfully!")
        print("  📊 7-sheet file created:")
        print("     - Input - Income Statement")
        print("     - Input - Assets")
        print("     - Input - Liabilities")
        print("     - Input - Cash flow")
        print("     - Summary - Income Statement")
        print("     - Summary - Balance Sheet")
        print("     - Summary - Cash Flow")
        print("="*70 + "\n")
        
        return result
        
    except ImportError as e:
        error_msg = f"Summary generator not available: {e}"
        print(f"  ⚠️  {error_msg}")
        return {'error': error_msg}
    except Exception as e:
        import traceback
        error_msg = f"Summary generation error: {e}"
        print(f"  ❌ {error_msg}")
        traceback.print_exc()
        return {'error': error_msg}


def _summarise(results: list[dict]) -> dict:
    """Count results by status."""
    summary: dict[str, int] = {}
    for r in results:
        status = r.get("status", "unknown")
        summary[status] = summary.get(status, 0) + 1
    summary["total"] = len(results)
    return summary


def run(
    excel_path: str,
    extraction_data: dict[str, list[dict]],
    target_year: int,
    provider: str | None = None,
    model: str | None = None,
    statement_types: list[str] | None = None,
    region: str = "US",
    enable_calculations: bool = True,
) -> dict:
    """
    Run the full mapping pipeline for all (or selected) statement categories.

    Args:
        excel_path:       Path to the BREF Excel template.
        extraction_data:  Dict keyed by statement type → list of extraction rows.
                          Keys: "income_statement", "balance_sheet", "cash_flow".
        target_year:      Fiscal year to populate (e.g. 2024).
        provider:         LLM provider ("maia" or "gemma"). None = use config.
        model:            Model ID string. None = use config.
        statement_types:  Subset of statement types to run. None = run all three.
        region:           "US", "APAC", or "EMEA" — drives sign corrections.

    Returns dict as described in module docstring.
    """
    types_to_run = statement_types or STATEMENT_TYPES

    output: dict = {"target_year": target_year}
    results_by_statement: dict[str, list[dict]] = {}

    for stmt_type in types_to_run:

        extraction_rows = extraction_data.get(stmt_type, [])

        print(f"{'#'*70}")
        print(f"# STATEMENT: {stmt_type.upper().replace('_', ' ')}")
        print(f"# Extraction rows : {len(extraction_rows)}")
        print(f"# Target year     : {target_year}")
        print(f"{'#'*70}")

        # ----------------------------------------------------------------
        # 1. Load BREF fields from template
        # ----------------------------------------------------------------
        if not excel_path:
            print(f"  ⚠️  No excel_path provided for {stmt_type} — skipping field load.")
            output[stmt_type] = {"results": [], "summary": {"error": "no_excel_path"}}
            continue

        try:
            fields, ref_year = load_bref_fields(excel_path, stmt_type, target_year)
        except ValueError as exc:
            print(f"  ⚠️  Skipping {stmt_type}: {exc}")
            output[stmt_type] = {"results": [], "summary": {"error": str(exc)}}
            continue

        if not fields:
            print(f"  ⚠️  No fields loaded for {stmt_type} — skipping.")
            output[stmt_type] = {"results": [], "summary": {"total": 0}}
            continue

        # ----------------------------------------------------------------
        # 2. If no extraction data, mark everything no_match immediately
        # ----------------------------------------------------------------
        if not extraction_rows:
            print(f"  ⚠️  No extraction rows for {stmt_type} — marking all as no_match.")
            results = [
                {
                    **f,
                    "matched_label":  None,
                    "target_value":   None,
                    "status":         "no_match",
                    "reason":         "No extraction data provided for this statement type.",
                    "pass":           0,
                    "confidence":     None,
                    "formula":        None,
                    "components":     None,
                    "sign_flipped":   None,
                    "years_verified": None,
                }
                for f in fields
            ]
            results_by_statement[stmt_type] = results
            output[stmt_type] = {"results": results, "summary": _summarise(results)}
            continue

        # ----------------------------------------------------------------
        # 3. Run three-pass mapping
        # ----------------------------------------------------------------
        results = map_fields(
            fields               = fields,
            extraction_rows      = extraction_rows,
            target_year          = target_year,
            ref_year             = ref_year,
            provider             = provider,
            model                = model,
            statement_type       = stmt_type,
            region               = region,
            enable_calculations  = False,   # Pass 4 runs separately via run_calculations()
        )

                # 3a. Validate — adds validation_status + final_confidence
        results = validate_results(results)

        # 3b. Sign correction logic — detect and correct sign inversions based on reference year comparison
        results = integrate_with_mapper_results(
            results, 
            ref_year=ref_year, 
            target_year=target_year,
            apply_to_all_years=False,  # Only correct target year
            tolerance=0.01,  # 1% tolerance for value matching
            verbose=True
        )

        # 3c. Region-specific sign corrections — enforce positive field rules
        results = apply_sign_corrections(results, region=region, statement_type=stmt_type)

        results_by_statement[stmt_type] = results
        output[stmt_type] = {
            "results": results,
            "summary": _summarise(results),
        }

        s = output[stmt_type]["summary"]
        print(
            f"📊 {stmt_type}: "
            f"mapped={s.get('mapped', 0)}  "
            f"alias={s.get('mapped_alias', 0)}  "
            f"derived={s.get('mapped_derived', 0)}  "
            f"no_match={s.get('no_match', 0)}  "
            f"ambiguous={s.get('ambiguous', 0)}  "
            f"blank={s.get('blank_reference', 0)}  "
            f"total={s.get('total', 0)}"
        )

    # --------------------------------------------------------------------
    # 4. Write all results back into the BREF Excel template
    # --------------------------------------------------------------------
    print(f"{'='*70}")
    print("Writing results to Excel template…")
    print(f"{'='*70}")

    try:
        excel_bytes = write_results(
            excel_path           = excel_path,
            results_by_statement = results_by_statement,
            target_year          = target_year,
            region               = region,
        )
        output["excel_bytes"] = excel_bytes
        print("  ✅ Excel written successfully.")
    except Exception as exc:
        print(f"  ❌ Failed to write Excel: {exc}")
        output["excel_bytes"] = None

    # --------------------------------------------------------------------
    # 5. Overall summary
    # --------------------------------------------------------------------
    print(f"{'='*70}")
    print("OVERALL PIPELINE SUMMARY")
    print(f"{'='*70}")

    total_mapped = total_alias = total_derived = total_no_match = total_blank = grand_total = 0

    for stmt_type in types_to_run:
        s   = output.get(stmt_type, {}).get("summary", {})
        m   = s.get("mapped", 0)
        ma  = s.get("mapped_alias", 0)
        md  = s.get("mapped_derived", 0)
        nm  = s.get("no_match", 0)
        bl  = s.get("blank_reference", 0)
        tot = s.get("total", 0)
        print(
            f"  {stmt_type:<25}  "
            f"mapped={m:>3}  alias={ma:>3}  derived={md:>3}  "
            f"no_match={nm:>3}  blank={bl:>3}  total={tot:>3}"
        )
        total_mapped   += m
        total_alias    += ma
        total_derived  += md
        total_no_match += nm
        total_blank    += bl
        grand_total    += tot

    print(f"  {'─'*65}")
    print(
        f"  {'TOTAL':<25}  "
        f"mapped={total_mapped:>3}  alias={total_alias:>3}  derived={total_derived:>3}  "
        f"no_match={total_no_match:>3}  blank={total_blank:>3}  total={grand_total:>3}"
    )
    print(f"{'='*70}")
    
    # Store results for potential summary generation
    output["results_by_statement"] = results_by_statement
    output["region"] = region

    return output


def run_calculations(
    results_by_statement: dict,
    excel_path: str,
    target_year: int,
    region: str = "US",
) -> dict:
    """
    Run Pass 4 (formula calculations) on already-mapped results.

    Stage 1 - run()              : LLM mapping (Pass 1 to 3)
    Stage 2 - run_calculations() : Formula evaluation (Pass 4.0 + 4)

    Args:
        results_by_statement: {statement_type: [result_dict, ...]}
                              as returned by run(), or reconstructed from a
                              corrected mapping Excel uploaded by the user.
        excel_path:           Path to BREF Excel template (for writing back
                              calculated values and producing excel_bytes).
        target_year:          Fiscal year (e.g. 2024).
        region:               "US", "APAC", or "EMEA".

    Returns dict with per-statement results, summaries, and excel_bytes.
    """
    ref_year = target_year - 1
    output = {"target_year": target_year}
    updated_by_statement = {}

    for stmt_type, results in results_by_statement.items():
        if not results:
            output[stmt_type] = {"results": [], "summary": {}}
            continue

        print("=" * 70)
        print("CALCULATIONS: " + stmt_type.upper().replace("_", " "))
        print("Fields in: " + str(len(results)))
        print("=" * 70)

        # Pass 4.0 - direct copy (no LLM, no formula)
        results = _pass4_direct_copy(results, ref_year)

        # Pass 4 - formula evaluation (iterative, up to 10 passes)
        results = _pass4_calculate(
            results        = results,
            statement_type = stmt_type,
            region         = region,
            ref_year       = ref_year,
            target_year    = target_year,
        )

                # Re-validate and re-apply sign corrections
        results = validate_results(results)
        
        # Re-apply sign correction logic after calculations
        results = integrate_with_mapper_results(
            results, 
            ref_year=ref_year, 
            target_year=target_year,
            apply_to_all_years=False,
            tolerance=0.01,
            verbose=False  # Less verbose for recalculation
        )
        
        results = apply_sign_corrections(results, region=region, statement_type=stmt_type)

        updated_by_statement[stmt_type] = results
        output[stmt_type] = {
            "results": results,
            "summary": _summarise(results),
        }

        s = output[stmt_type]["summary"]
        print(
            "Calc " + stmt_type + ": "
            "calculated_ok=" + str(s.get("calculated_ok", 0)) +
            "  calculated_missing=" + str(s.get("calculated_missing", 0)) +
            "  total=" + str(s.get("total", 0))
        )

        # Write updated results back into the BREF Excel template
    try:
        excel_bytes = write_results(
            excel_path           = excel_path,
            results_by_statement = updated_by_statement,
            target_year          = target_year,
            region               = region,
        )
        output["excel_bytes"] = excel_bytes
        print("  Excel written successfully.")
    except Exception as exc:
        print("  Failed to write Excel: " + str(exc))
        output["excel_bytes"] = None

    return output
