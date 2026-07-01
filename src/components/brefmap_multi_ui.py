"""
Simplified BREF Mapping UI with Multi-Statement Support
Allows users to select multiple statements via checkboxes and map them all at once.
Results are displayed in tabs similar to extraction results.
"""

import io
from datetime import datetime
import pandas as pd
import streamlit as st

try:
    from src.extraction.extraction_config import STATEMENT_LABELS
except ImportError:
    STATEMENT_LABELS = {
        "income_statement": "Income Statement",
        "balance_sheet": "Balance Sheet",
        "cash_flow": "Cash Flow Statement",
    }

try:
    from src.mapping import (
        map_all_fields,
        validate_mappings,
        get_field_mappings,
        load_bref_fields,
        create_clean_output_excel,
        STATEMENT_SHEET_MAP,
    )
    from src.mapping.fast_mapper import fast_map_fields
    BREF_MAPPING_AVAILABLE = True
except ImportError:
    BREF_MAPPING_AVAILABLE = False

from src.components.brefmap_ui import (
    BREFLiveLogger,
    find_year_column,
)


def render_multi_statement_mapping(extraction_results: dict, key_prefix: str = ""):
    """
    Render BREF mapping UI with multi-statement checkbox selection.
    
    Args:
        extraction_results: Dict of {statement_type: result_dict}
        key_prefix: Prefix for session state keys (e.g., "hkex", "manual", "sec")
    """
    if not BREF_MAPPING_AVAILABLE:
        st.warning("BREF mapping module not available")
        return
    
    if not extraction_results:
        st.info("No extraction results available. Please extract statements first.")
        return
    
    st.markdown("---")
    st.header("🎯 BREF Mapping")
    
    # Initialize session state for mapping results
    if 'bref_mapping_results' not in st.session_state:
        st.session_state.bref_mapping_results = {}
    
    # Memory cleanup: Clear old mapping results if too many
    if len(st.session_state.bref_mapping_results) > 10:
        # Keep only the 5 most recent mappings
        keys_to_keep = list(st.session_state.bref_mapping_results.keys())[-5:]
        st.session_state.bref_mapping_results = {
            k: v for k, v in st.session_state.bref_mapping_results.items() 
            if k in keys_to_keep
        }
    
    # Determine configuration based on client type and source
    is_manual_upload = (key_prefix == "manual" and 
                       'manual_pdf_fiscal_year' in st.session_state and
                       st.session_state.manual_pdf_fiscal_year is not None)
    
    # Get configuration values
    if is_manual_upload:
        # Manual upload: Use stored values from extraction
        bref_company_name = st.session_state.get('manual_extraction_report_title', 'Unknown Company').replace('.pdf', '')
        bref_target_year = st.session_state.get('manual_pdf_fiscal_year', datetime.now().year)
        _app_region = st.session_state.get("selected_region", "")
        # Map region: APAC and EMEA use same codes, US uses different codes
        bref_region = _app_region if _app_region in ("APAC", "EMEA") else "US"
        is_company_name_valid = True
        
        st.info(f"📝 Using fiscal year **{bref_target_year}** from manual upload")
    else:
        # HKEX/SEC: Get from extraction results
        first_result = next(iter(extraction_results.values()), {})
        bref_company_name = first_result.get("company", "Unknown Company")
        bref_target_year = first_result.get("target_year", datetime.now().year)
        _app_region = st.session_state.get("selected_region", "")
        # Map region: APAC and EMEA use same codes, US uses different codes
        bref_region = _app_region if _app_region in ("APAC", "EMEA") else "US"
        is_company_name_valid = True
        
        st.info(f"📝 Company: **{bref_company_name}** | Fiscal Year: **{bref_target_year}** | Region: **{bref_region}**")
    
    # Use Gemma by default for BREF mapping (no UI selection)
    from src.extraction.model_config import get_extraction_model
    bref_provider, bref_model_id = get_extraction_model()
    
    st.markdown("---")
    
    # ==================================================================
    # STEP 1: Select Statements to Map
    # ==================================================================
    st.subheader("Step 1: Select Statements to Map")
    
    # Client Type Selection inside Step 1
    client_type = st.selectbox(
        "Select Client Type",
        options=["New Client", "Existing Client"],
        key=f"{key_prefix}_client_type",
        help="New Client: Raw mapping without validation | Existing Client: Mapping with validation against template"
    )
    
    st.markdown("**Select which statements to map:**")
    
    # Create checkboxes for each available statement
    col1, col2, col3 = st.columns(3)
    
    selected_statements = []
    
    with col1:
        if "income_statement" in extraction_results:
            if st.checkbox(
                "Income Statement",
                value=True,  # Default checked
                key=f"{key_prefix}_multi_cb_income",
                help=f"{len(extraction_results['income_statement'].get('rows', []))} rows extracted"
            ):
                selected_statements.append("income_statement")
    
    with col2:
        if "balance_sheet" in extraction_results:
            if st.checkbox(
                "Balance Sheet",
                value=True,  # Default checked
                key=f"{key_prefix}_multi_cb_balance",
                help=f"{len(extraction_results['balance_sheet'].get('rows', []))} rows extracted"
            ):
                selected_statements.append("balance_sheet")
    
    with col3:
        if "cash_flow" in extraction_results:
            if st.checkbox(
                "Cash Flow Statement",
                value=True,  # Default checked
                key=f"{key_prefix}_multi_cb_cashflow",
                help=f"{len(extraction_results['cash_flow'].get('rows', []))} rows extracted"
            ):
                selected_statements.append("cash_flow")
    
    if not selected_statements:
        st.warning("⚠️ Please select at least one statement to map")
        return
    
    st.info(f"✓ Selected {len(selected_statements)} statement(s): {', '.join([STATEMENT_LABELS.get(s, s) for s in selected_statements])}")
    
    st.markdown("---")
    
    # ==================================================================
    # STEP 2: Mapping Mode (Based on Client Type)
    # ==================================================================
    st.subheader("Step 2: Start Mapping")
    
    if client_type == "New Client":
        # Raw Mapping for New Client
        st.markdown("### 🚀 New Client Mapping")
        st.markdown("""
        - Uses `field_mappings.py`
        - No Excel template needed
        - No validation
        - **Fast alias matching + calculations**
        - LLM only for unmatched fields
        """)
        
        use_fast_mapping = True  # Always enabled
        
        if st.button(
            f"🚀 Start Raw Mapping ({len(selected_statements)} statements)",
            key=f"{key_prefix}_multi_raw_map",
            use_container_width=True,
            type="primary",
            disabled=not is_company_name_valid,
            help="Company name is required" if not is_company_name_valid else f"Map {len(selected_statements)} statement(s) without validation"
        ):
            _run_multi_statement_mapping(
                selected_statements=selected_statements,
                extraction_results=extraction_results,
                company_name=bref_company_name,
                target_year=bref_target_year,
                region=bref_region,
                provider=bref_provider,
                model_id=bref_model_id,
                key_prefix=key_prefix,
                mode="raw",
                use_fast_mapping=use_fast_mapping
            )
    
    else:  # Existing Client
        # Validated Mapping for Existing Client
        st.markdown("### ✅ Existing Client Mapping")
        st.markdown("""
        - Requires Excel template
        - Validates against reference year
        - Higher accuracy
        - Human review for low confidence
        """)
        
        bref_file = st.file_uploader(
            "Upload BREF Template",
            type=["xlsx"],
            key=f"{key_prefix}_multi_bref_upload",
            help="Upload NEXTERA or similar template"
        )
        
        ignore_extract = st.checkbox(
            "Load all fields (ignore Extract column)",
            value=False,
            key=f"{key_prefix}_multi_ignore_extract",
            help="If checked, loads all fields regardless of Extract column value."
        )
        
        if bref_file:
            st.caption(f"✅ {bref_file.name}")
            
            if st.button(
                f"✅ Start Validated Mapping ({len(selected_statements)} statements)",
                use_container_width=True,
                type="primary",
                key=f"{key_prefix}_multi_validated_map",
                disabled=not is_company_name_valid,
                help="Company name is required" if not is_company_name_valid else f"Map {len(selected_statements)} statement(s) with validation"
            ):
                _run_multi_statement_mapping(
                    selected_statements=selected_statements,
                    extraction_results=extraction_results,
                    company_name=bref_company_name,
                    target_year=bref_target_year,
                    region=bref_region,
                    provider=bref_provider,
                    model_id=bref_model_id,
                    key_prefix=key_prefix,
                    mode="validated",
                    bref_file=bref_file,
                    ignore_extract=ignore_extract
                )
    
    st.markdown("---")
    
    # ==================================================================
    # STEP 4: Display Results in Tabs
    # ==================================================================
    # Check if we have any mapping results
    mapping_keys = [f"{key_prefix}_mapping_{stmt}" for stmt in selected_statements]
    available_results = [key for key in mapping_keys if key in st.session_state.bref_mapping_results]
    
    if available_results:
        st.subheader("Step 3: Results & Review")
        
        # Extract statement type from mapping key (e.g., "hkex_mapping_income_statement" -> "income_statement")
        statement_types_from_keys = []
        for key in available_results:
            # Remove key_prefix and "mapping_" to get statement type
            parts = key.replace(f"{key_prefix}_mapping_", "")
            statement_types_from_keys.append(parts)
        
        # Get target year and company name from first result (for later use in download)
        first_mapping = st.session_state.bref_mapping_results[available_results[0]]
        result_target_year = first_mapping.get("target_year", datetime.now().year)
        result_company_name = first_mapping.get("company_name", "Company")
        
        # Create tabs for each mapped statement
        tab_labels = [
            STATEMENT_LABELS.get(stmt_type, stmt_type) 
            for stmt_type in statement_types_from_keys
        ]
        tabs = st.tabs(tab_labels)
        
        for tab, mapping_key, statement_type in zip(tabs, available_results, statement_types_from_keys):
            with tab:
                _display_mapping_results_tab(mapping_key, statement_type, key_prefix)
        
        # Combined download button AFTER tabs
        st.markdown("---")
        st.markdown("### 📥 Download All Results")
        _render_combined_download(available_results, statement_types_from_keys, key_prefix, result_target_year, result_company_name)


def _run_multi_statement_mapping(
    selected_statements: list,
    extraction_results: dict,
    company_name: str,
    target_year: int,
    region: str,
    provider: str,
    model_id: str,
    key_prefix: str,
    mode: str = "raw",
    bref_file=None,
    ignore_extract: bool = False,
    use_fast_mapping: bool = True
):
    """Run mapping for multiple statements sequentially."""
    
    total_statements = len(selected_statements)
    
    # Show warning about long operation
    st.warning(f"⏳ **IMPORTANT:** Mapping {total_statements} statements may take 3-5 minutes. Please keep this page open and do NOT refresh!")
    
    # Create a progress bar
    progress_bar = st.progress(0)
    progress_text = st.empty()
    
    with st.status(f"🔄 Mapping {total_statements} statement(s)...", expanded=True) as status:
        for idx, statement_type in enumerate(selected_statements, 1):
            st.write(f"**[{idx}/{total_statements}] Mapping {STATEMENT_LABELS.get(statement_type, statement_type)}...**")
            st.write(f"  ⏳ This may take 60-90 seconds. Progress updates will appear in the logs below.")
            
            # Get extraction results for this statement
            result = extraction_results.get(statement_type, {})
            rows = result.get("rows", [])
            
            if not rows:
                st.warning(f"⚠️ No extracted data for {STATEMENT_LABELS.get(statement_type, statement_type)}")
                continue
            
            try:
                if mode == "raw":
                    # Raw mapping
                    _map_single_statement_raw(
                        statement_type=statement_type,
                        rows=rows,
                        company_name=company_name,
                        target_year=target_year,
                        region=region,
                        provider=provider,
                        model_id=model_id,
                        key_prefix=key_prefix,
                        use_fast_mapping=use_fast_mapping
                    )
                else:
                    # Validated mapping
                    _map_single_statement_validated(
                        statement_type=statement_type,
                        rows=rows,
                        company_name=company_name,
                        target_year=target_year,
                        region=region,
                        provider=provider,
                        model_id=model_id,
                        key_prefix=key_prefix,
                        bref_file=bref_file,
                        ignore_extract=ignore_extract
                    )
                
                st.success(f"✅ {STATEMENT_LABELS.get(statement_type, statement_type)} mapped successfully")
            
            except Exception as e:
                st.error(f"❌ Failed to map {STATEMENT_LABELS.get(statement_type, statement_type)}: {e}")
                import traceback
                with st.expander("🐛 Error Details"):
                    st.code(traceback.format_exc(), language="python")
        
        status.update(label=f"✅ Mapping completed for {total_statements} statement(s)!", state="complete")


def _map_single_statement_raw(
    statement_type: str,
    rows: list,
    company_name: str,
    target_year: int,
    region: str,
    provider: str,
    model_id: str,
    key_prefix: str,
    use_fast_mapping: bool = True
):
    """Map a single statement using raw mapping (no validation).
    
    Also extracts all available years from the extraction results.
    
    Args:
        use_fast_mapping: If True, use fast alias matching + calculations before LLM
    """
    
    # Get field mappings
    bref_field_dict = get_field_mappings(region).get(statement_type, {})
    
    if not bref_field_dict:
        raise ValueError(f"No BREF field mappings defined for {statement_type}")
    
    # Convert to fields list
    fields = []
    for label, field_data in bref_field_dict.items():
        if isinstance(field_data, dict):
            aliases = field_data.get('aliases', [])
            description = ", ".join(aliases) if aliases else ""
        elif isinstance(field_data, list):
            description = ", ".join(field_data)
        else:
            description = str(field_data)
        
        fields.append({
            "label": label,
            "description": description,
            "reference_value": None,
        })
    
    st.write(f"  📋 Comparing {len(fields)} BREF fields")
    
    # Extract all available years from the extraction results
    available_years = []
    if rows:
        first_row = rows[0]
        # Debug: Show all column names
        all_columns = list(first_row.keys())
        st.write(f"  🔍 DEBUG: All columns in extraction: {all_columns}")
        
        # Extract years from column names (handles formats like "2024-12-31 (FY)" or "2024")
        import re
        for col_name in first_row.keys():
            st.write(f"  🔍 DEBUG: Checking column '{col_name}'")
            if col_name not in ['label', 'parent', 'parent_abstract_concept', 'Currency', 'Unit']:
                # Try to extract 4-digit year from column name
                year_match = re.search(r'(\d{4})', str(col_name))
                if year_match:
                    year = int(year_match.group(1))
                    st.write(f"  🔍 DEBUG: Found year {year} in column '{col_name}'")
                    if year not in available_years and 2000 <= year <= 2030:  # Sanity check
                        available_years.append(year)
                        st.write(f"  ✅ DEBUG: Added year {year} to available_years")
        
        available_years = sorted(available_years)  # Sort ascending (2023, 2024, 2025...)
        st.write(f"  🔍 DEBUG: Years found (regex extraction): {available_years}")
        st.write(f"  🔍 DEBUG: Total years extracted: {len(available_years)}")
    
    # Check if reference year (target_year - 1) exists in the data
    # ONLY for SEC EDGAR workflow
    reference_year = target_year - 1
    if key_prefix == "sec" and reference_year not in available_years:
        # Check if reference year exists in the data by looking for column with that year
        ref_year_found = False
        if rows:
            for col_name in rows[0].keys():
                if str(reference_year) in str(col_name) and re.search(r'\d{4}', str(col_name)):
                    # Found a column with reference year
                    available_years.append(reference_year)
                    available_years = sorted(available_years)
                    ref_year_found = True
                    st.write(f"  ✅ Found reference year {reference_year} in column '{col_name}'")
                    break
        
        if not ref_year_found:
            st.write(f"  ⚠️ Reference year {reference_year} not found in extraction data")
    
    st.write(f"  📅 Available years in data: {', '.join(map(str, available_years))}")
    st.write(f"  📅 Target year for mapping: {target_year}")
    st.write(f"  📅 Reference year: {reference_year} {'(found)' if reference_year in available_years else '(not found)'}")
    
    # Store column name mapping for year access
    year_to_column = {}
    if rows:
        for col_name in rows[0].keys():
            year_match = re.search(r'(\d{4})', str(col_name))
            if year_match:
                year = int(year_match.group(1))
                if year in available_years:
                    year_to_column[year] = col_name
    st.write(f"  🗂️ Year to column mapping: {year_to_column}")
    
    # CRITICAL FIX: Adjust target_year if it's not in available_years
    # This happens with SEC EDGAR when fiscal year 2024 is requested but data only has 2021-2023
    if target_year not in available_years and available_years:
        original_target_year = target_year
        target_year = max(available_years)  # Use the most recent year in the data
        st.warning(f"⚠️ Target year {original_target_year} not found in data. Using most recent year: {target_year}")
        st.write(f"  📅 Adjusted target year: {original_target_year} → {target_year}")
    
    # Initialize mapped_fields
    mapped_fields = []
    
    # Create logger
    with st.expander("📝 Mapping Logs", expanded=True):
        mapping_log_placeholder = st.empty()
        mapping_logger = BREFLiveLogger(mapping_log_placeholder)
        
        import contextlib
        
        with contextlib.redirect_stdout(mapping_logger):
            # Phase 1: Fast mapping (alias matching + calculations)
            print("\n" + "="*60)
            print("FAST MAPPING (Alias Matching + Calculations)")
            print("="*60)
            
            matched_fields, unmatched_fields = fast_map_fields(
                fields=fields,
                extraction_rows=rows,
                available_years=available_years,
                field_mappings=bref_field_dict,
                target_year=target_year
            )
            
            # Add matched fields to mapped_fields
            mapped_fields.extend(matched_fields)
            
            # Phase 2: LLM mapping for unmatched fields only
            llm_mapped_fields = []
            if unmatched_fields:
                print("\n" + "="*60)
                print(f"LLM MAPPING ({len(unmatched_fields)} unmatched fields)")
                print("="*60)
                
                # Try with Gemma first, fallback to GPT-5.5 if it fails
                try:
                    llm_mapped_fields = map_all_fields(
                        fields=unmatched_fields,
                        extracted_rows=rows,
                        company_name=company_name,
                        target_year=target_year,
                        provider=provider,
                        model=model_id
                    )
                except Exception as e:
                    print(f"\n⚠️ Primary model ({provider}/{model_id}) failed: {str(e)}")
                    print("🔄 Falling back to GPT-5.5...\n")
                    
                    from src.extraction.model_config import get_fallback_model
                    fallback_provider, fallback_model = get_fallback_model()
                    
                    llm_mapped_fields = map_all_fields(
                        fields=unmatched_fields,
                        extracted_rows=rows,
                        company_name=company_name,
                        target_year=target_year,
                        provider=fallback_provider,
                        model=fallback_model
                    )
                
                # Combine results
                mapped_fields.extend(llm_mapped_fields)
            
            # CRITICAL FIX: Extract year_values for LLM-mapped fields from extraction rows
            # This runs for ALL LLM-mapped fields, not just unmatched ones
            if llm_mapped_fields:
                print("\n" + "="*60)
                print("EXTRACTING MULTI-YEAR VALUES FOR LLM-MAPPED FIELDS")
                print("="*60)
                
                for field in llm_mapped_fields:
                    matched_label = field.get("matched_label")
                    if matched_label and matched_label != "—":
                        # Find the matching row in extraction_rows
                        matching_row = None
                        for row in rows:
                            if row.get("label", "").lower().strip() == matched_label.lower().strip():
                                matching_row = row
                                break
                        
                        if matching_row:
                            # Extract values for all available years
                            year_values = {}
                            for year in available_years:
                                year_col = year_to_column.get(year)
                                if year_col and year_col in matching_row:
                                    year_value = matching_row[year_col]
                                    if year_value is not None and str(year_value).strip() != "":
                                        # CRITICAL FIX: Remove commas from numbers (LLM returns formatted numbers like '17,069')
                                        # This prevents float conversion errors
                                        try:
                                            # Remove commas and convert to float
                                            clean_value = str(year_value).replace(',', '').strip()
                                            year_values[str(year)] = float(clean_value)
                                        except (ValueError, TypeError):
                                            # Skip values that can't be converted to float
                                            print(f"  ⚠️  Could not convert '{year_value}' to float for year {year}")
                                            pass
                            
                            if year_values:
                                field["year_values"] = year_values
                                print(f"  ✅ Extracted {len(year_values)} year values for '{field.get('label')}': {year_values}")
                            else:
                                print(f"  ⚠️  No year values found for '{field.get('label')}'")
            
            print("\n" + "="*60)
            print("MAPPING COMPLETE")
            print("="*60)
            
            # ==================================================================
            # CRITICAL FIX: Apply sign corrections BEFORE calculations
            # ==================================================================
            print("\n" + "="*60)
            print("APPLYING SIGN CORRECTIONS (BEFORE CALCULATIONS)")
            print("="*60)
            
            from src.mapping.region_adjustments import apply_sign_corrections
            mapped_fields = apply_sign_corrections(
                fields=mapped_fields,
                region=region,
                statement_type=statement_type
            )
    
    # mapped_fields is now defined and available outside the context manager
    
    # ==================================================================
    # CALCULATED FIELDS - Add derived fields based on formulas
    # ==================================================================
    from src.mapping.bref_calculated import (
        calculate_all_fields,
        create_ordered_output,
        get_calculated_fields
    )
    from src.mapping.region_adjustments import apply_sign_corrections
    
    # Create a new expander for calculation logs
    with st.expander("🧮 Calculation Logs (Multi-Year)", expanded=True):
        calc_log_placeholder = st.empty()
        calc_logger = BREFLiveLogger(calc_log_placeholder)
        
        with contextlib.redirect_stdout(calc_logger):
            print("\n" + "="*60)
            print("CALCULATING DERIVED FIELDS FOR ALL YEARS")
            print("="*60)
            
            # Calculate for ALL available years, not just target year
            all_years_calculated = {}
            
            for year in available_years:
                year_str = str(year)
                print(f"\n  Calculating for year {year}...")
                
                # Convert mapped_fields list to dict for this year
                mapped_dict_year = {}
                for field in mapped_fields:
                    label = field.get("label")
                    # Try to get value for this specific year
                    year_value = None
                    if "year_values" in field and field["year_values"]:
                        year_value = field["year_values"].get(year_str)
                    elif year == target_year:
                        year_value = field.get("target_value")
                    
                    # CRITICAL FIX: Filter out None AND empty strings (SEC EDGAR returns '' for missing values)
                    # Empty strings cause calculation failures because they can't be converted to float
                    # ALSO: Remove commas from numbers (LLM returns formatted numbers like '17,069')
                    if label and year_value is not None and str(year_value).strip() != "":
                        try:
                            # Remove commas and convert to float to ensure it's a valid number
                            clean_value = str(year_value).replace(',', '').strip()
                            mapped_dict_year[label] = float(clean_value)
                        except (ValueError, TypeError):
                            # Skip invalid values (can't convert to number)
                            pass
                
                # Calculate all derived fields for this year
                mapped_with_calculated_year = calculate_all_fields(
                    mapped_values=mapped_dict_year,
                    statement_type=statement_type,
                    region=region
                )
                
                # CRITICAL FIX: Update mapped_dict_year with calculated values
                # This allows dependent calculations to use newly calculated fields
                mapped_dict_year.update(mapped_with_calculated_year)
                
                # Store calculated values for this year
                # IMPORTANT: Strip * prefix from calculated field keys for consistent lookup
                # CRITICAL FIX: Filter out empty strings when storing calculated values
                for field_key, value in mapped_with_calculated_year.items():
                    # Remove * prefix if present (calculated fields are marked with *)
                    clean_key = field_key.lstrip("*")
                    
                    # Skip empty strings and None values (SEC EDGAR returns '' for failed calculations)
                    if value is None or (isinstance(value, str) and value.strip() == ""):
                        continue
                    
                    if clean_key not in all_years_calculated:
                        all_years_calculated[clean_key] = {}
                    all_years_calculated[clean_key][year_str] = value
            
            # DEBUG: Show what's in all_years_calculated
            print(f"\n  DEBUG: all_years_calculated keys: {list(all_years_calculated.keys())[:10]}...")  # Show first 10
            for key in list(all_years_calculated.keys())[:3]:  # Show details for first 3
                print(f"  DEBUG: all_years_calculated['{key}'] = {all_years_calculated[key]}")
    
    # Use target year calculations for the final ordered output
    mapped_dict_target = {}
    for field in mapped_fields:
        label = field.get("label")
        target_value = field.get("target_value")
        if label and target_value is not None:
            mapped_dict_target[label] = target_value
    
    mapped_with_calculated = calculate_all_fields(
        mapped_values=mapped_dict_target,
        statement_type=statement_type,
        region=region
    )
    
    # Create ordered output (all fields in NEXTERA order)
    final_ordered = create_ordered_output(
        mapped_values=mapped_with_calculated,
        statement_type=statement_type,
        region=region
    )
    
    # Convert back to list format for storage
    # Merge calculated fields back into mapped_fields
    calculated_fields_dict = get_calculated_fields(statement_type, region)
    
    # CRITICAL FIX: Ensure ALL calculated fields appear in output, even if they couldn't be calculated
    # First, add all calculated fields that are NOT in mapped_fields yet
    existing_labels = {f.get("label") for f in mapped_fields}
    
    print(f"\n  DEBUG: Checking {len(calculated_fields_dict)} calculated fields...")
    print(f"  DEBUG: all_years_calculated has {len(all_years_calculated)} entries")
    
    for calc_field_label in calculated_fields_dict.keys():
        if calc_field_label not in existing_labels:
            # Check if we have calculated values for this field in all_years_calculated
            calc_year_values = all_years_calculated.get(calc_field_label, {})
            
            print(f"  DEBUG: Calculated field '{calc_field_label}':")
            print(f"    year_values from all_years_calculated: {calc_year_values}")
            
            if calc_year_values:
                # We have calculated values! Add them
                reference_year = target_year - 1
                reference_value = calc_year_values.get(str(reference_year))
                target_value = calc_year_values.get(str(target_year))
                
                print(f"    ✅ Adding with values: target={target_value}, ref={reference_value}")
                
                mapped_fields.append({
                    "label": calc_field_label,
                    "target_value": target_value,
                    "year_values": calc_year_values,
                    "reference_value": reference_value,
                    "mapping_method": "calculation",
                    "mapping_confidence": "high",
                    "matched_label": "(Calculated)",
                    "reason": f"Calculated using formula: {calculated_fields_dict[calc_field_label].get('calculation', 'N/A')}",
                    "is_calculated": True
                })
            else:
                # No calculated values - add as blank
                print(f"    ❌ No calculated values found - adding as blank")
                
                mapped_fields.append({
                    "label": calc_field_label,
                    "target_value": None,
                    "year_values": {},
                    "reference_value": None,
                    "mapping_method": "unmapped",
                    "mapping_confidence": "low",
                    "matched_label": "—",
                    "reason": f"Could not calculate (missing dependencies). Formula: {calculated_fields_dict[calc_field_label].get('calculation', 'N/A')}",
                    "is_calculated": True
                })
    
    for field_key, value in final_ordered.items():
        # Check if this is a calculated field (starts with *)
        is_calc_field = field_key.startswith("*")
        clean_key = field_key.lstrip("*")
        
        # Find if field already exists in mapped_fields
        existing_field = next((f for f in mapped_fields if f.get("label") == clean_key), None)
        
        # Check for validation flags
        validation_status = mapped_with_calculated.get(clean_key + "_validation")
        calculated_alt_value = mapped_with_calculated.get(clean_key + "_calculated")
        diff_percent = mapped_with_calculated.get(clean_key + "_diff_percent")
        
        if is_calc_field and value != "":
            # This is a calculated field with a value
            if existing_field:
                # Field was both extracted AND calculated
                # Update with validation info AND year_values
                existing_field["target_value"] = value
                existing_field["is_calculated"] = True
                
                # CRITICAL FIX: Update year_values with calculated values for ALL years
                calc_year_values = all_years_calculated.get(clean_key, {})
                print(f"  DEBUG: Field '{clean_key}' - calc_year_values from all_years_calculated: {calc_year_values}")
                
                if calc_year_values:
                    # Merge calculated year_values with existing year_values
                    if "year_values" not in existing_field:
                        existing_field["year_values"] = {}
                    
                    print(f"  DEBUG: Before update - existing year_values: {existing_field.get('year_values', {})}")
                    existing_field["year_values"].update(calc_year_values)
                    print(f"  DEBUG: After update - existing year_values: {existing_field['year_values']}")
                    
                    # Also update reference_value
                    reference_year = target_year - 1
                    reference_value = calc_year_values.get(str(reference_year))
                    if reference_value is not None:
                        existing_field["reference_value"] = reference_value
                        print(f"  DEBUG: Updated reference_value to: {reference_value}")
                else:
                    print(f"  WARNING: No calculated year values found for '{clean_key}' in all_years_calculated")
                
                if validation_status == "VALIDATED":
                    # Extracted and calculated values match
                    existing_field["mapping_method"] = "extracted+validated"
                    existing_field["mapping_confidence"] = "high"
                    existing_field["reason"] = f"Extracted value validated by calculation (formula: {calculated_fields_dict.get(clean_key, {}).get('calculation', 'N/A')})"
                elif validation_status == "MISMATCH":
                    # Extracted and calculated values don't match - flag for review
                    existing_field["mapping_method"] = "extracted+mismatch"
                    existing_field["mapping_confidence"] = "low"
                    existing_field["reason"] = f"⚠️ MISMATCH: Extracted={value}, Calculated={calculated_alt_value} (diff: {diff_percent:.1f}%). Please review!"
                    existing_field["calculated_value"] = calculated_alt_value
                    existing_field["diff_percent"] = diff_percent
                elif validation_status == "EXTRACTED_ONLY":
                    # Could not calculate for validation
                    existing_field["mapping_method"] = existing_field.get("mapping_method", "extracted")
                    existing_field["reason"] = existing_field.get("reason", "") + " (Could not validate via calculation)"
            else:
                # Add new calculated field (not extracted)
                confidence = "high" if validation_status == "CALCULATED_ONLY" else "medium"
                
                # Get year_values for this calculated field from all_years_calculated
                calc_year_values = all_years_calculated.get(clean_key, {})
                
                # Also get reference year value
                reference_year = target_year - 1
                reference_value = calc_year_values.get(str(reference_year))
                
                mapped_fields.append({
                    "label": clean_key,
                    "target_value": value,
                    "year_values": calc_year_values,  # Add year_values for all years
                    "reference_value": reference_value,  # Add reference year value
                    "mapping_method": "calculation",
                    "mapping_confidence": confidence,
                    "matched_label": "(Calculated)",
                    "reason": f"Calculated using formula: {calculated_fields_dict.get(clean_key, {}).get('calculation', 'N/A')}",
                    "is_calculated": True
                })
        elif not is_calc_field and value == "" and not existing_field:
            # This is an unmapped field - add it as blank
            mapped_fields.append({
                "label": clean_key,
                "target_value": None,
                "mapping_method": "unmapped",
                "mapping_confidence": "low",
                "matched_label": "—",
                "reason": "Not found in annual report",
                "is_calculated": False
            })
    
            print(f"\n✅ Final output: {len(final_ordered)} fields (including calculated and unmapped)")
            print("="*60)
    
    # ==================================================================
    # REORDER FIELDS - Sort according to template order
    # ==================================================================
    # Create a mapping of field labels to their order in the template
    field_order = {label: idx for idx, label in enumerate(bref_field_dict.keys())}
    
    # Sort mapped_fields according to template order
    def get_field_order(field):
        label = field.get("label")
        return field_order.get(label, 999999)  # Put unknown fields at the end
    
    mapped_fields.sort(key=get_field_order)
    print(f"\n✅ Sorted {len(mapped_fields)} fields according to template order")
    
    # ==================================================================
    # APPLY SIGN CORRECTIONS AGAIN (AFTER CALCULATIONS)
    # This ensures calculated fields also have correct signs
    # ==================================================================
    print(f"\n🔧 Applying sign corrections to calculated fields...")
    from src.mapping.region_adjustments import apply_sign_corrections
    mapped_fields = apply_sign_corrections(
        fields=mapped_fields,
        region=region,
        statement_type=statement_type
    )
    
    # Add metadata
    for field in mapped_fields:
        field["mode"] = "raw"
        field["final_confidence"] = field.get("mapping_confidence", "low")
        field["validation_status"] = "unverified"
    
    # Count by mapping method
    alias_matched = sum(1 for f in mapped_fields if f.get('mapping_method') == 'alias_match')
    calculated = sum(1 for f in mapped_fields if f.get('mapping_method') == 'calculation')
    llm_mapped = sum(1 for f in mapped_fields if f.get('mapping_method') not in ['alias_match', 'calculation', 'unmapped'])
    
    high_conf = sum(1 for f in mapped_fields if f.get('mapping_confidence') == 'high')
    low_conf = sum(1 for f in mapped_fields if f.get('mapping_confidence') == 'low')
    
    st.write(f"  ✅ {len(mapped_fields)} fields mapped:")
    st.write(f"     🎯 {alias_matched} via alias matching (instant)")
    st.write(f"     🧠 {calculated} via calculations (instant)")
    st.write(f"     🤖 {llm_mapped} via LLM (slow)")
    st.write(f"     ✅ {high_conf} high confidence, ⚠️ {low_conf} low confidence")
    
    # Note: available_years already includes reference year from earlier in the function
    # Don't re-extract it here as it would overwrite the corrected list
    st.write(f"  📅 Storing results with {len(available_years)} years: {', '.join(map(str, available_years))}")
    
    # Store results with all years (including reference year)
    mapping_key = f"{key_prefix}_mapping_{statement_type}"
    st.session_state.bref_mapping_results[mapping_key] = {
        "fields": mapped_fields,
        "mode": "raw",
        "target_year": target_year,
        "statement_type": statement_type,
        "company_name": company_name,
        "region": region,
        "available_years": available_years,
        "extraction_rows": rows,  # Store original rows for multi-year display
    }


def _map_single_statement_validated(
    statement_type: str,
    rows: list,
    company_name: str,
    target_year: int,
    region: str,
    provider: str,
    model_id: str,
    key_prefix: str,
    bref_file,
    ignore_extract: bool
):
    """Map a single statement using validated mapping (with Excel template)."""
    
    import tempfile
    import openpyxl
    import os
    
    # Save uploaded file to temp
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        tmp.write(bref_file.getvalue())
        tmp_path = tmp.name
    
    try:
        # Load template and find year columns
        wb = openpyxl.load_workbook(tmp_path)
        ws = wb.active
        
        ref_year = target_year - 1
        ref_col = find_year_column(ws, ref_year)
        target_col = find_year_column(ws, target_year)
        
        if ref_col:
            st.write(f"  ✅ Found reference year ({ref_year}) in column {ref_col}")
        if target_col:
            st.write(f"  ✅ Found target year ({target_year}) in column {target_col}")
        
        wb.close()
        
        # Load BREF fields from template
        field_mappings_dict = get_field_mappings(region).get(statement_type, {})
        fields = load_bref_fields(
            tmp_path,
            STATEMENT_SHEET_MAP[statement_type],
            target_year,
            field_mappings=field_mappings_dict,
            ignore_extract_column=ignore_extract
        )
        
        if not fields:
            raise ValueError(f"No BREF fields loaded from template for {statement_type}")
        
        ref_count = sum(1 for f in fields if f['reference_value'] is not None)
        st.write(f"  📋 Comparing {len(fields)} BREF fields ({ref_count} with reference values)")
        st.write(f"  🤖 Mapping using AI...")
        
        # Map fields
        with st.expander("📝 Mapping Logs", expanded=False):
            mapping_log_placeholder = st.empty()
            mapping_logger = BREFLiveLogger(mapping_log_placeholder)
            
            import contextlib
            
            with contextlib.redirect_stdout(mapping_logger):
                # Try with Gemma first, fallback to GPT-5.5 if it fails
                try:
                    mapped_fields = map_all_fields(
                        fields=fields,
                        extracted_rows=rows,
                        company_name=company_name,
                        target_year=target_year,
                        provider=provider,
                        model=model_id
                    )
                except Exception as e:
                    print(f"\n⚠️ Primary model ({provider}/{model_id}) failed: {str(e)}")
                    print("🔄 Falling back to GPT-5.5...\n")
                    
                    from src.extraction.model_config import get_fallback_model
                    fallback_provider, fallback_model = get_fallback_model()
                    
                    mapped_fields = map_all_fields(
                        fields=fields,
                        extracted_rows=rows,
                        company_name=company_name,
                        target_year=target_year,
                        provider=fallback_provider,
                        model=fallback_model
                    )
        
        st.write(f"  ✓ Validating mappings...")
        
        # Validate
        with st.expander("📝 Validation Logs", expanded=False):
            validation_log_placeholder = st.empty()
            validation_logger = BREFLiveLogger(validation_log_placeholder)
            
            import contextlib
            with contextlib.redirect_stdout(validation_logger):
                validated_fields = validate_mappings(mapped_fields)
        
        high_conf = sum(1 for f in validated_fields if f.get('final_confidence') == 'high')
        low_conf = sum(1 for f in validated_fields if f.get('final_confidence') == 'low')
        validated_count = sum(1 for f in validated_fields if f.get('validation_status') == 'validated')
        
        st.write(f"  ✅ {len(validated_fields)} fields: {high_conf} high confidence, {low_conf} low confidence, {validated_count} validated")
        
        # Generate Excel output
        excel_bytes = create_clean_output_excel(
            validated_fields,
            target_year=target_year,
            statement_type=statement_type
        )
        
        # Store results
        mapping_key = f"{key_prefix}_mapping_{statement_type}"
        st.session_state.bref_mapping_results[mapping_key] = {
            "fields": validated_fields,
            "mode": "validated",
            "target_year": target_year,
            "statement_type": statement_type,
            "company_name": company_name,
            "template_name": bref_file.name,
            "excel_bytes": excel_bytes,
            "region": region,
        }
    
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def _render_combined_download(mapping_keys: list, statement_types: list, key_prefix: str, target_year: int, company_name: str):
    """Render combined download button for all mapped statements."""
    
    # Create combined Excel with all statements
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for mapping_key, statement_type in zip(mapping_keys, statement_types):
            if mapping_key in st.session_state.bref_mapping_results:
                mapping_results = st.session_state.bref_mapping_results[mapping_key]
                fields = mapping_results["fields"]
                mode = mapping_results["mode"]
                available_years = mapping_results.get("available_years", [target_year])
                
                # Build dataframe
                reference_year = target_year - 1
                
                df_data = []
                for field in fields:
                    # Build row_data in Excel column order: BREF Field, Years (2023, 2024...), Annual Report Label, Confidence, Reason
                    row_data = {
                        "BREF Field": field.get("label"),
                    }
                    
                    # Add reference year first
                    ref_value = field.get("reference_value")
                    if ref_value is None:
                        # Try extracted_reference_value (used by LLM mapper)
                        ref_value = field.get("extracted_reference_value")
                    if ref_value is None and "year_values" in field:
                        ref_value = field["year_values"].get(str(reference_year))
                    row_data[str(reference_year)] = ref_value
                    
                    # Add all available years in ascending order
                    for year in available_years:
                        year_str = str(year)
                        year_value = None
                        
                        # Try year_values first
                        if "year_values" in field and year_str in field["year_values"]:
                            year_value = field["year_values"][year_str]
                        
                        # Fallback to target_value for target year
                        if year_value is None and year == target_year:
                            year_value = field.get("target_value")
                        
                        row_data[year_str] = year_value
                    
                    # Add Annual Report Label, Confidence, and Reason AFTER years
                    row_data["Annual Report Label"] = field.get("matched_label", "—")
                    row_data["Confidence"] = field.get("final_confidence", field.get("mapping_confidence", ""))
                    row_data["Reason"] = field.get("reason", "")
                    df_data.append(row_data)
                
                df = pd.DataFrame(df_data)
                
                # Use statement label as sheet name (truncate to 31 chars for Excel limit)
                sheet_name = STATEMENT_LABELS.get(statement_type, statement_type)[:31]
                df.to_excel(writer, sheet_name=sheet_name, index=False)
    
    output.seek(0)
    
    # Display download button
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.download_button(
            "📥 Download All Statements (Excel)",
            data=output.getvalue(),
            file_name=f"BREF_Mapping_{company_name.replace(' ', '_')}_{target_year}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            type="primary",
            key=f"{key_prefix}_multi_download_all"
        )
    
    # Show summary
    st.info(f"✅ Ready to download {len(statement_types)} statement(s): {', '.join([STATEMENT_LABELS.get(s, s) for s in statement_types])}")
    
    # ==================================================================
    # SUMMARY GENERATOR SECTION - Use the consolidated function from brefmap_ui
    # ==================================================================
    from src.components.brefmap_ui import _check_and_show_consolidated_download
    _check_and_show_consolidated_download(key_prefix)


def _display_mapping_results_tab(mapping_key: str, statement_type: str, key_prefix: str):

    """Display mapping results for a single statement in a tab"""
    
    if mapping_key not in st.session_state.bref_mapping_results:
        st.info("No mapping results available")
        return
    
    mapping_results = st.session_state.bref_mapping_results[mapping_key]
    fields = mapping_results["fields"]
    mode = mapping_results["mode"]
    target_year = mapping_results.get("target_year", datetime.now().year)
    available_years = mapping_results.get("available_years", [target_year])
    extraction_rows = mapping_results.get("extraction_rows", [])
    
    # Metrics
    col1, col2, col3, col4, col5 = st.columns(5)
    high_conf = sum(1 for f in fields if f.get('final_confidence', f.get('mapping_confidence')) == 'high')
    low_conf = sum(1 for f in fields if f.get('final_confidence', f.get('mapping_confidence')) == 'low')
    
    col1.metric("Total Fields", len(fields))
    col2.metric("High Confidence", high_conf)
    col3.metric("Low Confidence", low_conf)
    col4.metric("Mode", mode.upper())
    col5.metric("Years", f"{len(available_years)} years" if available_years else str(target_year))
    
    if low_conf > 0:
        st.warning(f"⚠️ {low_conf} field(s) have low confidence — edit values directly in the table below")
    
    st.markdown("---")
    
    # Build dataframe with all available years
    reference_year = target_year - 1
    
    # Create a lookup dict for extraction rows by label
    extraction_lookup = {}
    if extraction_rows:
        for row in extraction_rows:
            label = row.get('label', '')
            if label:
                extraction_lookup[label.lower().strip()] = row
    
    # Get field mappings to access indent_level
    from src.mapping.field_mappings import get_field_mappings
    field_mappings_dict = get_field_mappings(mapping_results.get("region", "US"))
    statement_fields = field_mappings_dict.get(statement_type, {})
    
    df_data = []
    for field in fields:
        # Mark calculated fields with * prefix
        field_label = field.get("label")
        
        # Get indent level from field mappings, or infer from field name
        field_def = statement_fields.get(field_label, {})
        indent_level = field_def.get("indent_level")
        
        # If indent_level not defined, infer from field name
        if indent_level is None:
            # Fields starting with "o/w" are indented (child fields)
            if "o/w" in field_label.lower():
                indent_level = 1
            else:
                indent_level = 0
        
        # Add indentation using non-breaking spaces (Streamlit strips regular spaces)
        # Use Unicode non-breaking space (\u00A0) which Streamlit preserves
        indent_prefix = "\u00A0\u00A0\u00A0\u00A0" * indent_level
        
        # Apply indentation FIRST (before any prefixes)
        field_label = indent_prefix + field_label
        
        # Then add calculated field marker
        if field.get("is_calculated", False):
            field_label = "*" + field_label
        
        # Add warning emoji for mismatched fields
        mapping_method = field.get("mapping_method", "")
        if "mismatch" in mapping_method.lower():
            field_label = "⚠️ " + field_label
        
        # Build row_data in Excel column order: BREF Field, Years (2023, 2024...), Annual Report Label, Confidence, Reason
        row_data = {
            "BREF Field": field_label,
        }
        
        # Add reference year first
        ref_year_str = str(reference_year)
        ref_value = None
        
        # Try reference_value first (used by fast_mapper)
        ref_value = field.get("reference_value")
        
        # Try extracted_reference_value (used by LLM mapper)
        if ref_value is None:
            ref_value = field.get("extracted_reference_value")
        
        # Try year_values
        if ref_value is None and "year_values" in field and field["year_values"]:
            ref_value = field["year_values"].get(ref_year_str)
        
        row_data[ref_year_str] = ref_value
        
        # Add all other years in ascending order
        for year in available_years:
            year_str = str(year)
            year_value = None
            
            # PRIORITY 1: Use year_values from field
            if "year_values" in field and field["year_values"]:
                year_value = field["year_values"].get(year_str)
            
            # PRIORITY 2: Use target_value for target year
            if year_value is None and year == target_year:
                year_value = field.get("target_value")
            
            # PRIORITY 3: Fallback to extraction rows (REMOVED - use corrected values only)
            # DO NOT fallback to extraction_rows as they contain uncorrected (negative) values
            # if year_value is None:
            #     matched_label = field.get("matched_label", "")
            #     if matched_label and extraction_lookup:
            #         extraction_row = extraction_lookup.get(matched_label.lower().strip())
            #         if extraction_row and year_str in extraction_row:
            #             year_value = extraction_row[year_str]
            
            row_data[year_str] = year_value
        
        # Add Annual Report Label, Confidence, and Reason AFTER years
        row_data["Annual Report Label"] = field.get("matched_label", "—")
        row_data["Confidence"] = field.get("final_confidence", field.get("mapping_confidence", ""))
        row_data["Reason"] = field.get("reason", "")
        df_data.append(row_data)
    
    df = pd.DataFrame(df_data)
    
    # Data editor
    column_config = {
        "BREF Field": st.column_config.TextColumn("BREF Field", disabled=True, width="large"),
        "Annual Report Label": st.column_config.TextColumn("Annual Report Label", help="Edit to correct the matched label", width="large"),
    }
    
    # Add reference year column (always show it, for both raw and validated modes)
    ref_year_col = str(reference_year)
    if ref_year_col in df.columns:
        column_config[ref_year_col] = st.column_config.NumberColumn(
            ref_year_col, 
            help=f"Reference year value ({reference_year})",
            format="%.2f"
        )
    
    # Add all year columns (excluding reference year as it's already added)
    for year in available_years:
        year_col = str(year)
        if year_col in df.columns and year != reference_year:
            column_config[year_col] = st.column_config.NumberColumn(
                year_col, 
                help=f"Value for year {year}",
                format="%.2f"
            )
    
    # Add confidence and reason columns
    column_config["Confidence"] = st.column_config.SelectboxColumn(
        "Confidence", options=["high", "low"], help="Set confidence level"
    )
    column_config["Reason"] = st.column_config.TextColumn("Reason", disabled=True)
    
    edited_df = st.data_editor(
        df,
        use_container_width=True,
        hide_index=True,
        height=min(400 + len(df) * 5, 800),
        column_config=column_config,
        key=f"{key_prefix}_multi_editor_{statement_type}",
    )
    
    # Action buttons - Save and Clear only (download is at the top for all statements)
    col_save, col_clear = st.columns([1, 1])
    
    with col_save:
        if st.button("💾 Save Changes", key=f"{key_prefix}_multi_save_{statement_type}", use_container_width=True, type="primary"):
            # Import recalculation functions
            from src.mapping.bref_calculated import recalculate_dependent_fields
            
            # Track which fields were updated
            updated_fields = {}
            
            for i, row in edited_df.iterrows():
                fields[i]["matched_label"] = row.get("Annual Report Label", fields[i].get("matched_label"))
                
                # Save reference year value (to both reference_value and extracted_reference_value)
                ref_year_str = str(reference_year)
                if ref_year_str in row:
                    ref_val = row.get(ref_year_str)
                    if ref_val is not None and str(ref_val).strip() != "":
                        try:
                            ref_val_float = float(ref_val)
                            fields[i]["reference_value"] = ref_val_float
                            fields[i]["extracted_reference_value"] = ref_val_float  # Also save to LLM mapper field
                        except (ValueError, TypeError):
                            pass
                
                # Save target year value and track changes
                target_year_str = str(target_year)
                if target_year_str in row:
                    new_val = row.get(target_year_str)
                    if new_val is not None and str(new_val).strip() != "":
                        try:
                            new_val_float = float(new_val)
                            old_val = fields[i].get("target_value")
                            
                            # Check if value changed
                            if old_val != new_val_float:
                                fields[i]["target_value"] = new_val_float
                                field_label = fields[i].get("label")
                                updated_fields[field_label] = new_val_float
                        except (ValueError, TypeError):
                            pass
                    elif new_val is None or str(new_val).strip() == "":
                        # Value was cleared - also track this as an update
                        old_val = fields[i].get("target_value")
                        if old_val is not None:
                            fields[i]["target_value"] = None
                            field_label = fields[i].get("label")
                            updated_fields[field_label] = None
                
                # Save all year values to a separate dict (including reference year)
                if "year_values" not in fields[i]:
                    fields[i]["year_values"] = {}
                
                # Save reference year to year_values
                if ref_year_str in row:
                    ref_val = row.get(ref_year_str)
                    if ref_val is not None and str(ref_val).strip() != "":
                        try:
                            fields[i]["year_values"][ref_year_str] = float(ref_val)
                        except (ValueError, TypeError):
                            pass
                
                # Save all other year values
                for year in available_years:
                    year_str = str(year)
                    if year_str in row:
                        year_val = row.get(year_str)
                        if year_val is not None and str(year_val).strip() != "":
                            try:
                                fields[i]["year_values"][year_str] = float(year_val)
                            except (ValueError, TypeError):
                                pass
                
                new_conf = row.get("Confidence", "")
                if new_conf in ("high", "low"):
                    fields[i]["final_confidence"] = new_conf
                    fields[i]["mapping_confidence"] = new_conf
                    if new_conf == "high" and fields[i].get("validation_status") != "validated":
                        fields[i]["validation_status"] = "human_verified"
            
            # Recalculate dependent fields if any values were updated
            if updated_fields:
                st.info(f"🔄 Recalculating {len(updated_fields)} updated field(s)...")
                
                # Build current values dict
                current_values = {}
                for field in fields:
                    label = field.get("label")
                    value = field.get("target_value")
                    if label and value is not None:
                        current_values[label] = value
                
                # Recalculate for each updated field
                all_recalculated = {}
                for field_label, new_value in updated_fields.items():
                    recalculated = recalculate_dependent_fields(
                        updated_field=field_label,
                        updated_value=new_value,
                        current_values=current_values,
                        statement_type=statement_type,
                        region=mapping_results.get("region", "US")
                    )
                    all_recalculated.update(recalculated)
                    # Update current_values for cascading calculations
                    current_values.update(recalculated)
                
                # Update fields with recalculated values
                if all_recalculated:
                    for field in fields:
                        field_label = field.get("label")
                        if field_label in all_recalculated:
                            field["target_value"] = all_recalculated[field_label]
                            field["mapping_confidence"] = "high"
                            field["final_confidence"] = "high"
                            field["reason"] = f"Recalculated due to dependency update"
                    
                    st.success(f"♻️ Recalculated {len(all_recalculated)} dependent field(s): {', '.join([k.split(' |')[0] for k in all_recalculated.keys()])}")
            
            st.session_state.bref_mapping_results[mapping_key]["fields"] = fields
            st.toast("✅ Changes saved")
            st.rerun()  # Refresh to show recalculated values
    
    with col_clear:
        if st.button("🗑️ Clear", key=f"{key_prefix}_multi_clear_{statement_type}", use_container_width=True):
            del st.session_state.bref_mapping_results[mapping_key]
            st.success("✅ Results cleared")
            st.rerun()
