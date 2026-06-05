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
    BREF_MAPPING_AVAILABLE = True
except ImportError:
    BREF_MAPPING_AVAILABLE = False

from src.components.brefmap_ui import (
    BREFLiveLogger,
    run_with_heartbeat,
    _extract_company_name_from_pdf,
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
    
    # ==================================================================
    # STEP 1: Configuration
    # ==================================================================
    st.subheader("Step 1: Configuration")
    
    col_name, col_year, col_region = st.columns([3, 1, 1])
    
    with col_name:
        # Auto-extract company name from first available statement
        auto_company_name = ""
        extraction_cache_key = f"{key_prefix}_multi_extracted_company_name"
        
        if extraction_cache_key in st.session_state:
            auto_company_name = st.session_state[extraction_cache_key]
        else:
            # Get first available statement for company name extraction
            first_statement = next(iter(extraction_results.values()), {})
            rows = first_statement.get("rows", [])
            
            if rows:
                with st.spinner("🔍 Extracting company name from PDF..."):
                    pdf_bytes = st.session_state.get('uploaded_pdf_bytes')
                    report_title = st.session_state.get(f'{key_prefix}_extraction_report_title', '')
                    auto_company_name = _extract_company_name_from_pdf(rows, report_title, pdf_bytes)
                    st.session_state[extraction_cache_key] = auto_company_name
        
        # Company name input
        manual_override_key = f"{key_prefix}_multi_manual_company_override"
        if manual_override_key not in st.session_state:
            st.session_state[manual_override_key] = False
        
        if auto_company_name:
            col_input, col_checkbox, col_reextract = st.columns([3, 0.5, 0.7])
            with col_input:
                if st.session_state[manual_override_key]:
                    bref_company_name = st.text_input(
                        "Company Name *",
                        value=auto_company_name,
                        key=f"{key_prefix}_multi_bref_company",
                        help="Company name extracted from PDF using AI"
                    )
                else:
                    st.text_input(
                        "Company Name *",
                        value=auto_company_name,
                        key=f"{key_prefix}_multi_bref_company_display",
                        disabled=True,
                        help="🤖 Auto-extracted from PDF using AI"
                    )
                    bref_company_name = auto_company_name
            
            with col_checkbox:
                st.markdown("<div style='padding-top: 28px;'></div>", unsafe_allow_html=True)
                st.checkbox(
                    "Edit",
                    value=st.session_state[manual_override_key],
                    key=f"{key_prefix}_multi_override_checkbox",
                    help="Enable manual editing of company name",
                    on_change=lambda: setattr(st.session_state, manual_override_key, not st.session_state[manual_override_key])
                )
            
            with col_reextract:
                st.markdown("<div style='padding-top: 28px;'></div>", unsafe_allow_html=True)
                if st.button("🔄", key=f"{key_prefix}_multi_reextract_company", help="Re-extract company name", use_container_width=True):
                    if extraction_cache_key in st.session_state:
                        del st.session_state[extraction_cache_key]
                    st.rerun()
        else:
            bref_company_name = st.text_input(
                "Company Name *",
                value="",
                placeholder="Enter Company Name (required)",
                key=f"{key_prefix}_multi_bref_company",
                help="Company name for BREF mapping (required)"
            )
    
    with col_year:
        # Get target year from first available result
        first_result = next(iter(extraction_results.values()), {})
        default_year = first_result.get("target_year", datetime.now().year)
        
        bref_target_year = st.number_input(
            "Target Year",
            min_value=2000,
            max_value=2030,
            value=default_year,
            step=1,
            key=f"{key_prefix}_multi_bref_year"
        )
    
    with col_region:
        _app_region = st.session_state.get("selected_region", "")
        _default_idx = 1 if _app_region == "APAC" else 0
        bref_region = st.selectbox(
            "Region",
            options=["US", "APAC"],
            index=_default_idx,
            key=f"{key_prefix}_multi_bref_region",
            help="US uses I-prefix codes, APAC uses Q-prefix codes"
        )
    
    # Model selection
    from src.extraction.model_config import render_model_selector
    st.markdown("**Select AI Model for Mapping:**")
    bref_provider, bref_model_id = render_model_selector(key_prefix=f"{key_prefix}_multi_bref")
    
    # Validate company name
    is_company_name_valid = bref_company_name and bref_company_name.strip() != ""
    if not is_company_name_valid:
        st.error("⚠️ Company name is required for BREF mapping")
    
    st.markdown("---")
    
    # ==================================================================
    # STEP 2: Select Statements to Map
    # ==================================================================
    st.subheader("Step 2: Select Statements to Map")
    
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
    # STEP 3: Select Mapping Mode
    # ==================================================================
    st.subheader("Step 3: Select Mapping Mode")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Raw Mapping")
        st.markdown("""
        - Uses `field_mappings.py`
        - No Excel template needed
        - No validation
        - Faster processing
        """)
        
        if st.button(
            f"🚀 Start Raw Mapping ({len(selected_statements)} statements)",
            key=f"{key_prefix}_multi_raw_map",
            use_container_width=True,
            type="secondary",
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
                mode="raw"
            )
    
    with col2:
        st.markdown("### ✅ Mapping with Validation")
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
        st.subheader("Step 4: Results & Review")
        
        # Create tabs for each mapped statement
        # Extract statement type from mapping key (e.g., "hkex_mapping_income_statement" -> "income_statement")
        statement_types_from_keys = []
        for key in available_results:
            # Remove key_prefix and "mapping_" to get statement type
            parts = key.replace(f"{key_prefix}_mapping_", "")
            statement_types_from_keys.append(parts)
        
        tab_labels = [
            STATEMENT_LABELS.get(stmt_type, stmt_type) 
            for stmt_type in statement_types_from_keys
        ]
        tabs = st.tabs(tab_labels)
        
        for tab, mapping_key, statement_type in zip(tabs, available_results, statement_types_from_keys):
            with tab:
                _display_mapping_results_tab(mapping_key, statement_type, key_prefix)


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
    ignore_extract: bool = False
):
    """Run mapping for multiple statements sequentially."""
    
    total_statements = len(selected_statements)
    
    with st.status(f"🔄 Mapping {total_statements} statement(s)...", expanded=True) as status:
        for idx, statement_type in enumerate(selected_statements, 1):
            st.write(f"**[{idx}/{total_statements}] Mapping {STATEMENT_LABELS.get(statement_type, statement_type)}...**")
            
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
                        key_prefix=key_prefix
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
    key_prefix: str
):
    """Map a single statement using raw mapping (no validation)."""
    
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
    
    st.write(f"  📋 Loaded {len(fields)} BREF fields")
    st.write(f"  🤖 Mapping using AI...")
    
    # Create progress placeholder for keep-alive
    progress_placeholder = st.empty()
    progress_placeholder.info("🔄 Mapping in progress...")
    
    # Create logger
    with st.expander("📝 Mapping Logs", expanded=False):
        mapping_log_placeholder = st.empty()
        mapping_logger = BREFLiveLogger(mapping_log_placeholder)
        
        import contextlib
        
        def _run_mapping():
            with contextlib.redirect_stdout(mapping_logger):
                return map_all_fields(
                    fields=fields,
                    extracted_rows=rows,
                    company_name=company_name,
                    target_year=target_year,
                    provider=provider,
                    model=model_id
                )
        
        mapped_fields = run_with_heartbeat(_run_mapping, progress_placeholder)
        progress_placeholder.success("✅ Mapping completed")
    
    # Add metadata
    for field in mapped_fields:
        field["mode"] = "raw"
        field["final_confidence"] = field.get("mapping_confidence", "low")
        field["validation_status"] = "unverified"
    
    high_conf = sum(1 for f in mapped_fields if f.get('mapping_confidence') == 'high')
    low_conf = sum(1 for f in mapped_fields if f.get('mapping_confidence') == 'low')
    st.write(f"  ✅ {len(mapped_fields)} fields: {high_conf} high confidence, {low_conf} low confidence")
    
    # Store results
    mapping_key = f"{key_prefix}_mapping_{statement_type}"
    st.session_state.bref_mapping_results[mapping_key] = {
        "fields": mapped_fields,
        "mode": "raw",
        "target_year": target_year,
        "statement_type": statement_type,
        "company_name": company_name,
        "region": region,
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
        st.write(f"  📋 Loaded {len(fields)} BREF fields ({ref_count} with reference values)")
        st.write(f"  🤖 Mapping using AI...")
        
        # Create progress placeholder
        progress_placeholder = st.empty()
        progress_placeholder.info("🔄 Mapping in progress...")
        
        # Map fields
        with st.expander("📝 Mapping Logs", expanded=False):
            mapping_log_placeholder = st.empty()
            mapping_logger = BREFLiveLogger(mapping_log_placeholder)
            
            import contextlib
            
            def _run_mapping():
                with contextlib.redirect_stdout(mapping_logger):
                    return map_all_fields(
                        fields=fields,
                        extracted_rows=rows,
                        company_name=company_name,
                        target_year=target_year,
                        provider=provider,
                        model=model_id
                    )
            
            mapped_fields = run_with_heartbeat(_run_mapping, progress_placeholder)
            progress_placeholder.success("✅ Mapping completed")
        
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


def _display_mapping_results_tab(mapping_key: str, statement_type: str, key_prefix: str):
    """Display mapping results for a single statement in a tab."""
    
    if mapping_key not in st.session_state.bref_mapping_results:
        st.info("No mapping results available")
        return
    
    mapping_results = st.session_state.bref_mapping_results[mapping_key]
    fields = mapping_results["fields"]
    mode = mapping_results["mode"]
    target_year = mapping_results.get("target_year", datetime.now().year)
    
    # Metrics
    col1, col2, col3, col4, col5 = st.columns(5)
    high_conf = sum(1 for f in fields if f.get('final_confidence', f.get('mapping_confidence')) == 'high')
    low_conf = sum(1 for f in fields if f.get('final_confidence', f.get('mapping_confidence')) == 'low')
    
    col1.metric("Total Fields", len(fields))
    col2.metric("High Confidence", high_conf)
    col3.metric("Low Confidence", low_conf)
    col4.metric("Mode", mode.upper())
    col5.metric("Year", target_year)
    
    if low_conf > 0:
        st.warning(f"⚠️ {low_conf} field(s) have low confidence — edit values directly in the table below")
    
    st.markdown("---")
    
    # Build dataframe
    reference_year = target_year - 1
    value_col = f"{target_year} (Extracted)"
    
    df_data = []
    for field in fields:
        row_data = {
            "Field": field.get("label"),
            "Matched Label": field.get("matched_label", "—"),
        }
        if mode == "validated":
            row_data[f"{reference_year} (Reference)"] = field.get("reference_value")
        row_data[value_col] = field.get("target_value")
        row_data["Confidence"] = field.get("final_confidence", field.get("mapping_confidence", ""))
        row_data["Reason"] = field.get("reason", "")
        df_data.append(row_data)
    
    df = pd.DataFrame(df_data)
    
    # Data editor
    column_config = {
        "Field": st.column_config.TextColumn("Field", disabled=True),
        "Matched Label": st.column_config.TextColumn("Matched Label", help="Edit to correct the matched label"),
        value_col: st.column_config.NumberColumn(value_col, help="Edit to correct the extracted value", format="%.2f"),
        "Confidence": st.column_config.SelectboxColumn("Confidence", options=["high", "low"], help="Set confidence level"),
        "Reason": st.column_config.TextColumn("Reason", disabled=True),
    }
    if mode == "validated":
        column_config[f"{reference_year} (Reference)"] = st.column_config.NumberColumn(
            f"{reference_year} (Reference)", disabled=True, format="%.2f"
        )
    
    edited_df = st.data_editor(
        df,
        use_container_width=True,
        hide_index=True,
        height=min(400 + len(df) * 5, 800),
        column_config=column_config,
        key=f"{key_prefix}_multi_editor_{statement_type}",
    )
    
    # Action buttons
    col_save, col_json, col_excel, col_clear = st.columns([1, 1, 1, 1])
    
    with col_save:
        if st.button("💾 Save Changes", key=f"{key_prefix}_multi_save_{statement_type}", use_container_width=True, type="primary"):
            for i, row in edited_df.iterrows():
                fields[i]["matched_label"] = row.get("Matched Label", fields[i].get("matched_label"))
                new_val = row.get(value_col)
                if new_val is not None and str(new_val).strip() != "":
                    try:
                        fields[i]["target_value"] = float(new_val)
                    except (ValueError, TypeError):
                        pass
                new_conf = row.get("Confidence", "")
                if new_conf in ("high", "low"):
                    fields[i]["final_confidence"] = new_conf
                    fields[i]["mapping_confidence"] = new_conf
                    if new_conf == "high" and fields[i].get("validation_status") != "validated":
                        fields[i]["validation_status"] = "human_verified"
            st.session_state.bref_mapping_results[mapping_key]["fields"] = fields
            st.toast("✅ Changes saved")
    
    with col_json:
        st.download_button(
            "📄 Download JSON",
            data=pd.DataFrame(fields).to_json(orient="records", indent=2),
            file_name=f"bref_{statement_type}_{target_year}.json",
            mime="application/json",
            use_container_width=True,
            key=f"{key_prefix}_multi_json_{statement_type}"
        )
    
    with col_excel:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name="BREF Mapping", index=False)
        output.seek(0)
        
        st.download_button(
            "📊 Download Excel",
            data=output.getvalue(),
            file_name=f"BREF_{statement_type}_{target_year}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key=f"{key_prefix}_multi_excel_{statement_type}"
        )
    
    with col_clear:
        if st.button("🗑️ Clear", key=f"{key_prefix}_multi_clear_{statement_type}", use_container_width=True):
            del st.session_state.bref_mapping_results[mapping_key]
            st.success("✅ Results cleared")
            st.rerun()
