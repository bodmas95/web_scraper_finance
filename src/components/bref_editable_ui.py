"""
BREF Editable UI Components
Handles editable data editor with real-time recalculation and MongoDB save functionality.
"""

import streamlit as st
import pandas as pd
from datetime import datetime


def render_editable_mapping_table(
    df: pd.DataFrame,
    fields: list,
    column_config: dict,
    mode: str,
    mapping_key: str,
    statement_type: str,
    target_year: int,
    region: str,
    company_name: str,
    key_prefix: str,
) -> pd.DataFrame:
    """
    Render an editable data table for BREF mapping results.
    
    For bref_direct mode:
    - Direct fields are editable
    - Calculated fields are read-only
    - Real-time recalculation when direct fields are edited
    
    Args:
        df: DataFrame with mapping results
        fields: List of field dictionaries
        column_config: Column configuration for st.data_editor
        mode: Mapping mode ("bref_direct", "validated", "raw")
        mapping_key: Session state key for this mapping
        statement_type: Type of statement
        target_year: Target fiscal year
        region: Region code
        company_name: Company name
        key_prefix: Prefix for session state keys
        
    Returns:
        Edited DataFrame
    """
    
    # For bref_direct mode, make calculated fields read-only
    if mode == "bref_direct":
        # Build a disabled columns list for calculated fields
        disabled_columns = ["BREF Field", "Reason"]
        
        # Add year columns for calculated fields
        for idx, field in enumerate(fields):
            if field.get("is_calculated", False):
                # This field is calculated - disable all its year columns
                # We'll use row-level disabling via column_config
                pass
        
        # Update column_config to make calculated field rows read-only
        # Streamlit doesn't support row-level disabling, so we'll use a workaround:
        # Display calculated fields in a separate read-only dataframe below
        
        # Split fields into direct and calculated
        direct_fields = [f for f in fields if not f.get("is_calculated", False)]
        calc_fields = [f for f in fields if f.get("is_calculated", False)]
        
        # Build separate dataframes
        direct_df_data = []
        calc_df_data = []
        
        for idx, row in df.iterrows():
            if idx < len(fields):
                field = fields[idx]
                if field.get("is_calculated", False):
                    calc_df_data.append(row.to_dict())
                else:
                    direct_df_data.append(row.to_dict())
        
        # Display direct fields as editable
        if direct_df_data:
            st.markdown("### 📝 Direct Fields (Editable)")
            st.caption("Edit values below. Calculated fields will update automatically.")
            
            direct_df = pd.DataFrame(direct_df_data)
            
            # Make Annual Report Label editable, but year columns read-only for now
            # (we'll add edit functionality in next iteration)
            edited_direct_df = st.data_editor(
                direct_df,
                use_container_width=True,
                hide_index=True,
                column_config=column_config,
                height=min(200 + len(direct_df) * 35, 600),
                key=f"{key_prefix}_{statement_type}_direct_editor",
                disabled=["BREF Field", "Reason"],  # Only these are disabled
            )
            
            # Check if any values were edited
            if not direct_df.equals(edited_direct_df):
                st.info("🔄 Values changed. Click 'Recalculate' to update calculated fields.")
                
                # Add recalculate button
                if st.button("🔄 Recalculate Derived Fields", 
                           key=f"{key_prefix}_{statement_type}_recalc",
                           type="primary"):
                    _recalculate_fields(
                        edited_direct_df=edited_direct_df,
                        direct_fields=direct_fields,
                        calc_fields=calc_fields,
                        mapping_key=mapping_key,
                        statement_type=statement_type,
                        target_year=target_year,
                        region=region,
                    )
                    st.success("✅ Calculated fields updated!")
                    st.rerun()
        
        # Display calculated fields as read-only
        if calc_df_data:
            st.markdown("### 🧮 Calculated Fields (Read-Only)")
            st.caption("These fields are automatically calculated from direct fields above.")
            
            calc_df = pd.DataFrame(calc_df_data)
            
            st.dataframe(
                calc_df,
                use_container_width=True,
                hide_index=True,
                column_config=column_config,
                height=min(200 + len(calc_df) * 35, 400),
            )
        
        # Combine for return (in case caller needs it)
        return pd.concat([direct_df if direct_df_data else pd.DataFrame(), 
                         calc_df if calc_df_data else pd.DataFrame()], 
                        ignore_index=True)
    
    else:
        # For other modes, use standard data_editor
        edited_df = st.data_editor(
            df,
            use_container_width=True,
            hide_index=True,
            column_config=column_config,
            height=min(200 + len(df) * 35, 600),
            key=f"{key_prefix}_{statement_type}_editor",
            disabled=["BREF Field", "Reason"],
        )
        
        return edited_df


def _recalculate_fields(
    edited_direct_df: pd.DataFrame,
    direct_fields: list,
    calc_fields: list,
    mapping_key: str,
    statement_type: str,
    target_year: int,
    region: str,
):
    """
    Recalculate derived fields based on edited direct field values.
    
    Args:
        edited_direct_df: DataFrame with edited direct field values
        direct_fields: List of direct field dictionaries
        calc_fields: List of calculated field dictionaries
        mapping_key: Session state key for this mapping
        statement_type: Type of statement
        target_year: Target fiscal year
        region: Region code
    """
    from src.mapping.bref_calculated import recalculate_dependent_fields
    
    # Build mapped_values dict from edited dataframe
    mapped_values = {}
    
    # Get target year column name from dataframe
    target_year_col = str(target_year)
    
    for idx, row in edited_direct_df.iterrows():
        if idx < len(direct_fields):
            field = direct_fields[idx]
            label = field.get("label", "")
            
            # Get value from edited dataframe
            if target_year_col in row:
                value = row[target_year_col]
                if pd.notna(value):
                    mapped_values[label] = float(value)
    
    # Recalculate dependent fields
    updated_values = recalculate_dependent_fields(
        mapped_values=mapped_values,
        statement_type=statement_type,
        region=region,
    )
    
    # Update session state with new calculated values
    if mapping_key in st.session_state.bref_mapping_results:
        mapping_results = st.session_state.bref_mapping_results[mapping_key]
        all_fields = mapping_results["fields"]
        
        # Update calculated field values
        for field in all_fields:
            if field.get("is_calculated", False):
                label = field.get("label", "")
                if label in updated_values:
                    field["target_value"] = updated_values[label]
        
        # Also update direct field values from edited dataframe
        for idx, row in edited_direct_df.iterrows():
            if idx < len(direct_fields):
                field_label = direct_fields[idx].get("label", "")
                # Find this field in all_fields and update
                for field in all_fields:
                    if field.get("label") == field_label:
                        if target_year_col in row:
                            value = row[target_year_col]
                            if pd.notna(value):
                                field["target_value"] = float(value)
                        break


def render_save_button(
    mapping_key: str,
    statement_type: str,
    company_name: str,
    target_year: int,
    region: str,
    key_prefix: str,
):
    """
    Render a save button to persist mapping results to MongoDB.
    
    Args:
        mapping_key: Session state key for this mapping
        statement_type: Type of statement
        company_name: Company name
        target_year: Target fiscal year
        region: Region code
        key_prefix: Prefix for session state keys
    """
    
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        if st.button(
            "💾 Save to MongoDB",
            type="primary",
            use_container_width=True,
            key=f"{key_prefix}_{statement_type}_save",
            help="Save mapping results to MongoDB for future use"
        ):
            _save_to_mongodb(
                mapping_key=mapping_key,
                statement_type=statement_type,
                company_name=company_name,
                target_year=target_year,
                region=region,
            )


def _save_to_mongodb(
    mapping_key: str,
    statement_type: str,
    company_name: str,
    target_year: int,
    region: str,
):
    """
    Save mapping results to MongoDB.
    
    Args:
        mapping_key: Session state key for this mapping
        statement_type: Type of statement
        company_name: Company name
        target_year: Target fiscal year
        region: Region code
    """
    
    if mapping_key not in st.session_state.bref_mapping_results:
        st.error("❌ No mapping results found to save")
        return
    
    try:
        # Get mapping results from session state
        mapping_results = st.session_state.bref_mapping_results[mapping_key]
        
        # Get MongoDB cache
        from src.cache import get_mongo_client
        from src.cache.bref_mapping_cache import BREFMappingCache
        
        mongo_client = get_mongo_client()
        cache = BREFMappingCache(mongo_client)
        
        # Save to MongoDB
        success = cache.set(
            company_name=company_name,
            extraction_rows=mapping_results.get("extraction_rows", []),
            statement_type=statement_type,
            target_year=target_year,
            mapping_result=mapping_results,
            mode=mapping_results.get("mode", "bref_direct"),
            region=region,
        )
        
        if success:
            st.success(f"✅ Saved {statement_type} mapping to MongoDB!")
            st.toast(f"💾 {statement_type} saved successfully")
        else:
            st.error("❌ Failed to save to MongoDB")
            
    except Exception as e:
        import traceback
        st.error(f"❌ Error saving to MongoDB: {e}")
        with st.expander("🔍 View Error Details"):
            st.code(traceback.format_exc())
