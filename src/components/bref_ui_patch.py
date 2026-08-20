"""
Patch to add to the end of _display_mapping_results_tab function in brefmap_multi_ui.py

Add this code after the dataframe sanitization and before the final display.
"""

# After the dataframe is built and sanitized, add this code:

# ══════════════════════════════════════════════════════════════════════════════
# EDITABLE UI WITH REAL-TIME RECALCULATION (for bref_direct mode)
# ══════════════════════════════════════════════════════════════════════════════

# Get currency and unit for formatted headers
currency = mapping_results.get("currency", "")
unit = mapping_results.get("unit", "")

# Build column configuration
column_config = {
    "BREF Field": st.column_config.TextColumn("BREF Field", disabled=True, width="large"),
    "Annual Report Label": st.column_config.TextColumn("Annual Report Label", help="Edit to correct the matched label", width="large"),
    "Reason": st.column_config.TextColumn("Reason", disabled=True, width="large"),
}

# Add year columns with formatted headers
from src.integration.currency_unit_display import format_year_header

ref_year_col = str(reference_year)
if ref_year_col in df.columns:
    ref_year_header = format_year_header(reference_year, currency, unit)
    column_config[ref_year_col] = st.column_config.NumberColumn(
        ref_year_header, format="%.2f", disabled=True
    )

for year in available_years:
    year_col = str(year)
    if year_col in df.columns:
        year_header = format_year_header(year, currency, unit)
        # For bref_direct mode, make year columns editable for direct fields
        if mode == "bref_direct":
            column_config[year_col] = st.column_config.NumberColumn(
                year_header, format="%.2f", disabled=False  # Editable
            )
        else:
            column_config[year_col] = st.column_config.NumberColumn(
                year_header, format="%.2f", disabled=True
            )

# Use the editable UI component
from src.components.bref_editable_ui import render_editable_mapping_table, render_save_button

edited_df = render_editable_mapping_table(
    df=df,
    fields=fields,
    column_config=column_config,
    mode=mode,
    mapping_key=mapping_key,
    statement_type=statement_type,
    target_year=target_year,
    region=region,
    company_name=mapping_results.get("company_name", ""),
    key_prefix=key_prefix,
)

# Add save button
st.markdown("---")
render_save_button(
    mapping_key=mapping_key,
    statement_type=statement_type,
    company_name=mapping_results.get("company_name", ""),
    target_year=target_year,
    region=region,
    key_prefix=key_prefix,
)
