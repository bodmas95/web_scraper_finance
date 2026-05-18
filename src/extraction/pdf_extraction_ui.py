"""
PDF Extraction UI Component for Streamlit
Matches bref-populator app.py style exactly
"""

import streamlit as st
import pandas as pd
import io
from datetime import datetime
from typing import Dict, List, Optional

try:
    from src.extraction.pdf_extractor import PDFExtractor, STATEMENT_LABELS
except ImportError:
    # Fallback if module not available
    PDFExtractor = None
    STATEMENT_LABELS = {
        "income_statement": "Income Statement",
        "balance_sheet": "Balance Sheet",
        "cash_flow": "Cash Flow Statement",
    }


def initialize_pdf_extraction_state():
    """Initialize session state for PDF extraction"""
    if 'pdf_extraction_results' not in st.session_state:
        st.session_state.pdf_extraction_results = {}
    if 'pdf_extraction_done' not in st.session_state:
        st.session_state.pdf_extraction_done = False
    if 'pdf_token_usage' not in st.session_state:
        st.session_state.pdf_token_usage = {"input": 0, "output": 0, "total": 0}
    if 'stored_pdf_bytes' not in st.session_state:
        st.session_state.stored_pdf_bytes = None


def render_pdf_extraction_section(
    company_name: str,
    company_type: str,
    pdf_bytes: bytes = None,
    target_year: int = None,
    selected_statements: List[str] = None
):
    """
    Render PDF extraction - performs extraction immediately when called
    Matches bref-populator app.py style
    
    Args:
        company_name: Name of the company
        company_type: Type of company (XBRL, HKEX, SEC)
        pdf_bytes: PDF file bytes to extract from
        target_year: Target fiscal year
        selected_statements: List of statement types to extract
    """
    initialize_pdf_extraction_state()
    
    # Check if LLM is configured
    if PDFExtractor is None:
        st.error("PDF extraction module not available. Please install required dependencies: PyMuPDF, openai")
        return
    
    # Perform extraction immediately
    if pdf_bytes and selected_statements:
        try:
            extractor = PDFExtractor()
            
            # Use text method by default (can be changed to vision in sidebar)
            method = "text"
            
            # Store PDF bytes
            st.session_state.stored_pdf_bytes = pdf_bytes
            
            # Show extraction logs in expanders (like bref-populator)
            results = {}
            
            for statement_type in selected_statements:
                statement_label = STATEMENT_LABELS.get(statement_type, statement_type)
                
                with st.expander(f"Extraction Log — {statement_label}", expanded=True):
                    log_placeholder = st.empty()
                    
                    with st.spinner(f"Extracting {statement_label}..."):
                        try:
                            # Extract single statement
                            result = extractor.extract_statements(
                                pdf_bytes=pdf_bytes,
                                statement_types=[statement_type],
                                extraction_method=method,
                                company_name=company_name,
                                target_year=target_year
                            )
                            
                            if result and statement_type in result:
                                results[statement_type] = result[statement_type]
                                log_placeholder.success(f"✅ Successfully extracted {statement_label}")
                            else:
                                log_placeholder.warning(f"Could not locate **{statement_label}** page — skipped.")
                        
                        except Exception as e:
                            log_placeholder.error(f"Extraction failed for **{statement_label}**: {str(e)}")
            
            if results:
                st.session_state.pdf_extraction_results = results
                st.session_state.pdf_extraction_done = True
                st.session_state.pdf_token_usage = extractor.get_token_usage()
                st.rerun()
            else:
                st.error("No statements could be extracted. Check the PDF and try again.")
        
        except Exception as e:
            st.error(f"❌ Extraction failed: {str(e)}")
            import traceback
            with st.expander("Error Details"):
                st.code(traceback.format_exc())
        
        return
    
    # Display extraction results if available
    if st.session_state.pdf_extraction_done and st.session_state.pdf_extraction_results:
        _display_extraction_results()


def _display_extraction_results():
    """Display extraction results in bref-populator style"""
    results = st.session_state.pdf_extraction_results
    statement_types = list(results.keys())
    
    # Display in tabs if multiple statements, otherwise single view
    if len(statement_types) == 1:
        _render_extraction_panel(statement_types[0], results[statement_types[0]])
    else:
        tab_labels = [STATEMENT_LABELS.get(st, st) for st in statement_types]
        tabs = st.tabs(tab_labels)
        
        for tab, statement_type in zip(tabs, statement_types):
            with tab:
                _render_extraction_panel(statement_type, results[statement_type])
    
    # Download button (centered, like bref-populator)
    _create_download_button(results)


def _render_extraction_panel(statement_type: str, result: Dict):
    """Render extraction panel for one statement (bref-populator style)"""
    # Metrics row
    col1, col2, col3 = st.columns(3)
    col1.metric("Page Located", result.get("page", "N/A"))
    col2.metric("Rows Extracted", result.get("total_rows", 0))
    years = result.get("year_headers", [])
    col3.metric("Year Columns", ", ".join(years) if years else "—")
    
    # Two-column layout: table on left, page image on right
    col_table, col_page = st.columns(2)
    
    # Right column: Source page image
    with col_page:
        st.markdown(
            f"<p class='centered-subheader'>Source Page {result.get('page', 'N/A')}</p>",
            unsafe_allow_html=True
        )
        
        # Try to render PDF page as image
        try:
            import fitz
            if st.session_state.stored_pdf_bytes:
                pdf_doc = fitz.open(stream=st.session_state.stored_pdf_bytes, filetype="pdf")
                pixmap = pdf_doc[result.get("page_num", 0)].get_pixmap(dpi=150)
                img_bytes = pixmap.tobytes("png")
                pdf_doc.close()
                
                st.image(img_bytes, use_container_width=True)
        except Exception as e:
            st.warning(f"Could not render page: {e}")
    
    # Left column: Extracted table
    with col_table:
        rows = result.get("rows", [])
        if rows:
            df = pd.DataFrame(rows)
            
            # Reorder columns: parent, label, then year columns
            cols = list(df.columns)
            ordered_cols = []
            if "parent" in cols:
                ordered_cols.append("parent")
            if "label" in cols:
                ordered_cols.append("label")
            # Add remaining columns (year columns)
            ordered_cols.extend([c for c in cols if c not in ["parent", "label"]])
            
            df = df[ordered_cols]
            
            st.markdown(
                f"<p class='centered-subheader'>Extracted Table ({result.get('extraction_method', 'text')})</p>",
                unsafe_allow_html=True
            )
            
            # Editable dataframe
            edited_df = st.data_editor(
                df,
                use_container_width=True,
                hide_index=True,
                height=500,
                key=f"extracted_table_editor_{statement_type}",
            )
            
            # Save button
            if st.button("Save", key=f"save_edits_{statement_type}", use_container_width=False):
                st.session_state.pdf_extraction_results[statement_type]["rows"] = edited_df.to_dict("records")
                st.toast("Edits saved.")
        else:
            st.info("No data extracted")


def _create_download_button(results: Dict):
    """Create centered download button for extracted data"""
    # Create Excel file with all extracted statements
    excel_bytes = _create_extraction_excel(results)
    
    if excel_bytes:
        # Get target year from first result
        target_year = next(iter(results.values())).get("target_year", "")
        
        # Centered download button
        _dl_l, _dl_c, _dl_r = st.columns([2, 1, 2])
        with _dl_c:
            st.download_button(
                label="Download Extracted Data",
                data=excel_bytes,
                file_name=f"extracted_{target_year}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                type="primary",
                key="dl_extracted_all",
            )


def _create_extraction_excel(results: Dict) -> bytes:
    """Create Excel file with extraction results (bref-populator style)"""
    try:
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            for statement_type, result in results.items():
                sheet_name = STATEMENT_LABELS.get(statement_type, statement_type)[:31]
                rows = result.get("rows", [])
                
                if rows:
                    df = pd.DataFrame(rows)
                    
                    # Reorder columns
                    cols = list(df.columns)
                    ordered_cols = []
                    if "parent" in cols:
                        ordered_cols.append("parent")
                    if "label" in cols:
                        ordered_cols.append("label")
                    ordered_cols.extend([c for c in cols if c not in ["parent", "label"]])
                    
                    df = df[ordered_cols]
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        output.seek(0)
        return output.getvalue()
    
    except Exception as e:
        st.error(f"Error creating Excel file: {str(e)}")
        return None
