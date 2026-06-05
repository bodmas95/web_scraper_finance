"""
PDF rendering, BREF mapping UI helpers.
Shared by HKEX extraction and manual PDF upload flows.
"""

import io
import base64
from datetime import datetime
from pathlib import Path

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
        FIELD_MAPPINGS,
        get_field_mappings,
        load_bref_fields,
        create_clean_output_excel,
        STATEMENT_SHEET_MAP,
    )
    BREF_MAPPING_AVAILABLE = True
except ImportError:
    BREF_MAPPING_AVAILABLE = False


# ==============================================================================
# UTILITY CLASSES & FUNCTIONS
# ==============================================================================

def stitch_images_vertical(image_bytes_list: list[bytes]) -> bytes:
    from PIL import Image
    images = [Image.open(io.BytesIO(b)) for b in image_bytes_list]
    max_width = max(img.width for img in images)
    total_height = sum(img.height for img in images)
    canvas = Image.new("RGB", (max_width, total_height), "white")
    y = 0
    for img in images:
        canvas.paste(img, (0, y))
        y += img.height
    buf = io.BytesIO()
    canvas.save(buf, format="PNG")
    return buf.getvalue()


class PDFLiveLogger:
    """Streams stdout to a Streamlit placeholder in real time."""

    def __init__(self, log_placeholder, token_placeholder=None):
        self._log = log_placeholder
        self._tokens = token_placeholder
        self._buf = ""

    def write(self, text):
        self._buf += text
        self._log.code(self._buf, language=None)
        if self._tokens:
            try:
                from src.extraction.llm_client import get_token_usage
                _tu = get_token_usage()
                self._tokens.markdown(
                    f"Input: **{_tu['input']:,}**  \n"
                    f"Output: **{_tu['output']:,}**  \n"
                    f"Total: **{_tu['total']:,}**"
                )
            except:
                pass
        return len(text)

    def flush(self):
        pass


class BREFLiveLogger:
    """Streams stdout to a Streamlit placeholder in real time."""
    def __init__(self, placeholder):
        self._placeholder = placeholder
        self._buf = ""

    def write(self, text):
        self._buf += text
        self._placeholder.code(self._buf, language=None)
        return len(text)

    def flush(self):
        pass


import threading
import time as _time

def keep_alive_heartbeat(placeholder, stop_event, interval=3):
    """Send periodic updates to keep WebSocket connection alive.
    
    Args:
        placeholder: Streamlit placeholder to update
        stop_event: Threading event to signal when to stop
        interval: Seconds between updates (default 3)
    """
    elapsed = 0
    while not stop_event.is_set():
        _time.sleep(interval)
        elapsed += interval
        if not stop_event.is_set():
            placeholder.info(f"🔄 Mapping in progress... ({elapsed}s elapsed)")


def run_with_heartbeat(func, progress_placeholder, *args, **kwargs):
    """Run a function with periodic heartbeat updates to keep connection alive.
    
    Args:
        func: Function to run
        progress_placeholder: Streamlit placeholder for progress updates
        *args, **kwargs: Arguments to pass to func
    
    Returns:
        Result of func
    """
    stop_event = threading.Event()
    heartbeat_thread = threading.Thread(
        target=keep_alive_heartbeat,
        args=(progress_placeholder, stop_event),
        daemon=True
    )
    
    try:
        heartbeat_thread.start()
        result = func(*args, **kwargs)
        return result
    finally:
        stop_event.set()
        heartbeat_thread.join(timeout=1)


@st.dialog("Full View", width="large")
def zoom_dialog(pdf_bytes, page_nums, crop_bbox):
    """Render zoomed PDF page in a modal dialog."""
    try:
        import fitz

        _doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        _hi_imgs = []
        for _pnum in page_nums:
            _fp = _doc[_pnum]
            if crop_bbox and _fp.rect.width > _fp.rect.height:
                _px = _fp.get_pixmap(dpi=250, clip=fitz.Rect(*crop_bbox))
            else:
                _px = _fp.get_pixmap(dpi=250)
            _hi_imgs.append(_px.tobytes("png"))
        _doc.close()

        _hi_bytes = stitch_images_vertical(_hi_imgs) if len(_hi_imgs) > 1 else _hi_imgs[0]
        st.image(_hi_bytes, use_container_width=True)
    except Exception as e:
        st.error(f"Could not render zoom view: {e}")


def load_bref_template(region: str = "US"):
    """Load region-specific BREF template.
    
    Args:
        region: "US" for NEXTERA 4.xlsx, "APAC" for KLN.xlsx
    
    Returns:
        bytes: Template file content or None if not found
    """
    try:
        if region == "APAC":
            # Try KLN.xlsx for APAC region
            template_path = Path("KLN.xlsx")
            if template_path.exists():
                return template_path.read_bytes()
            # Fallback to lowercase
            template_path = Path("kln.xlsx")
            if template_path.exists():
                return template_path.read_bytes()
        else:
            # Try NEXTERA 4.xlsx for US region
            template_path = Path("NEXTERA 4.xlsx")
            if template_path.exists():
                return template_path.read_bytes()
            # Fallback to subfolder
            template_path = Path("bref-populator-latest/NEXTERA 4.xlsx")
            if template_path.exists():
                return template_path.read_bytes()
    except Exception as e:
        print(f"Error loading {region} template: {e}")
    return None


def find_year_column(worksheet, year: int) -> int:
    for row_idx in range(1, 6):
        for col_idx in range(1, 30):
            cell_value = worksheet.cell(row=row_idx, column=col_idx).value
            if cell_value and str(year) in str(cell_value):
                return col_idx
    return None


# ==============================================================================
# COMPANY NAME EXTRACTION
# ==============================================================================

def _extract_company_name_from_pdf(rows: list, pdf_filename: str = "", pdf_bytes: bytes = None) -> str:
    """Extract company name from financial statement data using LLM.
    
    Args:
        rows: Extracted financial statement rows
        pdf_filename: PDF filename for fallback
        pdf_bytes: PDF file bytes to extract text from first page
    
    Returns:
        Extracted company name
    """
    import json
    from src.extraction.llm_client import get_client
    from src.extraction.extraction_config import LLM_MODEL
    
    # Try to extract from PDF first page if available
    pdf_text = ""
    if pdf_bytes:
        try:
            import fitz
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            if len(doc) > 0:
                # Get text from first page (usually has company name)
                first_page = doc[0]
                pdf_text = first_page.get_text()
                # Get first 500 characters (header area)
                pdf_text = pdf_text[:500]
            doc.close()
        except Exception as e:
            print(f"Could not extract PDF text: {e}")
    
    # Get first 20 rows for context (usually contains company name)
    sample_rows = rows[:20] if len(rows) > 20 else rows
    
    # Format rows as text
    rows_text = []
    for row in sample_rows:
        if isinstance(row, dict):
            label = row.get('label', '')
            parent = row.get('parent', '')
            if parent:
                rows_text.append(f"{parent} > {label}")
            else:
                rows_text.append(label)
        else:
            rows_text.append(str(row))
    
        context = "\n".join(rows_text[:15])  # Use first 15 rows
    
    prompt = f"""
Extract the company name from this financial statement.

Filename: {pdf_filename}

PDF Header (first page):
{pdf_text if pdf_text else 'Not available'}

Statement excerpt:
{context}

Instructions:
1. FIRST, look for the company name in the PDF header (first page text)
2. The company name is usually at the top of the first page, before the statement title
3. Look for patterns like:
   - "COMPANY NAME\nCONSOLIDATED STATEMENTS..."
   - "Annual Report of COMPANY NAME"
   - Company name in header/footer
4. Return the full legal company name (e.g., "Apple Inc.", "Tesla, Inc.", "Alibaba Group Holding Limited")
5. Include legal suffixes: Inc., Ltd., Limited, Corporation, Corp., Co., etc.
6. Do NOT include:
   - Report type (Annual Report, 10-K, etc.)
   - Year or date
   - Page numbers
   - Statement type (Income Statement, etc.)
7. If you cannot find a clear company name, use the filename as fallback

Examples:
- Good: "Apple Inc."
- Good: "Alibaba Group Holding Limited"
- Bad: "Apple Inc. Annual Report 2023"
- Bad: "Income Statement"

Respond with valid JSON only:
{{
  "company_name": "extracted company name",
  "confidence": "high|medium|low",
  "source": "pdf_header|statement|filename"
}}
"""
    
    try:
        client = get_client()
        response = client.chat.completions.create(
            model=LLM_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            response_format={"type": "json_object"},
        )
        
        result = json.loads(response.choices[0].message.content)
        company_name = result.get("company_name", "")
        confidence = result.get("confidence", "low")
        source = result.get("source", "unknown")
        
        print(f"\n📋 Company Name Extraction:")
        print(f"  Name: {company_name}")
        print(f"  Confidence: {confidence}")
        print(f"  Source: {source}")
        
        return company_name if company_name else pdf_filename.replace('.pdf', '').replace('_', ' ').strip()
    
    except Exception as e:
        print(f"⚠️ Company name extraction failed: {e}")
        # Fallback to filename
        return pdf_filename.replace('.pdf', '').replace('_', ' ').strip()


# ==============================================================================
# TRANSLATION HELPER
# ==============================================================================

def _translate_labels_to_english(rows: list) -> list:
    """Translate non-English labels and parent fields to English using LLM."""
    import json
    from src.extraction.llm_client import get_client
    from src.extraction.extraction_config import LLM_MODEL

    texts = set()
    for row in rows:
        if row.get("label"):
            texts.add(row["label"])
        if row.get("parent"):
            for part in row["parent"].split(" > "):
                stripped = part.strip()
                if stripped:
                    texts.add(stripped)

    if not texts:
        return rows

    prompt = (
        "Translate the following financial statement labels to English.\n"
        "If a label is already in English, return it unchanged.\n"
        "Keep financial terminology precise (e.g. Revenue, Expenses, Net Income).\n"
        "Return a JSON object mapping each original label to its English translation.\n\n"
        f"Labels:\n{json.dumps(sorted(texts), ensure_ascii=False)}\n\n"
        'Respond with valid JSON only: {"original": "english", ...}'
    )

    client = get_client()
    response = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        response_format={"type": "json_object"},
    )

    translations = json.loads(response.choices[0].message.content)

    translated_rows = []
    for row in rows:
        new_row = dict(row)
        if new_row.get("label") and new_row["label"] in translations:
            new_row["label"] = translations[new_row["label"]]
        if new_row.get("parent"):
            parts = [p.strip() for p in new_row["parent"].split(" > ")]
            translated_parts = [translations.get(p, p) for p in parts]
            new_row["parent"] = " > ".join(translated_parts)
        translated_rows.append(new_row)

    return translated_rows


# ==============================================================================
# PDF EXCEL DOWNLOAD
# ==============================================================================

def create_pdf_excel(results: dict, target_year: int):
    """Create centered download button for extracted data."""
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for statement_type, result in results.items():
            sheet_name = STATEMENT_LABELS.get(statement_type, statement_type)[:31]
            rows = result.get("rows", [])
            if rows:
                df = pd.DataFrame(rows)
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
    excel_bytes = output.getvalue()

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


# ==============================================================================
# HUMAN REVIEW UI
# ==============================================================================

def render_human_review_ui(fields: list, mapping_key: str, target_year: int):
    low_conf_fields = [
        (idx, f) for idx, f in enumerate(fields)
        if f.get('final_confidence', f.get('mapping_confidence')) == 'low'
    ]

    if not low_conf_fields:
        st.success("✅ All fields mapped with high confidence - no review needed!")
        return

    st.warning(f"⚠️ {len(low_conf_fields)} field(s) need human review")

    with st.expander("🔍 Review Low Confidence Mappings", expanded=True):
        for idx, field in low_conf_fields:
            st.markdown(f"**{field.get('label')}**")

            col1, col2 = st.columns([3, 2])
            with col1:
                st.text_input(
                    "Matched Label",
                    value=field.get('matched_label', ''),
                    key=f"{mapping_key}_matched_{idx}",
                    help="The label from the extracted data"
                )
            with col2:
                new_value = st.number_input(
                    f"Value ({target_year})",
                    value=float(field.get('target_value') or 0),
                    key=f"{mapping_key}_value_{idx}",
                    format="%.2f"
                )

            if st.button("✓ Confirm", key=f"{mapping_key}_confirm_{idx}", use_container_width=True, type="primary"):
                st.session_state.bref_mapping_results[mapping_key]['fields'][idx]['target_value'] = new_value
                st.session_state.bref_mapping_results[mapping_key]['fields'][idx]['matched_label'] = st.session_state[f"{mapping_key}_matched_{idx}"]
                st.session_state.bref_mapping_results[mapping_key]['fields'][idx]['final_confidence'] = 'high'
                st.session_state.bref_mapping_results[mapping_key]['fields'][idx]['validation_status'] = 'human_verified'
                st.success(f"✅ Updated {field.get('label')}")
                st.rerun()

            st.caption(f"Reason: {field.get('reason', 'N/A')}")
            st.markdown("---")


# ==============================================================================
# MAIN PDF PANEL RENDERER
# ==============================================================================

def render_pdf_panel(statement_type: str, result: dict, key_prefix: str = ""):
    """Render extraction panel for one statement (table + page image + BREF mapping)."""
    _all_pnums = result.get("all_page_nums", [result.get("page_num", 0)])
    _page_label = ", ".join(str(p + 1) for p in _all_pnums)

    col1, col2, col3 = st.columns(3)
    col1.metric("Page Located", _page_label)
    col2.metric("Rows Extracted", result.get("total_rows", 0))
    years = result.get("year_headers", [])
    col3.metric("Year Columns", ", ".join(years) if years else "—")

    col_table, col_page = st.columns(2)

    # Right column: Source page image with zoom
    with col_page:
        _spacer, _inner = st.columns([1, 12])
        with _inner:
            _hdr_col, _btn_col = st.columns([5, 1])
            with _hdr_col:
                _src_label = f"Source Page {_page_label}" if len(_all_pnums) == 1 else f"Source Pages {_page_label}"
                st.markdown(f"<p class='centered-subheader'>{_src_label}</p>", unsafe_allow_html=True)

            try:
                import fitz
                if st.session_state.uploaded_pdf_bytes:
                    _crop_bbox = result.get("landscape_crop_bbox")
                    _pdf_doc = fitz.open(stream=st.session_state.uploaded_pdf_bytes, filetype="pdf")
                    _page_imgs = []

                    for _pnum in _all_pnums:
                        _fp = _pdf_doc[_pnum]
                        if _crop_bbox and _fp.rect.width > _fp.rect.height:
                            _px = _fp.get_pixmap(dpi=150, clip=fitz.Rect(*_crop_bbox))
                        else:
                            _px = _fp.get_pixmap(dpi=150)
                        _page_imgs.append(_px.tobytes("png"))
                    _pdf_doc.close()

                    _img_bytes_display = stitch_images_vertical(_page_imgs) if len(_page_imgs) > 1 else _page_imgs[0]

                    with _btn_col:
                        zoom_btn_key = f"zoom_btn_{key_prefix}_{statement_type}"
                        if st.button("🔍", key=zoom_btn_key, help="Zoom in", use_container_width=True):
                            zoom_dialog(
                                st.session_state.uploaded_pdf_bytes,
                                _all_pnums,
                                _crop_bbox
                            )

                    _b64 = base64.b64encode(_img_bytes_display).decode()
                    st.markdown(
                        f'<div style="overflow-y: auto; max-height: 800px; border: 1px solid #e5e5e5; border-radius: 4px;">'
                        f'<img src="data:image/png;base64,{_b64}" style="width: 100%;" />'
                        f'</div>',
                        unsafe_allow_html=True,
                    )
            except Exception as e:
                st.warning(f"Could not render page: {e}")

    # Left column: Extracted table with formatting
    with col_table:
        rows = result.get("rows", [])
        if rows:
            df = pd.DataFrame(rows)

            cols = list(df.columns)
            ordered_cols = []
            if "parent" in cols:
                ordered_cols.append("parent")
            if "label" in cols:
                ordered_cols.append("label")
            ordered_cols.extend([c for c in cols if c not in ["parent", "label"]])
            df = df[ordered_cols]

            _year_headers = result.get("year_headers", [])
            _year_end_date = result.get("year_end_date")
            _year_currencies = result.get("year_currencies", {})
            _unit_scale = result.get("unit_scale")

            _tbl_hdr_col, _currency_col, _save_btn_col = st.columns([3, 2, 1])
            with _tbl_hdr_col:
                _method = result.get("extraction_method", "text")
                st.markdown(f"<p class='centered-subheader'>Extracted Table ({_method})</p>", unsafe_allow_html=True)

            _needs_currency_input = not _year_currencies or not any(_year_currencies.values())

            with _currency_col:
                if _needs_currency_input:
                    manual_currency = st.text_input(
                        "Currency (optional)",
                        placeholder="e.g., USD, RMB",
                        key=f"{key_prefix}_{statement_type}_currency",
                        help="Enter currency if not auto-detected",
                        label_visibility="visible"
                    )
                    if manual_currency:
                        _year_currencies = {yr: manual_currency.upper() for yr in _year_headers}
                else:
                    st.markdown("<div style='padding-top: 8px;'></div>", unsafe_allow_html=True)
                    st.caption(f"Currency: {', '.join(set(_year_currencies.values()))}")

            _rename = {}
            for yr in _year_headers:
                parts = []
                if _year_end_date:
                    _month_abbr = {
                        "January": "Jan", "February": "Feb", "March": "Mar", "April": "Apr",
                        "May": "May", "June": "Jun", "July": "Jul", "August": "Aug",
                        "September": "Sep", "October": "Oct", "November": "Nov", "December": "Dec",
                    }
                    tokens = _year_end_date.split()
                    if tokens and tokens[0] in _month_abbr:
                        tokens[0] = _month_abbr[tokens[0]]
                    parts.append(" ".join(tokens))

                base_year = yr.split('_')[0] if '_' in yr else yr
                parts.append(base_year)

                _currency = None
                if _year_currencies:
                    _currency = _year_currencies.get(yr)
                    if not _currency:
                        _currency = _year_currencies.get(base_year)

                if _currency and _unit_scale:
                    parts.append(f"{_currency} ({_unit_scale})")
                elif _currency:
                    parts.append(_currency)
                elif _unit_scale:
                    parts.append(f"({_unit_scale})")
                _rename[yr] = " ".join(parts)

            # Translate to English button
            _translated_flag = f"{key_prefix}_translated_{statement_type}"
            _already_translated = st.session_state.get(_translated_flag, False)
            _tr_spacer, _tr_btn = st.columns([4, 2])
            with _tr_btn:
                if st.button(
                    "Translate Labels to English" if not _already_translated else "Already Translated",
                    key=f"{key_prefix}_translate_{statement_type}" if key_prefix else f"translate_{statement_type}",
                    use_container_width=True,
                    type="secondary",
                    disabled=_already_translated,
                ):
                    with st.spinner("Translating labels..."):
                        translated_rows = _translate_labels_to_english(rows)
                    result["rows"] = translated_rows
                    st.session_state[_translated_flag] = True
                    st.rerun()

            _display_df = df.rename(columns=_rename).copy()

            for _ycol in _rename.values():
                if _ycol in _display_df.columns:
                    _display_df[_ycol] = _display_df[_ycol].apply(lambda x: f"{float(x):,.0f}" if x is not None and str(x).replace('.','').replace('-','').isdigit() else "")

            edited_df = st.data_editor(
                _display_df,
                use_container_width=True,
                hide_index=True,
                height=600,
                key=f"{key_prefix}_extracted_table_editor_{statement_type}" if key_prefix else f"extracted_table_editor_{statement_type}",
            )

            with _save_btn_col:
                if _needs_currency_input:
                    st.markdown("<label style='font-size:14px;visibility:hidden;display:block;margin-bottom:2px;'>&nbsp;</label>", unsafe_allow_html=True)
                else:
                    st.markdown("<div style='padding-top: 8px;'></div>", unsafe_allow_html=True)
                if st.button("Save", key=f"{key_prefix}_save_edits_{statement_type}" if key_prefix else f"save_edits_{statement_type}", use_container_width=True):
                    _reverse = {v: k for k, v in _rename.items()}
                    _save_df = edited_df.rename(columns=_reverse).copy()
                    for _yr in _year_headers:
                        if _yr in _save_df.columns:
                            _save_df[_yr] = pd.to_numeric(
                                _save_df[_yr].astype(str).str.replace(",", ""), errors="coerce"
                            )

                    if key_prefix == "hkex" and st.session_state.hkex_extraction_results:
                        st.session_state.hkex_extraction_results[statement_type]["rows"] = _save_df.to_dict("records")
                    elif key_prefix == "manual":
                        pass

                    st.toast("Edits saved.")
        else:
            st.info("No data extracted")

    # ==================================================================
    # BREF MAPPING SECTION - DISABLED
    # ==================================================================
    # The old single-statement mapping UI has been replaced by the new
    # multi-statement UI (brefmap_multi_ui.py) which allows mapping
    # multiple statements at once using checkboxes.
    # This section is completely disabled - no mapping UI in individual tabs
    # All mapping functionality is now in render_multi_statement_mapping()
    # The old code (500+ lines) has been removed to clean up the file.
    pass  # End of render_pdf_panel function


def _display_mapping_results(mapping_key: str, statement_type: str, key_prefix: str):
    """Display mapping results for a specific statement."""
    if mapping_key not in st.session_state.bref_mapping_results:
        return 
    col_header, col_clear = st.columns([3, 1])
    
    with col_header:
        st.markdown(f"**{STATEMENT_LABELS.get(statement_type, statement_type)}**")
    
    with col_clear:
        # Use mapping_key directly to ensure uniqueness
        # Don't add statement_type as it's already in mapping_key (e.g., manual_mapping_cash_flow)
        clear_key = f"clear_{key_prefix}_{mapping_key}".replace('_', '-')
        if st.button("🗑️ Clear", key=clear_key, use_container_width=True):
            del st.session_state.bref_mapping_results[mapping_key]
            st.success("✅ Results cleared")
            st.rerun()

    mapping_results = st.session_state.bref_mapping_results[mapping_key]
    fields = mapping_results["fields"]
    mode = mapping_results["mode"]
    bref_target_year = mapping_results.get("target_year", datetime.now().year)

    col1, col2, col3, col4 = st.columns(4)
    high_conf = sum(1 for f in fields if f.get('final_confidence', f.get('mapping_confidence')) == 'high')
    low_conf = sum(1 for f in fields if f.get('final_confidence', f.get('mapping_confidence')) == 'low')

    col1.metric("Total Fields", len(fields))
    col2.metric("High Confidence", high_conf)
    col3.metric("Low Confidence", low_conf)
    col4.metric("Mode", mode.upper())

    if low_conf > 0:
        st.warning(f"⚠️ {low_conf} field(s) have low confidence — edit values directly in the table below, then click **Save Changes**.")

    st.markdown("---")

    reference_year = bref_target_year - 1
    value_col = f"{bref_target_year} (Extracted)"

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

    editor_key = f"{key_prefix}_editor_{statement_type}_{mapping_key}"

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
        key=editor_key,
    )

    _save_col, _dl_json_col, _dl_excel_col = st.columns([1, 1, 1])

    with _save_col:
        save_key = f"{key_prefix}_save_{statement_type}_{mapping_key}".replace("_", "-")
        if st.button("Save Changes", key=save_key, use_container_width=True, type="primary"):
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
            st.toast("Changes saved.")

    with _dl_json_col:
        st.download_button(
            "Download JSON",
            data=pd.DataFrame(fields).to_json(orient="records", indent=2),
            file_name=f"bref_results_{statement_type}_{bref_target_year}.json",
            mime="application/json",
            use_container_width=True,
            key=f"{key_prefix}_json_download_{statement_type}_{mapping_key}"
        )

    with _dl_excel_col:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name="BREF Mapping", index=False)
        output.seek(0)

        st.download_button(
            "Download Excel",
            data=output.getvalue(),
            file_name=f"BREF_Output_{statement_type}_{bref_target_year}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key=f"{key_prefix}_excel_download_{statement_type}_{mapping_key}"
        )
