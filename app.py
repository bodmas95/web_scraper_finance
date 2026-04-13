"""
OVHcloud Financial Data Explorer — Streamlit App
Run from project root:  streamlit run app.py
"""

import sys
import os
import json
import re
import time
from pathlib import Path

# ── make sure project root is on the path ───────────────────────────────────
_ROOT = Path(__file__).parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import streamlit as st
import pandas as pd

# ── lazy-import parser helpers ───────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def _load_parser():
    from src.parser.ovh import parser as p
    return p

# ═══════════════════════════════════════════════════════════════════════════
# Page config
# ═══════════════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="OVHcloud Financials",
    page_icon="📊",
    layout="wide",
)

# ── session-state initialisation ────────────────────────────────────────────
def _init_state():
    defaults = {
        "filings":       None,   # list[dict] from api_discover
        "all_data":      {},     # {fy_label: {sheet_type: rows}}
        "all_facts":     [],     # flat list of XBRL fact records
        "concept_map":   {},     # {sheet_type: {label: concept}}
        "parsed_labels": set(),  # fy_labels already processed
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

_init_state()


# ═══════════════════════════════════════════════════════════════════════════
# Helper utilities
# ═══════════════════════════════════════════════════════════════════════════

def _rebuild_concept_map():
    """Rebuild concept map from everything parsed so far."""
    p = _load_parser()
    if not st.session_state.all_data:
        st.session_state.concept_map = {}
        return
    facts_by_year: dict = {}
    for fact in st.session_state.all_facts:
        facts_by_year.setdefault(fact["year"], []).append(fact)
    st.session_state.concept_map = p.build_concept_map(
        st.session_state.all_data, facts_by_year
    )


def _parse_filing(filing: dict):
    """Download + parse one filing, update session state."""
    p = _load_parser()
    pe = filing.get("period_end", "")
    if not pe:
        return
    fy_label = f"FY{pe[:4]}"
    if fy_label in st.session_state.parsed_labels:
        st.info(f"{fy_label} is already parsed.")
        return

    root_dir = Path(p.DOWNLOAD_DIR)
    root_dir.mkdir(exist_ok=True)
    fy_dir = root_dir / fy_label
    fy_dir.mkdir(exist_ok=True)

    with st.spinner(f"Downloading report for {fy_label} …"):
        report_path = p.download_report(filing, fy_dir)
    if not report_path:
        st.error(f"No report available for {fy_label}.")
        return

    with st.spinner(f"Extracting financial tables for {fy_label} …"):
        tables = p.extract_section_tables(report_path, fy_label)
    if not tables:
        st.warning(f"No tables found for {fy_label}.")
        return

    for tbl_name in tables:
        tables[tbl_name] = p._detect_unit_and_normalize(tables[tbl_name])
        tables[tbl_name] = p._add_english_column(tables[tbl_name])

    st.session_state.all_data[fy_label] = tables

    with st.spinner(f"Downloading XBRL facts for {fy_label} …"):
        json_path = p.download_xbrl_json(filing, fy_dir)
    if json_path:
        facts = p.parse_xbrl_facts(json_path, fy_label)
        st.session_state.all_facts.extend(facts)

    st.session_state.parsed_labels.add(fy_label)
    _rebuild_concept_map()
    n_matched = sum(len(v) for v in st.session_state.concept_map.values())
    st.success(f"✅ {fy_label}: {len(tables)} statement types parsed, {n_matched} XBRL concept matches total")


def _rows_to_df(rows: list, concept_map_for_sheet: dict, col_override: list | None = None) -> pd.DataFrame:
    """
    Convert raw table rows (first row = header) to a display DataFrame.
    Columns: French Label | English Label | XBRL Concept | <year cols …>
    """
    if not rows or len(rows) < 2:
        return pd.DataFrame()

    header = rows[0]   # e.g. ["Label (FR)", "Label (English)", "2025", "2024"]
    data_rows = rows[1:]

    # Detect label columns vs year/value columns
    label_col_indices = []
    value_col_indices = []
    for ci, h in enumerate(header):
        if re.search(r"\d{4}", str(h)):
            value_col_indices.append(ci)
        else:
            label_col_indices.append(ci)

    # Build dataframe rows: [FR label, EN label, XBRL concept, val1, val2, …]
    fr_col = label_col_indices[0] if label_col_indices else 0
    en_col = label_col_indices[1] if len(label_col_indices) > 1 else None

    year_headers = [header[ci] for ci in value_col_indices] if not col_override else col_override
    records = []
    for row in data_rows:
        fr_lbl = row[fr_col].strip() if fr_col < len(row) else ""
        en_lbl = row[en_col].strip() if (en_col is not None and en_col < len(row)) else ""
        concept = concept_map_for_sheet.get(fr_lbl, "")
        vals = [
            row[ci].strip() if ci < len(row) and row[ci] is not None else ""
            for ci in value_col_indices
        ]
        records.append([fr_lbl, en_lbl, concept] + vals)

    cols = ["French Label", "English Label", "XBRL Concept"] + [str(h) for h in year_headers]
    df = pd.DataFrame(records, columns=cols)
    # Drop fully empty rows
    df = df[df["French Label"].str.strip().astype(bool)].reset_index(drop=True)
    return df


def _consolidated_df(sheet_type: str) -> pd.DataFrame:
    """Build consolidated multi-year DataFrame for a sheet type."""
    p = _load_parser()
    rows = p._build_consolidated_rows(st.session_state.all_data, sheet_type)
    if not rows or len(rows) < 2:
        return pd.DataFrame()

    concept_map_sheet = st.session_state.concept_map.get(sheet_type, {})

    header = rows[0]   # [unit_label, "Label (English)", "2021", "2022", …]
    value_col_indices = []
    for ci, h in enumerate(header):
        try:
            int(str(h).strip())
            value_col_indices.append(ci)
        except ValueError:
            pass

    year_headers = [header[ci] for ci in value_col_indices]
    records = []
    for row in rows[1:]:
        fr_lbl = row[0].strip() if row else ""
        en_lbl = row[1].strip() if len(row) > 1 else ""
        concept = concept_map_sheet.get(fr_lbl, "")
        vals = [
            str(row[ci]).strip() if ci < len(row) and row[ci] is not None else ""
            for ci in value_col_indices
        ]
        records.append([fr_lbl, en_lbl, concept] + vals)

    cols = ["French Label", "English Label", "XBRL Concept"] + [str(h) for h in year_headers]
    df = pd.DataFrame(records, columns=cols)
    df = df[df["French Label"].str.strip().astype(bool)].reset_index(drop=True)
    return df


def _style_df(df: pd.DataFrame):
    """Apply light styling — bold totals, italic English labels."""
    p = _load_parser()

    def _row_style(row):
        label = row.get("French Label", "")
        is_total = p._is_total_row(str(label))
        base = "font-weight: bold; background-color: #f0f4ff;" if is_total else ""
        return [base] * len(row)

    styler = df.style.apply(_row_style, axis=1)
    if "English Label" in df.columns:
        styler = styler.set_properties(subset=["English Label"], **{"font-style": "italic", "color": "#555"})
    if "XBRL Concept" in df.columns:
        styler = styler.set_properties(subset=["XBRL Concept"], **{"color": "#1a6fb5", "font-size": "0.82em"})
    return styler


# ═══════════════════════════════════════════════════════════════════════════
# UI — Header
# ═══════════════════════════════════════════════════════════════════════════
st.title("📊 OVHcloud Financial Data Explorer")
st.caption("Source: XBRL / ESEF filings via filings.xbrl.org — IFRS consolidated statements")

st.divider()

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 1 — Available Filings
# ═══════════════════════════════════════════════════════════════════════════
st.subheader("1 · Available Filings")

col_fetch, col_info = st.columns([1, 4])
with col_fetch:
    fetch_clicked = st.button("🔍 Fetch Filings from API", use_container_width=True)

if fetch_clicked:
    p = _load_parser()
    with st.spinner("Querying XBRL filings API …"):
        try:
            st.session_state.filings = p.api_discover(p.LEI)
            # Cache to disk
            root_dir = Path(p.DOWNLOAD_DIR)
            root_dir.mkdir(exist_ok=True)
            (root_dir / "api_filings.json").write_text(
                json.dumps(st.session_state.filings, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
        except Exception as e:
            st.error(f"API error: {e}")

# --- load from disk if not in memory ---
if st.session_state.filings is None:
    p = _load_parser()
    cached_path = Path(p.DOWNLOAD_DIR) / "api_filings.json"
    if cached_path.exists():
        try:
            st.session_state.filings = json.loads(cached_path.read_text(encoding="utf-8"))
        except Exception:
            pass

if st.session_state.filings:
    filings = st.session_state.filings

    # Build display table
    rows = []
    for f in filings:
        pe = f.get("period_end", "")
        fy = f"FY{pe[:4]}" if pe else "—"
        rows.append({
            "FY":          fy,
            "Period End":  pe,
            "Entity":      f.get("entity_name", ""),
            "Report":      "✅" if f.get("report_url") else "❌",
            "Errors":      f.get("error_count", 0),
            "Parsed":      "✅" if fy in st.session_state.parsed_labels else "—",
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    st.caption(f"{len(filings)} filing(s) found.  "
               f"{'Loaded from disk cache.' if not fetch_clicked else 'Fetched live.'}")
else:
    st.info("Click **Fetch Filings from API** to discover available XBRL filings.")

st.divider()

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 2 — Parse a Single Filing
# ═══════════════════════════════════════════════════════════════════════════
st.subheader("2 · Parse a Single Filing Year")

if not st.session_state.filings:
    st.warning("Fetch filings first (Section 1).")
else:
    filings = st.session_state.filings
    options = []
    for f in filings:
        pe = f.get("period_end", "")
        if pe:
            fy = f"FY{pe[:4]}"
            label = f"{fy}  (period end: {pe})"
            options.append((label, f, fy))

    col_sel, col_parse = st.columns([3, 1])
    with col_sel:
        selected_idx = st.selectbox(
            "Select a filing year",
            range(len(options)),
            format_func=lambda i: options[i][0],
        )
    with col_parse:
        st.write("")   # vertical spacer
        parse_clicked = st.button("▶ Parse Selected", use_container_width=True)

    if parse_clicked and options:
        _, selected_filing, _ = options[selected_idx]
        _parse_filing(selected_filing)

    # ── Show parsed data for the selected year ─────────────────────────────
    if options:
        _, _, selected_fy = options[selected_idx]
        if selected_fy in st.session_state.parsed_labels:
            p = _load_parser()
            fy_tables = st.session_state.all_data.get(selected_fy, {})
            if fy_tables:
                st.markdown(f"**{selected_fy}** — {len(fy_tables)} statement type(s) extracted")

                tab_names = [t for t in p.CONSOLIDATED_SHEET_TYPES if t in fy_tables]
                tab_names += [t for t in fy_tables if t not in tab_names]

                if tab_names:
                    tabs = st.tabs(tab_names)
                    for tab, sheet_type in zip(tabs, tab_names):
                        with tab:
                            rows = fy_tables.get(sheet_type, [])
                            concept_map_sheet = st.session_state.concept_map.get(sheet_type, {})
                            df = _rows_to_df(rows, concept_map_sheet)
                            if df.empty:
                                st.info("No data for this statement.")
                            else:
                                # Summary metrics
                                n_concepts = df["XBRL Concept"].astype(bool).sum()
                                c1, c2, c3 = st.columns(3)
                                c1.metric("Rows", len(df))
                                c2.metric("XBRL Concepts matched", int(n_concepts))
                                c3.metric(
                                    "Match rate",
                                    f"{100*n_concepts/len(df):.0f}%" if len(df) else "—"
                                )

                                # Filter toggle
                                show_empty = st.checkbox(
                                    "Show rows with no XBRL concept",
                                    value=True,
                                    key=f"empty_{selected_fy}_{sheet_type}",
                                )
                                display_df = df if show_empty else df[df["XBRL Concept"] != ""]

                                st.dataframe(
                                    _style_df(display_df),
                                    use_container_width=True,
                                    hide_index=True,
                                    height=min(600, 40 + 35 * len(display_df)),
                                )
        elif parse_clicked:
            pass  # already handled above
        else:
            st.info(f"Click **▶ Parse Selected** to extract data for {selected_fy}.")

st.divider()

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 3 — Consolidated Multi-Year View
# ═══════════════════════════════════════════════════════════════════════════
st.subheader("3 · Consolidated Multi-Year View  (2021 – 2025)")

col_all, col_info2 = st.columns([1, 4])
with col_all:
    parse_all_clicked = st.button("⚡ Parse All Filings", use_container_width=True)
with col_info2:
    parsed = sorted(st.session_state.parsed_labels, reverse=True)
    if parsed:
        st.caption(f"Parsed so far: {', '.join(parsed)}")
    else:
        st.caption("No filings parsed yet — click Parse All or parse individual years above.")

if parse_all_clicked and st.session_state.filings:
    p = _load_parser()
    unparsed = [
        f for f in st.session_state.filings
        if f.get("period_end") and f"FY{f['period_end'][:4]}" not in st.session_state.parsed_labels
    ]
    if not unparsed:
        st.info("All available filings are already parsed.")
    else:
        progress = st.progress(0, text="Starting …")
        for i, filing in enumerate(unparsed):
            pe = filing.get("period_end", "")
            fy = f"FY{pe[:4]}"
            progress.progress((i) / len(unparsed), text=f"Parsing {fy} …")
            _parse_filing(filing)
            time.sleep(0.3)
        progress.progress(1.0, text="Done!")
        st.rerun()

# ── Show consolidated tables ──────────────────────────────────────────────
if st.session_state.all_data:
    p = _load_parser()

    tab_names = p.CONSOLIDATED_SHEET_TYPES
    tabs = st.tabs(tab_names)

    for tab, sheet_type in zip(tabs, tab_names):
        with tab:
            df = _consolidated_df(sheet_type)
            if df.empty:
                st.info(f"No consolidated data for **{sheet_type}** yet.")
                st.caption("Parse individual years or click **Parse All Filings** above.")
            else:
                year_cols = [c for c in df.columns if re.match(r"^\d{4}$", c)]
                n_concepts = df["XBRL Concept"].astype(bool).sum()

                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Rows", len(df))
                c2.metric("Years with data", len([c for c in year_cols if df[c].astype(bool).any()]))
                c3.metric("XBRL Concepts matched", int(n_concepts))
                c4.metric("Match rate", f"{100*n_concepts/len(df):.0f}%" if len(df) else "—")

                # Optional: filter to only XBRL-matched rows
                col_f1, col_f2 = st.columns([2, 2])
                with col_f1:
                    only_matched = st.checkbox(
                        "Show only XBRL-matched rows",
                        value=False,
                        key=f"match_{sheet_type}",
                    )
                with col_f2:
                    search_term = st.text_input(
                        "Filter rows",
                        placeholder="Type to search labels …",
                        key=f"search_{sheet_type}",
                        label_visibility="collapsed",
                    )

                display_df = df.copy()
                if only_matched:
                    display_df = display_df[display_df["XBRL Concept"] != ""]
                if search_term:
                    mask = (
                        display_df["French Label"].str.contains(search_term, case=False, na=False)
                        | display_df["English Label"].str.contains(search_term, case=False, na=False)
                        | display_df["XBRL Concept"].str.contains(search_term, case=False, na=False)
                    )
                    display_df = display_df[mask]

                display_df = display_df.reset_index(drop=True)

                st.dataframe(
                    _style_df(display_df),
                    use_container_width=True,
                    hide_index=True,
                    height=min(700, 40 + 35 * len(display_df)),
                )

                # Download button
                csv = display_df.to_csv(index=False).encode("utf-8-sig")
                st.download_button(
                    label=f"⬇ Download {sheet_type} as CSV",
                    data=csv,
                    file_name=f"ovhcloud_{sheet_type.lower().replace(' ', '_')}_consolidated.csv",
                    mime="text/csv",
                    key=f"dl_{sheet_type}",
                )
else:
    st.info("Parse at least one filing (Sections 1 & 2) or click **⚡ Parse All Filings** above.")

st.divider()

# ── Footer ────────────────────────────────────────────────────────────────
st.caption(
    f"Data source: [filings.xbrl.org](https://filings.xbrl.org)  ·  "
    f"LEI: `{_load_parser().LEI}`  ·  "
    f"OVHcloud consolidated IFRS statements"
)
