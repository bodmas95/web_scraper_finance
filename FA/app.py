"""
============================================================
  FINANCIAL ANALYSIS AGENTIC AI - STREAMLIT APP
============================================================
  LangGraph-based multi-agent system for financial analysis

  Langchain Version: 0.2.5
  Langgraph Version: 1.1.3
============================================================

REFORM NOTES
------------
- The app used to render twice (once from a wrapper, once from render_header()
  inside main()). The header now renders exactly ONCE, and set_page_config is
  guarded so a wrapper that already called it will not crash this file.
- The "Select Summary Source" radio is now Step 1 INSIDE main(). Selecting
  "Use Summary from Pipeline" loads the handed-over summary and immediately
  unlocks Steps 3-5 (P&L / BS / CF).
- Step 4 (analysis type selection) no longer silently disappears when no
  supporting documents are uploaded; it shows a clear message explaining what
  to do next.

PIPELINE HANDOFF
----------------
This file reads the pipeline summary from Streamlit session state:
    st.session_state._fa_company_name   -> client/company name (str)
    st.session_state._fa_summary_bytes  -> BREF summary Excel bytes
If your wrapper sets those two values before importing/calling main(), the
"Use Summary from Pipeline" option will work with no further changes.
"""

import streamlit as st
import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional

# Import LangGraph components
from graph import build_financial_analysis_graph
from agents import AgentState
from utils import (
    parse_document,
    save_parsed_json,
    get_session_folder,
    save_uploaded_file,
    save_file_sequencially,
    get_excel_sheets
)
# Import citation reorganization utilities
from app_utils import reorganize_citations_for_display, render_citations_section, escape_dollar_for_markdown
# Import BREF data extractor
from bref_data_extractor import extract_bref_data_sync

# API imports
import sys
import os as os_module
# Add path for API imports - works from sprint4 directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "bpce_api_setup"))
try:
    from call_bpce_llm import call_llm_api
    from prompts.prompts_FA import (
        PROMPT_BREF_ANALYSIS_PNL, PROMPT_BREF_ANALYSIS_BS, PROMPT_BREF_ANALYSIS_CF,
        PROMPT_DRIVER_ANALYSIS_PNL, PROMPT_DRIVER_ANALYSIS_BS, PROMPT_DRIVER_ANALYSIS_CF,
        PROMPT_COMBINE_ANALYSIS_PNL, PROMPT_COMBINE_ANALYSIS_BS, PROMPT_COMBINE_ANALYSIS_CF,
        BREF_VALIDATION_PROMPT_PNL, BREF_VALIDATION_PROMPT_BS, BREF_VALIDATION_PROMPT_CF
    )
except ImportError:
    # Fallback to parent directory
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "bpce_api_setup"))
    from call_bpce_llm import call_llm_api
    from prompts.prompts_FA import (
        PROMPT_BREF_ANALYSIS_PNL, PROMPT_BREF_ANALYSIS_BS, PROMPT_BREF_ANALYSIS_CF,
        PROMPT_DRIVER_ANALYSIS_PNL, PROMPT_DRIVER_ANALYSIS_BS, PROMPT_DRIVER_ANALYSIS_CF,
        PROMPT_COMBINE_ANALYSIS_PNL, PROMPT_COMBINE_ANALYSIS_BS, PROMPT_COMBINE_ANALYSIS_CF,
        BREF_VALIDATION_PROMPT_PNL, BREF_VALIDATION_PROMPT_BS, BREF_VALIDATION_PROMPT_CF
    )

# Import centralized configuration defaults
from config_defaults import (
    LLM_OPTIONS, CLIENT_LIST, BASE_UPLOAD_DIR, PARSED_DOCS_DIR,
    BREF_DEFAULT_TEMPERATURE, BREF_DEFAULT_MAX_TOKENS, BREF_DEFAULT_LLM_MODEL,
    FA_DEFAULT_TEMPERATURE, FA_DEFAULT_MAX_TOKENS, FA_DEFAULT_LLM_MODEL,
    COMBINED_DEFAULT_TEMPERATURE, COMBINED_DEFAULT_MAX_TOKENS, COMBINED_DEFAULT_LLM_MODEL,
    TEMPERATURE_MIN, TEMPERATURE_MAX, TEMPERATURE_STEP,
    BREF_TOKENS_MIN, BREF_TOKENS_MAX, BREF_TOKENS_STEP,
    FA_TOKENS_MIN, FA_TOKENS_MAX, FA_TOKENS_STEP,
    COMBINED_TOKENS_MIN, COMBINED_TOKENS_MAX, COMBINED_TOKENS_STEP
)

# Import logger configuration
from logger_config import setup_logger, log_section_start, log_section_end, log_step, log_file_saved
# Import progress tracker
from progress_tracker import get_tracker
# Import status callback
from status_callback import set_status_callback


# ─── CONFIGURATION SAVE HELPER ────────────────────────────
def save_configuration_to_folder(config_dict: dict, config_type: str, session_folder: str):
    """Save configuration to session folder as JSON"""
    config_filename = f"{config_type}_configuration.json"
    config_path = Path(session_folder) / config_filename

    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(config_dict, f, indent=2, ensure_ascii=False)

    log_file_saved(f"{config_type.upper()} Configuration", str(config_path))
    return str(config_path)


# ─── UI CONFIGURATION COMPONENTS ───────────────────────────
def render_configuration_options(config_type: str, key_prefix: str, analysis_type: str = "pnl"):
    """Render configuration options for different stages

    Args:
        config_type: Type of configuration ("bref", "fa", or "combined")
        key_prefix: Unique key prefix for session state
        analysis_type: Type of analysis ("pnl", "bs", or "cf") - default is "pnl"
    """

    # Set default values based on config type
    if config_type == "bref":
        default_temp = BREF_DEFAULT_TEMPERATURE
        default_tokens = BREF_DEFAULT_MAX_TOKENS
        default_llm = BREF_DEFAULT_LLM_MODEL
        tokens_min = BREF_TOKENS_MIN
        tokens_max = BREF_TOKENS_MAX
        tokens_step = BREF_TOKENS_STEP
    elif config_type == "fa":
        default_temp = FA_DEFAULT_TEMPERATURE
        default_tokens = FA_DEFAULT_MAX_TOKENS
        default_llm = FA_DEFAULT_LLM_MODEL
        tokens_min = FA_TOKENS_MIN
        tokens_max = FA_TOKENS_MAX
        tokens_step = FA_TOKENS_STEP
    else:  # combined
        default_temp = COMBINED_DEFAULT_TEMPERATURE
        default_tokens = COMBINED_DEFAULT_MAX_TOKENS
        default_llm = COMBINED_DEFAULT_LLM_MODEL
        tokens_min = COMBINED_TOKENS_MIN
        tokens_max = COMBINED_TOKENS_MAX
        tokens_step = COMBINED_TOKENS_STEP

    # Get LLM index for default
    try:
        default_llm_index = LLM_OPTIONS.index(default_llm)
    except ValueError:
        default_llm_index = 0

    # Configuration visibility toggle
    config_visible_key = f"{key_prefix}_config_visible"
    if config_visible_key not in st.session_state:
        st.session_state[config_visible_key] = False

    # Configuration button
    button_label = f"⚙️ Configure {config_type.upper()} Output"
    if st.button(button_label, key=f"{key_prefix}_config_toggle_btn"):
        st.session_state[config_visible_key] = not st.session_state[config_visible_key]

    # Show configuration only if toggled
    if st.session_state[config_visible_key]:
        # Create tabs for Generation and Validation Instructions
        if config_type in ["bref", "fa"]:
            tab1, tab2 = st.tabs(["Generation Instructions", "Validation Instructions"])
        else:
            tab1 = st.tabs(["Generation Instructions"])[0]

        # Tab 1: Generation Instructions
        with tab1:
            col1, col2, col3, col4 = st.columns([1, 1, 1, 1])

            with col1:
                st.write("**Creativity Level**")
                creativity_help = """
                • 0.0 - 0.2: Low/Deterministic (Conservative analysis)
                • 0.3 - 0.6: Moderate/Balanced (Standard approach)
                • 0.7 - 1.0: High/Creative (Innovative insights)
                • 1.1 - 2.0+: Very High (Maximum creativity)
                """
                temperature = st.slider(
                    "Select Creativity",
                    min_value=TEMPERATURE_MIN,
                    max_value=TEMPERATURE_MAX,
                    value=default_temp,
                    step=TEMPERATURE_STEP,
                    key=f"{key_prefix}_temperature",
                    help=creativity_help
                )

            with col2:
                st.write("**LLM Model**")
                llm_model = st.selectbox(
                    "Select LLM",
                    options=LLM_OPTIONS,
                    index=default_llm_index,
                    key=f"{key_prefix}_llm_model"
                )

            with col3:
                st.write("**Report Size (Tokens)**")
                max_tokens = st.slider(
                    f"{config_type.upper()} Report Size",
                    min_value=tokens_min,
                    max_value=tokens_max,
                    value=default_tokens,
                    step=tokens_step,
                    key=f"{key_prefix}_tokens"
                )

            with col4:
                st.write("**Generation Instructions**")
                if st.button("✏️ Edit Instructions", key=f"{key_prefix}_customize_btn"):
                    st.session_state[f"{key_prefix}_show_editor"] = not st.session_state.get(f"{key_prefix}_show_editor", False)

            # Markdown editor for custom instructions
            if st.session_state.get(f"{key_prefix}_show_editor", False):
                st.write("**Edit Generation Instructions:**")

                # Select default prompt based on config_type AND analysis_type
                if config_type == "bref":
                    if analysis_type == "pnl":
                        default_prompt = PROMPT_BREF_ANALYSIS_PNL
                    elif analysis_type == "bs":
                        default_prompt = PROMPT_BREF_ANALYSIS_BS
                    else:  # cf
                        default_prompt = PROMPT_BREF_ANALYSIS_CF
                elif config_type == "fa":
                    if analysis_type == "pnl":
                        default_prompt = PROMPT_DRIVER_ANALYSIS_PNL
                    elif analysis_type == "bs":
                        default_prompt = PROMPT_DRIVER_ANALYSIS_BS
                    else:  # cf
                        default_prompt = PROMPT_DRIVER_ANALYSIS_CF
                else:  # combined
                    if analysis_type == "pnl":
                        default_prompt = PROMPT_COMBINE_ANALYSIS_PNL
                    elif analysis_type == "bs":
                        default_prompt = PROMPT_COMBINE_ANALYSIS_BS
                    else:  # cf
                        default_prompt = PROMPT_COMBINE_ANALYSIS_CF

                custom_prompt = st.text_area(
                    "Custom Instructions",
                    value=st.session_state.get(f"{key_prefix}_custom_prompt", default_prompt),
                    height=300,
                    key=f"{key_prefix}_prompt_editor"
                )

                col_save, col_reset = st.columns([1, 1])
                with col_save:
                    if st.button("💾 Save Instructions", key=f"{key_prefix}_save"):
                        st.session_state[f"{key_prefix}_custom_prompt"] = custom_prompt
                        st.success("Instructions saved!")

                with col_reset:
                    if st.button("🔄 Reset to Default", key=f"{key_prefix}_reset"):
                        st.session_state[f"{key_prefix}_custom_prompt"] = default_prompt
                        st.rerun()

        # Tab 2: Validation Instructions (only for BREF and FA)
        if config_type in ["bref", "fa"]:
            with tab2:
                st.write("**Validation Instructions**")
                if st.button("✏️ Edit Instructions", key=f"{key_prefix}_customize_validation_btn"):
                    st.session_state[f"{key_prefix}_show_validation_editor"] = not st.session_state.get(f"{key_prefix}_show_validation_editor", False)

                # Validation instructions editor
                if st.session_state.get(f"{key_prefix}_show_validation_editor", False):
                    st.write("**Edit Validation Instructions:**")

                    # Select default validation prompt based on config_type AND analysis_type
                    if config_type == "bref":
                        if analysis_type == "pnl":
                            default_validation_prompt = BREF_VALIDATION_PROMPT_PNL
                        elif analysis_type == "bs":
                            default_validation_prompt = BREF_VALIDATION_PROMPT_BS
                        else:  # cf
                            default_validation_prompt = BREF_VALIDATION_PROMPT_CF
                    else:  # fa
                        if analysis_type == "pnl":
                            default_validation_prompt = BREF_VALIDATION_PROMPT_PNL
                        elif analysis_type == "bs":
                            default_validation_prompt = BREF_VALIDATION_PROMPT_CF
                        else:  # cf
                            default_validation_prompt = BREF_VALIDATION_PROMPT_CF

                    custom_validation_prompt = st.text_area(
                        "Custom Validation Instructions",
                        value=st.session_state.get(f"{key_prefix}_custom_validation_prompt", default_validation_prompt),
                        height=300,
                        key=f"{key_prefix}_validation_prompt_editor"
                    )

                    col_save_val, col_reset_val = st.columns([1, 1])
                    with col_save_val:
                        if st.button("💾 Save Validation Instructions", key=f"{key_prefix}_save_validation"):
                            st.session_state[f"{key_prefix}_custom_validation_prompt"] = custom_validation_prompt
                            st.success("Validation instructions saved!")

                    with col_reset_val:
                        if st.button("🔄 Reset Validation to Default", key=f"{key_prefix}_reset_validation"):
                            st.session_state[f"{key_prefix}_custom_validation_prompt"] = default_validation_prompt
                            st.rerun()

    # Calculate default prompts based on config_type and analysis_type (for config_dict)
    if config_type == "bref":
        if analysis_type == "pnl":
            default_generation_prompt = PROMPT_BREF_ANALYSIS_PNL
        elif analysis_type == "bs":
            default_generation_prompt = PROMPT_BREF_ANALYSIS_BS
        else:  # cf
            default_generation_prompt = PROMPT_BREF_ANALYSIS_CF
    elif config_type == "fa":
        if analysis_type == "pnl":
            default_generation_prompt = PROMPT_DRIVER_ANALYSIS_PNL
        elif analysis_type == "bs":
            default_generation_prompt = PROMPT_DRIVER_ANALYSIS_BS
        else:  # cf
            default_generation_prompt = PROMPT_DRIVER_ANALYSIS_CF
    else:  # combined
        if analysis_type == "pnl":
            default_generation_prompt = PROMPT_COMBINE_ANALYSIS_PNL
        elif analysis_type == "bs":
            default_generation_prompt = PROMPT_COMBINE_ANALYSIS_BS
        else:  # cf
            default_generation_prompt = PROMPT_COMBINE_ANALYSIS_CF

    # Build return dictionary - ALWAYS return current values from session state
    config_dict = {
        'temperature': st.session_state.get(f"{key_prefix}_temperature", default_temp),
        'llm_model': st.session_state.get(f"{key_prefix}_llm_model", default_llm),
        'max_tokens': st.session_state.get(f"{key_prefix}_tokens", default_tokens),
        'custom_prompt': st.session_state.get(f"{key_prefix}_custom_prompt", default_generation_prompt)
    }

    # Add validation prompt for BREF and FA
    if config_type in ["bref", "fa"]:
        if config_type == "bref":
            if analysis_type == "pnl":
                default_val_prompt = BREF_VALIDATION_PROMPT_PNL
            elif analysis_type == "bs":
                default_val_prompt = BREF_VALIDATION_PROMPT_BS
            else:  # cf
                default_val_prompt = BREF_VALIDATION_PROMPT_CF
        else:  # fa
            if analysis_type == "pnl":
                default_val_prompt = BREF_VALIDATION_PROMPT_PNL
            elif analysis_type == "bs":
                default_val_prompt = BREF_VALIDATION_PROMPT_CF
            else:  # cf
                default_val_prompt = BREF_VALIDATION_PROMPT_CF

        config_dict['custom_validation_prompt'] = st.session_state.get(
            f"{key_prefix}_custom_validation_prompt",
            default_val_prompt
        )

    return config_dict


# ─── UI COMPONENTS ─────────────────────────────────────────
def add_status_update(message: str, status_type: str = "info"):
    """Add a status update to the log"""
    if 'status_log' not in st.session_state:
        st.session_state.status_log = []

    timestamp = datetime.now().strftime("%H:%M:%S")
    st.session_state.status_log.append({
        'timestamp': timestamp,
        'message': message,
        'type': status_type  # 'info', 'success', 'warning', 'error'
    })


def render_status_log():
    """Render the status log with auto-scroll"""
    if 'status_log' not in st.session_state or not st.session_state.status_log:
        return

    st.markdown("""
    <style>
    .status-container {
        max-height: 400px;
        overflow-y: auto;
        border: 1px solid #e0e0e0;
        border-radius: 5px;
        padding: 10px;
        background-color: #f8f9fa;
        font-family: 'Courier New', monospace;
        font-size: 0.8rem;
    }
    .status-entry {
        margin-bottom: 3px;
        padding: 2px 0;
        line-height: 1.4;
    }
    .status-timestamp {
        color: #6c757d;
        font-size: 0.75rem;
    }
    .status-message {
        color: #212529;
        margin-left: 5px;
    }
    </style>
    """, unsafe_allow_html=True)

    status_entries = []
    for entry in st.session_state.status_log:
        if entry['type'] == 'success':
            icon = '✅'
        elif entry['type'] == 'warning':
            icon = '⚠️'
        elif entry['type'] == 'error':
            icon = '❌'
        else:  # info
            icon = '🔄'

        status_entries.append(
            f"<div class='status-entry'>"
            f"<span class='status-timestamp'>[{entry['timestamp']}]</span> "
            f"{icon} "
            f"<span class='status-message'>{entry['message']}</span>"
            f"</div>"
        )

    status_html = f"<div class='status-container'>{''.join(status_entries)}</div>"
    st.markdown(status_html, unsafe_allow_html=True)


def render_header():
    """Render application header (called exactly once)"""
    st.title("🤖 Financial Analysis - Agentic AI System")
    st.caption("Multi-Agent Workflow for Intelligent Financial Analysis")
    st.divider()


def render_progress_bar(state: AgentState):
    """Render progress bar showing current workflow status"""
    progress = state.get('progress', 0.0)
    status = state.get('status_message', 'Initializing...')
    current_step = state.get('current_step', 'start')

    st.progress(progress, text=f"Progress: {int(progress * 100)}%")

    col1, col2 = st.columns([3, 1])
    with col1:
        st.info(f"📍 **Current Step:** {current_step}")
    with col2:
        st.metric("Status", status)

    if state.get('messages'):
        with st.expander("📋 Agent Activity Log", expanded=False):
            for msg in state['messages'][-10:]:
                if hasattr(msg, 'content'):
                    st.text(f"• {msg.content}")
                elif isinstance(msg, dict):
                    st.text(f"• [{msg.get('role', 'system')}] {msg.get('content', '')}")


def section_header(icon: str, title: str, subtitle: str = ""):
    """Render section header with icon and title"""
    st.markdown(f"""
    <div style="display:flex;align-items:center;gap:10px;margin:22px 0 8px 0;">
      <span style="font-size:1.4rem;">{icon}</span>
      <div>
        <div style="font-weight:600;font-size:1.05rem;color:#0f172a;">{title}</div>
        {"<div style='font-size:0.82rem;color:#64748b;'>" + subtitle + "</div>" if subtitle else ""}
      </div>
    </div>
    <hr style="margin:0 0 16px 0;border:none;border-top:1px solid #e2e8f0;">
    """, unsafe_allow_html=True)


def render_report_tabs(state: AgentState):
    """Render report tabs for different analysis sections"""
    tabs_to_show = []
    tab_contents = []

    if state.get('pnl_final_draft'):
        tabs_to_show.append("📊 P&L Analysis")
        tab_contents.append(('pnl', state['pnl_final_draft']))

    if state.get('bs_final_draft'):
        tabs_to_show.append("📈 Balance Sheet Analysis")
        tab_contents.append(('bs', state['bs_final_draft']))

    if state.get('cf_final_draft'):
        tabs_to_show.append("💰 Cash Flow Analysis")
        tab_contents.append(('cf', state['cf_final_draft']))

    if tabs_to_show:
        st.divider()
        section_header("📑", "Generated Reports", "AI-generated financial analysis reports")

        st.markdown("""
        <style>
        .report-container {
            max-width: 100%;
            overflow-x: auto;
            overflow-y: auto;
            max-height: 600px;
            padding: 20px;
            background-color: #f8f9fa;
            border-radius: 8px;
            border: 1px solid #e0e0e0;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            line-height: 1.6;
            white-space: pre-wrap;
            word-wrap: break-word;
            overflow-wrap: break-word;
        }
        .report-container p {
            white-space: pre-wrap;
            word-wrap: break-word;
            overflow-wrap: break-word;
            margin-bottom: 1em;
        }
        .report-container h1, .report-container h2, .report-container h3,
        .report-container h4, .report-container h5, .report-container h6 {
            margin-top: 1.5em;
            margin-bottom: 0.5em;
            font-weight: 600;
        }
        .report-container ul, .report-container ol {
            margin-left: 1.5em;
            margin-bottom: 1em;
        }
        .report-container li {
            margin-bottom: 0.5em;
        }
        .report-container code {
            background-color: #e9ecef;
            padding: 2px 6px;
            border-radius: 3px;
            font-family: 'Courier New', monospace;
        }
        .report-container pre {
            background-color: #e9ecef;
            padding: 12px;
            border-radius: 5px;
            overflow-x: auto;
            white-space: pre-wrap;
            word-wrap: break-word;
        }
        .report-container table {
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 1em;
        }
        .report-container th, .report-container td {
            border: 1px solid #dee2e6;
            padding: 8px 12px;
            text-align: left;
        }
        .report-container th {
            background-color: #e9ecef;
            font-weight: 600;
        }
        </style>
        """, unsafe_allow_html=True)

        tabs = st.tabs(tabs_to_show)

        for tab, (report_type, report_data) in zip(tabs, tab_contents):
            with tab:
                # Display extracted BREF financial data at the top
                if st.session_state.get('bref_extracted_data'):
                    st.markdown("## 📊 BREF Financial Data")
                    st.markdown("---")

                    extracted_data = st.session_state['bref_extracted_data']

                    if report_type == 'pnl' and extracted_data.get('pnl'):
                        st.markdown("### Income Statement (P&L)")
                        st.markdown(escape_dollar_for_markdown(extracted_data['pnl']))
                        st.markdown("---")
                    elif report_type == 'bs' and extracted_data.get('bs'):
                        st.markdown("### Balance Sheet")
                        st.markdown(escape_dollar_for_markdown(extracted_data['bs']))
                        st.markdown("---")
                    elif report_type == 'cf' and extracted_data.get('cf'):
                        st.markdown("### Cash Flow Statement")
                        st.markdown(escape_dollar_for_markdown(extracted_data['cf']))
                        st.markdown("---")

                report_content = report_data.get('report', {})

                # Define expected sections for each report type
                if report_type == 'pnl':
                    expected_sections = [
                        "Revenue Analysis",
                        "Gross profit margin Analysis",
                        "EBITDA and EBITDA Margin Analysis",
                        "Other income Analysis",
                        "Interest expenses Analysis",
                        "Net income and earnings quality Analysis",
                        "Key credit metrics from P&L Analysis"
                    ]
                elif report_type == 'bs':
                    expected_sections = [
                        "Total Asset Analysis",
                        "Total Equity Analysis",
                        "Debt & Leverage Analysis",
                        "Liquidity Analysis",
                        "Overall Balance Sheet Strength Analysis"
                    ]
                elif report_type == 'cf':
                    expected_sections = [
                        "Funds From Operations(FFO) Analysis",
                        "Working capital changes(WC) Analysis",
                        "Cash flow from operations(CFO) Analysis",
                        "Net capex Analysis",
                        "Free Cash Flow(FCF) Analysis",
                        "Business acquisitions and disposals(net) Analysis",
                        "Dividends received from JV/associates/subsidiaries Analysis",
                        "Dividends paid to shareholders & non‑controlling interests Analysis",
                        "Net debt drawn/repaid Analysis",
                        "Miscellaneous items Analysis",
                        "Net change in cash Analysis"
                    ]
                else:
                    expected_sections = []

                if isinstance(report_content, dict):
                    if 'raw_response' in report_content:
                        with st.container():
                            st.markdown(escape_dollar_for_markdown(report_content['raw_response']))
                    else:
                        report_without_citations, citations_by_section = reorganize_citations_for_display(report_content)

                        sections_displayed = []

                        section_mapping = {}
                        for key in report_without_citations.keys():
                            section_mapping[key.lower()] = key

                        for section in expected_sections:
                            actual_section_key = None
                            if section in report_without_citations:
                                actual_section_key = section
                            elif section.lower() in section_mapping:
                                actual_section_key = section_mapping[section.lower()]

                            if actual_section_key:
                                sections_displayed.append(actual_section_key)
                                st.markdown(f"### {section}")
                                section_data = report_without_citations[actual_section_key]

                                if isinstance(section_data, list) and len(section_data) > 0:
                                    item = section_data[0]
                                    if 'Analysis' in item and item['Analysis'] != 'Not Available':
                                        st.markdown("**Analysis:**")
                                        st.markdown(escape_dollar_for_markdown(item['Analysis']))
                                    if 'source_table' in item and item['source_table'] != 'Not Available':
                                        st.markdown("**Supporting Data:**")
                                        st.markdown(escape_dollar_for_markdown(item['source_table']))
                                elif isinstance(section_data, dict):
                                    if 'Analysis' in section_data and section_data['Analysis'] != 'Not Available':
                                        st.markdown("**Analysis:**")
                                        st.markdown(escape_dollar_for_markdown(section_data['Analysis']))
                                    if 'source_table' in section_data and section_data['source_table'] != 'Not Available':
                                        st.markdown("**Supporting Data:**")
                                        st.markdown(escape_dollar_for_markdown(section_data['source_table']))
                                elif isinstance(section_data, str):
                                    st.markdown(escape_dollar_for_markdown(section_data))

                                st.markdown("---")

                        # Display any additional sections not in expected_sections
                        for section_name, section_data in report_without_citations.items():
                            if section_name not in sections_displayed and section_name not in ['error', 'raw_response']:
                                st.markdown(f"### {section_name}")

                                if isinstance(section_data, list) and len(section_data) > 0:
                                    item = section_data[0]
                                    if 'Analysis' in item and item['Analysis'] != 'Not Available':
                                        st.markdown("**Analysis:**")
                                        st.markdown(item['Analysis'])
                                    if 'source_table' in item and item['source_table'] != 'Not Available':
                                        st.markdown("**Supporting Data:**")
                                        st.markdown(item['source_table'])
                                elif isinstance(section_data, dict):
                                    if 'Analysis' in section_data and section_data['Analysis'] != 'Not Available':
                                        st.markdown("**Analysis:**")
                                        st.markdown(section_data['Analysis'])
                                    if 'source_table' in section_data and section_data['source_table'] != 'Not Available':
                                        st.markdown("**Supporting Data:**")
                                        st.markdown(section_data['source_table'])
                                elif isinstance(section_data, str):
                                    st.markdown(section_data)
                                else:
                                    st.markdown(escape_dollar_for_markdown(str(section_data)))

                                st.markdown("---")

                        # Display all citations at the end
                        if citations_by_section:
                            st.markdown("---")
                            citations_markdown = render_citations_section(citations_by_section)
                            st.markdown(escape_dollar_for_markdown(citations_markdown))

                        with st.expander("🔍 View Raw JSON", expanded=False):
                            st.json(report_content)
                else:
                    with st.container():
                        st.markdown(escape_dollar_for_markdown(str(report_content)))

                st.download_button(
                    label=f"⬇️ Download {report_type.upper()} Report (JSON)",
                    data=json.dumps(report_data, indent=2),
                    file_name=f"{report_type}_final_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json",
                )


# ─── INITIALIZE STATE ──────────────────────────────────────
def initialize_agent_state(client_name: str, session_folder: str) -> AgentState:
    """Initialize the agent state"""
    return {
        'messages': [],
        'client_name': client_name,
        'session_folder': session_folder,

        # Document data
        'financial_summary_text': None,
        'financial_summary_path': None,
        'other_documents': [],
        'chunks': [],

        # BREF Summary
        'bref_summary': None,
        'bref_validation_result': None,
        'bref_retry_count': 0,
        'bref_accepted': False,

        # Configuration
        'bref_config': None,
        'fa_config': None,
        'combined_config': None,

        # P&L Analysis
        'pnl_chunks': [],
        'pnl_reports': [],
        'pnl_validation_results': [],
        'pnl_accepted_reports': [],

        # Balance Sheet Analysis
        'bs_chunks': [],
        'bs_reports': [],
        'bs_validation_results': [],
        'bs_accepted_reports': [],

        # Cash Flow Analysis
        'cf_chunks': [],
        'cf_reports': [],
        'cf_validation_results': [],
        'cf_accepted_reports': [],

        # Final outputs
        'pnl_final_draft': None,
        'bs_final_draft': None,
        'cf_final_draft': None,

        # Orchestrator state
        'has_all_sections': False,
        'sections_found': [],

        # Error handling
        'errors': [],
        'current_step': 'initialization',

        # UI state
        'progress': 0.0,
        'status_message': 'Ready to start',
    }


# ─── SUMMARY LOADERS ───────────────────────────────────────
def load_pipeline_summary(state: AgentState, summary_bytes: bytes):
    """Load a BREF summary handed over from the pipeline (Excel bytes).

    Non-interactive: parses the first sheet, stores the text, extracts BREF
    data tables, and marks bref_accepted=True so downstream steps unlock.
    """
    import tempfile

    session_folder = state['session_folder']
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False, dir=session_folder) as tmp:
        tmp.write(summary_bytes)
        tmp_path = tmp.name

    sheet_names = get_excel_sheets(tmp_path)
    selected_sheet = sheet_names[0] if sheet_names else None

    if selected_sheet:
        pages = parse_document(tmp_path, sheet_name=selected_sheet)
        full_text = "\n\n".join(p["text"] for p in pages)

        state['financial_summary_text'] = full_text
        state['financial_summary_path'] = tmp_path
        state['bref_accepted'] = True  # Skip BREF generation for pipeline summaries

        try:
            extracted_data = extract_bref_data_sync(full_text)
            st.session_state['bref_extracted_data'] = extracted_data
        except Exception as e:
            st.session_state.logger.error(f"Error extracting BREF data: {e}")
            st.session_state['bref_extracted_data'] = None

        st.session_state.logger.info(
            f"Pipeline mode: Processed summary with {len(pages)} pages"
        )


# ─── BREF GENERATION / VALIDATION (manual upload path) ─────
def render_bref_generation(state: AgentState):
    """Render BREF configuration, generation, validation, and summaries.

    Only relevant when a summary was uploaded manually and has not yet been
    accepted. For pipeline summaries bref_accepted is already True, so this is
    skipped entirely.
    """
    st.divider()
    st.caption("Generate and validate BREF financial summary")

    # BREF Configuration tabs
    bref_tabs = st.tabs(["📊 P&L BREF", "📈 BS BREF", "💰 CF BREF"])

    with bref_tabs[0]:
        bref_config_pnl = render_configuration_options("bref", "bref_pnl", "pnl")
        state['bref_config_pnl'] = bref_config_pnl
        if state.get('session_folder'):
            save_configuration_to_folder(bref_config_pnl, "bref_pnl", state['session_folder'])

    with bref_tabs[1]:
        bref_config_bs = render_configuration_options("bref", "bref_bs", "bs")
        state['bref_config_bs'] = bref_config_bs
        if state.get('session_folder'):
            save_configuration_to_folder(bref_config_bs, "bref_bs", state['session_folder'])

    with bref_tabs[2]:
        bref_config_cf = render_configuration_options("bref", "bref_cf", "cf")
        state['bref_config_cf'] = bref_config_cf
        if state.get('session_folder'):
            save_configuration_to_folder(bref_config_cf, "bref_cf", state['session_folder'])

    # Generate BREF button
    if st.button("🤖 Generate & Validate All BREFs (P&L, BS, CF)", key="generate_bref", type="primary"):
        st.session_state.workflow_running = True

        with st.spinner("🤖 Generating and validating all BREFs (P&L → BS → CF) with retry logic..."):
            workflow_app = st.session_state.workflow_app
            try:
                state = workflow_app.invoke(state)
                st.session_state.agent_state = state
                st.session_state.logger.info("BREF generation workflow completed")

                all_accepted = (
                    state.get('bref_accepted_pnl', False) and
                    state.get('bref_accepted_bs', False) and
                    state.get('bref_accepted_cf', False)
                )

                if all_accepted:
                    st.success("✅ All BREFs generated and validated successfully!")
                else:
                    failed_brefs = []
                    if not state.get('bref_accepted_pnl', False):
                        failed_brefs.append("P&L")
                    if not state.get('bref_accepted_bs', False):
                        failed_brefs.append("Balance Sheet")
                    if not state.get('bref_accepted_cf', False):
                        failed_brefs.append("Cash Flow")
                    if failed_brefs:
                        st.warning(f"⚠️ Some BREFs need attention: {', '.join(failed_brefs)}")

            except Exception as e:
                error_msg = f"BREF generation workflow failed: {str(e)}"
                st.session_state.logger.error(error_msg)
                st.error(error_msg)
                state['errors'].append(error_msg)
                st.session_state.agent_state = state

            st.rerun()

    # BREF validation results
    if (state.get('bref_validation_result_pnl') or
            state.get('bref_validation_result_bs') or
            state.get('bref_validation_result_cf')):
        st.divider()
        section_header("✅", "BREF Validation Results", "Validation status for all BREF types")

        st.markdown("""
        <style>
        .small-metric { font-size: 0.85rem !important; }
        .small-metric [data-testid="stMetricValue"] { font-size: 1.2rem !important; }
        .small-metric [data-testid="stMetricLabel"] { font-size: 0.75rem !important; }
        </style>
        """, unsafe_allow_html=True)

        result_tabs = []
        if state.get('bref_validation_result_pnl'):
            result_tabs.append("📊 P&L BREF")
        if state.get('bref_validation_result_bs'):
            result_tabs.append("📈 BS BREF")
        if state.get('bref_validation_result_cf'):
            result_tabs.append("💰 CF BREF")

        if result_tabs:
            tabs = st.tabs(result_tabs)
            tab_idx = 0

            def _render_validation(validation, retry_count, ok_label):
                col1, col2, col3 = st.columns(3)
                with col1:
                    status = "✅ Accepted" if validation.get('accepted') else "❌ Rejected"
                    st.markdown('<div class="small-metric">', unsafe_allow_html=True)
                    st.metric("Status", status)
                    st.markdown('</div>', unsafe_allow_html=True)
                with col2:
                    st.markdown('<div class="small-metric">', unsafe_allow_html=True)
                    st.metric("Score", f"{validation.get('score', 0)}/100")
                    st.markdown('</div>', unsafe_allow_html=True)
                with col3:
                    st.markdown('<div class="small-metric">', unsafe_allow_html=True)
                    st.metric("Retry Count", f"{retry_count}/3")
                    st.markdown('</div>', unsafe_allow_html=True)

                if not validation.get('accepted'):
                    st.warning(f"**Feedback:** {validation.get('feedback', 'No feedback provided')}")
                    if validation.get('issues'):
                        st.markdown("**Issues:**")
                        for issue in validation.get('issues', []):
                            st.markdown(f"- {issue}")
                else:
                    st.success(ok_label)

            if state.get('bref_validation_result_pnl'):
                with tabs[tab_idx]:
                    _render_validation(
                        state['bref_validation_result_pnl'],
                        state.get('bref_retry_count_pnl', 0),
                        "✅ P&L BREF accepted!"
                    )
                tab_idx += 1

            if state.get('bref_validation_result_bs'):
                with tabs[tab_idx]:
                    _render_validation(
                        state['bref_validation_result_bs'],
                        state.get('bref_retry_count_bs', 0),
                        "✅ Balance Sheet BREF accepted!"
                    )
                tab_idx += 1

            if state.get('bref_validation_result_cf'):
                with tabs[tab_idx]:
                    _render_validation(
                        state['bref_validation_result_cf'],
                        state.get('bref_retry_count_cf', 0),
                        "✅ Cash Flow BREF accepted!"
                    )

    # Max retries handling
    max_retries_reached = (
        state.get('bref_retry_count_pnl', 0) >= 3 or
        state.get('bref_retry_count_bs', 0) >= 3 or
        state.get('bref_retry_count_cf', 0) >= 3
    )

    if max_retries_reached:
        st.error("⚠️ Maximum retries reached for one or more BREFs.")
        st.warning("💡 **Recommendation:** Try changing the LLM model and starting a new session.")

        col1, col2 = st.columns([2, 1])
        with col1:
            st.markdown("**Select a different LLM model:**")
            new_llm_model = st.selectbox(
                "Choose LLM Model",
                options=LLM_OPTIONS,
                key="bref_retry_llm_selector",
                help="Select a different LLM model to retry BREF generation"
            )
        with col2:
            st.markdown("**Start fresh:**")
            if st.button("🔄 Change LLM & Start New Session", key="bref_change_llm_new_session", type="primary"):
                current_client = state['client_name']
                session_folder = get_session_folder(current_client, BASE_UPLOAD_DIR)
                logger = setup_logger(session_folder, current_client)

                st.session_state.agent_state = initialize_agent_state(current_client, session_folder)
                st.session_state.logger = logger

                st.session_state.agent_state['bref_config'] = {
                    'temperature': BREF_DEFAULT_TEMPERATURE,
                    'max_tokens': BREF_DEFAULT_MAX_TOKENS,
                    'llm_model': new_llm_model,
                    'custom_prompt': PROMPT_BREF_ANALYSIS_PNL
                }

                keys_to_remove = [key for key in st.session_state.keys() if key.startswith('uploaded_')]
                for key in keys_to_remove:
                    del st.session_state[key]

                log_section_start(f"NEW SESSION: {current_client} (Changed LLM to {new_llm_model})")
                log_step("Session folder created", session_folder)

                st.success(f"✅ New session created with LLM: {new_llm_model}")
                st.info("🔄 Please re-upload your financial summary document to start fresh BREF generation.")
                st.rerun()

    # BREF summaries
    if (state.get('bref_summary_pnl') or
            state.get('bref_summary_bs') or
            state.get('bref_summary_cf')):
        with st.expander("📄 View BREF Summaries", expanded=False):
            summary_tabs = []
            if state.get('bref_summary_pnl'):
                summary_tabs.append("📊 P&L BREF")
            if state.get('bref_summary_bs'):
                summary_tabs.append("📈 BS BREF")
            if state.get('bref_summary_cf'):
                summary_tabs.append("💰 CF BREF")

            if summary_tabs:
                tabs = st.tabs(summary_tabs)
                tab_idx = 0

                if state.get('bref_summary_pnl'):
                    with tabs[tab_idx]:
                        st.markdown("### P&L BREF Summary")
                        st.markdown(escape_dollar_for_markdown(state['bref_summary_pnl']))
                    tab_idx += 1

                if state.get('bref_summary_bs'):
                    with tabs[tab_idx]:
                        st.markdown("### Balance Sheet BREF Summary")
                        st.markdown(escape_dollar_for_markdown(state['bref_summary_bs']))
                    tab_idx += 1

                if state.get('bref_summary_cf'):
                    with tabs[tab_idx]:
                        st.markdown("### Cash Flow BREF Summary")
                        st.markdown(escape_dollar_for_markdown(state['bref_summary_cf']))

    # Original BREF document data
    if state.get('financial_summary_text'):
        with st.expander("📄 View Original BREF Document", expanded=False):
            st.markdown("### Original Financial Summary Document")
            st.text_area(
                "Original BREF Data",
                value=state['financial_summary_text'],
                height=400,
                disabled=True,
                key="original_bref_data_display"
            )


# ─── STEP 3/4/5 + RUN ──────────────────────────────────────
def render_analysis_workflow(state: AgentState):
    """Render Step 3 (upload docs), Step 4 (select agents), Step 5 (configure),
    and the Run button. Requires bref_accepted."""
    section_header("📚", "Step 3: Upload Additional Documents", "Upload supporting documents for analysis")

    reports_generated = (
        state.get('pnl_final_draft') is not None or
        state.get('bs_final_draft') is not None or
        state.get('cf_final_draft') is not None
    )

    if reports_generated:
        st.warning("⚠️ **Reports have already been generated for this session.**")
        st.info("📋 To upload new documents and generate fresh reports, please start a new session.")

        col_warn1, col_warn2 = st.columns([1, 3])
        with col_warn1:
            if st.button("🔄 Start New Session", key="start_new_session_btn", type="primary"):
                current_client = state['client_name']
                session_folder = get_session_folder(current_client, BASE_UPLOAD_DIR)
                logger = setup_logger(session_folder, current_client)

                st.session_state.agent_state = initialize_agent_state(current_client, session_folder)
                st.session_state.logger = logger

                keys_to_remove = [key for key in st.session_state.keys() if key.startswith('uploaded_')]
                for key in keys_to_remove:
                    del st.session_state[key]

                log_section_start(f"NEW SESSION: {current_client}")
                log_step("Session folder created", session_folder)

                st.success(f"✅ New session created: `{session_folder}`")
                st.info("🔄 Please re-upload your documents to start fresh analysis.")
                st.rerun()

        with col_warn2:
            st.caption("Starting a new session will reset all uploaded documents and generated reports.")

        st.divider()
        st.info("💡 **Tip:** You can view and download the existing reports below before starting a new session.")
        return  # reports already exist; don't show upload/config UI

    # ── Upload widgets (four categories) ──
    col1, col2, col3, col4 = st.columns(4)

    def _handle_uploads(files, category_key):
        """Shared upload handler for a category of documents."""
        for up in (files or []):
            if f"uploaded_{up.name}" in st.session_state:
                st.info(f"📝 Already uploaded: {up.name}")
                continue

            if up.name.lower().endswith(('.xlsx', '.xls', '.xlsm')):
                saved = save_uploaded_file(up, state['session_folder'])
                try:
                    sheet_names = get_excel_sheets(saved)
                    selected_sheet = st.selectbox(
                        f"Select sheet from {up.name}:",
                        options=sheet_names,
                        key=f"{category_key}_sheet_{up.name}"
                    )
                    if st.button(f"Process {up.name}", key=f"process_{category_key}_{up.name}"):
                        pages = parse_document(saved, sheet_name=selected_sheet)
                        save_parsed_json(pages, f"{up.name}_{selected_sheet}", PARSED_DOCS_DIR)
                        state['other_documents'].extend(pages)
                        st.session_state[f"uploaded_{up.name}"] = True
                        st.success(f"✅ {up.name}: {len(pages)} pages")
                        st.rerun()
                except Exception as e:
                    st.error(f"Error: {e}")
            else:
                saved = save_uploaded_file(up, state['session_folder'])
                pages = parse_document(saved)
                save_parsed_json(pages, up.name, PARSED_DOCS_DIR)
                state['other_documents'].extend(pages)
                st.session_state[f"uploaded_{up.name}"] = True
                st.success(f"✅ {up.name}: {len(pages)} pages")

    with col1:
        st.markdown("#### 🎙️ Earnings Transcripts")
        tr_ups = st.file_uploader(
            "Upload Transcript", type=["pdf", "docx", "txt", "html", "xlsx", "xls", "xlsm"],
            accept_multiple_files=True, key="transcript_up"
        )
        _handle_uploads(tr_ups, "transcript")

    with col2:
        st.markdown("#### 📰 News Articles")
        news_ups = st.file_uploader(
            "Upload News", type=["pdf", "docx", "txt", "html", "xlsx", "xls", "xlsm"],
            accept_multiple_files=True, key="news_up"
        )
        _handle_uploads(news_ups, "news")

    with col3:
        st.markdown("#### 📊 Presentations")
        pres_ups = st.file_uploader(
            "Upload Presentations", type=["pdf", "docx", "txt", "html", "xlsx", "xls", "xlsm"],
            accept_multiple_files=True, key="pres_up"
        )
        _handle_uploads(pres_ups, "pres")

    with col4:
        st.markdown("#### 📑 Annual Reports")
        annual_ups = st.file_uploader(
            "Upload Annual Reports", type=["pdf", "docx", "txt", "html", "xlsx", "xls", "xlsm"],
            accept_multiple_files=True, key="annual_up"
        )
        _handle_uploads(annual_ups, "annual")

    if state['other_documents']:
        st.info(f"📁 **{len(state['other_documents'])} pages** loaded from additional sources")

    st.divider()

    # ── Step 4 no longer vanishes: tell the user what to do ──
    if not state['other_documents']:
        st.info(
            "📄 Upload at least one supporting document above to enable "
            "**P&L / Balance Sheet / Cash Flow** analysis (Steps 4 & 5)."
        )
        return

    # ─── STEP 4: SELECT AGENTS TO RUN ──────────────────────
    section_header("🤖", "Step 4: Select Agents to Run", "Choose which analysis agents you want to execute")

    st.markdown("### Select Analysis Types")
    col1, col2, col3 = st.columns(3)

    with col1:
        run_pnl = st.checkbox("📊 P&L Analysis", value=True, key="run_pnl_agent")
        st.caption("Analyzes Profit & Loss statements")
    with col2:
        run_bs = st.checkbox("📈 Balance Sheet Analysis", value=True, key="run_bs_agent")
        st.caption("Analyzes Balance Sheet data")
    with col3:
        run_cf = st.checkbox("💰 Cash Flow Analysis", value=True, key="run_cf_agent")
        st.caption("Analyzes Cash Flow statements")

    state['sections_found'] = []
    if run_pnl:
        state['sections_found'].append('P&L')
    if run_bs:
        state['sections_found'].append('Balance Sheet')
    if run_cf:
        state['sections_found'].append('Cash Flow')

    if not state['sections_found']:
        st.warning("⚠️ Please select at least one analysis type to proceed.")
    else:
        st.info(f"✅ Selected: {', '.join(state['sections_found'])}")

    st.divider()

    # ─── STEP 5: CONFIGURE ANALYSIS ────────────────────────
    section_header("⚙️", "Step 5: Configure Analysis", "Set parameters for financial analysis")

    st.caption("FA Subagents to generate multiple FA Reports")
    fa_tabs = st.tabs(["📊 P&L Configuration", "📈 BS Configuration", "💰 CF Configuration"])

    with fa_tabs[0]:
        fa_config_pnl = render_configuration_options("fa", "fa_pnl", "pnl")
        state['fa_config_pnl'] = fa_config_pnl
        if state.get('session_folder'):
            save_configuration_to_folder(fa_config_pnl, "fa_pnl", state['session_folder'])
    with fa_tabs[1]:
        fa_config_bs = render_configuration_options("fa", "fa_bs", "bs")
        state['fa_config_bs'] = fa_config_bs
        if state.get('session_folder'):
            save_configuration_to_folder(fa_config_bs, "fa_bs", state['session_folder'])
    with fa_tabs[2]:
        fa_config_cf = render_configuration_options("fa", "fa_cf", "cf")
        state['fa_config_cf'] = fa_config_cf
        if state.get('session_folder'):
            save_configuration_to_folder(fa_config_cf, "fa_cf", state['session_folder'])

    st.markdown("---")

    st.markdown("### Final Report Configuration")
    st.caption("FA Sr. Analyst to Synthesize generated multiple reports into single Unified FA summary")

    combined_tabs = st.tabs(["📊 P&L Combined", "📈 BS Combined", "💰 CF Combined"])

    with combined_tabs[0]:
        combined_config_pnl = render_configuration_options("combined", "combined_pnl", "pnl")
        state['combined_config_pnl'] = combined_config_pnl
        if state.get('session_folder'):
            save_configuration_to_folder(combined_config_pnl, "combined_pnl", state['session_folder'])
    with combined_tabs[1]:
        combined_config_bs = render_configuration_options("combined", "combined_bs", "bs")
        state['combined_config_bs'] = combined_config_bs
        if state.get('session_folder'):
            save_configuration_to_folder(combined_config_bs, "combined_bs", state['session_folder'])
    with combined_tabs[2]:
        combined_config_cf = render_configuration_options("combined", "combined_cf", "cf")
        state['combined_config_cf'] = combined_config_cf
        if state.get('session_folder'):
            save_configuration_to_folder(combined_config_cf, "combined_cf", state['session_folder'])

    st.divider()

    # ── Run Analysis ──
    if st.button("🚀 Run Selected Financial Analysis", key="run_analysis", type="primary", disabled=not state['sections_found']):
        _run_analysis(state)


def _run_analysis(state: AgentState):
    """Execute the selected P&L / BS / CF parallel analyses."""
    st.session_state.workflow_running = True

    # Clear previous reports
    for prefix in ('pnl', 'bs', 'cf'):
        state[f'{prefix}_reports'] = []
        state[f'{prefix}_validation_results'] = []
        state[f'{prefix}_accepted_reports'] = []
        state[f'{prefix}_final_draft'] = None

    session_id = state.get('session_folder', 'default')
    tracker = get_tracker(session_id)
    tracker.reset()

    st.session_state.status_log = []
    st.session_state.logger.info(f"Starting analysis for: {', '.join(state['sections_found'])}")

    progress_placeholder = st.empty()
    status_container = st.container()

    with status_container:
        st.markdown("### 📊 Agent Execution Status")
        status_display = st.empty()

    def status_update_callback(message: str, status_type: str = "info"):
        add_status_update(message, status_type)
        with status_display.container():
            render_status_log()

    set_status_callback(status_update_callback)

    with st.spinner("🤖 Multi-Agent Workflow Running (LangGraph Parallel Processing)..."):
        from graph import chunk_generator_node
        from utils import save_dataframe
        from dynamic_graph import build_parallel_analysis_graph

        # Generate or reuse chunks
        if not state.get('chunks') or len(state.get('chunks', [])) == 0:
            add_status_update("Generating document chunks...", "info")
            with status_display.container():
                render_status_log()
            st.session_state.logger.info("Generating chunks from uploaded documents...")
            state = chunk_generator_node(state)
            add_status_update(f"Generated {len(state['chunks'])} chunks successfully", "success")
            with status_display.container():
                render_status_log()
        else:
            add_status_update(f"Using existing {len(state['chunks'])} chunks", "info")
            with status_display.container():
                render_status_log()
            st.session_state.logger.info(f"Using existing {len(state['chunks'])} chunks")

            from utils import build_context_chunk
            all_chunk_data = []
            for idx, chunk_pages in enumerate(state['chunks'], start=1):
                context = build_context_chunk(
                    state['bref_summary'],
                    chunk_pages,
                    "QUALITATIVE SOURCE"
                )
                all_chunk_data.append({
                    "chunk_index": idx,
                    "pages": chunk_pages,
                    "context": context
                })

            state['pnl_chunks'] = all_chunk_data if 'P&L' in state['sections_found'] else []
            state['bs_chunks'] = all_chunk_data if 'Balance Sheet' in state['sections_found'] else []
            state['cf_chunks'] = all_chunk_data if 'Cash Flow' in state['sections_found'] else []

        all_docs_path = save_dataframe(
            state['other_documents'],
            state['session_folder'],
            label="all_uploaded_documents"
        )
        st.session_state.logger.info(f"Saved all documents data to: {all_docs_path}")

        def _run_phase(section_label, chunk_key, prefix, phase_name):
            add_status_update(
                f"Starting {phase_name} with {len(state[chunk_key])} parallel sub-agents", "info"
            )
            with status_display.container():
                render_status_log()
            st.session_state.logger.info(
                f"Starting {phase_name} with {len(state[chunk_key])} parallel sub-agents..."
            )

            tracker = get_tracker(state.get('session_folder', 'default'))
            tracker.start_phase(phase_name)

            graph = build_parallel_analysis_graph(state, prefix)
            if graph:
                add_status_update(f"{phase_name} executing in parallel...", "info")
                with status_display.container():
                    render_status_log()

                result_state = graph.invoke(state)
                tracker.complete_phase(phase_name, "completed")
                st.session_state.logger.info(f"{phase_name} completed (LangGraph Parallel)")

                add_status_update(f"{phase_name} completed successfully", "success")
                with status_display.container():
                    render_status_log()

                with progress_placeholder.container():
                    tracker.render_progress_ui()
                return result_state
            else:
                add_status_update(f"No {phase_name} chunks to process", "warning")
                with status_display.container():
                    render_status_log()
                tracker.complete_phase(phase_name, "failed")
                return state

        if 'P&L' in state['sections_found']:
            state = _run_phase('P&L', 'pnl_chunks', 'pnl', "P&L Analysis")
        if 'Balance Sheet' in state['sections_found']:
            state = _run_phase('Balance Sheet', 'bs_chunks', 'bs', "Balance Sheet Analysis")
        if 'Cash Flow' in state['sections_found']:
            state = _run_phase('Cash Flow', 'cf_chunks', 'cf', "Cash Flow Analysis")

        st.session_state.agent_state = state
        st.session_state.workflow_running = False
        st.session_state.logger.info(
            "All selected analyses completed successfully using LangGraph parallel execution"
        )

        add_status_update("All analyses completed successfully!", "success")
        with status_display.container():
            render_status_log()

        with progress_placeholder.container():
            get_tracker(state.get('session_folder', 'default')).render_progress_ui()

        pnl_rejected = any(not v.get('accepted', True) for v in state.get('pnl_validation_results', []))
        bs_rejected = any(not v.get('accepted', True) for v in state.get('bs_validation_results', []))
        cf_rejected = any(not v.get('accepted', True) for v in state.get('cf_validation_results', []))

        if pnl_rejected or bs_rejected or cf_rejected:
            st.warning("⚠️ Some FA reports were rejected during validation.")
            st.info("💡 **Recommendation:** Review the validation results below or try changing the LLM model.")
        else:
            st.success("✅ Analysis Complete! (LangGraph Parallel Execution)")

        st.rerun()


# ─── EVALUATION SECTION ────────────────────────────────────
def render_evaluation_section(state: AgentState):
    """Report evaluation UI shown once reports exist."""
    st.divider()
    section_header("🔍", "Report Evaluation", "Upload additional reports or evaluate generated reports")

    st.markdown("### Upload Additional Reports for Evaluation (Optional)")
    st.caption("You can upload existing report files to evaluate them alongside generated reports.")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 📊 Upload PNL Report")
        pnl_upload = st.file_uploader(
            "Upload pnl_final_draft.json", type=["json"], key="pnl_report_upload",
            help="Upload an existing PNL report JSON file for evaluation"
        )
        if pnl_upload:
            try:
                pnl_data = json.load(pnl_upload)
                if 'manual_pnl_report' not in st.session_state:
                    st.session_state.manual_pnl_report = pnl_data
                st.success(f"✅ PNL report uploaded: {pnl_upload.name}")
                pnl_path = Path(state['session_folder']) / "pnl_final_draft_manual.json"
                with open(pnl_path, 'w', encoding='utf-8') as f:
                    json.dump(pnl_data, f, indent=2)
                st.session_state.logger.info(f"Manual PNL report saved: {pnl_path}")
            except Exception as e:
                st.error(f"Error loading PNL report: {e}")

    with col2:
        st.markdown("#### 📈 Upload BS Report")
        bs_upload = st.file_uploader(
            "Upload bs_final_draft.json", type=["json"], key="bs_report_upload",
            help="Upload an existing Balance Sheet report JSON file for evaluation"
        )
        if bs_upload:
            try:
                bs_data = json.load(bs_upload)
                if 'manual_bs_report' not in st.session_state:
                    st.session_state.manual_bs_report = bs_data
                st.success(f"✅ BS report uploaded: {bs_upload.name}")
                bs_path = Path(state['session_folder']) / "bs_final_draft_manual.json"
                with open(bs_path, 'w', encoding='utf-8') as f:
                    json.dump(bs_data, f, indent=2)
                st.session_state.logger.info(f"Manual BS report saved: {bs_path}")
            except Exception as e:
                st.error(f"Error loading BS report: {e}")

    st.markdown("---")
    st.markdown("### Available Reports for Evaluation")

    available_reports = []
    if state.get('pnl_final_draft') or st.session_state.get('manual_pnl_report'):
        available_reports.append("📊 PNL Report")
    if state.get('bs_final_draft') or st.session_state.get('manual_bs_report'):
        available_reports.append("📈 Balance Sheet Report")
    if state.get('cf_final_draft'):
        available_reports.append("💰 Cash Flow Report")

    if available_reports:
        st.info(f"**Reports ready for evaluation:** {', '.join(available_reports)}")
    else:
        st.warning("⚠️ No reports available. Please generate reports or upload existing ones.")

    st.markdown("---")
    evaluation_disabled = len(available_reports) == 0
    if st.button("🔍 Run Insight Quality Evaluation", key="run_comprehensive_eval_btn", type="primary", disabled=evaluation_disabled):
        with st.spinner("🤖 Running insight quality evaluation..."):
            try:
                import asyncio
                from fa_evaluation import evaluate_report

                pnl_path = None
                bs_path = None
                cf_path = None

                if st.session_state.get('manual_pnl_report'):
                    pnl_path = Path(state['session_folder']) / "pnl_final_draft_manual.json"
                    st.session_state.logger.info("Using manually uploaded PNL report for evaluation")
                elif state.get('pnl_final_draft'):
                    pnl_path = Path(state['session_folder']) / "pnl_final_draft.json"
                    with open(pnl_path, 'w', encoding='utf-8') as f:
                        json.dump(state['pnl_final_draft'], f, indent=2)
                    st.session_state.logger.info("Using generated PNL report for evaluation")

                if st.session_state.get('manual_bs_report'):
                    bs_path = Path(state['session_folder']) / "bs_final_draft_manual.json"
                    st.session_state.logger.info("Using manually uploaded BS report for evaluation")
                elif state.get('bs_final_draft'):
                    bs_path = Path(state['session_folder']) / "bs_final_draft.json"
                    with open(bs_path, 'w', encoding='utf-8') as f:
                        json.dump(state['bs_final_draft'], f, indent=2)
                    st.session_state.logger.info("Using generated BS report for evaluation")

                if state.get('cf_final_draft'):
                    cf_path = Path(state['session_folder']) / "cf_final_draft.json"
                    with open(cf_path, 'w', encoding='utf-8') as f:
                        json.dump(state['cf_final_draft'], f, indent=2)
                    st.session_state.logger.info("Using generated CF report for evaluation")

                comprehensive_results = {}

                if pnl_path:
                    st.session_state.logger.info("Evaluating PNL report...")
                    comprehensive_results['pnl'] = asyncio.run(evaluate_report(str(pnl_path), state['session_folder']))
                if bs_path:
                    st.session_state.logger.info("Evaluating BS report...")
                    comprehensive_results['bs'] = asyncio.run(evaluate_report(str(bs_path), state['session_folder']))
                if cf_path:
                    st.session_state.logger.info("Evaluating CF report...")
                    comprehensive_results['cf'] = asyncio.run(evaluate_report(str(cf_path), state['session_folder']))

                eval_folder = Path(state['session_folder']) / "evaluations"
                eval_folder.mkdir(exist_ok=True)

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                eval_filename = f"insight_quality_evaluation_{state['client_name']}_{timestamp}.json"
                eval_filepath = eval_folder / eval_filename

                with open(eval_filepath, 'w', encoding='utf-8') as f:
                    json.dump(comprehensive_results, f, indent=2, ensure_ascii=False)

                st.session_state.comprehensive_evaluation_results = comprehensive_results
                st.session_state.logger.info(f"Insight quality evaluation saved to: {eval_filepath}")
                st.success(f"✅ Insight quality evaluation completed! Results saved to: {eval_filepath}")
                st.rerun()

            except Exception as e:
                st.error(f"❌ Insight quality evaluation failed: {str(e)}")
                st.session_state.logger.error(f"Insight quality evaluation error: {e}")
                import traceback
                st.session_state.logger.error(traceback.format_exc())

    # Display comprehensive evaluation results
    if st.session_state.get('comprehensive_evaluation_results'):
        comp_results = st.session_state.comprehensive_evaluation_results

        st.divider()
        st.markdown("### 📊 Evaluation Results")

        comp_eval_tabs = []
        comp_eval_tab_data = []
        for report_type, report_eval in comp_results.items():
            comp_eval_tabs.append(f"{report_type.upper()} Report")
            comp_eval_tab_data.append((report_type, report_eval))

        if comp_eval_tabs:
            tabs = st.tabs(comp_eval_tabs)

            for tab, (report_type, report_eval) in zip(tabs, comp_eval_tab_data):
                with tab:
                    st.markdown("#### 🎯 Overall Report-Level Scores")
                    st.caption("Aggregated scores across all sections")

                    if 'report_level_scores' in report_eval:
                        report_level = report_eval['report_level_scores']
                        faithfulness_score = report_level.get('faithfulness', 0)
                        st.metric(
                            "📊 Faithfulness", f"{faithfulness_score:.1f}%",
                            help="Overall factual consistency with source documents"
                        )
                        if faithfulness_score >= 90:
                            st.success("✅ Excellent")
                        elif faithfulness_score >= 70:
                            st.info("✓ Good")
                        elif faithfulness_score >= 50:
                            st.warning("⚠ Fair")
                        else:
                            st.error("❌ Poor")

                        if 'claim_statistics' in report_level:
                            st.markdown("---")
                            st.markdown("**📋 Overall Claim Validation Summary**")
                            claim_stats = report_level['claim_statistics']
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.metric("Total Claims", claim_stats.get('total_claims', 0))
                            with col2:
                                st.metric("✅ Passed", claim_stats.get('passed_claims', 0))
                            with col3:
                                st.metric("❌ Failed", claim_stats.get('failed_claims', 0))
                            with col4:
                                st.metric("Pass Rate", claim_stats.get('overall_pass_rate', '0%'))

                        st.markdown("---")

                    st.markdown("#### 📄 Section-Level Details")
                    st.caption("Detailed scores for each section")

                    section_data = report_eval.get('section_scores', report_eval.get('section_level_metrics', {}))

                    for section_name, section_metrics in section_data.items():
                        if section_metrics.get('skipped'):
                            continue

                        with st.expander(f"📄 {section_name}", expanded=False):
                            st.markdown("**📊 Section Scores:**")
                            if 'faithfulness' in section_metrics:
                                faith_score = section_metrics['faithfulness'].get('percentage', 'N/A')
                                st.metric("Faithfulness", faith_score)
                            else:
                                st.metric("Faithfulness", "N/A")

                            st.markdown("---")

                            if 'claim_validation' in section_metrics:
                                st.markdown("**📋 Claim Validation:**")
                                claim_val = section_metrics['claim_validation']
                                col1, col2, col3 = st.columns(3)
                                with col1:
                                    st.metric("Total Claims", claim_val.get('total_claims', 0))
                                with col2:
                                    st.metric("✅ Passed", claim_val.get('passed', 0))
                                with col3:
                                    st.metric("❌ Failed", claim_val.get('failed', 0))

                                st.info(f"Pass Rate: {claim_val.get('pass_rate', '0%')}")

                                if claim_val.get('claims'):
                                    with st.expander("🔍 View Individual Claims", expanded=False):
                                        for claim in claim_val.get('claims', []):
                                            status_icon = "✅" if claim.get('status') == 'Passed' else "❌"
                                            st.markdown(f"{status_icon} **Claim {claim.get('claim_number', 0)}**: {claim.get('claim_text', '')}")
                                            st.caption(f"**Status**: {claim.get('status', 'Unknown')} | **Score**: {claim.get('score', 0)}")
                                            st.caption(f"**Remarks**: {claim.get('remarks', 'N/A')}")
                                            st.markdown("---")

        st.markdown("---")
        st.download_button(
            label="⬇️ Download Insight Quality Evaluation Results (JSON)",
            data=json.dumps(comp_results, indent=2),
            file_name=f"insight_quality_evaluation_{state['client_name']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json",
        )


# ─── MAIN APPLICATION ─────────────────────────────────────
def main():
    # Guard set_page_config so a wrapper that already called it won't crash.
    try:
        st.set_page_config(
            page_title="FA Agentic AI",
            page_icon="🤖",
            layout="wide",
            initial_sidebar_state="collapsed",
        )
    except Exception:
        pass

    # Header renders exactly once.
    render_header()

    # ── Session-state init ──
    st.session_state.setdefault('agent_state', None)
    st.session_state.setdefault('workflow_running', False)
    if 'workflow_app' not in st.session_state:
        st.session_state.workflow_app = build_financial_analysis_graph()

    # ── Pipeline handoff values (set by the wrapper, if any) ──
    pipeline_company = st.session_state.get('_fa_company_name')
    pipeline_bytes = st.session_state.get('_fa_summary_bytes')

    # ═══════════════════════════════════════════════════════
    # STEP 1: CLIENT & SUMMARY SOURCE
    # ═══════════════════════════════════════════════════════
    section_header("🏢", "Step 1: Client & Summary Source",
                   "Choose the client and how to provide the BREF summary")

    # Resolve client name
    if pipeline_company:
        client_name = pipeline_company
        st.markdown(f"**🏢 Client:** {client_name}")
    else:
        client_name = st.selectbox("Select Client", ["— Select —"] + CLIENT_LIST, key="client_select")
        if client_name == "— Select —":
            st.info("👆 Please select a client to begin.")
            return

    # Ensure agent_state exists for this client
    if (st.session_state.agent_state is None or
            st.session_state.agent_state['client_name'] != client_name):
        session_folder = get_session_folder(client_name, BASE_UPLOAD_DIR)
        st.session_state.logger = setup_logger(session_folder, client_name)
        st.session_state.agent_state = initialize_agent_state(client_name, session_folder)
        log_section_start(f"NEW SESSION: {client_name}")
        log_step("Session folder created", session_folder)

    state = st.session_state.agent_state

    # Summary source radio (this is the "Step 1" radio, now living inside main)
    default_idx = 0 if pipeline_bytes else 1
    summary_source = st.radio(
        "Choose how to provide the BREF summary:",
        ["Use Summary from Pipeline", "Upload BREF Summary Table"],
        index=default_idx,
        key="summary_source_choice",
    )

    # ── Branch A: pipeline summary ──
    if summary_source == "Use Summary from Pipeline":
        if not pipeline_bytes:
            st.warning(
                "⚠️ No pipeline summary was handed over. "
                "Switch to **Upload BREF Summary Table**, or run the ingestion "
                "pipeline first so it can provide a summary."
            )
            return

        # Load once; parsing sets bref_accepted=True and unlocks Step 3+.
        if not state.get('bref_accepted'):
            with st.spinner("⏳ Loading summary from pipeline..."):
                try:
                    load_pipeline_summary(state, pipeline_bytes)
                except Exception as e:
                    st.error(f"Error processing pipeline summary: {e}")
                    return

        st.success("✅ Using summary generated from pipeline")

    # ── Branch B: manual BREF upload ──
    else:
        section_header("📄", "Upload Financial Summary", "Upload BREF financial summary document")

        f_up = st.file_uploader(
            "Upload Financial Summary (BREF)",
            type=["pdf", "docx", "txt", "html", "xlsx", "xls", "xlsm"],
            key="f_summary_uploader",
            help="Upload your prepared financial summary document",
        )

        if f_up and not state.get('financial_summary_path'):
            if f_up.name.lower().endswith(('.xlsx', '.xls', '.xlsm')):
                saved_path = save_uploaded_file(f_up, state['session_folder'])
                try:
                    sheet_names = get_excel_sheets(saved_path)
                    st.info(f"📊 Excel file detected with {len(sheet_names)} sheet(s)")
                    selected_sheet = st.selectbox(
                        "Select the sheet to process:",
                        options=sheet_names,
                        key="f_summary_sheet_selector"
                    )
                    if st.button("Process Selected Sheet", key="process_f_summary_sheet"):
                        with st.spinner(f"⏳ Processing sheet '{selected_sheet}'..."):
                            pages = parse_document(saved_path, sheet_name=selected_sheet)
                            save_parsed_json(pages, f"{f_up.name}_{selected_sheet}", PARSED_DOCS_DIR)
                            full_text = "\n\n".join(p["text"] for p in pages)
                            state['financial_summary_text'] = full_text
                            state['financial_summary_path'] = saved_path

                            try:
                                extracted_data = extract_bref_data_sync(full_text)
                                st.session_state['bref_extracted_data'] = extracted_data
                            except Exception as e:
                                st.session_state.logger.error(f"Error extracting BREF data: {e}")
                                st.session_state['bref_extracted_data'] = None

                            st.session_state.logger.info(
                                f"Financial summary uploaded: {f_up.name} (Sheet: {selected_sheet}), {len(pages)} pages"
                            )
                            st.success(f"✅ Processed sheet '{selected_sheet}' - {len(pages)} pages")
                            st.rerun()
                except Exception as e:
                    st.error(f"Error processing Excel file: {e}")
            else:
                with st.spinner("⏳ Processing Financial Summary..."):
                    saved_path = save_uploaded_file(f_up, state['session_folder'])
                    pages = parse_document(saved_path)
                    save_parsed_json(pages, f_up.name, PARSED_DOCS_DIR)

                full_text = "\n\n".join(p["text"] for p in pages)
                state['financial_summary_text'] = full_text
                state['financial_summary_path'] = saved_path

                with st.spinner("⏳ Extracting financial data tables..."):
                    try:
                        extracted_data = extract_bref_data_sync(full_text)
                        st.session_state['bref_extracted_data'] = extracted_data
                        st.session_state.logger.info("BREF financial data extracted successfully")
                    except Exception as e:
                        st.session_state.logger.error(f"Error extracting BREF data: {e}")
                        st.session_state['bref_extracted_data'] = None

                st.session_state.logger.info(f"Financial summary uploaded: {f_up.name}, {len(pages)} pages")
                st.success(f"✅ Processed {len(pages)} pages")

        # BREF generation / validation until accepted
        if state.get('financial_summary_path') and not state.get('bref_accepted'):
            render_bref_generation(state)

        # If BREF not yet accepted, don't proceed to Step 3+
        if not state.get('bref_accepted'):
            st.info("ℹ️ Generate and accept the BREF summary above to continue to document upload.")
            return

    # Optional progress bar
    if state.get('progress', 0) > 0:
        render_progress_bar(state)

    st.divider()

    # ═══════════════════════════════════════════════════════
    # STEP 3 → 5 (requires an accepted BREF summary)
    # ═══════════════════════════════════════════════════════
    if state.get('bref_accepted'):
        render_analysis_workflow(state)

    # ═══════════════════════════════════════════════════════
    # RESULTS + EVALUATION
    # ═══════════════════════════════════════════════════════
    if state.get('pnl_final_draft') or state.get('bs_final_draft') or state.get('cf_final_draft'):
        render_report_tabs(state)
        render_evaluation_section(state)

    # ── Errors ──
    if state.get('errors'):
        st.divider()
        with st.expander("⚠️ Errors & Warnings", expanded=False):
            for error in state['errors']:
                st.error(error)


# ─── ENTRY POINT ───────────────────────────────────────────
if __name__ == "__main__":
    main()
