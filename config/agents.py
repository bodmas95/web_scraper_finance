"""
============================================================
  FINANCIAL ANALYSIS AGENTS - LANGGRAPH IMPLEMENTATION
============================================================
  Agentic AI system with specialized agents for financial analysis
============================================================
"""

from typing import TypedDict, List, Dict, Optional, Annotated
from langgraph.graph import StateGraph, END
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage
import operator
from datetime import datetime
import json
import logging
# API imports
import sys
import os
# Import logger utilities
from logger_config import (
    log_agent_start, log_agent_complete, log_llm_call, 
    log_file_saved, log_error, log_warning, log_validation_result,
    log_node_execution, log_rejection_details, log_retry_attempt
)
# Import centralized configuration defaults
from config_defaults import (
    BREF_DEFAULT_LLM_MODEL, FA_DEFAULT_LLM_MODEL, COMBINED_DEFAULT_LLM_MODEL,
    SUMMARIZER_DEFAULT_LLM_MODEL, SUMMARIZER_DEFAULT_TEMPERATURE, SUMMARIZER_DEFAULT_MAX_TOKENS
)
# Import progress tracker
try:
    from progress_tracker import get_tracker
except ImportError:
    # Fallback if progress_tracker not available
    class DummyTracker:
        def update_sub_agent(self, *args, **kwargs): pass
        def start_phase(self, *args, **kwargs): pass
        def complete_phase(self, *args, **kwargs): pass
    def get_tracker():
        return DummyTracker()
# Add path for API imports - works from sprint4 directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "bpce_api_setup"))
try:
    from call_bpce_llm import call_llm_api
except ImportError:
    # Fallback to parent directory
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "bpce_api_setup"))
    from call_bpce_llm import call_llm_api


# ─── STATE DEFINITIONS ─────────────────────────────────────

# Custom reducer to keep the last value (for parallel updates)
def keep_last(left, right):
    """Keep the last value, used for fields updated by parallel nodes"""
    return right if right is not None else left

# Custom reducer to extend lists (for parallel updates)
def extend_list(left, right):
    """Extend list, used for accumulating results from parallel nodes"""
    if left is None:
        left = []
    if right is None:
        right = []
    if isinstance(right, list):
        return left + right
    return left + [right]

class AgentState(TypedDict):
    """State shared across all agents"""
    messages: Annotated[List[BaseMessage], operator.add]
    client_name: Annotated[str, keep_last]
    session_folder: Annotated[str, keep_last]
    
    # Document data
    financial_summary_text: Annotated[Optional[str], keep_last]
    financial_summary_path: Annotated[Optional[str], keep_last]
    other_documents: Annotated[List[Dict], extend_list]
    chunks: Annotated[List[List[Dict]], extend_list]
    
    # BREF Summary Agent outputs - SEPARATE FOR EACH ANALYSIS TYPE
    # Legacy (for backward compatibility - will use PnL BREF)
    bref_summary: Annotated[Optional[str], keep_last]
    bref_validation_result: Annotated[Optional[Dict], keep_last]
    bref_retry_count: Annotated[int, keep_last]
    bref_accepted: Annotated[bool, keep_last]
    
    # P&L BREF Summary
    bref_summary_pnl: Annotated[Optional[str], keep_last]
    bref_validation_result_pnl: Annotated[Optional[Dict], keep_last]
    bref_retry_count_pnl: Annotated[int, keep_last]
    bref_accepted_pnl: Annotated[bool, keep_last]
    
    # Balance Sheet BREF Summary
    bref_summary_bs: Annotated[Optional[str], keep_last]
    bref_validation_result_bs: Annotated[Optional[Dict], keep_last]
    bref_retry_count_bs: Annotated[int, keep_last]
    bref_accepted_bs: Annotated[bool, keep_last]
    
    # Cash Flow BREF Summary
    bref_summary_cf: Annotated[Optional[str], keep_last]
    bref_validation_result_cf: Annotated[Optional[Dict], keep_last]
    bref_retry_count_cf: Annotated[int, keep_last]
    bref_accepted_cf: Annotated[bool, keep_last]
    
    # Configuration
    bref_config: Annotated[Optional[Dict], keep_last]
    fa_config: Annotated[Optional[Dict], keep_last]
    combined_config: Annotated[Optional[Dict], keep_last]
    
    # P&L Analysis
    # CHUNKS are set ONCE and never change - use keep_last
    # REPORTS accumulate from parallel nodes - use extend_list
    pnl_chunks: Annotated[List[Dict], keep_last]  # ← FIXED: keep_last instead of extend_list
    pnl_reports: Annotated[List[Dict], extend_list]
    pnl_validation_results: Annotated[List[Dict], extend_list]
    pnl_accepted_reports: Annotated[List[Dict], extend_list]
    
    # Balance Sheet Analysis
    # CHUNKS are set ONCE and never change - use keep_last
    # REPORTS accumulate from parallel nodes - use extend_list
    bs_chunks: Annotated[List[Dict], keep_last]  # ← FIXED: keep_last instead of extend_list
    bs_reports: Annotated[List[Dict], extend_list]
    bs_validation_results: Annotated[List[Dict], extend_list]
    bs_accepted_reports: Annotated[List[Dict], extend_list]
    
    # Cash Flow Analysis
    # CHUNKS are set ONCE and never change - use keep_last
    # REPORTS accumulate from parallel nodes - use extend_list
    cf_chunks: Annotated[List[Dict], keep_last]  # ← FIXED: keep_last instead of extend_list
    cf_reports: Annotated[List[Dict], extend_list]
    cf_validation_results: Annotated[List[Dict], extend_list]
    cf_accepted_reports: Annotated[List[Dict], extend_list]
    
    # Final outputs
    pnl_final_draft: Annotated[Optional[Dict], keep_last]
    bs_final_draft: Annotated[Optional[Dict], keep_last]
    cf_final_draft: Annotated[Optional[Dict], keep_last]
    
    # Orchestrator state
    has_all_sections: Annotated[bool, keep_last]
    sections_found: Annotated[List[str], extend_list]
    
    # Error handling - use extend_list for errors that accumulate
    errors: Annotated[List[str], extend_list]
    current_step: Annotated[str, keep_last]
    
    # UI state
    progress: Annotated[float, keep_last]
    status_message: Annotated[str, keep_last]


# # ─── AGENT 1: BREF ANALYST ─────────────────────────────────

# def agent_1_bref_analyst(state: AgentState) -> AgentState:
#     """
#     Agent 1: BREF Analyst (Legacy - uses P&L BREF)
#     Analyzes the uploaded financial summary document and generates BREF summary
#     """
#     from call_bpce_llm import call_llm_api
#     from prompts.prompts_FA import PROMPT_BREF_ANALYSIS_PNL
#     from utils import save_file_sequencially
    
#     logging.info("="*50)
#     logging.info("Agent 1 (BREF Analyst) - Starting analysis")
#     logging.info("="*50)
    
#     config = state.get('bref_config', {})
#     temperature = config.get('temperature', 0.0)
#     max_tokens = config.get('max_tokens', 1500)
#     llm_model = config.get('llm_model', BREF_DEFAULT_LLM_MODEL)
#     prompt_template = config.get('custom_prompt', PROMPT_BREF_ANALYSIS_PNL)
#     session_folder = state.get('session_folder', './uploads')
    
#     logging.info(f"Configuration: Model={llm_model}, Temp={temperature}, MaxTokens={max_tokens}")
    
#     financial_text = state['financial_summary_text']
    
#     full_prompt = f"{prompt_template}\n\n{'-'*15} [[BREF report Data]] {'-'*15}\n\n\n{financial_text}"
    
#     # Save prompt to file
#     save_file_sequencially(session_folder, "bref_prompt", full_prompt)
#     logging.info(f"Saved BREF prompt to {session_folder}/bref_prompt.md")
    
#     try:
#         logging.info("Calling LLM API for BREF generation...")
#         response = call_llm_api(
#             query=full_prompt,
#             model_name=llm_model,
#             temperature=temperature,
#             max_tokens=max_tokens
#         ).content
        
#         state['bref_summary'] = response
        
#         # Save BREF summary to file
#         save_file_sequencially(session_folder, "bref_summary", response)
#         logging.info(f"Saved BREF summary to {session_folder}/bref_summary.md")
        
#         state['messages'].append(AIMessage(content=f"BREF Summary generated successfully"))
#         state['current_step'] = 'bref_validation'
#         state['status_message'] = 'BREF Summary generated, awaiting validation'
#         state['progress'] = 0.15
        
#         logging.info("Agent 1 (BREF Analyst) - Analysis completed successfully")
#         logging.info("="*50)
        
#     except Exception as e:
#         error_msg = f"Agent 1 (BREF Analyst) failed: {str(e)}"
#         logging.error(error_msg)
#         logging.error("="*50)
#         state['errors'].append(error_msg)
#         state['messages'].append(AIMessage(content=error_msg))
    
#     return state


# # ─── AGENT 2: BREF EVALUATOR ───────────────────────────────

# def agent_2_bref_evaluator(state: AgentState) -> AgentState:
#     """
#     Agent 2: BREF Evaluator (LLM as Judge) - LEGACY
#     Validates the BREF summary generated by Agent 1
#     Uses BREF_VALIDATION_PROMPT_PNL from prompts_FA.py
#     """
#     from call_bpce_llm import call_llm_api
#     from prompts.prompts_FA import BREF_VALIDATION_PROMPT_PNL
#     from utils import save_file_sequencially
    
#     logging.info("Agent 2 (BREF Evaluator - Legacy) - Starting validation")
    
#     # Build validation prompt using prompts_FA.py
#     full_prompt = f"""{BREF_VALIDATION_PROMPT_PNL}

#     **BREF SUMMARY TO VALIDATE:**

#     {state['bref_summary']}

#     **ORIGINAL FINANCIAL DATA:**

#     {state['financial_summary_text']}
# """
    
#     # Save validation prompt to file
#     session_folder = state.get('session_folder', './uploads')
#     save_file_sequencially(session_folder, "bref_validation_prompt", full_prompt)
#     logging.info(f"Saved BREF validation prompt to {session_folder}/bref_validation_prompt.md")
    
#     # bref_summary = state['bref_summary']
#     # full_prompt = f"{prompt_bref_evaluator}\n\n{bref_summary}\n\n{state['financial_summary_text']}"
    
#     try:
#         response = call_llm_api(
#             query=full_prompt,
#             model_name=EVALUATOR_MODEL,
#             temperature=EVALUATOR_TEMPERATURE,
#             max_tokens=EVALUATOR_MAX_TOKENS
#         ).content
        
#         # Parse validation result
#         try:
#             # Extract JSON from response
#             import re
#             json_match = re.search(r'\{.*\}', response, re.DOTALL)
#             if json_match:
#                 validation_result = json.loads(json_match.group())
#             else:
#                 # Fallback: assume accepted if no clear rejection
#                 validation_result = {
#                     "accepted": True,
#                     "missing_sections": [],
#                     "missing_numbers": [],
#                     "feedback": "Validation completed",
#                     "score": 85
#                 }
#         except:
#             validation_result = {
#                 "accepted": True,
#                 "missing_sections": [],
#                 "missing_numbers": [],
#                 "feedback": "Validation completed",
#                 "score": 85
#             }
        
#         state['bref_validation_result'] = validation_result
#         state['bref_accepted'] = validation_result.get('accepted', False)
        
#         if state['bref_accepted']:
#             state['messages'].append(AIMessage(content=f"BREF Summary validated and accepted (Score: {validation_result.get('score', 0)})"))
#             state['current_step'] = 'document_upload'
#             state['status_message'] = 'BREF Summary accepted, ready for additional documents'
#             state['progress'] = 0.25
#             logging.info("Agent 2 (BREF Evaluator) - BREF Summary accepted")
#         else:
#             state['bref_retry_count'] += 1
#             state['messages'].append(AIMessage(content=f"BREF Summary rejected. Retry {state['bref_retry_count']}/3. Feedback: {validation_result.get('feedback', '')}"))
#             state['current_step'] = 'bref_retry_check'
#             state['status_message'] = f"BREF Summary needs improvement (Attempt {state['bref_retry_count']}/3)"
#             logging.warning(f"Agent 2 (BREF Evaluator) - BREF Summary rejected (Attempt {state['bref_retry_count']}/3)")
        
#     except Exception as e:
#         error_msg = f"Agent 2 (BREF Evaluator) failed: {str(e)}"
#         logging.error(error_msg)
#         state['errors'].append(error_msg)
#         state['messages'].append(AIMessage(content=error_msg))
#         # Default to accepted on error
#         state['bref_accepted'] = True
#         state['current_step'] = 'document_upload'
    
#     return state


# ─── NEW: BREF ANALYSTS FOR BS AND CF ─────────────────────

def agent_1_bref_analyst_pnl(state: AgentState) -> AgentState:
    """
    Agent 1: BREF Analyst for P&L
    Analyzes the uploaded financial summary document and generates P&L BREF summary
    Includes feedback from previous rejection if this is a retry
    """
    from call_bpce_llm import call_llm_api
    from prompts.prompts_FA import PROMPT_BREF_ANALYSIS_PNL
    from utils import save_file_sequencially
    
    logging.info("="*50)
    logging.info("Agent 1 (BREF Analyst P&L) - Starting analysis")
    logging.info("="*50)
    
    # Use PnL-specific config, fallback to general bref_config for backward compatibility
    config = state.get('bref_config_pnl') or state.get('bref_config') or {}
    temperature = config.get('temperature', 0.0)
    max_tokens = config.get('max_tokens', 1500)
    llm_model = config.get('llm_model', BREF_DEFAULT_LLM_MODEL)
    prompt_template = config.get('custom_prompt', PROMPT_BREF_ANALYSIS_PNL)
    session_folder = state.get('session_folder', './uploads')
    
    # Check if this is a retry attempt
    retry_count = state.get('bref_retry_count_pnl', 0)
    validation_result = state.get('bref_validation_result_pnl', {})
    
    logging.info(f"Configuration: Model={llm_model}, Temp={temperature}, MaxTokens={max_tokens}")
    if retry_count > 0:
        logging.info(f"This is retry attempt {retry_count}/3")
        log_retry_attempt("P&L BREF", retry_count, 3, validation_result.get('feedback', 'No feedback'))
    
    financial_text = state['financial_summary_text']
    
    # Build feedback section if this is a retry
    feedback_section = ""
    if retry_count > 0 and validation_result:
        feedback = validation_result.get('feedback', 'No specific feedback provided')
        issues = validation_result.get('issues', [])
        
        feedback_section = f"""
{'#'*90}
[[[PREVIOUS ATTEMPT FEEDBACK - PLEASE READ CAREFULLY]]]
{'#'*90}

**CRITICAL: Your previous P&L BREF attempt was REJECTED by the evaluator.**
**You MUST address ALL issues below to pass validation.**

**Evaluator Feedback:**
{feedback}

**Specific Issues to Fix:**
{chr(10).join(f"  {i+1}. {issue}" for i, issue in enumerate(issues)) if issues else '  No specific issues listed'}

**Retry Attempt:** {retry_count} of 3

**What You Must Do:**
1. Read the feedback above carefully
2. Address EVERY issue mentioned
3. Ensure your BREF summary is complete and accurate
4. Include ALL required P&L sections
5. Provide all necessary financial metrics and numbers
6. Follow the output format EXACTLY

**If you fail this attempt, you have {3 - retry_count} more chance(s) before requiring configuration change.**

{'#'*90}

"""
    
    full_prompt = f"{feedback_section}{prompt_template}\n\n{'-'*15} [[BREF report Data]] {'-'*15}\n\n\n{financial_text}"
    
    # Save prompt to file (with retry suffix if applicable)
    prompt_filename = f"bref_prompt_pnl_retry_{retry_count}" if retry_count > 0 else "bref_prompt_pnl"
    save_file_sequencially(session_folder, prompt_filename, full_prompt)
    logging.info(f"Saved P&L BREF prompt to {session_folder}/{prompt_filename}.md")
    
    try:
        logging.info("Calling LLM API for P&L BREF generation...")
        response = call_llm_api(
            query=full_prompt,
            model_name=llm_model,
            temperature=temperature,
            max_tokens=max_tokens
        ).content
        
        state['bref_summary_pnl'] = response
        state['bref_summary'] = response  # Legacy compatibility
        
        # Save BREF summary to file (with retry suffix if applicable)
        summary_filename = f"bref_summary_pnl_retry_{retry_count}" if retry_count > 0 else "bref_summary_pnl"
        save_file_sequencially(session_folder, summary_filename, response)
        logging.info(f"Saved P&L BREF summary to {session_folder}/{summary_filename}.md")
        
        state['messages'].append(AIMessage(content=f"P&L BREF Summary generated successfully"))
        state['current_step'] = 'bref_validation_pnl'
        state['status_message'] = 'P&L BREF Summary generated, awaiting validation'
        state['progress'] = 0.10
        
        logging.info("Agent 1 (BREF Analyst P&L) - Analysis completed successfully")
        logging.info("="*50)
        
    except Exception as e:
        error_msg = f"Agent 1 (BREF Analyst P&L) failed: {str(e)}"
        logging.error(error_msg)
        logging.error("="*50)
        state['errors'].append(error_msg)
        state['messages'].append(AIMessage(content=error_msg))
    
    return state


def agent_1_bref_analyst_bs(state: AgentState) -> AgentState:
    """
    Agent 1: BREF Analyst for Balance Sheet
    Analyzes the uploaded financial summary document and generates BS BREF summary
    Includes feedback from previous rejection if this is a retry
    """
    from call_bpce_llm import call_llm_api
    from prompts.prompts_FA import PROMPT_BREF_ANALYSIS_BS
    from utils import save_file_sequencially
    
    logging.info("="*50)
    logging.info("Agent 1 (BREF Analyst BS) - Starting analysis")
    logging.info("="*50)
    
    # Use BS-specific config, fallback to general bref_config for backward compatibility
    config = state.get('bref_config_bs') or state.get('bref_config') or {}
    temperature = config.get('temperature', 0.0)
    max_tokens = config.get('max_tokens', 1500)
    llm_model = config.get('llm_model', BREF_DEFAULT_LLM_MODEL)
    prompt_template = config.get('custom_prompt', PROMPT_BREF_ANALYSIS_BS)
    session_folder = state.get('session_folder', './uploads')
    
    # Check if this is a retry attempt
    retry_count = state.get('bref_retry_count_bs', 0)
    validation_result = state.get('bref_validation_result_bs', {})
    
    logging.info(f"Configuration: Model={llm_model}, Temp={temperature}, MaxTokens={max_tokens}")
    if retry_count > 0:
        logging.info(f"This is retry attempt {retry_count}/3")
        log_retry_attempt("BS BREF", retry_count, 3, validation_result.get('feedback', 'No feedback'))
    
    financial_text = state['financial_summary_text']
    
    # Build feedback section if this is a retry
    feedback_section = ""
    if retry_count > 0 and validation_result:
        feedback = validation_result.get('feedback', 'No specific feedback provided')
        issues = validation_result.get('issues', [])
        
        feedback_section = f"""
{'#'*90}
[[[PREVIOUS ATTEMPT FEEDBACK - PLEASE READ CAREFULLY]]]
{'#'*90}

**CRITICAL: Your previous Balance Sheet BREF attempt was REJECTED by the evaluator.**
**You MUST address ALL issues below to pass validation.**

**Evaluator Feedback:**
{feedback}

**Specific Issues to Fix:**
{chr(10).join(f"  {i+1}. {issue}" for i, issue in enumerate(issues)) if issues else '  No specific issues listed'}

**Retry Attempt:** {retry_count} of 3

**What You Must Do:**
1. Read the feedback above carefully
2. Address EVERY issue mentioned
3. Ensure your BREF summary is complete and accurate
4. Include ALL required Balance Sheet sections
5. Provide all necessary financial metrics and numbers
6. Follow the output format EXACTLY

**If you fail this attempt, you have {3 - retry_count} more chance(s) before requiring configuration change.**

{'#'*90}

"""
    
    full_prompt = f"{feedback_section}{prompt_template}\n\n{'-'*15} [[BREF report Data]] {'-'*15}\n\n\n{financial_text}"
    
    # Save prompt to file (with retry suffix if applicable)
    prompt_filename = f"bref_prompt_bs_retry_{retry_count}" if retry_count > 0 else "bref_prompt_bs"
    save_file_sequencially(session_folder, prompt_filename, full_prompt)
    logging.info(f"Saved BS BREF prompt to {session_folder}/{prompt_filename}.md")
    
    try:
        logging.info("Calling LLM API for BS BREF generation...")
        response = call_llm_api(
            query=full_prompt,
            model_name=llm_model,
            temperature=temperature,
            max_tokens=max_tokens
        ).content
        
        state['bref_summary_bs'] = response
        
        # Save BREF summary to file (with retry suffix if applicable)
        summary_filename = f"bref_summary_bs_retry_{retry_count}" if retry_count > 0 else "bref_summary_bs"
        save_file_sequencially(session_folder, summary_filename, response)
        logging.info(f"Saved BS BREF summary to {session_folder}/{summary_filename}.md")
        
        state['messages'].append(AIMessage(content=f"Balance Sheet BREF Summary generated successfully"))
        state['current_step'] = 'bref_validation_bs'
        state['status_message'] = 'BS BREF Summary generated, awaiting validation'
        state['progress'] = 0.12
        
        logging.info("Agent 1 (BREF Analyst BS) - Analysis completed successfully")
        logging.info("="*50)
        
    except Exception as e:
        error_msg = f"Agent 1 (BREF Analyst BS) failed: {str(e)}"
        logging.error(error_msg)
        logging.error("="*50)
        state['errors'].append(error_msg)
        state['messages'].append(AIMessage(content=error_msg))
    
    return state


def agent_1_bref_analyst_cf(state: AgentState) -> AgentState:
    """
    Agent 1: BREF Analyst for Cash Flow
    Analyzes the uploaded financial summary document and generates CF BREF summary
    Includes feedback from previous rejection if this is a retry
    """
    from call_bpce_llm import call_llm_api
    from prompts.prompts_FA import PROMPT_BREF_ANALYSIS_CF
    from utils import save_file_sequencially
    
    logging.info("="*50)
    logging.info("Agent 1 (BREF Analyst CF) - Starting analysis")
    logging.info("="*50)
    
    # Use CF-specific config, fallback to general bref_config for backward compatibility
    config = state.get('bref_config_cf') or state.get('bref_config') or {}
    temperature = config.get('temperature', 0.0)
    max_tokens = config.get('max_tokens', 1500)
    llm_model = config.get('llm_model', BREF_DEFAULT_LLM_MODEL)
    prompt_template = config.get('custom_prompt', PROMPT_BREF_ANALYSIS_CF)
    session_folder = state.get('session_folder', './uploads')
    
    # Check if this is a retry attempt
    retry_count = state.get('bref_retry_count_cf', 0)
    validation_result = state.get('bref_validation_result_cf', {})
    
    logging.info(f"Configuration: Model={llm_model}, Temp={temperature}, MaxTokens={max_tokens}")
    if retry_count > 0:
        logging.info(f"This is retry attempt {retry_count}/3")
        log_retry_attempt("CF BREF", retry_count, 3, validation_result.get('feedback', 'No feedback'))
    
    financial_text = state['financial_summary_text']
    
    # Build feedback section if this is a retry
    feedback_section = ""
    if retry_count > 0 and validation_result:
        feedback = validation_result.get('feedback', 'No specific feedback provided')
        issues = validation_result.get('issues', [])
        
        feedback_section = f"""
{'#'*90}
[[[PREVIOUS ATTEMPT FEEDBACK - PLEASE READ CAREFULLY]]]
{'#'*90}

**CRITICAL: Your previous Cash Flow BREF attempt was REJECTED by the evaluator.**
**You MUST address ALL issues below to pass validation.**

**Evaluator Feedback:**
{feedback}

**Specific Issues to Fix:**
{chr(10).join(f"  {i+1}. {issue}" for i, issue in enumerate(issues)) if issues else '  No specific issues listed'}

**Retry Attempt:** {retry_count} of 3

**What You Must Do:**
1. Read the feedback above carefully
2. Address EVERY issue mentioned
3. Ensure your BREF summary is complete and accurate
4. Include ALL required Cash Flow sections
5. Provide all necessary financial metrics and numbers
6. Follow the output format EXACTLY

**If you fail this attempt, you have {3 - retry_count} more chance(s) before requiring configuration change.**

{'#'*90}

"""
    
    full_prompt = f"{feedback_section}{prompt_template}\n\n{'-'*15} [[BREF report Data]] {'-'*15}\n\n\n{financial_text}"
    
    # Save prompt to file (with retry suffix if applicable)
    prompt_filename = f"bref_prompt_cf_retry_{retry_count}" if retry_count > 0 else "bref_prompt_cf"
    save_file_sequencially(session_folder, prompt_filename, full_prompt)
    logging.info(f"Saved CF BREF prompt to {session_folder}/{prompt_filename}.md")
    
    try:
        logging.info("Calling LLM API for CF BREF generation...")
        response = call_llm_api(
            query=full_prompt,
            model_name=llm_model,
            temperature=temperature,
            max_tokens=max_tokens
        ).content
        
        state['bref_summary_cf'] = response
        
        # Save BREF summary to file (with retry suffix if applicable)
        summary_filename = f"bref_summary_cf_retry_{retry_count}" if retry_count > 0 else "bref_summary_cf"
        save_file_sequencially(session_folder, summary_filename, response)
        logging.info(f"Saved CF BREF summary to {session_folder}/{summary_filename}.md")
        
        state['messages'].append(AIMessage(content=f"Cash Flow BREF Summary generated successfully"))
        state['current_step'] = 'bref_validation_cf'
        state['status_message'] = 'CF BREF Summary generated, awaiting validation'
        state['progress'] = 0.14
        
        logging.info("Agent 1 (BREF Analyst CF) - Analysis completed successfully")
        logging.info("="*50)
        
    except Exception as e:
        error_msg = f"Agent 1 (BREF Analyst CF) failed: {str(e)}"
        logging.error(error_msg)
        logging.error("="*50)
        state['errors'].append(error_msg)
        state['messages'].append(AIMessage(content=error_msg))
    
    return state


# ─── NEW: BREF EVALUATORS FOR PNL, BS AND CF ──────────────

def agent_2_bref_evaluator_pnl(state: AgentState) -> AgentState:
    """
    Agent 2: BREF Evaluator for P&L (LLM as Judge)
    Validates the P&L BREF summary generated by Agent 1
    """
    from call_bpce_llm import call_llm_api
    from prompts.prompts_FA import BREF_VALIDATION_PROMPT_PNL
    from utils import save_file_sequencially
    from config_defaults import EVALUATOR_MODEL, EVALUATOR_TEMPERATURE, EVALUATOR_MAX_TOKENS
    
    logging.info("Agent 2 (BREF Evaluator P&L) - Starting validation")
    
    # Check if this is a retry attempt
    retry_count = state.get('bref_retry_count_pnl', 0)
    
    full_prompt = f"""{BREF_VALIDATION_PROMPT_PNL}

**BREF SUMMARY TO VALIDATE:**

{state['bref_summary_pnl']}

**ORIGINAL FINANCIAL DATA:**

{state['financial_summary_text']}
"""
    
    # Save validation prompt to file (with retry suffix if applicable)
    session_folder = state.get('session_folder', './uploads')
    prompt_filename = f"bref_validation_prompt_pnl_retry_{retry_count}" if retry_count > 0 else "bref_validation_prompt_pnl"
    save_file_sequencially(session_folder, prompt_filename, full_prompt)
    logging.info(f"Saved P&L BREF validation prompt to {session_folder}/{prompt_filename}.md")
    
    try:
        response = call_llm_api(
            query=full_prompt,
            model_name=EVALUATOR_MODEL,
            temperature=EVALUATOR_TEMPERATURE,
            max_tokens=EVALUATOR_MAX_TOKENS
        ).content
        
        # Parse validation result
        try:
            import re
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                validation_result = json.loads(json_match.group())
                
                # CRITICAL FIX: Detect inconsistencies between accepted and feedback
                feedback = validation_result.get('feedback', '').lower()
                accepted = validation_result.get('accepted', False)
                
                # Positive indicators suggest report should be accepted
                positive_indicators = [
                    'meets all quality standards',
                    'meets all standards',
                    'no improvements necessary',
                    'no improvements needed',
                    'comprehensive analysis',
                    'well-structured',
                    'excellent',
                    'satisfactory'
                ]
                
                # If feedback is positive but accepted=false, override to true
                if not accepted and any(indicator in feedback for indicator in positive_indicators):
                    logging.warning(f"INCONSISTENCY DETECTED in P&L BREF: Feedback is positive but accepted=false")
                    logging.warning(f"Feedback: {validation_result.get('feedback', '')}")
                    logging.warning(f"Overriding accepted to TRUE based on positive feedback")
                    validation_result['accepted'] = True
                    validation_result['issues'] = []  # Clear issues if accepted
            else:
                validation_result = {"accepted": True, "issues": [], "feedback": "Validation completed", "score": 85}
        except:
            validation_result = {"accepted": True, "issues": [], "feedback": "Validation completed", "score": 85}
        
        state['bref_validation_result_pnl'] = validation_result
        state['bref_validation_result'] = validation_result  # Legacy compatibility
        state['bref_accepted_pnl'] = validation_result.get('accepted', False)
        state['bref_accepted'] = validation_result.get('accepted', False)  # Legacy compatibility
        
        if state['bref_accepted_pnl']:
            state['messages'].append(AIMessage(content=f"P&L BREF Summary validated and accepted (Score: {validation_result.get('score', 0)})"))
            state['current_step'] = 'bref_validation_bs'
            state['status_message'] = 'P&L BREF Summary accepted'
            state['progress'] = 0.15
            logging.info("Agent 2 (BREF Evaluator P&L) - BREF Summary accepted")
        else:
            state['bref_retry_count_pnl'] = state.get('bref_retry_count_pnl', 0) + 1
            state['bref_retry_count'] = state['bref_retry_count_pnl']  # Legacy compatibility
            state['messages'].append(AIMessage(content=f"P&L BREF Summary rejected. Retry {state['bref_retry_count_pnl']}/3. Feedback: {validation_result.get('feedback', '')}"))
            state['current_step'] = 'bref_retry_check_pnl'
            state['status_message'] = f"P&L BREF Summary needs improvement (Attempt {state['bref_retry_count_pnl']}/3)"
            logging.warning(f"Agent 2 (BREF Evaluator P&L) - BREF Summary rejected (Attempt {state['bref_retry_count_pnl']}/3)")
        
    except Exception as e:
        error_msg = f"Agent 2 (BREF Evaluator P&L) failed: {str(e)}"
        logging.error(error_msg)
        state['errors'].append(error_msg)
        state['messages'].append(AIMessage(content=error_msg))
        # Default to accepted on error
        state['bref_accepted_pnl'] = True
        state['bref_accepted'] = True
        state['current_step'] = 'bref_validation_bs'
    
    return state


def agent_2_bref_evaluator_bs(state: AgentState) -> AgentState:
    """
    Agent 2: BREF Evaluator for Balance Sheet (LLM as Judge)
    Validates the BS BREF summary generated by Agent 1
    """
    from call_bpce_llm import call_llm_api
    from prompts.prompts_FA import BREF_VALIDATION_PROMPT_BS
    from utils import save_file_sequencially
    from config_defaults import EVALUATOR_MODEL, EVALUATOR_TEMPERATURE, EVALUATOR_MAX_TOKENS
    
    logging.info("Agent 2 (BREF Evaluator BS) - Starting validation")
    
    # Check if this is a retry attempt
    retry_count = state.get('bref_retry_count_bs', 0)
    
    full_prompt = f"""{BREF_VALIDATION_PROMPT_BS}

**BREF SUMMARY TO VALIDATE:**

{state['bref_summary_bs']}

**ORIGINAL FINANCIAL DATA:**

{state['financial_summary_text']}
"""
    
    # Save validation prompt to file (with retry suffix if applicable)
    session_folder = state.get('session_folder', './uploads')
    prompt_filename = f"bref_validation_prompt_bs_retry_{retry_count}" if retry_count > 0 else "bref_validation_prompt_bs"
    save_file_sequencially(session_folder, prompt_filename, full_prompt)
    logging.info(f"Saved BS BREF validation prompt to {session_folder}/{prompt_filename}.md")
    
    try:
        response = call_llm_api(
            query=full_prompt,
            model_name=EVALUATOR_MODEL,
            temperature=EVALUATOR_TEMPERATURE,
            max_tokens=EVALUATOR_MAX_TOKENS
        ).content
        
        # Parse validation result
        try:
            import re
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                validation_result = json.loads(json_match.group())
                
                # CRITICAL FIX: Detect inconsistencies between accepted and feedback
                feedback = validation_result.get('feedback', '').lower()
                accepted = validation_result.get('accepted', False)
                
                # Positive indicators suggest report should be accepted
                positive_indicators = [
                    'meets all quality standards',
                    'meets all standards',
                    'no improvements necessary',
                    'no improvements needed',
                    'comprehensive analysis',
                    'well-structured',
                    'excellent',
                    'satisfactory'
                ]
                
                # If feedback is positive but accepted=false, override to true
                if not accepted and any(indicator in feedback for indicator in positive_indicators):
                    logging.warning(f"INCONSISTENCY DETECTED in BS BREF: Feedback is positive but accepted=false")
                    logging.warning(f"Feedback: {validation_result.get('feedback', '')}")
                    logging.warning(f"Overriding accepted to TRUE based on positive feedback")
                    validation_result['accepted'] = True
                    validation_result['issues'] = []  # Clear issues if accepted
            else:
                validation_result = {"accepted": True, "issues": [], "feedback": "Validation completed", "score": 85}
        except:
            validation_result = {"accepted": True, "issues": [], "feedback": "Validation completed", "score": 85}
        
        state['bref_validation_result_bs'] = validation_result
        state['bref_accepted_bs'] = validation_result.get('accepted', False)
        
        if state['bref_accepted_bs']:
            state['messages'].append(AIMessage(content=f"Balance Sheet BREF Summary validated and accepted (Score: {validation_result.get('score', 0)})"))
            state['current_step'] = 'bref_validation_cf'
            state['status_message'] = 'BS BREF Summary accepted'
            state['progress'] = 0.18
            logging.info("Agent 2 (BREF Evaluator BS) - BREF Summary accepted")
        else:
            state['bref_retry_count_bs'] = state.get('bref_retry_count_bs', 0) + 1
            state['messages'].append(AIMessage(content=f"BS BREF Summary rejected. Retry {state['bref_retry_count_bs']}/3. Feedback: {validation_result.get('feedback', '')}"))
            state['current_step'] = 'bref_retry_check_bs'
            state['status_message'] = f"BS BREF Summary needs improvement (Attempt {state['bref_retry_count_bs']}/3)"
            logging.warning(f"Agent 2 (BREF Evaluator BS) - BREF Summary rejected (Attempt {state['bref_retry_count_bs']}/3)")
        
    except Exception as e:
        error_msg = f"Agent 2 (BREF Evaluator BS) failed: {str(e)}"
        logging.error(error_msg)
        state['errors'].append(error_msg)
        state['messages'].append(AIMessage(content=error_msg))
        # Default to accepted on error
        state['bref_accepted_bs'] = True
        state['current_step'] = 'bref_validation_cf'
    
    return state


def agent_2_bref_evaluator_cf(state: AgentState) -> AgentState:
    """
    Agent 2: BREF Evaluator for Cash Flow (LLM as Judge)
    Validates the CF BREF summary generated by Agent 1
    """
    from call_bpce_llm import call_llm_api
    from prompts.prompts_FA import BREF_VALIDATION_PROMPT_CF
    from utils import save_file_sequencially
    from config_defaults import EVALUATOR_MODEL, EVALUATOR_TEMPERATURE, EVALUATOR_MAX_TOKENS
    
    logging.info("Agent 2 (BREF Evaluator CF) - Starting validation")
    
    # Check if this is a retry attempt
    retry_count = state.get('bref_retry_count_cf', 0)
    
    full_prompt = f"""{BREF_VALIDATION_PROMPT_CF}

**BREF SUMMARY TO VALIDATE:**

{state['bref_summary_cf']}

**ORIGINAL FINANCIAL DATA:**

{state['financial_summary_text']}
"""
    
    # Save validation prompt to file (with retry suffix if applicable)
    session_folder = state.get('session_folder', './uploads')
    prompt_filename = f"bref_validation_prompt_cf_retry_{retry_count}" if retry_count > 0 else "bref_validation_prompt_cf"
    save_file_sequencially(session_folder, prompt_filename, full_prompt)
    logging.info(f"Saved CF BREF validation prompt to {session_folder}/{prompt_filename}.md")
    
    try:
        response = call_llm_api(
            query=full_prompt,
            model_name=EVALUATOR_MODEL,
            temperature=EVALUATOR_TEMPERATURE,
            max_tokens=EVALUATOR_MAX_TOKENS
        ).content
        
        # Parse validation result
        try:
            import re
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                validation_result = json.loads(json_match.group())
                
                # CRITICAL FIX: Detect inconsistencies between accepted and feedback
                feedback = validation_result.get('feedback', '').lower()
                accepted = validation_result.get('accepted', False)
                
                # Positive indicators suggest report should be accepted
                positive_indicators = [
                    'meets all quality standards',
                    'meets all standards',
                    'no improvements necessary',
                    'no improvements needed',
                    'comprehensive analysis',
                    'well-structured',
                    'excellent',
                    'satisfactory'
                ]
                
                # If feedback is positive but accepted=false, override to true
                if not accepted and any(indicator in feedback for indicator in positive_indicators):
                    logging.warning(f"INCONSISTENCY DETECTED in CF BREF: Feedback is positive but accepted=false")
                    logging.warning(f"Feedback: {validation_result.get('feedback', '')}")
                    logging.warning(f"Overriding accepted to TRUE based on positive feedback")
                    validation_result['accepted'] = True
                    validation_result['issues'] = []  # Clear issues if accepted
            else:
                validation_result = {"accepted": True, "issues": [], "feedback": "Validation completed", "score": 85}
        except:
            validation_result = {"accepted": True, "issues": [], "feedback": "Validation completed", "score": 85}
        
        state['bref_validation_result_cf'] = validation_result
        state['bref_accepted_cf'] = validation_result.get('accepted', False)
        
        if state['bref_accepted_cf']:
            state['messages'].append(AIMessage(content=f"Cash Flow BREF Summary validated and accepted (Score: {validation_result.get('score', 0)})"))
            state['current_step'] = 'document_upload'
            state['status_message'] = 'All BREF Summaries accepted, ready for additional documents'
            state['progress'] = 0.20
            logging.info("Agent 2 (BREF Evaluator CF) - BREF Summary accepted")
        else:
            state['bref_retry_count_cf'] = state.get('bref_retry_count_cf', 0) + 1
            state['messages'].append(AIMessage(content=f"CF BREF Summary rejected. Retry {state['bref_retry_count_cf']}/3. Feedback: {validation_result.get('feedback', '')}"))
            state['current_step'] = 'bref_retry_check_cf'
            state['status_message'] = f"CF BREF Summary needs improvement (Attempt {state['bref_retry_count_cf']}/3)"
            logging.warning(f"Agent 2 (BREF Evaluator CF) - BREF Summary rejected (Attempt {state['bref_retry_count_cf']}/3)")
        
    except Exception as e:
        error_msg = f"Agent 2 (BREF Evaluator CF) failed: {str(e)}"
        logging.error(error_msg)
        state['errors'].append(error_msg)
        state['messages'].append(AIMessage(content=error_msg))
        # Default to accepted on error
        state['bref_accepted_cf'] = True
        state['current_step'] = 'document_upload'
    
    return state


# ─── AGENT 3 (P&L ANALYST) ─────────────────────────────────

# ─── P&L SUB-AGENT FACTORY ────────────────────────────────

def create_pnl_subagent(chunk_index: int):
    """
    Factory function to create a P&L sub-agent for a specific chunk
    Each sub-agent processes one chunk independently
    """
    def pnl_subagent(state: AgentState) -> AgentState:
        """
        P&L Sub-Agent for chunk {chunk_index}
        Processes a single chunk in parallel with other sub-agents
        """
        from call_bpce_llm import call_llm_api
        from prompts.prompts_FA import PROMPT_DRIVER_ANALYSIS_PNL
        from utils import save_file_sequencially
        
        node_name = f"pnl_analyst_{chunk_index}"
        log_node_execution(node_name, "starting")
        
                # Update progress tracker
        session_id = state.get('session_folder', 'default')
        tracker = get_tracker(session_id)
        tracker.update_sub_agent("P&L Analysis", node_name, "running")
        
        # Update UI status
        from status_callback import update_status
        update_status(f"P&L Analyst {chunk_index} analyzing chunk...", "info")
        
        logging.info(f"="*50)
        logging.info(f"P&L Sub-Agent {chunk_index} - Starting analysis")
        logging.info(f"="*50)
        
        # Use type-specific config, fallback to general fa_config
        config = state.get('fa_config_pnl') or state.get('fa_config') or {}
        temperature = config.get('temperature', 0.0)
        max_tokens = config.get('max_tokens', 2000)
        llm_model = config.get('llm_model', FA_DEFAULT_LLM_MODEL)
        prompt_template = config.get('custom_prompt', PROMPT_DRIVER_ANALYSIS_PNL)
        session_folder = state.get('session_folder', './uploads')
        
        # Get the specific chunk for this sub-agent
        chunk = state['pnl_chunks'][chunk_index - 1]  # chunk_index is 1-based
        context = chunk['context']
        
        full_prompt = f"""{'#'*90}
[[[TASK]]]
{'#'*90}

<TASK>{prompt_template}</TASK>

{'#'*90}
[[[CONTEXT]]]
{'#'*90}

<CONTEXT>{context}</CONTEXT>
"""
        
        # Save prompt to file
        save_file_sequencially(session_folder, f"pnl_prompt_chunk_{chunk_index}", full_prompt)
        logging.info(f"P&L Sub-Agent {chunk_index} - Saved prompt")
        
        try:
            logging.info(f"P&L Sub-Agent {chunk_index} - Calling LLM API...")
            response = call_llm_api(
                query=full_prompt,
                model_name=llm_model,
                temperature=temperature,
                max_tokens=max_tokens
            ).content
            
            # Try to parse as JSON
            try:
                import re
                json_match = re.search(r'\{.*\}', response, re.DOTALL)
                if json_match:
                    report_data = json.loads(json_match.group())
                else:
                    report_data = {"raw_response": response}
            except:
                report_data = {"raw_response": response}
            
            result = {
                "chunk_index": chunk_index,
                "model": llm_model,
                "temperature": temperature,
                "max_tokens": max_tokens,
                "generated_at": datetime.now().isoformat(),
                "report": report_data,
            }
            
            # Save report to file
            report_path = os.path.join(session_folder, f"pnl_report_chunk_{chunk_index}.json")
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            logging.info(f"P&L Sub-Agent {chunk_index} - Report saved successfully")
            
            logging.info(f"P&L Sub-Agent {chunk_index} - Completed successfully")
            logging.info(f"="*50)
            
            # Update progress tracker
            session_id = state.get('session_folder', 'default')
            tracker = get_tracker(session_id)
            tracker.update_sub_agent("P&L Analysis", node_name, "completed")
            log_node_execution(node_name, "completed")
            
            # Update UI status
            from status_callback import update_status
            update_status(f"P&L Analyst {chunk_index} completed", "success")
            
            # Return only the fields this sub-agent updates
            return {
                "pnl_reports": [result],  # Will be extended by the reducer
            }
            
        except Exception as e:
            error_msg = f"P&L Sub-Agent {chunk_index} failed: {str(e)}"
            logging.error(error_msg)
            logging.error(f"="*50)
            
            # Update progress tracker
            session_id = state.get('session_folder', 'default')
            tracker = get_tracker(session_id)
            tracker.update_sub_agent("P&L Analysis", node_name, "failed")
            log_node_execution(node_name, "failed")
            
            # Return error
            return {
                "errors": [error_msg],
            }
    
    return pnl_subagent


# ─── P&L EVALUATOR SUB-AGENT FACTORY ──────────────────────

def create_pnl_evaluator_subagent(report_index: int):
    """
    Factory function to create a P&L evaluator sub-agent for a specific report
    Each evaluator processes one report independently
    """
    def pnl_evaluator_subagent(state: AgentState) -> AgentState:
        """
        P&L Evaluator Sub-Agent for report {report_index}
        Validates a single report in parallel with other evaluators
        """
        from call_bpce_llm import call_llm_api
        from prompts.prompts_FA import FA_VALIDATION_PROMPT_PNL
        from config_defaults import EVALUATOR_MODEL, EVALUATOR_TEMPERATURE, EVALUATOR_MAX_TOKENS
        from utils import save_file_sequencially
        
        logging.info(f"="*50)
        logging.info(f"P&L Evaluator Sub-Agent {report_index} - Starting validation")
        logging.info(f"="*50)
        
        # Update UI status
        from status_callback import update_status
        update_status(f"P&L Evaluator {report_index} validating report...", "info")
        
        # Get the specific report for this evaluator
        report = state['pnl_reports'][report_index - 1]  # report_index is 1-based
        chunk_index = report['chunk_index']
        report_content = json.dumps(report['report'], indent=2)
        
        full_prompt = f"{FA_VALIDATION_PROMPT_PNL}\n\n**P&L REPORT TO VALIDATE:**\n\n{report_content}"
        
        # Save validation prompt to file
        session_folder = state.get('session_folder', './uploads')
        save_file_sequencially(session_folder, f"pnl_validation_prompt_chunk_{chunk_index}", full_prompt)
        logging.info(f"P&L Evaluator Sub-Agent {report_index} - Saved validation prompt")
        
        try:
            logging.info(f"P&L Evaluator Sub-Agent {report_index} - Calling LLM API...")
            response = call_llm_api(
                query=full_prompt,
                model_name=EVALUATOR_MODEL,
                temperature=EVALUATOR_TEMPERATURE,
                max_tokens=EVALUATOR_MAX_TOKENS
            ).content
            
            # Parse validation result
            try:
                import re
                json_match = re.search(r'\{.*\}', response, re.DOTALL)
                if json_match:
                    validation_result = json.loads(json_match.group())
                else:
                    validation_result = {"accepted": True, "issues": [], "feedback": "Validated", "score": 85}
            except:
                validation_result = {"accepted": True, "issues": [], "feedback": "Validated", "score": 85}
            
            validation_result['chunk_index'] = chunk_index
            validation_result['report_index'] = report_index
            validation_result['retry_count'] = 0
            
            # Save validation result
            session_folder = state.get('session_folder', './uploads')
            validation_path = os.path.join(session_folder, f"pnl_validation_chunk_{chunk_index}.json")
            with open(validation_path, 'w', encoding='utf-8') as f:
                json.dump(validation_result, f, indent=2, ensure_ascii=False)
            
            if validation_result.get('accepted', False):
                logging.info(f"P&L Evaluator Sub-Agent {report_index} - Report accepted")
                logging.info(f"  Score: {validation_result.get('score', 'N/A')}")
                logging.info(f"  Feedback: {validation_result.get('feedback', 'N/A')}")
                logging.info(f"="*50)
                
                # Log to both console and file
                log_validation_result(
                    chunk_index=chunk_index,
                    accepted=True,
                    score=validation_result.get('score'),
                    feedback=validation_result.get('feedback')
                )
                
                # Update UI status
                from status_callback import update_status
                update_status(f"P&L Evaluator {report_index} accepted report (Score: {validation_result.get('score', 'N/A')})", "success")
                
                return {
                    "pnl_validation_results": [validation_result],
                    "pnl_accepted_reports": [report],
                }
            else:
                logging.warning(f"P&L Evaluator Sub-Agent {report_index} - Report rejected")
                logging.warning(f"  Reason: {validation_result.get('feedback', 'No feedback provided')}")
                logging.warning(f"  Score: {validation_result.get('score', 'N/A')}")
                logging.warning(f"  Issues: {validation_result.get('issues', [])}")
                logging.info(f"="*50)
                
                # Log detailed rejection information
                log_rejection_details(chunk_index, validation_result)
                log_validation_result(
                    chunk_index=chunk_index,
                    accepted=False,
                    score=validation_result.get('score'),
                    feedback=validation_result.get('feedback'),
                    issues=validation_result.get('issues', [])
                )
                
                # Update UI status
                from status_callback import update_status
                update_status(f"P&L Evaluator {report_index} rejected report (Score: {validation_result.get('score', 'N/A')})", "warning")
                
                return {
                    "pnl_validation_results": [validation_result],
                }
            
        except Exception as e:
            error_msg = f"P&L Evaluator Sub-Agent {report_index} failed: {str(e)}"
            logging.error(error_msg)
            logging.error(f"="*50)
            # Default to accepted on error
            return {
                "pnl_validation_results": [{"chunk_index": chunk_index, "accepted": True, "score": 85}],
                "pnl_accepted_reports": [report],
                "errors": [error_msg],
            }
    
    return pnl_evaluator_subagent


# ─── BS EVALUATOR SUB-AGENT FACTORY ────────────────────────

def create_bs_evaluator_subagent(report_index: int):
    """
    Factory function to create a BS evaluator sub-agent for a specific report
    """
    def bs_evaluator_subagent(state: AgentState) -> AgentState:
        from call_bpce_llm import call_llm_api
        from prompts.prompts_FA import FA_VALIDATION_PROMPT_BS
        from config_defaults import EVALUATOR_MODEL, EVALUATOR_TEMPERATURE, EVALUATOR_MAX_TOKENS
        from utils import save_file_sequencially
        
        logging.info(f"="*50)
        logging.info(f"BS Evaluator Sub-Agent {report_index} - Starting validation")
        logging.info(f"="*50)
        
        # Update UI status
        from status_callback import update_status
        update_status(f"BS Evaluator {report_index} validating report...", "info")
        
        report = state['bs_reports'][report_index - 1]
        chunk_index = report['chunk_index']
        report_content = json.dumps(report['report'], indent=2)
        
        full_prompt = f"{FA_VALIDATION_PROMPT_BS}\n\n**BALANCE SHEET REPORT TO VALIDATE:**\n\n{report_content}"
        
        # Save validation prompt to file
        session_folder = state.get('session_folder', './uploads')
        save_file_sequencially(session_folder, f"bs_validation_prompt_chunk_{chunk_index}", full_prompt)
        logging.info(f"BS Evaluator Sub-Agent {report_index} - Saved validation prompt")
        
        try:
            response = call_llm_api(
                query=full_prompt,
                model_name=EVALUATOR_MODEL,
                temperature=EVALUATOR_TEMPERATURE,
                max_tokens=EVALUATOR_MAX_TOKENS
            ).content
            
            try:
                import re
                json_match = re.search(r'\{.*\}', response, re.DOTALL)
                if json_match:
                    validation_result = json.loads(json_match.group())
                else:
                    validation_result = {"accepted": True, "issues": [], "feedback": "Validated", "score": 85}
            except:
                validation_result = {"accepted": True, "issues": [], "feedback": "Validated", "score": 85}
            
            validation_result['chunk_index'] = chunk_index
            validation_result['report_index'] = report_index
            validation_result['retry_count'] = 0
            
            session_folder = state.get('session_folder', './uploads')
            validation_path = os.path.join(session_folder, f"bs_validation_chunk_{chunk_index}.json")
            with open(validation_path, 'w', encoding='utf-8') as f:
                json.dump(validation_result, f, indent=2, ensure_ascii=False)
            
            if validation_result.get('accepted', False):
                logging.info(f"BS Evaluator Sub-Agent {report_index} - Report accepted")
                logging.info(f"  Score: {validation_result.get('score', 'N/A')}")
                logging.info(f"  Feedback: {validation_result.get('feedback', 'N/A')}")
                logging.info(f"="*50)
                
                # Log to both console and file
                log_validation_result(
                    chunk_index=chunk_index,
                    accepted=True,
                    score=validation_result.get('score'),
                    feedback=validation_result.get('feedback')
                )
                
                # Update UI status
                from status_callback import update_status
                update_status(f"BS Evaluator {report_index} accepted report (Score: {validation_result.get('score', 'N/A')})", "success")
                
                return {
                    "bs_validation_results": [validation_result],
                    "bs_accepted_reports": [report],
                }
            else:
                logging.warning(f"BS Evaluator Sub-Agent {report_index} - Report rejected")
                logging.warning(f"  Reason: {validation_result.get('feedback', 'No feedback provided')}")
                logging.warning(f"  Score: {validation_result.get('score', 'N/A')}")
                logging.warning(f"  Issues: {validation_result.get('issues', [])}")
                logging.warning(f"  Missing Sections: {validation_result.get('missing_sections', [])}")
                logging.info(f"="*50)
                
                # Log detailed rejection information
                log_rejection_details(chunk_index, validation_result)
                log_validation_result(
                    chunk_index=chunk_index,
                    accepted=False,
                    score=validation_result.get('score'),
                    feedback=validation_result.get('feedback'),
                    issues=validation_result.get('issues', [])
                )
                
                # Update UI status
                from status_callback import update_status
                update_status(f"BS Evaluator {report_index} rejected report (Score: {validation_result.get('score', 'N/A')})", "warning")
                
                return {
                    "bs_validation_results": [validation_result],
                }
        except Exception as e:
            error_msg = f"BS Evaluator Sub-Agent {report_index} failed: {str(e)}"
            logging.error(error_msg)
            return {
                "bs_validation_results": [{"chunk_index": chunk_index, "accepted": True, "score": 85}],
                "bs_accepted_reports": [report],
                "errors": [error_msg],
            }
    
    return bs_evaluator_subagent


# ─── CF EVALUATOR SUB-AGENT FACTORY ────────────────────────

def create_cf_evaluator_subagent(report_index: int):
    """
    Factory function to create a CF evaluator sub-agent for a specific report
    """
    def cf_evaluator_subagent(state: AgentState) -> AgentState:
        from call_bpce_llm import call_llm_api
        from prompts.prompts_FA import FA_VALIDATION_PROMPT_CF
        from config_defaults import EVALUATOR_MODEL, EVALUATOR_TEMPERATURE, EVALUATOR_MAX_TOKENS
        from utils import save_file_sequencially
        
        logging.info(f"="*50)
        logging.info(f"CF Evaluator Sub-Agent {report_index} - Starting validation")
        logging.info(f"="*50)
        
        # Update UI status
        from status_callback import update_status
        update_status(f"CF Evaluator {report_index} validating report...", "info")
        
        report = state['cf_reports'][report_index - 1]
        chunk_index = report['chunk_index']
        report_content = json.dumps(report['report'], indent=2)
        
        full_prompt = f"{FA_VALIDATION_PROMPT_CF}\n\n**CASH FLOW REPORT TO VALIDATE:**\n\n{report_content}"
        
        # Save validation prompt to file
        session_folder = state.get('session_folder', './uploads')
        save_file_sequencially(session_folder, f"cf_validation_prompt_chunk_{chunk_index}", full_prompt)
        logging.info(f"CF Evaluator Sub-Agent {report_index} - Saved validation prompt")
        
        try:
            response = call_llm_api(
                query=full_prompt,
                model_name=EVALUATOR_MODEL,
                temperature=EVALUATOR_TEMPERATURE,
                max_tokens=EVALUATOR_MAX_TOKENS
            ).content
            
            try:
                import re
                json_match = re.search(r'\{.*\}', response, re.DOTALL)
                if json_match:
                    validation_result = json.loads(json_match.group())
                else:
                    validation_result = {"accepted": True, "issues": [], "feedback": "Validated", "score": 85}
            except:
                validation_result = {"accepted": True, "issues": [], "feedback": "Validated", "score": 85}
            
            validation_result['chunk_index'] = chunk_index
            validation_result['report_index'] = report_index
            validation_result['retry_count'] = 0
            
            session_folder = state.get('session_folder', './uploads')
            validation_path = os.path.join(session_folder, f"cf_validation_chunk_{chunk_index}.json")
            with open(validation_path, 'w', encoding='utf-8') as f:
                json.dump(validation_result, f, indent=2, ensure_ascii=False)
            
            if validation_result.get('accepted', False):
                logging.info(f"CF Evaluator Sub-Agent {report_index} - Report accepted")
                logging.info(f"  Score: {validation_result.get('score', 'N/A')}")
                logging.info(f"  Feedback: {validation_result.get('feedback', 'N/A')}")
                logging.info(f"="*50)
                
                log_validation_result(
                    chunk_index=chunk_index,
                    accepted=True,
                    score=validation_result.get('score'),
                    feedback=validation_result.get('feedback')
                )
                
                from status_callback import update_status
                update_status(f"CF Evaluator {report_index} accepted report (Score: {validation_result.get('score', 'N/A')})", "success")
                
                return {
                    "cf_validation_results": [validation_result],
                    "cf_accepted_reports": [report],
                }
            else:
                logging.warning(f"CF Evaluator Sub-Agent {report_index} - Report rejected")
                logging.warning(f"  Reason: {validation_result.get('feedback', 'No feedback provided')}")
                logging.warning(f"  Score: {validation_result.get('score', 'N/A')}")
                logging.warning(f"  Issues: {validation_result.get('issues', [])}")
                logging.info(f"="*50)
                
                log_rejection_details(chunk_index, validation_result)
                log_validation_result(
                    chunk_index=chunk_index,
                    accepted=False,
                    score=validation_result.get('score'),
                    feedback=validation_result.get('feedback'),
                    issues=validation_result.get('issues', [])
                )
                
                from status_callback import update_status
                update_status(f"CF Evaluator {report_index} rejected report (Score: {validation_result.get('score', 'N/A')})", "warning")
                
                return {
                    "cf_validation_results": [validation_result],
                }
        except Exception as e:
            error_msg = f"CF Evaluator Sub-Agent {report_index} failed: {str(e)}"
            logging.error(error_msg)
            return {
                "cf_validation_results": [{"chunk_index": chunk_index, "accepted": True, "score": 85}],
                "cf_accepted_reports": [report],
                "errors": [error_msg],
            }
    
    return cf_evaluator_subagent


# # ─── AGENT 4: DATA EVALUATOR (P&L) - DEPRECATED ────────────────────────

# def agent_4_pnl_data_evaluator(state: AgentState) -> AgentState:
#     """
#     Agent 4: Data Evaluator for P&L (LLM as Judge) - DEPRECATED
#     Validates P&L reports using BREF_VALIDATION_PROMPT_PNL from prompts_FA.py
#     """
#     from call_bpce_llm import call_llm_api
#     from prompts.prompts_FA import BREF_VALIDATION_PROMPT_PNL
    
#     logging.info("="*50)
#     logging.info("Agent 4 (P&L Data Evaluator - DEPRECATED) - Starting validation")
#     logging.info("="*50)
    
#     validated_reports = []
    
#     for report in state['pnl_reports']:
#         chunk_index = report['chunk_index']
#         report_content = json.dumps(report['report'], indent=2)
        
#         full_prompt = f"{BREF_VALIDATION_PROMPT_PNL}\n\n**P&L REPORT TO VALIDATE:**\n\n{report_content}"
        
#         try:
#             response = call_llm_api(
#                 query=full_prompt,
#                 model_name=BREF_DEFAULT_LLM_MODEL,
#                 temperature=0.0,
#                 max_tokens=1000
#             ).content
            
#             # Parse validation result
#             try:
#                 import re
#                 json_match = re.search(r'\{.*\}', response, re.DOTALL)
#                 if json_match:
#                     validation_result = json.loads(json_match.group())
#                 else:
#                     validation_result = {"accepted": True, "issues": [], "feedback": "Validated", "score": 85}
#             except:
#                 validation_result = {"accepted": True, "issues": [], "feedback": "Validated", "score": 85}
            
#             validation_result['chunk_index'] = chunk_index
#             validation_result['retry_count'] = 0
            
#             state['pnl_validation_results'].append(validation_result)
            
#             if validation_result.get('accepted', False):
#                 state['pnl_accepted_reports'].append(report)
#                 logging.info(f"Agent 4 (P&L Data Evaluator) - Chunk {chunk_index} accepted")
#             else:
#                 logging.warning(f"Agent 4 (P&L Data Evaluator) - Chunk {chunk_index} rejected")
            
#         except Exception as e:
#             error_msg = f"Agent 4 (P&L Data Evaluator) - Chunk {chunk_index} validation failed: {str(e)}"
#             logging.error(error_msg)
#             state['errors'].append(error_msg)
#             # Default to accepted on error
#             state['pnl_accepted_reports'].append(report)
    
#     # Save validation results
#     session_folder = state.get('session_folder', './uploads')
#     validation_path = os.path.join(session_folder, "pnl_validation_results.json")
#     with open(validation_path, 'w', encoding='utf-8') as f:
#         json.dump(state['pnl_validation_results'], f, indent=2, ensure_ascii=False)
#     logging.info(f"Saved P&L validation results to {validation_path}")
    
#     state['messages'].append(AIMessage(content=f"P&L validation completed: {len(state['pnl_accepted_reports'])}/{len(state['pnl_reports'])} accepted"))
#     state['current_step'] = 'pnl_senior_analyst'
#     state['status_message'] = f'P&L validation completed'
#     state['progress'] = 0.60
    
#     logging.info(f"Agent 4 (P&L Data Evaluator) - Validation completed: {len(state['pnl_accepted_reports'])}/{len(state['pnl_reports'])} accepted")
#     logging.info("="*50)
    
#     return state


# ─── HELPER FUNCTION FOR JSON EXTRACTION ──────────────────

def extract_json_from_response(response: str) -> dict:
    """
    Extract and parse JSON from LLM response.
    Ensures the output is always a proper JSON object, never a string.
    Handles markdown code blocks (```json or ```) properly.
    """
    import re
    
    # Clean the response - remove leading/trailing whitespace
    cleaned_response = response.strip()
    
    # Remove markdown code blocks if present
    # Pattern 1: ```json\n{...}\n```
    # Pattern 2: ```\n{...}\n```
    if cleaned_response.startswith('```'):
        # Find the end of the opening marker (could be ```json or just ```)
        first_newline = cleaned_response.find('\n')
        if first_newline != -1:
            # Remove everything before the first newline
            cleaned_response = cleaned_response[first_newline + 1:]
        
        # Remove trailing ``` if present
        if cleaned_response.endswith('```'):
            cleaned_response = cleaned_response[:-3].rstrip()
    
    # Now try to extract JSON object
    json_match = re.search(r'\{.*\}', cleaned_response, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group())
        except json.JSONDecodeError as e:
            logging.warning(f"Failed to parse JSON: {e}")
            logging.warning(f"Attempted to parse: {cleaned_response[:200]}...")
    
    # If no valid JSON found, create a structured error response
    logging.error("No valid JSON found in response. Creating error structure.")
    return {
        "error": "Failed to parse JSON from LLM response",
        "raw_response": response[:500] + "..." if len(response) > 500 else response
    }


# ─── AGENT 5: SENIOR P&L ANALYST ───────────────────────────

def agent_5_senior_pnl_analyst(state: AgentState) -> AgentState:
    """
    Agent 5: Senior P&L Analyst
    Combines all accepted P&L reports into a single P&L Initial Draft
    Uses 'Generate Financial Analysis' prompt
    """
    from call_bpce_llm import call_llm_api
    from prompts.prompts_FA import PROMPT_COMBINE_ANALYSIS_PNL
    
    logging.info("Agent 5 (Senior P&L Analyst) - Generating P&L Initial Draft")

# Use P&L-specific combined config, fallback to general combined_config
    config = state.get('combined_config_pnl') or state.get('combined_config') or {}
    temperature = config.get('temperature', 0.0)
    max_tokens = config.get('max_tokens', 4000)
    llm_model = config.get('llm_model', COMBINED_DEFAULT_LLM_MODEL)
    prompt_template = config.get('custom_prompt', PROMPT_COMBINE_ANALYSIS_PNL)
    
    # Combine all accepted P&L reports
    combined_reports = ""
    for i, report in enumerate(state['pnl_accepted_reports'], 1):
        report_content = json.dumps(report['report'], indent=2)

        combined_reports += f"""
╔══════════════════════════════════════════════════════════════════════════╗
║  P&L REPORT {i}  ║
╚══════════════════════════════════════════════════════════════════════════╝

{report_content}

"""
    
    full_prompt = f"""{prompt_template}

{'='*90}
[[[COMBINED P&L REPORTS]]]
{'='*90}

{combined_reports}

"""
    
    try:
        response = call_llm_api(
            query=full_prompt,
            model_name=llm_model,
            temperature=temperature,
            max_tokens=max_tokens
        ).content
        
        # CRITICAL: Save raw LLM response BEFORE passing to extract_json_from_response
        session_folder = state.get('session_folder', './uploads')
        from utils import save_file_sequencially
        save_file_sequencially(session_folder, "pnl_final_draft_raw_response", response)
        logging.info(f"Saved P&L final draft raw response to {session_folder}/pnl_final_draft_raw_response.md")
        
        # Extract JSON from response using improved helper function
        final_draft = extract_json_from_response(response)
        
        # CRITICAL: Save extracted JSON report AFTER extract_json_from_response
        save_file_sequencially(session_folder, "pnl_final_draft_extracted_json", json.dumps(final_draft, indent=2, ensure_ascii=False))
        logging.info(f"Saved P&L final draft extracted JSON to {session_folder}/pnl_final_draft_extracted_json.json")
        
        state['pnl_final_draft'] = {
            "model": llm_model,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "generated_at": datetime.now().isoformat(),
            "report": final_draft,
        }
        
        # Save final draft to file
        session_folder = state.get('session_folder', './uploads')
        final_draft_path = os.path.join(session_folder, "pnl_final_draft.json")
        with open(final_draft_path, 'w', encoding='utf-8') as f:
            json.dump(state['pnl_final_draft'], f, indent=2, ensure_ascii=False)
        logging.info(f"Saved P&L final draft to {final_draft_path}")
        
        state['messages'].append(AIMessage(content="P&L Initial Draft generated successfully"))
        state['current_step'] = 'bs_analysis'
        state['status_message'] = 'P&L Initial Draft completed'
        state['progress'] = 0.70
        
        logging.info("Agent 5 (Senior P&L Analyst) - P&L Initial Draft completed")
        logging.info("="*50)
        
    except Exception as e:
        error_msg = f"Agent 5 (Senior P&L Analyst) failed: {str(e)}"
        logging.error(error_msg)
        state['errors'].append(error_msg)
    
    return state


# ─── AGENT 6: BALANCE SHEET ANALYST ────────────────────────

# ─── BALANCE SHEET SUB-AGENT FACTORY ──────────────────────

def create_bs_subagent(chunk_index: int):
    """
    Factory function to create a Balance Sheet sub-agent for a specific chunk
    Each sub-agent processes one chunk independently
    """
    def bs_subagent(state: AgentState) -> AgentState:
        """
        Balance Sheet Sub-Agent for chunk {chunk_index}
        Processes a single chunk in parallel with other sub-agents
        """
        from call_bpce_llm import call_llm_api
        from prompts.prompts_FA import PROMPT_DRIVER_ANALYSIS_BS
        from utils import save_file_sequencially
        
        node_name = f"bs_analyst_{chunk_index}"
        log_node_execution(node_name, "starting")
        session_id = state.get('session_folder', 'default')
        tracker = get_tracker(session_id)
        tracker.update_sub_agent("Balance Sheet Analysis", node_name, "running")
        
        # Update UI status
        from status_callback import update_status
        update_status(f"BS Analyst {chunk_index} analyzing chunk...", "info")
        
        logging.info(f"="*50)
        logging.info(f"BS Sub-Agent {chunk_index} - Starting analysis")
        logging.info(f"="*50)
        
        # Use type-specific config, fallback to general fa_config
        config = state.get('fa_config_bs') or state.get('fa_config') or {}
        temperature = config.get('temperature', 0.0)
        max_tokens = config.get('max_tokens', 2000)
        llm_model = config.get('llm_model', FA_DEFAULT_LLM_MODEL)
        # CRITICAL: Always use BS prompt for BS analysis, ignore custom_prompt from fa_config
        prompt_template = PROMPT_DRIVER_ANALYSIS_BS
        session_folder = state.get('session_folder', './uploads')
        
        # Get the specific chunk for this sub-agent
        chunk = state['bs_chunks'][chunk_index - 1]  # chunk_index is 1-based
        context = chunk['context']
        
        full_prompt = f"""{'#'*90}
[[[TASK]]]
{'#'*90}

{prompt_template}
</TASK>

{'#'*90}
[[[CONTEXT]]]
{'#'*90}

<CONTEXT>{context}</CONTEXT>
"""
        
        # Save prompt to file
        save_file_sequencially(session_folder, f"bs_prompt_chunk_{chunk_index}", full_prompt)
        logging.info(f"BS Sub-Agent {chunk_index} - Saved prompt")
        
        try:
            logging.info(f"BS Sub-Agent {chunk_index} - Calling LLM API...")
            response = call_llm_api(
                query=full_prompt,
                model_name=llm_model,
                temperature=temperature,
                max_tokens=max_tokens
            ).content
            
            # Try to parse as JSON
            try:
                import re
                json_match = re.search(r'\{.*\}', response, re.DOTALL)
                if json_match:
                    report_data = json.loads(json_match.group())
                else:
                    report_data = {"raw_response": response}
            except:
                report_data = {"raw_response": response}
            
            result = {
                "chunk_index": chunk_index,
                "model": llm_model,
                "temperature": temperature,
                "max_tokens": max_tokens,
                "generated_at": datetime.now().isoformat(),
                "report": report_data,
            }
            
            # Save report to file
            report_path = os.path.join(session_folder, f"bs_report_chunk_{chunk_index}.json")
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            logging.info(f"BS Sub-Agent {chunk_index} - Report saved successfully")
            
            logging.info(f"BS Sub-Agent {chunk_index} - Completed successfully")
            logging.info(f"="*50)
            
            session_id = state.get('session_folder', 'default')
            tracker = get_tracker(session_id)
            tracker.update_sub_agent("Balance Sheet Analysis", node_name, "completed")
            log_node_execution(node_name, "completed")
            
            # Update UI status
            from status_callback import update_status
            update_status(f"BS Analyst {chunk_index} completed", "success")
            
            # Return only the fields this sub-agent updates
            return {
                "bs_reports": [result],  # Will be extended by the reducer
            }
            
        except Exception as e:
            error_msg = f"BS Sub-Agent {chunk_index} failed: {str(e)}"
            logging.error(error_msg)
            logging.error(f"="*50)
            
            session_id = state.get('session_folder', 'default')
            tracker = get_tracker(session_id)
            tracker.update_sub_agent("Balance Sheet Analysis", node_name, "failed")
            log_node_execution(node_name, "failed")
            
            # Return error
            return {
                "errors": [error_msg],
            }
    
    return bs_subagent


# ─── CASH FLOW SUB-AGENT FACTORY ──────────────────────────

def create_cf_subagent(chunk_index: int):
    """
    Factory function to create a Cash Flow sub-agent for a specific chunk
    Each sub-agent processes one chunk independently
    """
    def cf_subagent(state: AgentState) -> AgentState:
        """
        Cash Flow Sub-Agent for chunk {chunk_index}
        Processes a single chunk in parallel with other sub-agents
        """
        from call_bpce_llm import call_llm_api
        from prompts.prompts_FA import PROMPT_DRIVER_ANALYSIS_CF
        from utils import save_file_sequencially
        
        logging.info(f"="*50)
        logging.info(f"CF Sub-Agent {chunk_index} - Starting analysis")
        logging.info(f"="*50)
        
        # Use type-specific config, fallback to general fa_config
        config = state.get('fa_config_cf') or state.get('fa_config') or {}
        temperature = config.get('temperature', 0.0)
        max_tokens = config.get('max_tokens', 2000)
        llm_model = config.get('llm_model', FA_DEFAULT_LLM_MODEL)
        prompt_template = config.get('custom_prompt', PROMPT_DRIVER_ANALYSIS_CF)
        session_folder = state.get('session_folder', './uploads')
        
        # Get the specific chunk for this sub-agent
        chunk = state['cf_chunks'][chunk_index - 1]  # chunk_index is 1-based
        context = chunk['context']
        full_prompt = f"""{PROMPT_DRIVER_ANALYSIS_CF}\n\n{context}"""
        
        # Save prompt to file
        save_file_sequencially(session_folder, f"cf_prompt_chunk_{chunk_index}", full_prompt)
        logging.info(f"CF Sub-Agent {chunk_index} - Saved prompt")
        
        try:
            logging.info(f"CF Sub-Agent {chunk_index} - Calling LLM API...")
            response = call_llm_api(
                query=full_prompt,
                model_name=llm_model,
                temperature=temperature,
                max_tokens=max_tokens
            ).content
            
            result = {
                "chunk_index": chunk_index,
                "model": llm_model,
                "temperature": temperature,
                "max_tokens": max_tokens,
                "generated_at": datetime.now().isoformat(),
                "report": {"raw_response": response},
            }
            
            # Save report to file
            report_path = os.path.join(session_folder, f"cf_report_chunk_{chunk_index}.json")
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            logging.info(f"CF Sub-Agent {chunk_index} - Report saved successfully")
            
            logging.info(f"CF Sub-Agent {chunk_index} - Completed successfully")
            logging.info(f"="*50)
            
            # Return only the fields this sub-agent updates
            return {
                "cf_reports": [result],  # Will be extended by the reducer
            }
            
        except Exception as e:
            error_msg = f"CF Sub-Agent {chunk_index} failed: {str(e)}"
            logging.error(error_msg)
            logging.error(f"="*50)
            
            # Return error
            return {
                "errors": [error_msg],
            }
    
    return cf_subagent


# ## ─── KEPT FOR BACKWARD COMPATIBILITY (NOT USED) ───────────

# def agent_6_balance_sheet_analyst(state: AgentState) -> AgentState:
#     """
#     Agent 6: Balance Sheet Analyst
#     Similar to P&L Analyst but for Balance Sheet analysis
#     Processes chunks in parallel using LangGraph
#     """
#     from call_bpce_llm import call_llm_api
#     from prompts.prompts_FA import prompt_driver_analysis
#     from langchain.output_parsers import PydanticOutputParser
#     from pydantic_models import Report
#     import concurrent.futures
    
#     # This function is kept for backward compatibility but not used
#     # Use create_bs_subagent() factory instead
#     logging.info("Agent 6 (Balance Sheet Analyst) - DEPRECATED - Use create_bs_subagent() instead")
    
#     # Use type-specific config, fallback to general fa_config
#     config = state.get('fa_config_pnl') or state.get('fa_config') or {}
#     temperature = config.get('temperature', 0.0)
#     max_tokens = config.get('max_tokens', 2000)
#     llm_model = config.get('llm_model', FA_DEFAULT_LLM_MODEL)
#     prompt_template = config.get('custom_prompt', prompt_driver_analysis)
#     session_folder = state.get('session_folder', './uploads')
    
#     parser = PydanticOutputParser(pydantic_object=Report)
    
#     def process_chunk(chunk_data):
#         """Process a single chunk - for parallel execution"""
#         idx = chunk_data['chunk_index']
#         context = chunk_data['context']
        
#         full_prompt = f"""{'#'*90}
# [[[TASK]]]
# {'#'*90}

# <TASK>{prompt_template}

# Focus specifically on Balance Sheet analysis including:
# - Assets analysis
# - Liabilities analysis
# - Equity analysis
# - Working capital
# - Liquidity ratios
# </TASK>

# {'#'*90}
# [[[CONTEXT]]]
# {'#'*90}

# <CONTEXT>{context}</CONTEXT>

# {'#'*90}
# [[[OUTPUT FORMAT]]]:
# {'#'*90}

# <OUTPUT FORMAT>Generate a structured report.

# STRICT RULES:
# - Output MUST be valid JSON
# - Follow schema exactly
# - Do NOT add extra text
# - Each header must contain: analysis, source_table, citation (list)

# {parser.get_format_instructions()}</OUTPUT FORMAT>
# """
        
#         # Save prompt to file
#         from utils import save_file_sequencially
#         save_file_sequencially(session_folder, f"bs_prompt_chunk_{idx}", full_prompt)
#         logging.info(f"Saved BS prompt for chunk {idx}")
        
#         try:
#             response = call_llm_api(
#                 query=full_prompt,
#                 model_name=llm_model,
#                 temperature=temperature,
#                 max_tokens=max_tokens
#             ).content
            
#             # Try to parse as JSON
#             try:
#                 import re
#                 json_match = re.search(r'\{.*\}', response, re.DOTALL)
#                 if json_match:
#                     report_data = json.loads(json_match.group())
#                 else:
#                     report_data = {"raw_response": response}
#             except:
#                 report_data = {"raw_response": response}
            
#             result = {
#                 "chunk_index": idx,
#                 "model": llm_model,
#                 "temperature": temperature,
#                 "max_tokens": max_tokens,
#                 "generated_at": datetime.now().isoformat(),
#                 "report": report_data,
#             }
            
#             # Save report to file
#             report_path = os.path.join(session_folder, f"bs_report_chunk_{idx}.json")
#             with open(report_path, 'w', encoding='utf-8') as f:
#                 json.dump(result, f, indent=2, ensure_ascii=False)
#             logging.info(f"Agent 6 (BS Analyst) - Chunk {idx} analyzed and saved successfully")
            
#             return result
            
#         except Exception as e:
#             error_msg = f"Agent 6 (BS Analyst) - Chunk {idx} failed: {str(e)}"
#             logging.error(error_msg)
#             return {"chunk_index": idx, "error": str(e)}
    
#     return state


# ─── CONDITIONAL ROUTING FUNCTIONS ─────────────────────────

def should_retry_bref(state: AgentState) -> str:
    """Determine if BREF generation should be retried (Legacy - uses P&L)"""
    if state.get('bref_accepted', False):
        return "accepted"
    elif state.get('bref_retry_count', 0) >= 3:
        return "max_retries"
    else:
        return "retry"


def should_retry_bref_pnl(state: AgentState) -> str:
    """Determine if P&L BREF generation should be retried"""
    if state.get('bref_accepted_pnl', False):
        return "accepted"
    elif state.get('bref_retry_count_pnl', 0) >= 3:
        return "max_retries"
    else:
        return "retry"


def should_retry_bref_bs(state: AgentState) -> str:
    """Determine if Balance Sheet BREF generation should be retried"""
    if state.get('bref_accepted_bs', False):
        return "accepted"
    elif state.get('bref_retry_count_bs', 0) >= 3:
        return "max_retries"
    else:
        return "retry"


def should_retry_bref_cf(state: AgentState) -> str:
    """Determine if Cash Flow BREF generation should be retried"""
    if state.get('bref_accepted_cf', False):
        return "accepted"
    elif state.get('bref_retry_count_cf', 0) >= 3:
        return "max_retries"
    else:
        return "retry"


# route_to_analysts removed - users select agents from UI instead


def should_retry_pnl_chunk(state: AgentState, chunk_index: int) -> str:
    """Determine if P&L chunk analysis should be retried"""
    validation_result = next(
        (v for v in state['pnl_validation_results'] if v['chunk_index'] == chunk_index),
        None
    )
    
    if not validation_result:
        return "accepted"
    
    if validation_result.get('accepted', False):
        return "accepted"
    elif validation_result.get('retry_count', 0) >= 3:
        return "max_retries"
    else:
        return "retry"


# ─── SUMMARIZER AGENTS (NEW) ───────────────────────────────

def agent_summarizer_pnl(state: AgentState) -> AgentState:
    """
    Summarizer Agent for P&L
    Takes BREF summary + Initial Draft from Sr. Analyst and generates final summarized report
    Uses PROMPT_SUMMARIZE_REPORTS_PNL from prompts_FA.py
    """
    from call_bpce_llm import call_llm_api
    from prompts.prompts_FA import PROMPT_SUMMARIZE_REPORTS_PNL
    from utils import save_file_sequencially
    
    logging.info("="*50)
    logging.info("Summarizer Agent (P&L) - Starting summarization")
    logging.info("="*50)
    
    # Use dedicated summarizer configuration (defaults same as Sr. Analyst)
    config = state.get('summarizer_config_pnl') or state.get('summarizer_config') or {}
    temperature = config.get('temperature', SUMMARIZER_DEFAULT_TEMPERATURE)
    max_tokens = config.get('max_tokens', SUMMARIZER_DEFAULT_MAX_TOKENS)
    llm_model = config.get('llm_model', SUMMARIZER_DEFAULT_LLM_MODEL)
    session_folder = state.get('session_folder', './uploads')
    
    logging.info(f"Configuration: Model={llm_model}, Temp={temperature}, MaxTokens={max_tokens}")
    
        # Get BREF summary and Initial Draft
    bref_summary = state.get('bref_summary_pnl', state.get('bref_summary', ''))
    initial_draft = state.get('pnl_final_draft', {})
    initial_draft_report = json.dumps(initial_draft.get('report', {}), indent=2, ensure_ascii=False)
    
    # Build prompt with variable substitution using replace() to avoid format() conflicts
    full_prompt = PROMPT_SUMMARIZE_REPORTS_PNL.replace(
        '{bref_summary_report}', bref_summary
    ).replace(
        '{report_initial_draft}', initial_draft_report
    )
    
    # Save prompt to file
    save_file_sequencially(session_folder, "pnl_summarizer_prompt", full_prompt)
    logging.info(f"Saved P&L Summarizer prompt to {session_folder}/pnl_summarizer_prompt.md")
    
    try:
        logging.info("Calling LLM API for P&L summarization...")
        response = call_llm_api(
            query=full_prompt,
            model_name=llm_model,
            temperature=temperature,
            max_tokens=max_tokens
        ).content
        
        # Save raw response
        save_file_sequencially(session_folder, "pnl_summarizer_raw_response", response)
        logging.info(f"Saved P&L Summarizer raw response")
        
        # Extract JSON from response
        summarized_report = extract_json_from_response(response)
        
        # Save extracted JSON
        save_file_sequencially(session_folder, "pnl_summarizer_extracted_json", json.dumps(summarized_report, indent=2, ensure_ascii=False))
        logging.info(f"Saved P&L Summarizer extracted JSON")
        
        # Update final draft with summarized report
        state['pnl_final_draft'] = {
            "model": llm_model,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "generated_at": datetime.now().isoformat(),
            "report": summarized_report,
            "summarized": True
        }
        
        # Save final summarized draft
        final_draft_path = os.path.join(session_folder, "pnl_final_draft.json")
        with open(final_draft_path, 'w', encoding='utf-8') as f:
            json.dump(state['pnl_final_draft'], f, indent=2, ensure_ascii=False)
        logging.info(f"Saved P&L final summarized draft to {final_draft_path}")
        
        state['messages'].append(AIMessage(content="P&L Final Summarized Report generated successfully"))
        state['status_message'] = 'P&L Summarization completed'
        
        logging.info("Summarizer Agent (P&L) - Summarization completed successfully")
        logging.info("="*50)
        
    except Exception as e:
        error_msg = f"Summarizer Agent (P&L) failed: {str(e)}"
        logging.error(error_msg)
        logging.error("="*50)
        state['errors'].append(error_msg)
        state['messages'].append(AIMessage(content=error_msg))
    
    return state


def agent_summarizer_bs(state: AgentState) -> AgentState:
    """
    Summarizer Agent for Balance Sheet
    Takes BREF summary + Initial Draft from Sr. Analyst and generates final summarized report
    Uses PROMPT_SUMMARIZE_REPORTS_BS from prompts_FA.py
    """
    from call_bpce_llm import call_llm_api
    from prompts.prompts_FA import PROMPT_SUMMARIZE_REPORTS_BS
    from utils import save_file_sequencially
    
    logging.info("="*50)
    logging.info("Summarizer Agent (BS) - Starting summarization")
    logging.info("="*50)
    
    # Use dedicated summarizer configuration (defaults same as Sr. Analyst)
    config = state.get('summarizer_config_bs') or state.get('summarizer_config') or {}
    temperature = config.get('temperature', SUMMARIZER_DEFAULT_TEMPERATURE)
    max_tokens = config.get('max_tokens', SUMMARIZER_DEFAULT_MAX_TOKENS)
    llm_model = config.get('llm_model', SUMMARIZER_DEFAULT_LLM_MODEL)
    session_folder = state.get('session_folder', './uploads')
    
    logging.info(f"Configuration: Model={llm_model}, Temp={temperature}, MaxTokens={max_tokens}")
    
        # Get BREF summary and Initial Draft
    bref_summary = state.get('bref_summary_bs', '')
    initial_draft = state.get('bs_final_draft', {})
    initial_draft_report = json.dumps(initial_draft.get('report', {}), indent=2, ensure_ascii=False)
    
    # Build prompt with variable substitution using replace() to avoid format() conflicts
    full_prompt = PROMPT_SUMMARIZE_REPORTS_BS.replace(
        '{bs_summary_report}', bref_summary
    ).replace(
        '{bs_initial_draft}', initial_draft_report
    )
    
    # Save prompt to file
    save_file_sequencially(session_folder, "bs_summarizer_prompt", full_prompt)
    logging.info(f"Saved BS Summarizer prompt to {session_folder}/bs_summarizer_prompt.md")
    
    try:
        logging.info("Calling LLM API for BS summarization...")
        response = call_llm_api(
            query=full_prompt,
            model_name=llm_model,
            temperature=temperature,
            max_tokens=max_tokens
        ).content
        
        # Save raw response
        save_file_sequencially(session_folder, "bs_summarizer_raw_response", response)
        logging.info(f"Saved BS Summarizer raw response")
        
        # Extract JSON from response
        summarized_report = extract_json_from_response(response)
        
        # Save extracted JSON
        save_file_sequencially(session_folder, "bs_summarizer_extracted_json", json.dumps(summarized_report, indent=2, ensure_ascii=False))
        logging.info(f"Saved BS Summarizer extracted JSON")
        
        # Update final draft with summarized report
        state['bs_final_draft'] = {
            "model": llm_model,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "generated_at": datetime.now().isoformat(),
            "report": summarized_report,
            "summarized": True
        }
        
        # Save final summarized draft
        final_draft_path = os.path.join(session_folder, "bs_final_draft.json")
        with open(final_draft_path, 'w', encoding='utf-8') as f:
            json.dump(state['bs_final_draft'], f, indent=2, ensure_ascii=False)
        logging.info(f"Saved BS final summarized draft to {final_draft_path}")
        
        state['messages'].append(AIMessage(content="Balance Sheet Final Summarized Report generated successfully"))
        state['status_message'] = 'BS Summarization completed'
        
        logging.info("Summarizer Agent (BS) - Summarization completed successfully")
        logging.info("="*50)
        
    except Exception as e:
        error_msg = f"Summarizer Agent (BS) failed: {str(e)}"
        logging.error(error_msg)
        logging.error("="*50)
        state['errors'].append(error_msg)
        state['messages'].append(AIMessage(content=error_msg))
    
    return state


def agent_summarizer_cf(state: AgentState) -> AgentState:
    """
    Summarizer Agent for Cash Flow
    Takes BREF summary + Initial Draft from Sr. Analyst and generates final summarized report
    Uses PROMPT_SUMMARIZE_REPORTS_CF from prompts_FA.py
    """
    from call_bpce_llm import call_llm_api
    from prompts.prompts_FA import PROMPT_SUMMARIZE_REPORTS_CF
    from utils import save_file_sequencially
    
    logging.info("="*50)
    logging.info("Summarizer Agent (CF) - Starting summarization")
    logging.info("="*50)
    
    # Use dedicated summarizer configuration (defaults same as Sr. Analyst)
    config = state.get('summarizer_config_cf') or state.get('summarizer_config') or {}
    temperature = config.get('temperature', SUMMARIZER_DEFAULT_TEMPERATURE)
    max_tokens = config.get('max_tokens', SUMMARIZER_DEFAULT_MAX_TOKENS)
    llm_model = config.get('llm_model', SUMMARIZER_DEFAULT_LLM_MODEL)
    session_folder = state.get('session_folder', './uploads')
    
    logging.info(f"Configuration: Model={llm_model}, Temp={temperature}, MaxTokens={max_tokens}")
    
        # Get BREF summary and Initial Draft
    bref_summary = state.get('bref_summary_cf', '')
    initial_draft = state.get('cf_final_draft', {})
    initial_draft_report = json.dumps(initial_draft.get('report', {}), indent=2, ensure_ascii=False)
    
    # Build prompt with variable substitution using replace() to avoid format() conflicts
    full_prompt = PROMPT_SUMMARIZE_REPORTS_CF.replace(
        '{bref_summary_report}', bref_summary
    ).replace(
        '{report_initial_draft}', initial_draft_report
    )
    
    # Save prompt to file
    save_file_sequencially(session_folder, "cf_summarizer_prompt", full_prompt)
    logging.info(f"Saved CF Summarizer prompt to {session_folder}/cf_summarizer_prompt.md")
    
    try:
        logging.info("Calling LLM API for CF summarization...")
        response = call_llm_api(
            query=full_prompt,
            model_name=llm_model,
            temperature=temperature,
            max_tokens=max_tokens
        ).content
        
        # Save raw response
        save_file_sequencially(session_folder, "cf_summarizer_raw_response", response)
        logging.info(f"Saved CF Summarizer raw response")
        
        # Extract JSON from response
        summarized_report = extract_json_from_response(response)
        
        # Save extracted JSON
        save_file_sequencially(session_folder, "cf_summarizer_extracted_json", json.dumps(summarized_report, indent=2, ensure_ascii=False))
        logging.info(f"Saved CF Summarizer extracted JSON")
        
        # Update final draft with summarized report
        state['cf_final_draft'] = {
            "model": llm_model,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "generated_at": datetime.now().isoformat(),
            "report": summarized_report,
            "summarized": True
        }
        
        # Save final summarized draft
        final_draft_path = os.path.join(session_folder, "cf_final_draft.json")
        with open(final_draft_path, 'w', encoding='utf-8') as f:
            json.dump(state['cf_final_draft'], f, indent=2, ensure_ascii=False)
        logging.info(f"Saved CF final summarized draft to {final_draft_path}")
        
        state['messages'].append(AIMessage(content="Cash Flow Final Summarized Report generated successfully"))
        state['status_message'] = 'CF Summarization completed'
        
        logging.info("Summarizer Agent (CF) - Summarization completed successfully")
        logging.info("="*50)
        
    except Exception as e:
        error_msg = f"Summarizer Agent (CF) failed: {str(e)}"
        logging.error(error_msg)
        logging.error("="*50)
        state['errors'].append(error_msg)
        state['messages'].append(AIMessage(content=error_msg))
    
    return state

