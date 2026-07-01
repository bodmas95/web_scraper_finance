"""
============================================================
  LANGGRAPH WORKFLOW DEFINITION
============================================================
  Defines the agent workflow graph for financial analysis
============================================================
"""

from langgraph.graph import StateGraph, END
from agents import (
    AgentState,
    # agent_1_bref_analyst,
    # agent_2_bref_evaluator,
    
    #Currently we are Using these only --
    agent_1_bref_analyst_pnl,
    agent_1_bref_analyst_bs,
    agent_1_bref_analyst_cf,

    agent_2_bref_evaluator_pnl,
    agent_2_bref_evaluator_bs,
    agent_2_bref_evaluator_cf,

    # agent_4_pnl_data_evaluator,
    agent_5_senior_pnl_analyst,

    should_retry_bref,    
    should_retry_bref_pnl,
    should_retry_bref_bs,
    should_retry_bref_cf,
)

import logging
import sys
import os
from datetime import datetime
import json

# Import centralized configuration defaults
from config_defaults import (
    FA_DEFAULT_LLM_MODEL, COMBINED_DEFAULT_LLM_MODEL
)

# Add path for API imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "bpce_api_setup"))

# ─── BUILD WORKFLOW GRAPH ──────────────────────────────────

def build_financial_analysis_graph():
    """
    Build the LangGraph workflow for financial analysis
    
    NEW Workflow (Sequential BREF Generation for P&L, BS, CF):
    1. Upload Financial Summary → Generate ALL 3 BREFs
    2. P&L BREF: Agent 1 (BREF Analyst P&L) → Agent 2 (BREF Evaluator P&L) → Retry loop (max 3)
    3. BS BREF: Agent 1 (BREF Analyst BS) → Agent 2 (BREF Evaluator BS) → Retry loop (max 3)
    4. CF BREF: Agent 1 (BREF Analyst CF) → Agent 2 (BREF Evaluator CF) → Retry loop (max 3)
    5. Upload other documents → Create chunks with respective BREF summaries
    6. Analysis flow handled by dynamic_graph.py
    """
    
    workflow = StateGraph(AgentState)
    
    # ─── Add Nodes ─────────────────────────────────────────
    
    # BREF Analysis Flow - ALL 3 TYPES
    workflow.add_node("bref_analyst_pnl", agent_1_bref_analyst_pnl)
    workflow.add_node("bref_evaluator_pnl", agent_2_bref_evaluator_pnl)
    workflow.add_node("request_config_change_pnl", request_config_change_node_pnl)
    
    workflow.add_node("bref_analyst_bs", agent_1_bref_analyst_bs)
    workflow.add_node("bref_evaluator_bs", agent_2_bref_evaluator_bs)
    workflow.add_node("request_config_change_bs", request_config_change_node_bs)
    
    workflow.add_node("bref_analyst_cf", agent_1_bref_analyst_cf)
    workflow.add_node("bref_evaluator_cf", agent_2_bref_evaluator_cf)
    workflow.add_node("request_config_change_cf", request_config_change_node_cf)
    
    # # Legacy nodes (for backward compatibility)
    # workflow.add_node("bref_analyst", agent_1_bref_analyst)
    # workflow.add_node("bref_evaluator", agent_2_bref_evaluator)
    # workflow.add_node("request_config_change", request_config_change_node)
    
    # ─── Define Edges ──────────────────────────────────────
    
    # Entry point - Start with P&L BREF
    workflow.set_entry_point("bref_analyst_pnl")
    
    # P&L BREF Flow
    workflow.add_edge("bref_analyst_pnl", "bref_evaluator_pnl")
    workflow.add_conditional_edges(
        "bref_evaluator_pnl",
        should_retry_bref_pnl,
        {
            "accepted": "bref_analyst_bs",  # Move to BS BREF
            "retry": "bref_analyst_pnl",
            "max_retries": "request_config_change_pnl"
        }
    )
    workflow.add_edge("request_config_change_pnl", END)
    
    # BS BREF Flow
    workflow.add_edge("bref_analyst_bs", "bref_evaluator_bs")
    workflow.add_conditional_edges(
        "bref_evaluator_bs",
        should_retry_bref_bs,
        {
            "accepted": "bref_analyst_cf",  # Move to CF BREF
            "retry": "bref_analyst_bs",
            "max_retries": "request_config_change_bs"
        }
    )
    workflow.add_edge("request_config_change_bs", END)
    
    # CF BREF Flow
    workflow.add_edge("bref_analyst_cf", "bref_evaluator_cf")
    workflow.add_conditional_edges(
        "bref_evaluator_cf",
        should_retry_bref_cf,
        {
            "accepted": END,  # All BREFs completed
            "retry": "bref_analyst_cf",
            "max_retries": "request_config_change_cf"
        }
    )
    workflow.add_edge("request_config_change_cf", END)
    
    # Note: Analysis flow now handled by dynamic_graph.py
    # See build_parallel_analysis_graph() for parallel execution
    
    # Compile the graph
    app = workflow.compile()
    
    return app


# ─── HELPER NODES ──────────────────────────────────────────

def request_config_change_node(state: AgentState) -> AgentState:
    """
    Node to request user to change LLM configuration (Legacy)
    This node STOPS the workflow and requires user to start a new session
    """
    logging.error("BREF validation failed 3 times - STOPPING workflow")
    logging.error("User must change LLM model and start a new session")
    
    state['messages'].append({
        "role": "system",
        "content": "BREF validation failed after 3 attempts. Maximum retries reached. Please change LLM model and start a new session."
    })
    state['current_step'] = 'max_retries_reached'
    state['status_message'] = 'Max retries reached - Change LLM and start new session'
    
    # DO NOT reset retry count - keep it at 3 to show max retries reached
    # state['bref_retry_count'] = 0  # REMOVED - causes infinite loop
    
    return state


def request_config_change_node_pnl(state: AgentState) -> AgentState:
    """
    Node to request user to change LLM configuration for P&L BREF
    This node STOPS the workflow and requires user to start a new session
    """
    logging.error("P&L BREF validation failed 3 times - STOPPING workflow")
    logging.error("User must change LLM model and start a new session")
    
    state['messages'].append({
        "role": "system",
        "content": "P&L BREF validation failed after 3 attempts. Maximum retries reached. Please change LLM model and start a new session."
    })
    state['current_step'] = 'max_retries_reached_pnl'
    state['status_message'] = 'Max retries reached - Change LLM and start new session'
    
    # DO NOT reset retry count - keep it at 3 to show max retries reached
    # state['bref_retry_count_pnl'] = 0  # REMOVED - causes infinite loop
    # state['bref_retry_count'] = 0  # REMOVED - causes infinite loop
    
    return state


def request_config_change_node_bs(state: AgentState) -> AgentState:
    """
    Node to request user to change LLM configuration for BS BREF
    This node STOPS the workflow and requires user to start a new session
    """
    logging.error("Balance Sheet BREF validation failed 3 times - STOPPING workflow")
    logging.error("User must change LLM model and start a new session")
    
    state['messages'].append({
        "role": "system",
        "content": "Balance Sheet BREF validation failed after 3 attempts. Maximum retries reached. Please change LLM model and start a new session."
    })
    state['current_step'] = 'max_retries_reached_bs'
    state['status_message'] = 'Max retries reached - Change LLM and start new session'
    
    # DO NOT reset retry count - keep it at 3 to show max retries reached
    # state['bref_retry_count_bs'] = 0  # REMOVED - causes infinite loop
    
    return state


def request_config_change_node_cf(state: AgentState) -> AgentState:
    """
    Node to request user to change LLM configuration for CF BREF
    This node STOPS the workflow and requires user to start a new session
    """
    logging.error("Cash Flow BREF validation failed 3 times - STOPPING workflow")
    logging.error("User must change LLM model and start a new session")
    
    state['messages'].append({
        "role": "system",
        "content": "Cash Flow BREF validation failed after 3 attempts. Maximum retries reached. Please change LLM model and start a new session."
    })
    state['current_step'] = 'max_retries_reached_cf'
    state['status_message'] = 'Max retries reached - Change LLM and start new session'
    
    # DO NOT reset retry count - keep it at 3 to show max retries reached
    # state['bref_retry_count_cf'] = 0  # REMOVED - causes infinite loop
    
    return state


def chunk_generator_node(state: AgentState) -> AgentState:
    """
    Generate chunks from uploaded documents
    Separates chunks by document type (P&L, BS, CF)
    Saves all chunk data to JSON files
    """
    from utils import make_chunks, build_context_chunk, save_chunks
    import json
    
    logging.info("="*50)
    logging.info("Chunk Generator - Creating document chunks")
    logging.info("="*50)
    
    session_folder = state.get('session_folder', './uploads')
    
    # Create chunks from other documents
    chunks = make_chunks(state['other_documents'])
    logging.info(f"Created {len(chunks)} chunks from {len(state['other_documents'])} pages")
    
    # Save raw chunks to JSON
    chunk_paths = save_chunks(chunks, session_folder)
    logging.info(f"Saved {len(chunk_paths)} chunk files to {session_folder}")
    
    # Create chunk data with context - SEPARATE FOR EACH ANALYSIS TYPE
    # Each analysis type uses its own BREF summary
    
    # P&L chunks with P&L BREF summary
    pnl_chunks = []
    if 'P&L' in state['sections_found']:
        for idx, chunk_pages in enumerate(chunks, start=1):
            context = build_context_chunk(
                state.get('bref_summary_pnl', state.get('bref_summary', '')),  # Use P&L BREF
                chunk_pages,
                "QUALITATIVE SOURCE"
            )
            
            chunk_data = {
                "chunk_index": idx,
                "pages": chunk_pages,
                "context": context
            }
            
            pnl_chunks.append(chunk_data)
            
            # Save context for each chunk
            context_path = os.path.join(session_folder, f"chunk_{idx}_pnl_context.txt")
            with open(context_path, 'w', encoding='utf-8') as f:
                f.write(context)
            logging.info(f"Saved P&L context for chunk {idx}")
    
    # Balance Sheet chunks with BS BREF summary
    bs_chunks = []
    if 'Balance Sheet' in state['sections_found']:
        for idx, chunk_pages in enumerate(chunks, start=1):
            context = build_context_chunk(
                state.get('bref_summary_bs', ''),  # Use BS BREF
                chunk_pages,
                "QUALITATIVE SOURCE"
            )
            
            chunk_data = {
                "chunk_index": idx,
                "pages": chunk_pages,
                "context": context
            }
            
            bs_chunks.append(chunk_data)
            
            # Save context for each chunk
            context_path = os.path.join(session_folder, f"chunk_{idx}_bs_context.txt")
            with open(context_path, 'w', encoding='utf-8') as f:
                f.write(context)
            logging.info(f"Saved BS context for chunk {idx}")
    
    # Cash Flow chunks with CF BREF summary
    cf_chunks = []
    if 'Cash Flow' in state['sections_found']:
        for idx, chunk_pages in enumerate(chunks, start=1):
            context = build_context_chunk(
                state.get('bref_summary_cf', ''),  # Use CF BREF
                chunk_pages,
                "QUALITATIVE SOURCE"
            )
            
            chunk_data = {
                "chunk_index": idx,
                "pages": chunk_pages,
                "context": context
            }
            
            cf_chunks.append(chunk_data)
            
            # Save context for each chunk
            context_path = os.path.join(session_folder, f"chunk_{idx}_cf_context.txt")
            with open(context_path, 'w', encoding='utf-8') as f:
                f.write(context)
            logging.info(f"Saved CF context for chunk {idx}")
    
    state['chunks'] = chunks
    state['pnl_chunks'] = pnl_chunks
    state['bs_chunks'] = bs_chunks
    state['cf_chunks'] = cf_chunks
    
    # Save chunk assignment info
    chunk_info = {
        "total_chunks": len(chunks),
        "pnl_chunks": len(pnl_chunks),
        "bs_chunks": len(bs_chunks),
        "cf_chunks": len(cf_chunks),
        "selected_analyses": state['sections_found'],
        "generated_at": datetime.now().isoformat()
    }
    
    chunk_info_path = os.path.join(session_folder, "chunk_info.json")
    with open(chunk_info_path, 'w', encoding='utf-8') as f:
        json.dump(chunk_info, f, indent=2)
    logging.info(f"Saved chunk info to {chunk_info_path}")
    
    state['messages'].append({
        "role": "system",
        "content": f"Generated {len(chunks)} chunks: P&L={len(pnl_chunks)}, BS={len(bs_chunks)}, CF={len(cf_chunks)}"
    })
    state['current_step'] = 'analysis'
    state['status_message'] = f'Chunks created: {len(chunks)} total'
    state['progress'] = 0.40
    
    logging.info(f"Chunk Generator - Completed:")
    logging.info(f"  Total chunks created: {len(chunks)}")
    logging.info(f"  P&L chunks assigned: {len(pnl_chunks)} (using P&L BREF summary)")
    logging.info(f"  BS chunks assigned: {len(bs_chunks)} (using BS BREF summary)")
    logging.info(f"  CF chunks assigned: {len(cf_chunks)} (using CF BREF summary)")
    logging.info(f"  NOTE: Each analysis type uses its own BREF summary in context")
    logging.info("="*50)
    
    return state


def bs_data_evaluator_node(state: AgentState) -> AgentState:
    """Balance Sheet Data Evaluator - similar to P&L evaluator"""
    from call_bpce_llm import call_llm_api
    import json
    from prompts.prompts_FA import FA_VALIDATION_PROMPT_BS
    
    logging.info("Balance Sheet Data Evaluator - Starting validation")    
    validated_reports = []
    
    for report in state['bs_reports']:
        chunk_index = report['chunk_index']
        report_content = json.dumps(report['report'], indent=2)
        
        full_prompt = f"{FA_VALIDATION_PROMPT_BS}\n\n{report_content}"
        
        try:
            response = call_llm_api(
                query=full_prompt,
                model_name=FA_DEFAULT_LLM_MODEL,
                temperature=0.0,
                max_tokens=1000
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
            validation_result['retry_count'] = 0
            
            state['bs_validation_results'].append(validation_result)
            
            if validation_result.get('accepted', False):
                state['bs_accepted_reports'].append(report)
                logging.info(f"BS Data Evaluator - Chunk {chunk_index} accepted")
            else:
                logging.warning(f"BS Data Evaluator - Chunk {chunk_index} rejected")
            
        except Exception as e:
            error_msg = f"BS Data Evaluator - Chunk {chunk_index} validation failed: {str(e)}"
            logging.error(error_msg)
            state['errors'].append(error_msg)
            state['bs_accepted_reports'].append(report)
    
    state['messages'].append({
        "role": "system",
        "content": f"BS validation completed: {len(state['bs_accepted_reports'])}/{len(state['bs_reports'])} accepted"
    })
    state['current_step'] = 'bs_senior_analyst'
    state['progress'] = 0.85
    
    return state


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


def bs_senior_analyst_node(state: AgentState) -> AgentState:
    """Senior Balance Sheet Analyst - combines BS reports"""
    from call_bpce_llm import call_llm_api
    from prompts.prompts_FA import PROMPT_COMBINE_ANALYSIS_BS
    from datetime import datetime
    import json
    
    logging.info("="*50)
    logging.info("Senior BS Analyst - Generating BS Initial Draft")
    logging.info("="*50)
    
    # Update UI status
    from status_callback import update_status
    update_status("Senior BS Analyst combining reports...", "info")
    
    # FIX: Handle None config - use 'or {}' pattern to ensure config is always a dict
    # Use type-specific combined config, fallback to general combined_config
    config = state.get('combined_config_bs') or state.get('combined_config') or {}
    temperature = config.get('temperature', 0.0)
    max_tokens = config.get('max_tokens', 4000)
    llm_model = config.get('llm_model', COMBINED_DEFAULT_LLM_MODEL)
    prompt_template = config.get('custom_prompt', PROMPT_COMBINE_ANALYSIS_BS)
    
    # Debug: Check state contents
    logging.info(f"State keys: {list(state.keys())}")
    logging.info(f"BS reports count: {len(state.get('bs_reports', []))}")
    logging.info(f"BS validation results count: {len(state.get('bs_validation_results', []))}")
    logging.info(f"BS accepted reports count: {len(state.get('bs_accepted_reports', []))}")
    
    # Check if we have reports to combine
    if not state.get('bs_accepted_reports') or len(state.get('bs_accepted_reports', [])) == 0:
        error_msg = f"No BS accepted reports to combine. BS reports: {len(state.get('bs_reports', []))}, Accepted: {len(state.get('bs_accepted_reports', []))}"
        logging.error(error_msg)
        logging.error("This usually means:")
        logging.error("  1. BS analysts didn't generate reports")
        logging.error("  2. BS evaluators didn't accept any reports")
        logging.error("  3. State is not being passed correctly between nodes")
        state['errors'].append(error_msg)
        
        # Update UI status
        update_status(f"Senior BS Analyst failed: No reports to combine", "error")
        
        return state
    
    logging.info(f"Combining {len(state['bs_accepted_reports'])} BS reports")
    
    # Combine all accepted BS reports
    combined_reports = ""
    for i, report in enumerate(state['bs_accepted_reports'], 1):
        report_content = json.dumps(report['report'], indent=2)
        combined_reports += f"""
╔══════════════════════════════════════════════════════════════════════════╗
║  BALANCE SHEET REPORT {i}  ║
╚══════════════════════════════════════════════════════════════════════════╝

{report_content}

"""
    
        # Build the full prompt with reports AFTER the instructions
    full_prompt = f"""{prompt_template}

{'='*90}
[[[COMBINED BALANCE SHEET REPORTS]]]
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
        save_file_sequencially(session_folder, "bs_final_draft_raw_response", response)
        logging.info(f"Saved BS final draft raw response to {session_folder}/bs_final_draft_raw_response.md")
        
        # Extract JSON from response using improved helper function
        final_draft = extract_json_from_response(response)
        
        # CRITICAL: Save extracted JSON report AFTER extract_json_from_response
        save_file_sequencially(session_folder, "bs_final_draft_extracted_json", json.dumps(final_draft, indent=2, ensure_ascii=False))
        logging.info(f"Saved BS final draft extracted JSON to {session_folder}/bs_final_draft_extracted_json.json")
        
        state['bs_final_draft'] = {
            "model": llm_model,
            "generated_at": datetime.now().isoformat(),
            "report": final_draft,
        }
        
        # Save final draft to file
        session_folder = state.get('session_folder', './uploads')
        final_draft_path = os.path.join(session_folder, "bs_final_draft.json")
        with open(final_draft_path, 'w', encoding='utf-8') as f:
            json.dump(state['bs_final_draft'], f, indent=2, ensure_ascii=False)
        logging.info(f"Saved BS final draft to {final_draft_path}")
        
        state['messages'].append({"role": "system", "content": "Balance Sheet Initial Draft generated"})
        state['progress'] = 0.90
        
        logging.info("Senior BS Analyst - BS Initial Draft completed")
        logging.info("="*50)
        
        # Update UI status
        from status_callback import update_status
        update_status("Senior BS Analyst completed final report", "success")
        
    except Exception as e:
        logging.error(f"Senior BS Analyst failed: {str(e)}")
        state['errors'].append(str(e))
        
        # Update UI status
        from status_callback import update_status
        update_status(f"Senior BS Analyst failed: {str(e)}", "error")
    
    return state


def cf_analyst_node(state: AgentState) -> AgentState:
    """Cash Flow Analyst - similar to P&L analyst with parallel processing"""
    from call_bpce_llm import call_llm_api
    from prompts.prompts_FA import PROMPT_DRIVER_ANALYSIS_CF
    from utils import save_file_sequencially
    import concurrent.futures
    
    logging.info("Cash Flow Analyst - Starting analysis (Parallel Processing)")
    
    config = state.get('fa_config', {})
    session_folder = state.get('session_folder', './uploads')
    
    def process_chunk(chunk_data):
        """Process a single chunk - for parallel execution"""
        idx = chunk_data['chunk_index']
        context = chunk_data['context']
        full_prompt= f"""{PROMPT_DRIVER_ANALYSIS_CF}\n\n{context}"""
#         full_prompt = f"""Analyze the following Cash Flow data and generate a comprehensive report.

# Focus on:
# - Operating cash flow
# - Investing cash flow
# - Financing cash flow
# - Free cash flow
# - Cash flow trends

# {context}
# """
        
        # Save prompt to file
        save_file_sequencially(session_folder, f"cf_prompt_chunk_{idx}", PROMPT_DRIVER_ANALYSIS_CF)
        logging.info(f"Saved CF prompt for chunk {idx}")
        
        try:
            response = call_llm_api(
                query=full_prompt,
                model_name=config.get('llm_model', FA_DEFAULT_LLM_MODEL),
                temperature=config.get('temperature', 0.0),
                max_tokens=config.get('max_tokens', 2000)
            ).content
            
            result = {
                "chunk_index": idx,
                "generated_at": datetime.now().isoformat(),
                "report": {"raw_response": response},
            }
            
            # Save report to file
            report_path = os.path.join(session_folder, f"cf_report_chunk_{idx}.json")
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            logging.info(f"CF Analyst - Chunk {idx} analyzed and saved successfully")
            
            return result
            
        except Exception as e:
            error_msg = f"CF Analyst - Chunk {idx} failed: {str(e)}"
            logging.error(error_msg)
            return {"chunk_index": idx, "error": str(e)}
    
    # Process chunks in parallel
    cf_reports = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_chunk, chunk) for chunk in state['cf_chunks']]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if 'error' not in result:
                cf_reports.append(result)
            else:
                state['errors'].append(result.get('error', 'Unknown error'))
    
    # Sort by chunk index
    cf_reports.sort(key=lambda x: x['chunk_index'])
    
    state['cf_reports'] = cf_reports
    state['progress'] = 0.92
    
    logging.info(f"CF Analyst - Completed parallel processing of {len(cf_reports)} chunks")
    
    return state


def cf_data_evaluator_node(state: AgentState) -> AgentState:
    """Cash Flow Data Evaluator"""
    logging.info("CF Data Evaluator - Validating reports")
    
    # Simple validation - accept all for now
    state['cf_accepted_reports'] = state['cf_reports']
    state['cf_validation_results'] = [
        {"chunk_index": r['chunk_index'], "accepted": True, "score": 85}
        for r in state['cf_reports']
    ]
    state['progress'] = 0.94
    
    return state


def cf_senior_analyst_node(state: AgentState) -> AgentState:
    """Senior Cash Flow Analyst - combines CF reports"""
    from call_bpce_llm import call_llm_api
    from prompts.prompts_FA import PROMPT_COMBINE_ANALYSIS_CF
    from datetime import datetime
    import json
    
    logging.info("="*50)
    logging.info("Senior CF Analyst - Generating CF Initial Draft")
    logging.info("="*50)
    
    # Update UI status
    from status_callback import update_status
    update_status("Senior CF Analyst combining reports...", "info")
    
    # FIX: Handle None config - use 'or {}' pattern to ensure config is always a dict
    # Use type-specific combined config, fallback to general combined_config
    config = state.get('combined_config_cf') or state.get('combined_config') or {}
    temperature = config.get('temperature', 0.0)
    max_tokens = config.get('max_tokens', 4000)
    llm_model = config.get('llm_model', COMBINED_DEFAULT_LLM_MODEL)
    prompt_template = config.get('custom_prompt', PROMPT_COMBINE_ANALYSIS_CF)
    
    # Debug: Check state contents
    logging.info(f"State keys: {list(state.keys())}")
    logging.info(f"CF reports count: {len(state.get('cf_reports', []))}")
    logging.info(f"CF validation results count: {len(state.get('cf_validation_results', []))}")
    logging.info(f"CF accepted reports count: {len(state.get('cf_accepted_reports', []))}")
    
    # Check if we have reports to combine
    if not state.get('cf_accepted_reports') or len(state.get('cf_accepted_reports', [])) == 0:
        error_msg = f"No CF accepted reports to combine. CF reports: {len(state.get('cf_reports', []))}, Accepted: {len(state.get('cf_accepted_reports', []))}"
        logging.error(error_msg)
        logging.error("This usually means:")
        logging.error("  1. CF analysts didn't generate reports")
        logging.error("  2. CF evaluators didn't accept any reports")
        logging.error("  3. State is not being passed correctly between nodes")
        state['errors'].append(error_msg)
        
        # Update UI status
        update_status(f"Senior CF Analyst failed: No reports to combine", "error")
        
        return state
    
    logging.info(f"Combining {len(state['cf_accepted_reports'])} CF reports")
    
    # Combine all accepted CF reports
    combined_reports = ""
    for i, report in enumerate(state['cf_accepted_reports'], 1):
        report_content = json.dumps(report['report'], indent=2)
        combined_reports += f"""
╔══════════════════════════════════════════════════════════════════════════╗
║  CASH FLOW REPORT {i}  ║
╚══════════════════════════════════════════════════════════════════════════╝

{report_content}

"""
    
        # Build the full prompt with reports AFTER the instructions
    full_prompt = f"""{prompt_template}

{'='*90}
[[[COMBINED CASH FLOW REPORTS]]]
{'='*90}

{combined_reports}

{'='*90}
[[[CRITICAL INSTRUCTIONS]]]
{'='*90}
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
        save_file_sequencially(session_folder, "cf_final_draft_raw_response", response)
        logging.info(f"Saved CF final draft raw response to {session_folder}/cf_final_draft_raw_response.md")
        
        # Extract JSON from response using improved helper function
        final_draft = extract_json_from_response(response)
        
        # CRITICAL: Save extracted JSON report AFTER extract_json_from_response
        save_file_sequencially(session_folder, "cf_final_draft_extracted_json", json.dumps(final_draft, indent=2, ensure_ascii=False))
        logging.info(f"Saved CF final draft extracted JSON to {session_folder}/cf_final_draft_extracted_json.json")
        
        state['cf_final_draft'] = {
            "model": llm_model,
            "generated_at": datetime.now().isoformat(),
            "report": final_draft,
        }
        
        # Save final draft to file
        session_folder = state.get('session_folder', './uploads')
        final_draft_path = os.path.join(session_folder, "cf_final_draft.json")
        with open(final_draft_path, 'w', encoding='utf-8') as f:
            json.dump(state['cf_final_draft'], f, indent=2, ensure_ascii=False)
        logging.info(f"Saved CF final draft to {final_draft_path}")
        
        state['messages'].append({"role": "system", "content": "Cash Flow Initial Draft generated"})
        state['progress'] = 1.0
        state['status_message'] = 'All analysis completed'
        
        logging.info("Senior CF Analyst - CF Initial Draft completed")
        logging.info("="*50)
        
        # Update UI status
        from status_callback import update_status
        update_status("Senior CF Analyst completed final report", "success")
        
    except Exception as e:
        logging.error(f"Senior CF Analyst failed: {str(e)}")
        state['errors'].append(str(e))
        
        # Update UI status
        from status_callback import update_status
        update_status(f"Senior CF Analyst failed: {str(e)}", "error")
    
    return state
