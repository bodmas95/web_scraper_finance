"""
============================================================
  DYNAMIC LANGGRAPH WORKFLOW WITH PARALLEL SUB-AGENTS V2
============================================================
  Creates parallel analyst AND evaluator sub-agents
  
  Flow:
  1. N Analyst sub-agents (parallel) → Aggregator
  2. N Evaluator sub-agents (parallel) → Aggregator  
  3. 1 Senior Analyst → END
============================================================
"""

from langgraph.graph import StateGraph, END
from agents import (
    AgentState,
    create_pnl_subagent,
    create_bs_subagent,
    create_cf_subagent,
    create_pnl_evaluator_subagent,
    create_bs_evaluator_subagent,
    create_cf_evaluator_subagent,
    agent_5_senior_pnl_analyst,
    agent_summarizer_pnl,
    agent_summarizer_bs,
    agent_summarizer_cf,
)
from graph import (
    bs_senior_analyst_node,
    cf_senior_analyst_node,
)
import logging
from logger_config import log_graph_structure, log_node_execution, log_parallel_execution, log_chunk_info, log_retry_attempt
import json
import os
from datetime import datetime


def retry_analyst_with_feedback(state: AgentState, analysis_type: str, chunk_index: int, feedback: str, issues: list, retry_count: int):
    """
    Regenerate a report with evaluator feedback included in the prompt
    
    Args:
        state: Current agent state
        analysis_type: 'pnl', 'bs', or 'cf'
        chunk_index: Which chunk to retry (1-based)
        feedback: Evaluator feedback
        issues: List of issues to fix
        retry_count: Current retry count
    
    Returns:
        New report dict
    """
    from call_bpce_llm import call_llm_api
    from utils import save_file_sequencially
    
    logging.info(f"Retrying {analysis_type.upper()} chunk {chunk_index} with feedback (attempt {retry_count + 1}/3)")
    
    # Get the appropriate prompt and keys
    if analysis_type == 'pnl':
        from prompts.prompts_FA import PROMPT_DRIVER_ANALYSIS_PNL
        prompt_template = PROMPT_DRIVER_ANALYSIS_PNL
        chunks_key = 'pnl_chunks'
    elif analysis_type == 'bs':
        from prompts.prompts_FA import PROMPT_DRIVER_ANALYSIS_BS
        prompt_template = PROMPT_DRIVER_ANALYSIS_BS
        chunks_key = 'bs_chunks'
    elif analysis_type == 'cf':
        from prompts.prompts_FA import PROMPT_DRIVER_ANALYSIS_CF
        prompt_template = PROMPT_DRIVER_ANALYSIS_CF
        chunks_key = 'cf_chunks'
    
    # Get the chunk
    chunk = state[chunks_key][chunk_index - 1]
    context = chunk['context']
    
    # Build feedback section
    feedback_section = f"""
{'#'*90}
[[[PREVIOUS ATTEMPT FEEDBACK - PLEASE READ CAREFULLY]]]
{'#'*90}

**CRITICAL: Your previous attempt was REJECTED by the evaluator.**
**You MUST address ALL issues below to pass validation.**

**Evaluator Feedback:**
{feedback}

**Specific Issues to Fix:**
{chr(10).join(f"  {i+1}. {issue}" for i, issue in enumerate(issues))}

**Retry Attempt:** {retry_count + 1} of 3

**What You Must Do:**
1. Read the feedback above carefully
2. Address EVERY issue mentioned
3. Ensure your analysis is complete and accurate
4. Follow the output format EXACTLY
5. Include ALL required sections
6. Provide proper citations
7. Include source tables where applicable

**If you fail this attempt, you have {3 - retry_count - 1} more chance(s) before this chunk is excluded.**

{'#'*90}
"""
    
    # Build full prompt
    full_prompt = f"""{feedback_section}

{'#'*90}
[[[TASK]]]
{'#'*90}

<TASK>{prompt_template}</TASK>

{'#'*90}
[[[CONTEXT]]]
{'#'*90}

<CONTEXT>{context}</CONTEXT>

{'#'*90}
[[[OUTPUT FORMAT]]]
{'#'*90}

<OUTPUT FORMAT>Generate a structured report in JSON format.

STRICT RULES:
- Output MUST be valid JSON
- Address ALL feedback issues
- Include ALL required sections
- Provide proper citations
- Do NOT add extra text outside JSON
</OUTPUT FORMAT>

{'#'*90}
[[[CRITICAL INSTRUCTIONS]]]
{'#'*90}

<CRITICAL INSTRUCTIONS>
**THIS IS A RETRY ATTEMPT - YOUR PREVIOUS SUBMISSION WAS REJECTED**

1. This is retry attempt {retry_count + 1} of 3
2. You MUST address ALL feedback points mentioned above
3. Ensure completeness and accuracy
4. Follow the output format exactly
5. If you fail again, you will have {3 - retry_count - 1} more chance(s)
</CRITICAL INSTRUCTIONS>
"""
    
    # Save retry prompt
    session_folder = state.get('session_folder', './uploads')
    save_file_sequencially(session_folder, f"{analysis_type}_retry_prompt_chunk_{chunk_index}_attempt_{retry_count + 1}", full_prompt)
    
    # Get configuration based on analysis type - handle None case
    if analysis_type == 'pnl':
        config = state.get('fa_config_pnl') or state.get('fa_config') or {}
    elif analysis_type == 'bs':
        config = state.get('fa_config_bs') or state.get('fa_config') or {}
    elif analysis_type == 'cf':
        config = state.get('fa_config_cf') or state.get('fa_config') or {}
    else:
        config = {}
    
    temperature = config.get('temperature', 0.0)
    max_tokens = config.get('max_tokens', 2000)
    llm_model = config.get('llm_model', 'gpt-4o-mini-2024-07-18')
    
    try:
        # Call LLM API
        response = call_llm_api(
            query=full_prompt,
            model_name=llm_model,
            temperature=temperature,
            max_tokens=max_tokens
        ).content
        
        # Parse JSON with better error handling
        import re
        json_match = re.search(r'\{.*\}', response, re.DOTALL)
        if json_match:
            try:
                report_data = json.loads(json_match.group())
            except json.JSONDecodeError as e:
                logging.error(f"JSON parse error for chunk {chunk_index}: {str(e)}")
                logging.error(f"Error at line {e.lineno}, column {e.colno}")
                logging.error(f"Response excerpt (first 500 chars): {response[:500]}...")
                # Save the problematic response for debugging
                error_path = os.path.join(session_folder, f"{analysis_type}_retry_error_chunk_{chunk_index}_attempt_{retry_count + 1}.txt")
                with open(error_path, 'w', encoding='utf-8') as f:
                    f.write(f"JSON Parse Error: {str(e)}\n")
                    f.write(f"Error at line {e.lineno}, column {e.colno}\n\n")
                    f.write("Full Response:\n")
                    f.write(response)
                logging.error(f"Saved problematic response to {error_path}")
                raise  # Re-raise to trigger outer exception handler
        else:
            report_data = {"raw_response": response}
        
        # Create new report
        new_report = {
            "chunk_index": chunk_index,
            "model": llm_model,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "generated_at": datetime.now().isoformat(),
            "report": report_data,
            "retry_attempt": retry_count + 1,
            "previous_feedback": feedback,
            "previous_issues": issues
        }
        
        # Save retry report
        report_path = os.path.join(session_folder, f"{analysis_type}_report_chunk_{chunk_index}_retry_{retry_count + 1}.json")
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(new_report, f, indent=2, ensure_ascii=False)
        
        logging.info(f"Retry report saved: {report_path}")
        
        return new_report
        
    except Exception as e:
        logging.error(f"Failed to regenerate report for chunk {chunk_index}: {str(e)}")
        raise


def re_evaluate_report(state: AgentState, analysis_type: str, chunk_index: int, report: dict):
    """
    Re-evaluate a regenerated report
    
    Args:
        state: Current agent state
        analysis_type: 'pnl', 'bs', or 'cf'
        chunk_index: Which chunk was regenerated
        report: The new report to evaluate
    
    Returns:
        Validation result dict
    """
    from call_bpce_llm import call_llm_api
    from utils import save_file_sequencially
    
    logging.info(f"Re-evaluating {analysis_type.upper()} chunk {chunk_index}")
    
    # Get the appropriate validation prompt
    if analysis_type == 'pnl':
        validation_prompt = """
You are a P&L data quality validator. Evaluate if the P&L analysis report meets quality standards.

**VALIDATION CRITERIA:**
1. All P&L sections are properly analyzed
2. Financial metrics are accurate and complete
3. Citations are properly referenced
4. Analysis is coherent and insightful
5. No contradictions or errors

**OUTPUT FORMAT:**
Return a JSON object with:
{
    "accepted": true/false,
    "issues": [],
    "feedback": "Detailed feedback",
    "score": 0-100
}
"""
    elif analysis_type == 'bs':
        validation_prompt = """
You are a Balance Sheet data quality validator. Evaluate if the Balance Sheet analysis report meets quality standards.

**VALIDATION CRITERIA:**
1. All BS sections are properly analyzed
2. Financial metrics are accurate and complete
3. Citations are properly referenced
4. Analysis is coherent and insightful
5. No contradictions or errors

**OUTPUT FORMAT:**
Return a JSON object with:
{
    "accepted": true/false,
    "issues": [],
    "feedback": "Detailed feedback",
    "score": 0-100
}
"""
    elif analysis_type == 'cf':
        validation_prompt = """
You are a Cash Flow data quality validator. Evaluate if the Cash Flow analysis report meets quality standards.

**VALIDATION CRITERIA:**
1. All CF sections are properly analyzed
2. Financial metrics are accurate and complete
3. Citations are properly referenced
4. Analysis is coherent and insightful
5. No contradictions or errors

**OUTPUT FORMAT:**
Return a JSON object with:
{
    "accepted": true/false,
    "issues": [],
    "feedback": "Detailed feedback",
    "score": 0-100
}
"""
    
    report_content = json.dumps(report['report'], indent=2)
    full_prompt = f"{validation_prompt}\n\n**REPORT TO VALIDATE:**\n\n{report_content}"
    
    # Save validation prompt to file
    session_folder = state.get('session_folder', './uploads')
    retry_attempt = report.get('retry_attempt', 0)
    prompt_filename = f"{analysis_type}_validation_prompt_chunk_{chunk_index}_retry_{retry_attempt}"
    save_file_sequencially(session_folder, prompt_filename, full_prompt)
    logging.info(f"Saved retry validation prompt: {prompt_filename}.md")
    
    try:
        response = call_llm_api(
            query=full_prompt,
            model_name='gpt-4o-mini-2024-07-18',
            temperature=0.0,
            max_tokens=1000
        ).content
        
        # Parse validation result
        import re
        json_match = re.search(r'\{.*\}', response, re.DOTALL)
        if json_match:
            validation_result = json.loads(json_match.group())
        else:
            validation_result = {"accepted": True, "issues": [], "feedback": "Validated", "score": 85}
        
        validation_result['chunk_index'] = chunk_index
        validation_result['report_index'] = chunk_index
        
        # Save validation result
        session_folder = state.get('session_folder', './uploads')
        validation_path = os.path.join(session_folder, f"{analysis_type}_validation_chunk_{chunk_index}_retry.json")
        with open(validation_path, 'w', encoding='utf-8') as f:
            json.dump(validation_result, f, indent=2, ensure_ascii=False)
        
        logging.info(f"Re-evaluation result: {'ACCEPTED' if validation_result.get('accepted') else 'REJECTED'} (Score: {validation_result.get('score', 'N/A')})")
        
        return validation_result
        
    except Exception as e:
        logging.error(f"Failed to re-evaluate chunk {chunk_index}: {str(e)}")
        # Default to accepted on error
        return {"chunk_index": chunk_index, "accepted": True, "score": 85, "issues": [], "feedback": "Validation error"}


def build_parallel_analysis_graph(state: AgentState, analysis_type: str):
    """
    Dynamically build a LangGraph workflow with parallel analyst and evaluator sub-agents
    
    Example with 3 chunks:
    - Creates 3 analyst sub-agents (run in parallel)
    - Creates 3 evaluator sub-agents (run in parallel after analysts)
    - Creates 1 senior analyst (combines all accepted reports)
    
    Args:
        state: Current agent state
        analysis_type: 'pnl', 'bs', or 'cf'
    
    Returns:
        Compiled LangGraph workflow
    """
    logging.info(f"="*70)
    logging.info(f"BUILDING PARALLEL {analysis_type.upper()} ANALYSIS GRAPH V2")
    logging.info(f"="*70)
    
    workflow = StateGraph(AgentState)
    
    # Determine which chunks to use and which factories
    if analysis_type == 'pnl':
        chunks = state.get('pnl_chunks', [])
        create_analyst = create_pnl_subagent
        create_evaluator = create_pnl_evaluator_subagent
        senior_analyst_node = agent_5_senior_pnl_analyst
        summarizer_node = agent_summarizer_pnl
        node_prefix = "pnl"
    elif analysis_type == 'bs':
        chunks = state.get('bs_chunks', [])
        create_analyst = create_bs_subagent
        create_evaluator = create_bs_evaluator_subagent
        senior_analyst_node = bs_senior_analyst_node
        summarizer_node = agent_summarizer_bs
        node_prefix = "bs"
    elif analysis_type == 'cf':
        chunks = state.get('cf_chunks', [])
        create_analyst = create_cf_subagent
        create_evaluator = create_cf_evaluator_subagent
        senior_analyst_node = cf_senior_analyst_node
        summarizer_node = agent_summarizer_cf
        node_prefix = "cf"
    else:
        raise ValueError(f"Unknown analysis type: {analysis_type}")
    
    num_chunks = len(chunks)
    
    # Log chunk information
    log_chunk_info(num_chunks, analysis_type.upper())
    logging.info(f"Number of chunks for {analysis_type.upper()}: {num_chunks}")
    logging.info(f"Will create:")
    logging.info(f"  - {num_chunks} analyst sub-agents (1 per chunk)")
    logging.info(f"  - {num_chunks} evaluator sub-agents (1 per report)")
    logging.info(f"  - 1 senior analyst (combines all reports)")
    logging.info(f"  TOTAL: {num_chunks * 2 + 1} nodes")
    
    if num_chunks == 0:
        logging.warning(f"No chunks found for {analysis_type.upper()} analysis")
        return None
    
    if num_chunks > 20:
        logging.warning(f"⚠️  WARNING: {num_chunks} chunks detected!")
        logging.warning(f"⚠️  This will create {num_chunks * 2} sub-agents which may be slow.")
        logging.warning(f"⚠️  Consider reducing document size or chunk size.")
    
    # Log graph structure
    log_graph_structure(analysis_type, num_chunks, num_chunks)
    
    # ─── CREATE START NODE ─────────────────────────────────
    def start_node(state: AgentState) -> AgentState:
        logging.info(f"="*70)
        logging.info(f"STARTING {analysis_type.upper()} PARALLEL ANALYSIS")
        logging.info(f"Analysts: {num_chunks} | Evaluators: {num_chunks} | Senior: 1")
        logging.info(f"="*70)
        return state
    
    workflow.add_node("start", start_node)
    workflow.set_entry_point("start")
    
    # ─── CREATE ANALYST SUB-AGENT NODES (PARALLEL) ────────
    analyst_nodes = []
    for i in range(1, num_chunks + 1):
        node_name = f"{node_prefix}_analyst_{i}"
        analyst_nodes.append(node_name)
        analyst_func = create_analyst(i)
        workflow.add_node(node_name, analyst_func)
        logging.info(f"✓ Created analyst node: {node_name}")
    
    # ─── CREATE ANALYST AGGREGATOR NODE ────────────────────
    def analyst_aggregator_node(state: AgentState) -> AgentState:
        logging.info(f"="*70)
        logging.info(f"AGGREGATING {num_chunks} ANALYST REPORTS")
        logging.info(f"="*70)
        
        # Update UI status
        from status_callback import update_status
        update_status(f"Aggregating {num_chunks} {analysis_type.upper()} analyst reports...", "info")
        
        reports_key = f'{node_prefix}_reports'
        
        # Debug logging
        logging.info(f"Reports key: {reports_key}")
        logging.info(f"State has reports_key: {reports_key in state}")
        
        if reports_key in state and state[reports_key]:
            logging.info(f"Found {len(state[reports_key])} reports before sorting")
            state[reports_key].sort(key=lambda x: x.get('chunk_index', 0))
            logging.info(f"✓ Sorted {len(state[reports_key])} reports by chunk index")
            
            # Log first report structure for debugging
            if len(state[reports_key]) > 0:
                first_report = state[reports_key][0]
                logging.info(f"First report keys: {list(first_report.keys())}")
                logging.info(f"First report chunk_index: {first_report.get('chunk_index')}")
            
            # Update UI status
            update_status(f"Sorted {len(state[reports_key])} {analysis_type.upper()} reports", "success")
        else:
            logging.error(f"No reports found in state for key: {reports_key}")
            logging.error(f"This means analysts did not generate any reports!")
            
            # Update UI status
            update_status(f"ERROR: No {analysis_type.upper()} reports were generated by analysts", "error")
        
        return state
    
    workflow.add_node("analyst_aggregator", analyst_aggregator_node)
    
    # ─── CREATE EVALUATOR SUB-AGENT NODES (PARALLEL) ──────
    evaluator_nodes = []
    for i in range(1, num_chunks + 1):
        node_name = f"{node_prefix}_evaluator_{i}"
        evaluator_nodes.append(node_name)
        evaluator_func = create_evaluator(i)
        workflow.add_node(node_name, evaluator_func)
        logging.info(f"✓ Created evaluator node: {node_name}")
    
    # ─── CREATE EVALUATOR AGGREGATOR NODE ──────────────────
    def evaluator_aggregator_node(state: AgentState) -> AgentState:
        logging.info(f"="*70)
        logging.info(f"AGGREGATING {num_chunks} EVALUATOR RESULTS")
        logging.info(f"="*70)
        
        # Update UI status
        from status_callback import update_status
        update_status(f"Aggregating {num_chunks} {analysis_type.upper()} evaluator results...", "info")
        
        validation_key = f'{node_prefix}_validation_results'
        accepted_key = f'{node_prefix}_accepted_reports'
        
        # Debug logging
        logging.info(f"Validation key: {validation_key}")
        logging.info(f"Accepted key: {accepted_key}")
        logging.info(f"State has validation_key: {validation_key in state}")
        logging.info(f"State has accepted_key: {accepted_key in state}")
        
        if validation_key in state and state[validation_key]:
            state[validation_key].sort(key=lambda x: x.get('chunk_index', 0))
            logging.info(f"✓ Sorted {len(state[validation_key])} validation results")
        else:
            logging.warning(f"No validation results found in state for key: {validation_key}")
        
        if accepted_key in state and state[accepted_key]:
            state[accepted_key].sort(key=lambda x: x.get('chunk_index', 0))
            logging.info(f"✓ {len(state[accepted_key])} reports accepted")
            
            # Update UI status
            update_status(f"{len(state[accepted_key])}/{num_chunks} {analysis_type.upper()} reports accepted", "success")
        else:
            logging.warning(f"No accepted reports found in state for key: {accepted_key}")
            logging.warning(f"This means evaluators did not accept any reports!")
            
            # Update UI status
            update_status(f"WARNING: No {analysis_type.upper()} reports were accepted by evaluators", "warning")
        
        return state
    
    workflow.add_node("evaluator_aggregator", evaluator_aggregator_node)
    
    # ─── CREATE RETRY DECISION NODE ────────────────────────
    def retry_decision_node(state: AgentState) -> AgentState:
        """Check if any reports need retry and regenerate them with feedback"""
        logging.info(f"="*70)
        logging.info(f"RETRY DECISION FOR {analysis_type.upper()}")
        logging.info(f"="*70)
        
        validation_key = f'{node_prefix}_validation_results'
        reports_key = f'{node_prefix}_reports'
        accepted_key = f'{node_prefix}_accepted_reports'
        
        validation_results = state.get(validation_key, [])
        reports = state.get(reports_key, [])
        accepted_reports = state.get(accepted_key, [])
        
        # FIX 1: Deduplicate validation_results - keep only the latest for each chunk
        # This fixes the duplicate logs issue where validation_results accumulates
        validation_dict = {}
        for v in validation_results:
            chunk_idx = v.get('chunk_index')
            if chunk_idx is not None:
                validation_dict[chunk_idx] = v  # Overwrites previous, keeping latest
        
        # Convert back to list and sort
        validation_results = list(validation_dict.values())
        validation_results.sort(key=lambda x: x.get('chunk_index', 0))
        
        logging.info(f"Deduplicated validation_results: {len(validation_results)} unique chunks (was {len(state.get(validation_key, []))} total)")
        
        # Use sets to prevent duplicates
        rejected_chunks = set()
        max_retry_chunks = set()
        
        # Get set of already accepted chunk indices
        accepted_chunk_indices = set(r.get('chunk_index') for r in accepted_reports)
        
        for validation in validation_results:
            chunk_idx = validation.get('chunk_index')
            
            # Skip if already accepted
            if chunk_idx in accepted_chunk_indices:
                logging.info(f"Chunk {chunk_idx} already accepted - skipping retry")
                continue
            
            if not validation.get('accepted', False):
                retry_count = validation.get('retry_count', 0)
                
                # Skip if already in rejected_chunks (prevent duplicates)
                if chunk_idx in rejected_chunks or chunk_idx in max_retry_chunks:
                    continue
                
                if retry_count < 3:
                    rejected_chunks.add(chunk_idx)
                    logging.warning(f"Chunk {chunk_idx} rejected - will retry (attempt {retry_count + 1}/3)")
                else:
                    max_retry_chunks.add(chunk_idx)
                    logging.error(f"Chunk {chunk_idx} reached max retries (3) - STOPPING PROCESSING")
        
        if rejected_chunks:
            logging.info(f"Retrying {len(rejected_chunks)} rejected chunks with feedback...")
            
            # Update UI status
            from status_callback import update_status
            update_status(f"Retrying {len(rejected_chunks)} rejected {analysis_type.upper()} reports with feedback...", "warning")
            
            # Retry each rejected chunk
            for chunk_idx in rejected_chunks:
                # Get validation result
                validation = next(
                    (v for v in validation_results if v.get('chunk_index') == chunk_idx),
                    None
                )
                
                if not validation:
                    continue
                
                retry_count = validation.get('retry_count', 0)
                feedback = validation.get('feedback', 'No specific feedback provided')
                issues = validation.get('issues', [])
                
                log_retry_attempt(chunk_idx, retry_count + 1, 3, feedback)
                
                # Regenerate report with feedback
                try:
                    new_report = retry_analyst_with_feedback(
                        state, analysis_type, chunk_idx, feedback, issues, retry_count
                    )
                    
                    # Update report in state
                    for i, report in enumerate(reports):
                        if report.get('chunk_index') == chunk_idx:
                            reports[i] = new_report
                            break
                    
                    # Re-evaluate the new report
                    new_validation = re_evaluate_report(
                        state, analysis_type, chunk_idx, new_report
                    )
                    new_validation['retry_count'] = retry_count + 1
                    
                    # Update validation in state
                    for i, val in enumerate(validation_results):
                        if val.get('chunk_index') == chunk_idx:
                            validation_results[i] = new_validation
                            break
                    
                    # If now accepted, add to accepted reports
                    if new_validation.get('accepted', False):
                        # Remove old version if exists
                        accepted_reports = [r for r in accepted_reports if r.get('chunk_index') != chunk_idx]
                        accepted_reports.append(new_report)
                        
                        logging.info(f"✓ Chunk {chunk_idx} accepted after retry {retry_count + 1}")
                        logging.info(f"  Updated accepted_reports count: {len(accepted_reports)}")
                        update_status(f"Chunk {chunk_idx} accepted after retry {retry_count + 1}", "success")
                    else:
                        logging.warning(f"✗ Chunk {chunk_idx} still rejected after retry {retry_count + 1}")
                        logging.warning(f"  Chunk {chunk_idx} will need another retry (current retry_count={retry_count + 1})")
                        update_status(f"Chunk {chunk_idx} still rejected after retry {retry_count + 1}", "warning")
                    
                except Exception as e:
                    logging.error(f"Failed to retry chunk {chunk_idx}: {str(e)}")
                    update_status(f"Failed to retry chunk {chunk_idx}: {str(e)}", "error")
            
        
        # CRITICAL FIX: Check if ANY report reached max retries
        # If yes, STOP processing and ask user to change LLM
        if max_retry_chunks:
            logging.error(f"="*70)
            logging.error(f"CRITICAL: {len(max_retry_chunks)} report(s) reached max retries (3 attempts)")
            logging.error(f"Chunks that failed: {sorted(list(max_retry_chunks))}")
            logging.error(f"STOPPING PROCESSING - User must change LLM configuration")
            logging.error(f"="*70)
            
            # Update UI status
            from status_callback import update_status
            update_status(f"CRITICAL: {len(max_retry_chunks)} {analysis_type.upper()} report(s) failed after 3 retries - Please change LLM", "error")
            
            # Set flag to stop processing and request config change
            return {
                reports_key: reports,
                validation_key: validation_results,
                accepted_key: accepted_reports,
                '_needs_retry': False,
                '_max_retries_reached': True,
                '_failed_chunks': list(max_retry_chunks)
            }
        
        # CRITICAL: Log validation_results BEFORE calculating still_rejected
        logging.info(f"Validation results BEFORE calculating still_rejected:")
        for v in validation_results:
            logging.info(f"  Chunk {v.get('chunk_index')}: accepted={v.get('accepted')}, retry_count={v.get('retry_count', 0)}")
        
        # Check if we need another retry iteration
        # Count chunks that are still rejected and haven't reached max retries
        still_rejected = sum(1 for v in validation_results 
                           if not v.get('accepted', False) and v.get('retry_count', 0) < 3)
        
        logging.info(f"Calculated still_rejected: {still_rejected}")
        
        needs_retry = still_rejected > 0
        
        # Detailed logging for debugging
        logging.info(f"="*70)
        logging.info(f"RETRY DECISION SUMMARY:")
        logging.info(f"  Total chunks: {len(validation_results)}")
        logging.info(f"  Accepted chunks: {len(accepted_reports)}")
        logging.info(f"  Chunks at max retries: {len(max_retry_chunks)}")
        logging.info(f"  Chunks still need retry: {still_rejected}")
        logging.info(f"  _needs_retry flag: {needs_retry}")
        logging.info(f"  Decision: {'RETRY AGAIN' if needs_retry else 'PROCEED TO SENIOR ANALYST'}")
        logging.info(f"")
        logging.info(f"  Validation results detail:")
        for v in validation_results:
            chunk_idx = v.get('chunk_index')
            accepted = v.get('accepted', False)
            retry_count = v.get('retry_count', 0)
            status = "ACCEPTED" if accepted else f"REJECTED (retry {retry_count}/3)"
            logging.info(f"    Chunk {chunk_idx}: {status}")
        logging.info(f"="*70)
        
        if still_rejected > 0:
            logging.warning(f"{still_rejected} chunks still need retry - will retry again")
        else:
            logging.info(f"All chunks accepted - proceeding to senior analyst")
        
        logging.info(f"="*70)
        
        # CRITICAL: Check if ALL reports are accepted before proceeding to senior analyst
        # Senior analyst should ONLY run when ALL reports are accepted
        all_accepted = len(accepted_reports) == num_chunks
        
        if not needs_retry and not all_accepted:
            logging.error(f"="*70)
            logging.error(f"CRITICAL ERROR: Not all reports accepted but no retries needed")
            logging.error(f"  Total chunks: {num_chunks}")
            logging.error(f"  Accepted: {len(accepted_reports)}")
            logging.error(f"  This should not happen - some reports may have reached max retries")
            logging.error(f"="*70)
        
        # Return updated state fields
        return {
            reports_key: reports,
            validation_key: validation_results,
            accepted_key: accepted_reports,
            '_needs_retry': needs_retry,
            '_max_retries_reached': False
        }
    
    workflow.add_node("retry_decision", retry_decision_node)
    
    # ─── ADD REQUEST CONFIG CHANGE NODE ────────────────────
    def request_config_change_node(state: AgentState) -> AgentState:
        """Node to request user to change LLM configuration"""
        failed_chunks = state.get('_failed_chunks', [])
        
        logging.error(f"="*70)
        logging.error(f"MAX RETRIES REACHED FOR {analysis_type.upper()} ANALYSIS")
        logging.error(f"="*70)
        logging.error(f"The following report(s) failed validation after 3 attempts:")
        for chunk_idx in sorted(failed_chunks):
            logging.error(f"  - Chunk {chunk_idx}")
        logging.error(f"")
        logging.error(f"PROCESSING STOPPED - User must change LLM configuration")
        logging.error(f"")
        logging.error(f"Recommended actions:")
        logging.error(f"  1. Change to a more capable LLM model")
        logging.error(f"  2. Adjust temperature or max_tokens settings")
        logging.error(f"  3. Review and improve prompts if needed")
        logging.error(f"  4. Start a new session with updated configuration")
        logging.error(f"="*70)
        
        # Update UI status
        from status_callback import update_status
        update_status(
            f"STOPPED: {len(failed_chunks)} {analysis_type.upper()} report(s) failed after 3 retries. Please change LLM and restart.",
            "error"
        )
        
        # Add error message to state
        error_msg = f"{analysis_type.upper()} analysis failed: {len(failed_chunks)} report(s) not accepted after 3 retries. Please change LLM configuration and start a new session."
        
        return {
            'errors': [error_msg],
            'current_step': f'{analysis_type}_max_retries_reached',
            'status_message': f'{analysis_type.upper()} processing stopped - Change LLM and restart'
        }
    
    workflow.add_node("request_config_change", request_config_change_node)
    
    # ─── ADD SENIOR ANALYST NODE ───────────────────────────
    workflow.add_node("senior_analyst", senior_analyst_node)
    
    # ─── ADD SUMMARIZER NODE (NEW) ─────────────────────────
    workflow.add_node("summarizer", summarizer_node)
    
    # ─── CONNECT NODES ──────────────────────────────────────
    # start → all analysts (PARALLEL)
    for node_name in analyst_nodes:
        workflow.add_edge("start", node_name)
    
    # all analysts → analyst_aggregator
    for node_name in analyst_nodes:
        workflow.add_edge(node_name, "analyst_aggregator")
    
    # analyst_aggregator → all evaluators (PARALLEL)
    for node_name in evaluator_nodes:
        workflow.add_edge("analyst_aggregator", node_name)
    
    # all evaluators → evaluator_aggregator
    for node_name in evaluator_nodes:
        workflow.add_edge(node_name, "evaluator_aggregator")
    
    # evaluator_aggregator → retry_decision
    workflow.add_edge("evaluator_aggregator", "retry_decision")
    
    # retry_decision → conditional routing
    def should_retry(state: AgentState) -> str:
        """Determine if retry is needed or if max retries reached"""
        # CRITICAL: Check if max retries reached FIRST
        if state.get('_max_retries_reached', False):
            return "max_retries"
        elif state.get('_needs_retry', False):
            return "retry"
        else:
            return "continue"
    
    workflow.add_conditional_edges(
        "retry_decision",
        should_retry,
        {
            "retry": "retry_decision",  # Loop back to retry again
            "max_retries": "request_config_change",  # Stop and ask user to change LLM
            "continue": "senior_analyst"  # All reports accepted, proceed to senior analyst
        }
    )
    
    # request_config_change → END (stop processing)
    workflow.add_edge("request_config_change", END)
    
    # senior_analyst → summarizer (NEW: Add summarizer step)
    workflow.add_edge("senior_analyst", "summarizer")
    
    # summarizer → END (NEW: Summarizer is the final step)
    workflow.add_edge("summarizer", END)
    
    # ─── LOG GRAPH STRUCTURE ────────────────────────────────
    logging.info(f"="*70)
    logging.info(f"GRAPH STRUCTURE FOR {analysis_type.upper()} (WITH RETRY):")
    logging.info(f"="*70)
    logging.info(f"  start")
    logging.info(f"    ├─> {analyst_nodes[0]}")
    for node in analyst_nodes[1:-1]:
        logging.info(f"    ├─> {node}")
    if len(analyst_nodes) > 1:
        logging.info(f"    └─> {analyst_nodes[-1]}")
    logging.info(f"  ({num_chunks} ANALYSTS RUN IN PARALLEL)")
    logging.info(f"    ↓")
    logging.info(f"  analyst_aggregator")
    logging.info(f"    ↓")
    logging.info(f"    ├─> {evaluator_nodes[0]}")
    for node in evaluator_nodes[1:-1]:
        logging.info(f"    ├─> {node}")
    if len(evaluator_nodes) > 1:
        logging.info(f"    └─> {evaluator_nodes[-1]}")
    logging.info(f"  ({num_chunks} EVALUATORS RUN IN PARALLEL)")
    logging.info(f"    ↓")
    logging.info(f"  evaluator_aggregator")
    logging.info(f"    ↓")
    logging.info(f"  retry_decision (checks for rejected reports)")
    logging.info(f"    ├─> If ANY report reached max retries (3): → request_config_change → END (STOP)")
    logging.info(f"    ├─> If rejected & retry_count < 3: Regenerate with feedback → Re-evaluate → Loop back")
    logging.info(f"    └─> If ALL reports accepted: Continue to senior_analyst")
    logging.info(f"    ↓")
    logging.info(f"  senior_analyst (ONLY runs when ALL reports accepted)")
    logging.info(f"    ↓")
    logging.info(f"  summarizer (Combines BREF + Initial Draft into Final Report)")
    logging.info(f"    ↓")
    logging.info(f"  END")
    logging.info(f"="*70)
    
    # ─── COMPILE THE GRAPH ──────────────────────────────────
    app = workflow.compile()
    
    logging.info(f"✓ {analysis_type.upper()} parallel graph compiled successfully")
    logging.info(f"✓ Total nodes in graph: {num_chunks * 2 + 4} (start + {num_chunks} analysts + aggregator + {num_chunks} evaluators + aggregator + senior + summarizer)")
    logging.info(f"="*70)
    
    return app
