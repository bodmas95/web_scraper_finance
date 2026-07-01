"""
============================================================
  FINANCIAL ANALYSIS EVALUATION MODULE
============================================================
  Functions for extracting claims, adding context, and 
  preparing evaluation data for financial analysis reports
============================================================
"""

import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional
import logging

# Add path for API imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "bpce_api_setup"))
try:
    from call_bpce_llm import call_llm_api
except ImportError:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "bpce_api_setup"))
    from call_bpce_llm import call_llm_api

from prompts.prompts_FA import PROMPT_DRIVER_ANALYSIS_PNL, PROMPT_DRIVER_ANALYSIS_BS, PROMPT_DRIVER_ANALYSIS_CF


# ─── TASK 3.1 & 3.2: CLAIM EXTRACTOR ──────────────────────────────────

def claim_extractor(report_data: Dict, report_type: str = "pnl") -> Dict:
    """
    Extract claims/facts/insights from the 'Analysis' field of each section in the report.
    
    Args:
        report_data: Complete JSON report (pnl_final_draft.json, bs_final_draft.json, or cf_final_draft.json)
        report_type: Type of report - "pnl", "bs", or "cf"
    
    Returns:
        Dict: Report with extracted claims appended to each section
    
    Task 3.1: Creates a function that extracts all facts/claims/insights from the 'Analysis' key
    Task 3.2: Uses call_llm_api to extract claims in a single API call for all sections
    Task 3.3: Appends extracted claims to the original report as 'claims' key
    """
    logging.info(f"Starting claim extraction for {report_type.upper()} report")
    
    # Get the report content
    report_content = report_data.get('report', {})
    
    # Build the extraction prompt for ALL sections at once
    extraction_prompt = f"""You are an expert financial analyst. Your task is to extract ALL claims, facts, insights, and drivers mentioned in the 'Analysis' sections of the following financial report.

**INSTRUCTIONS:**
1. Read each section's 'Analysis' field carefully
2. Extract EVERY claim, fact, insight, driver, or observation mentioned
3. Return a structured JSON with section names as keys and lists of extracted claims as values
4. Each claim should be a concise statement (1-2 sentences)
5. Preserve the exact meaning and context of each claim
6. Do NOT add any claims that are not explicitly stated in the Analysis
7. Extract 3-7 claims per section (focus on the most important ones)

**OUTPUT FORMAT:**
Return ONLY valid JSON in this exact format (no markdown, no code blocks, no extra text):
{{
  "Section Name 1": ["claim 1", "claim 2", "claim 3"],
  "Section Name 2": ["claim 1", "claim 2"],
  "Section Name 3": ["claim 1", "claim 2", "claim 3"]
}}

**IMPORTANT:**
- Use exact section names from the report
- Ensure valid JSON (proper quotes, commas, brackets)
- No trailing commas
- Return ONLY the JSON object, nothing else

**FINANCIAL REPORT TO ANALYZE:**

"""
    
    # Add all sections to the prompt
    sections_to_analyze = []
    for section_name, section_data in report_content.items():
        if isinstance(section_data, list) and len(section_data) > 0:
            analysis_text = section_data[0].get('Analysis', '')
            if analysis_text and analysis_text != 'Not Available':
                sections_to_analyze.append(f"### {section_name}\n{analysis_text}\n")
        elif isinstance(section_data, str) and section_data != 'Not Available':
            # Handle Executive Summary or other string sections
            sections_to_analyze.append(f"### {section_name}\n{section_data}\n")
    
    extraction_prompt += "\n\n".join(sections_to_analyze)
    
    extraction_prompt += """

**CRITICAL REQUIREMENTS:**
1. Return ONLY the JSON object
2. No markdown code blocks (no ```json)
3. No explanatory text before or after
4. Ensure valid JSON syntax
5. Use exact section names as shown above
6. Extract all the key claims in every section

**OUTPUT (JSON only):**
"""
    
    try:
        # Single LLM API call to extract all claims
        logging.info("Calling LLM API to extract claims from all sections...")
        response = call_llm_api(
            query=extraction_prompt,
            model_name="gpt-5-mini-2025-08-07",
            temperature=0.0,
            max_tokens=6000  # Increased for larger reports
        ).content
        
        # Parse the JSON response with better error handling
        import re
        
        # Try multiple JSON extraction patterns
        extracted_claims = {}
        
        # Pattern 1: Look for JSON object
        json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', response, re.DOTALL)
        if json_match:
            try:
                extracted_claims = json.loads(json_match.group())
                logging.info(f"Successfully extracted claims for {len(extracted_claims)} sections")
            except json.JSONDecodeError as e:
                logging.error(f"JSON decode error: {e}")
                logging.error(f"Problematic JSON: {json_match.group()[:500]}...")
                
                # Try to fix common JSON issues
                json_str = json_match.group()
                # Remove any trailing commas before closing braces/brackets
                json_str = re.sub(r',\s*}', '}', json_str)
                json_str = re.sub(r',\s*]', ']', json_str)
                
                try:
                    extracted_claims = json.loads(json_str)
                    logging.info(f"Successfully extracted claims after JSON cleanup for {len(extracted_claims)} sections")
                except json.JSONDecodeError as e2:
                    logging.error(f"Still failed after cleanup: {e2}")
                    logging.error(f"Full LLM response: {response}")
                    extracted_claims = {}
        else:
            logging.warning("Failed to extract JSON from LLM response")
            logging.warning(f"LLM response: {response[:1000]}...")
            extracted_claims = {}
        
        # Fallback: If batch extraction failed, try section by section
        if not extracted_claims:
            logging.info("Attempting fallback: extracting claims section by section...")
            
            for section_name, section_data in report_content.items():
                if isinstance(section_data, list) and len(section_data) > 0:
                    analysis_text = section_data[0].get('Analysis', '')
                elif isinstance(section_data, str) and section_data != 'Not Available':
                    analysis_text = section_data
                else:
                    continue
                
                if analysis_text and analysis_text != 'Not Available':
                    # Extract claims for this section only
                    section_prompt = f"""Extract 3-7 key claims from this financial analysis section.

**Section: {section_name}**

**Analysis:**
{analysis_text}

**Return ONLY a JSON array of claims (no other text, no markdown):**
["claim 1", "claim 2", "claim 3"]
"""
                    try:
                        section_response = call_llm_api(
                            query=section_prompt,
                            model_name="gpt-5-mini-2025-08-07",
                            temperature=0.0,
                            max_tokens=1000
                        ).content
                        
                        # Extract JSON array
                        array_match = re.search(r'\[.*?\]', section_response, re.DOTALL)
                        if array_match:
                            section_claims = json.loads(array_match.group())
                            extracted_claims[section_name] = section_claims
                            logging.info(f"Extracted {len(section_claims)} claims for {section_name}")
                        else:
                            logging.warning(f"No JSON array found for {section_name}")
                            extracted_claims[section_name] = []
                    except Exception as e:
                        logging.error(f"Failed to extract claims for {section_name}: {e}")
                        extracted_claims[section_name] = []
        
        # Append claims to each section in the original report
        for section_name, claims_list in extracted_claims.items():
            if section_name in report_content:
                if isinstance(report_content[section_name], list):
                    # For list-based sections, add claims to the first item
                    if len(report_content[section_name]) > 0:
                        report_content[section_name][0]['claims'] = claims_list
                elif isinstance(report_content[section_name], str):
                    # For string sections (like Executive Summary), convert to dict
                    original_content = report_content[section_name]
                    report_content[section_name] = {
                        'content': original_content,
                        'claims': claims_list
                    }
        
        # Update the report data with claims
        report_data['report'] = report_content
        
        logging.info("Claims successfully appended to report sections")
        return report_data
        
    except Exception as e:
        logging.error(f"Error in claim extraction: {str(e)}")
        import traceback
        logging.error(f"Full traceback: {traceback.format_exc()}")
        # Return original report if extraction fails
        return report_data


# ─── TASK 3.4: CONTEXT ADDER ──────────────────────────────────────────

def add_context_to_report(report_data: Dict, report_type: str, client_folder: str) -> Dict:
    """
    Add original document context to each section based on Citations.
    
    Uses the SAME documents that were used by P&L/BS/CF analysis subagents.
    Loads from all_uploaded_documents_parsed.csv in the session folder.
    
    Args:
        report_data: Report with claims already extracted
        report_type: Type of report - "pnl", "bs", or "cf"
        client_folder: Path to client session folder containing uploaded documents
    
    Returns:
        Dict: Report with 'Context' field added to each section
    
    Task 3.4: Reads citations, extracts page content from original documents,
              and appends as 'Context' to each section including all page separators,
              metadata, and tags as used in P&L and BS Analysis agents
    """
    logging.info(f"Starting context addition for {report_type.upper()} report")
    
    # Get the report content
    report_content = report_data.get('report', {})
    
    # Load all uploaded documents from CSV (same as used by subagents)
    session_path = Path(client_folder)
    all_docs_csv = session_path / "all_uploaded_documents_parsed.csv"
    
    if not all_docs_csv.exists():
        logging.error(f"all_uploaded_documents_parsed.csv not found in {client_folder}")
        logging.error("Cannot add context without the uploaded documents data")
        return report_data
    
    # Load documents from CSV
    import pandas as pd
    try:
        df = pd.read_csv(all_docs_csv)
        all_documents = df.to_dict('records')
        logging.info(f"Loaded {len(all_documents)} pages from all_uploaded_documents_parsed.csv")
    except Exception as e:
        logging.error(f"Error loading documents from CSV: {e}")
        return report_data
    
    # Load BREF summary from the ORIGINAL uploaded financial summary file (Step 2)
    # This is the file uploaded with key="f_summary_uploader" in the UI
    # It's parsed and saved in parsedDocs/{filename}.json
    bref_summary_text = ""
    bref_summary_pages = []
    
    # Find the uploaded financial summary PDF in the session folder
    # It typically has "Summary" or "summary_data" in the filename
    financial_summary_pdf = None
    for file in session_path.glob("*.pdf"):
        filename_lower = file.name.lower()
        if any(keyword in filename_lower for keyword in ['summary', 'bref', 'financial']):
            financial_summary_pdf = file
            logging.info(f"Found financial summary PDF: {file.name}")
            break
    
    # Load the parsed version from parsedDocs folder
    if financial_summary_pdf:
        # The parsed JSON is in parsedDocs with same base name
        parsed_docs_folder = session_path.parent.parent / "parsedDocs"
        parsed_json_name = financial_summary_pdf.stem + ".json"
        parsed_json_path = parsed_docs_folder / parsed_json_name
        
        if parsed_json_path.exists():
            try:
                with open(parsed_json_path, 'r', encoding='utf-8') as f:
                    bref_summary_pages = json.load(f)
                    # Combine all pages
                    bref_summary_text = "\n\n".join([p.get('text', '') for p in bref_summary_pages])
                    logging.info(f"Loaded BREF summary from parsedDocs: {parsed_json_name} ({len(bref_summary_pages)} pages)")
            except Exception as e:
                logging.warning(f"Error loading parsed BREF summary: {e}")
        else:
            logging.warning(f"Parsed BREF summary not found at: {parsed_json_path}")
    
    # Fallback: Try to find in all_documents CSV (if it was added there)
    if not bref_summary_text:
        for doc in all_documents:
            doc_source = doc.get('source', '').lower()
            if any(keyword in doc_source for keyword in ['summary', 'bref']):
                bref_pages = [d for d in all_documents if d.get('source', '') == doc.get('source', '')]
                bref_summary_text = "\n\n".join([p.get('text', '') for p in bref_pages])
                logging.info(f"Loaded BREF summary from CSV: {doc.get('source', '')} ({len(bref_pages)} pages)")
                break
    
    # Final fallback: Use bref_summary.json (generated summary)
    if not bref_summary_text:
        bref_summary_path = session_path / "bref_summary.json"
        if bref_summary_path.exists():
            try:
                with open(bref_summary_path, 'r', encoding='utf-8') as f:
                    bref_data = json.load(f)
                    bref_summary_text = bref_data.get('summary', '')
                    logging.warning("Using generated BREF summary from bref_summary.json (fallback)")
            except Exception as e:
                logging.warning(f"Error loading BREF summary: {e}")
    
    def extract_page_content(citation: str, all_documents: List[Dict], bref_summary: str) -> str:
        """Extract content from a specific page based on citation with proper formatting"""
        # Parse citation format: "[Source: Document Name | Page Number]" or "[Source: BREF Summary]"
        if "BREF Summary" in citation or "BREF FINANCIAL SUMMARY" in citation:
            # Return BREF summary with proper formatting (same as other documents)
            formatted_content = f"""
╔══════════════════════════════════════════════════════════════════════════╗
║  DOCUMENT START  ·  1     ·  Document Type: FINANCIAL ANALYSIS SUMMARY    ║
╚══════════════════════════════════════════════════════════════════════════╝
<DOCUMENT
  id="DOC-FINANCIAL-STATS-SOURCE"
  name="BREF FINANCIAL SUMMARY"
  type="FINANCIAL ANALYSIS SUMMARY"
  position="1"
  pages="1"
  date="NA"
>
  <DOC_HEADER>
    Name : BREF FINANCIAL SUMMARY
    Type : FINANCIAL ANALYSIS SUMMARY
    Page: 1
  </DOC_HEADER>

{bref_summary}

  <DOC_FOOTER>
    END OF DOCUMENT: BREF FINANCIAL SUMMARY  |  Page 1  | Type: FINANCIAL ANALYSIS SUMMARY
  </DOC_FOOTER>
</DOCUMENT>
╔══════════════════════════════════════════════════════════════════════════╗
║  DOCUMENT END  ·  1     ·  BREF FINANCIAL SUMMARY                         ║
╚══════════════════════════════════════════════════════════════════════════╝
"""
            return formatted_content
        
        # Extract document name and page number
        import re
        match = re.search(r'\[Source:\s*(.+?)\s*\|\s*Page\s*(\d+)\]', citation)
        if not match:
            logging.warning(f"Could not parse citation: {citation}")
            return f"Could not parse citation: {citation}"
        
        doc_name = match.group(1).strip()
        page_num = int(match.group(2))
        
        # Find the page in all_documents
        matching_page = None
        for page in all_documents:
            # Check if source matches and page number matches
            page_source = page.get('source', '')
            page_number = page.get('page number', page.get('page_number', 0))
            
            # Match by document name and page number
            if doc_name.lower() in page_source.lower() and page_number == page_num:
                matching_page = page
                break
        
        if not matching_page:
            logging.warning(f"Page {page_num} not found in {doc_name}")
            return f"Page {page_num} not found in {doc_name}"
        
        # Format the page content with metadata and tags (same as subagents)
        doc_type_label = matching_page.get('doc_type', 'QUALITATIVE SOURCE')
        position = f"{page_num}"
        
        formatted_content = f"""
╔══════════════════════════════════════════════════════════════════════════╗
║  DOCUMENT START  ·  {position:<6}·  Document Type: {doc_type_label:<34}║
╚══════════════════════════════════════════════════════════════════════════╝
<DOCUMENT
  id="DOC-{page_num:03d}"
  name="{doc_name}"
  type="{doc_type_label}"
  position="{position}"
  pages="{page_num}"
  date="{matching_page.get('date', 'N/A')}"
>
  <DOC_HEADER>
    Name : {doc_name}
    Type : {doc_type_label}
    Page: {page_num}
  </DOC_HEADER>

{matching_page.get('text', 'No text content found')}

  <DOC_FOOTER>
    END OF DOCUMENT: {doc_name}  |  Page {page_num}  | Type: {doc_type_label}
  </DOC_FOOTER>
</DOCUMENT>
╔══════════════════════════════════════════════════════════════════════════╗
║  DOCUMENT END  ·  {position:<6}·  {doc_name:<52}║
╚══════════════════════════════════════════════════════════════════════════╝
"""
        return formatted_content
    
    # Process each section
    for section_name, section_data in report_content.items():
        context_list = []
        
        if isinstance(section_data, list) and len(section_data) > 0:
            section_item = section_data[0]
            citations = section_item.get('Citation', [])
            
            # Extract content for each citation
            for citation in citations:
                if isinstance(citation, str):
                    page_content = extract_page_content(citation, all_documents, bref_summary_text)
                    context_list.append(page_content)
            
            # Add Context field to the section as a list of formatted content
            section_item['Context'] = context_list
            logging.info(f"Added {len(context_list)} context items to {section_name}")
            
        elif isinstance(section_data, dict):
            # Handle dict-based sections (like Executive Summary after claim extraction)
            citations = section_data.get('Citation', [])
            for citation in citations:
                if isinstance(citation, str):
                    page_content = extract_page_content(citation, all_documents, bref_summary_text)
                    context_list.append(page_content)
            section_data['Context'] = context_list
            logging.info(f"Added {len(context_list)} context items to {section_name}")
    
    # Update the report data
    report_data['report'] = report_content
    
    logging.info("Context successfully added to report sections")
    return report_data


# ─── TASK 3.4 (QUERY): ADD SECTION PROMPTS ────────────────────────────

def add_query_to_report(report_data: Dict, report_type: str) -> Dict:
    """
    Add the driver analysis prompt (used to generate the report) to each section.
    
    This uses the SAME prompt that was used to generate the report, ensuring that
    evaluation is done against the actual instructions given to the LLM.
    
    Args:
        report_data: Report with claims and context already added
        report_type: Type of report - "pnl", "bs", or "cf"
    
    Returns:
        Dict: Report with 'query' field added to each section
    
    Task 3.4: Appends the DRIVER_ANALYSIS_PROMPT to each section as 'query'
    """
    logging.info(f"Starting query addition for {report_type.upper()} report")
    
    # Select the appropriate driver analysis prompt based on report type
    # These are the ACTUAL prompts used to generate the reports
    if report_type.lower() == "pnl":
        driver_prompt = PROMPT_DRIVER_ANALYSIS_PNL
    elif report_type.lower() == "bs":
        driver_prompt = PROMPT_DRIVER_ANALYSIS_BS
    elif report_type.lower() == "cf":
        driver_prompt = PROMPT_DRIVER_ANALYSIS_CF
    else:
        logging.warning(f"Unknown report type: {report_type}. Using PNL prompts as default.")
        driver_prompt = PROMPT_DRIVER_ANALYSIS_PNL
    
    # Get the report content
    report_content = report_data.get('report', {})
    
    # Add the driver analysis prompt to ALL sections
    # This is the same prompt that was used to generate the entire report
    for section_name, section_data in report_content.items():
        if isinstance(section_data, list) and len(section_data) > 0:
            section_data[0]['query'] = driver_prompt
        elif isinstance(section_data, dict):
            section_data['query'] = driver_prompt
    
    # Update the report data
    report_data['report'] = report_content
    
    logging.info(f"Driver analysis prompt successfully added to all sections")
    logging.info(f"Using prompt: PROMPT_DRIVER_ANALYSIS_{report_type.upper()}")
    return report_data


# ─── TASK 3.5: SAVE EVALUATION DATA ───────────────────────────────────

def save_evaluation_data(report_data: Dict, report_type: str, client_folder: str) -> str:
    """
    Save the evaluation-ready report to the client folder.
    
    Args:
        report_data: Complete report with claims, context, and queries
        report_type: Type of report - "pnl", "bs", or "cf"
        client_folder: Path to client session folder
    
    Returns:
        str: Path to saved evaluation data file
    
    Task 3.5: Saves the appended report with appropriate naming convention
    """
    # Determine output filename based on report type
    filename_map = {
        "pnl": "pnl_evaluation_data.json",
        "bs": "bs_evaluation_data.json",
        "cf": "cf_evaluation_data.json"
    }
    
    output_filename = filename_map.get(report_type.lower(), f"{report_type}_evaluation_data.json")
    output_path = Path(client_folder) / output_filename
    
    # Save the report
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(report_data, f, indent=2, ensure_ascii=False)
    
    logging.info(f"Evaluation data saved to: {output_path}")
    return str(output_path)


# ─── CUSTOM EVALUATION METRICS (Using call_llm_api) ───────────────────

async def calculate_faithfulness_batch(report_sections: Dict) -> Dict:
    """
    Calculate Faithfulness scores for ALL sections in a SINGLE API call.
    
    Faithfulness measures how factually consistent each section's analysis is with its retrieved context.
    
    Args:
        report_sections: Dict of section_name -> section_data with Analysis, Context, query
    
    Returns:
        Dict with faithfulness scores for each section
    """
    try:
        from prompts.prompts_FA import EVALUATION_FAITHFULNESS_BATCH_PROMPT
        
        logging.info(f"Calculating Faithfulness scores for {len(report_sections)} sections in single API call...")
        
        # Build sections data for batch evaluation
        sections_to_evaluate = []
        
        for section_name, section_data in report_sections.items():
            # Extract data
            if isinstance(section_data, list) and len(section_data) > 0:
                section_item = section_data[0]
            elif isinstance(section_data, dict):
                section_item = section_data
            else:
                continue
            
            response = section_item.get('Analysis', '')
            if not response or response == 'Not Available':
                continue
            
            query = section_item.get('query', '')
            context = section_item.get('Context', [])
            
            # Limit context to first 3 items for brevity
            context_text = "\n\n---\n\n".join(context[:3]) if context else 'No context available'
            
            # Add to batch
            sections_to_evaluate.append(f"""
### SECTION: {section_name}

**Query:** {query}

**Context:**
{context_text}

**Analysis to Evaluate:**
{response}

---
""")
        
        if not sections_to_evaluate:
            logging.warning("No sections to evaluate for Faithfulness")
            return {}
        
        # Build batch evaluation prompt
        all_sections_text = "\n".join(sections_to_evaluate)
        
        batch_prompt = EVALUATION_FAITHFULNESS_BATCH_PROMPT.format(
            num_sections=len(sections_to_evaluate),
            all_sections=all_sections_text
        )
        
        # Single LLM API call for all sections
        response_text = call_llm_api(
            query=batch_prompt,
            model_name="gpt-5-mini-2025-08-07",
            temperature=0.0,
            max_tokens=6000
        ).content
        
        # Parse JSON response
        import re
        json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
        if json_match:
            batch_results = json.loads(json_match.group())
        else:
            logging.warning("Failed to extract JSON from batch Faithfulness response")
            batch_results = {"sections": []}
        
        # Convert list results to dict keyed by section name
        results_dict = {}
        for section_result in batch_results.get('sections', []):
            section_name = section_result.get('section_name', '')
            if section_name:
                score = section_result.get('faithfulness_score', 0.0)
                results_dict[section_name] = {
                    "score": float(score),
                    "metric": "faithfulness",
                    "supported_claims": section_result.get('supported_claims', 0),
                    "total_claims": section_result.get('total_claims', 1),
                    "details": section_result.get('details', [])
                }
                logging.info(f"Faithfulness for {section_name}: {score:.2f}")
        
        logging.info(f"Batch Faithfulness evaluation completed for {len(results_dict)} sections")
        return results_dict
        
    except Exception as e:
        logging.error(f"Error calculating batch Faithfulness: {str(e)}")
        return {}


async def calculate_faithfulness(query: str, context: List[str], response: str) -> Dict:
    """Legacy function - kept for backward compatibility but not used in main workflow."""
    """
    Calculate Faithfulness score using custom LLM implementation.
    
    Faithfulness measures how factually consistent the response is with the retrieved context.
    A response is considered faithful if all its claims can be supported by the retrieved context.
    
    Args:
        query: The query/question
        context: List of retrieved context strings
        response: The generated response/analysis
    
    Returns:
        Dict with faithfulness score and details
    """
    try:
        from prompts.prompts_FA import EVALUATION_FAITHFULNESS_PROMPT
        
        logging.info("Calculating Faithfulness score...")
        
        # Build faithfulness evaluation prompt
        context_text = "\n\n---\n\n".join(context[:5])  # Limit to first 5 contexts
        
        faithfulness_prompt = EVALUATION_FAITHFULNESS_PROMPT.format(
            query=query,
            context=context_text,
            response=response
        )
        
        # Call custom LLM API
        llm_response = call_llm_api(
            query=faithfulness_prompt,
            model_name="gpt-5-mini-2025-08-07",
            temperature=0.0,
            max_tokens=2000
        ).content
        
        # Parse JSON response
        import re
        json_match = re.search(r'\{.*\}', llm_response, re.DOTALL)
        if json_match:
            result = json.loads(json_match.group())
            score = result.get('faithfulness_score', 0.0)
            supported_claims = result.get('supported_claims', 0)
            total_claims = result.get('total_claims', 1)
            
            logging.info(f"Faithfulness score calculated: {score} ({supported_claims}/{total_claims} claims supported)")
            
            return {
                "score": float(score),
                "metric": "faithfulness",
                "description": "Measures how factually consistent the response is with the retrieved context",
                "supported_claims": supported_claims,
                "total_claims": total_claims,
                "details": result.get('details', [])
            }
        else:
            logging.warning("Failed to parse faithfulness JSON response")
            return {
                "score": 0.0,
                "metric": "faithfulness",
                "error": "Failed to parse LLM response"
            }
        
    except Exception as e:
        logging.error(f"Error calculating Faithfulness: {str(e)}")
        return {
            "score": 0.0,
            "metric": "faithfulness",
            "error": str(e)
        }


async def calculate_context_precision_batch(report_sections: Dict) -> Dict:
    """
    Calculate Context Precision scores for ALL sections in a SINGLE API call.
    
    Context Precision evaluates the retriever's ability to rank relevant chunks higher
    than irrelevant ones for each section.
    
    Args:
        report_sections: Dict of section_name -> section_data with Analysis, Context, query
    
    Returns:
        Dict with context precision scores for each section
    """
    try:
        from prompts.prompts_FA import EVALUATION_CONTEXT_PRECISION_BATCH_PROMPT
        
        logging.info(f"Calculating Context Precision scores for {len(report_sections)} sections in single API call...")
        
        # Build sections data for batch evaluation
        sections_to_evaluate = []
        
        for section_name, section_data in report_sections.items():
            # Extract data
            if isinstance(section_data, list) and len(section_data) > 0:
                section_item = section_data[0]
            elif isinstance(section_data, dict):
                section_item = section_data
            else:
                continue
            
            response = section_item.get('Analysis', '')
            if not response or response == 'Not Available':
                continue
            
            query = section_item.get('query', '')
            context = section_item.get('Context', [])
            
            # Build ranked contexts (limit to first 5)
            ranked_contexts = "\n\n".join([
                f"**Context Rank {i+1}:**\n{ctx[:500]}..." 
                for i, ctx in enumerate(context[:5])
            ]) if context else 'No context available'
            
            # Add to batch
            sections_to_evaluate.append(f"""
### SECTION: {section_name}

**Query:** {query}

**Ranked Contexts:**
{ranked_contexts}

**Ground Truth (Analysis):**
{response[:500]}...

---
""")
        
        if not sections_to_evaluate:
            logging.warning("No sections to evaluate for Context Precision")
            return {}
        
        # Build batch evaluation prompt
        all_sections_text = "\n".join(sections_to_evaluate)
        
        batch_prompt = EVALUATION_CONTEXT_PRECISION_BATCH_PROMPT.format(
            num_sections=len(sections_to_evaluate),
            all_sections=all_sections_text
        )
        
        # Single LLM API call for all sections
        response_text = call_llm_api(
            query=batch_prompt,
            model_name="gpt-5-mini-2025-08-07",
            temperature=0.0,
            max_tokens=6000
        ).content
        
        # Parse JSON response
        import re
        json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
        if json_match:
            batch_results = json.loads(json_match.group())
        else:
            logging.warning("Failed to extract JSON from batch Context Precision response")
            batch_results = {"sections": []}
        
        # Convert list results to dict keyed by section name
        results_dict = {}
        for section_result in batch_results.get('sections', []):
            section_name = section_result.get('section_name', '')
            if section_name:
                score = section_result.get('context_precision_score', 0.0)
                results_dict[section_name] = {
                    "score": float(score),
                    "metric": "context_precision",
                    "relevant_at_top": section_result.get('relevant_contexts_at_top', 0),
                    "total_relevant": section_result.get('total_relevant_contexts', 1),
                    "context_relevance": section_result.get('context_relevance', [])
                }
                logging.info(f"Context Precision for {section_name}: {score:.2f}")
        
        logging.info(f"Batch Context Precision evaluation completed for {len(results_dict)} sections")
        return results_dict
        
    except Exception as e:
        logging.error(f"Error calculating batch Context Precision: {str(e)}")
        return {}


async def calculate_context_precision(query: str, context: List[str], ground_truth: str) -> Dict:
    """Legacy function - kept for backward compatibility but not used in main workflow."""
    """
    Calculate Context Precision score using custom LLM implementation.
    
    Context Precision evaluates the retriever's ability to rank relevant chunks higher
    than irrelevant ones for a given query in the retrieved context.
    
    Args:
        query: The query/question
        context: List of retrieved context strings
        ground_truth: The ground truth answer (we'll use the response as ground truth)
    
    Returns:
        Dict with context precision score and details
    """
    try:
        from prompts.prompts_FA import EVALUATION_CONTEXT_PRECISION_PROMPT
        
        logging.info("Calculating Context Precision score...")
        
        # Build context with rankings
        ranked_contexts = "\n\n".join([
            f"**Context Rank {i+1}:**\n{ctx}" 
            for i, ctx in enumerate(context[:10])  # Limit to first 10 contexts
        ])
        
        context_precision_prompt = EVALUATION_CONTEXT_PRECISION_PROMPT.format(
            query=query,
            ranked_contexts=ranked_contexts,
            ground_truth=ground_truth
        )
        
        # Call custom LLM API
        llm_response = call_llm_api(
            query=context_precision_prompt,
            model_name="gpt-5-mini-2025-08-07",
            temperature=0.0,
            max_tokens=2000
        ).content
        
        # Parse JSON response
        import re
        json_match = re.search(r'\{.*\}', llm_response, re.DOTALL)
        if json_match:
            result = json.loads(json_match.group())
            score = result.get('context_precision_score', 0.0)
            relevant_at_top = result.get('relevant_contexts_at_top', 0)
            total_relevant = result.get('total_relevant_contexts', 1)
            
            logging.info(f"Context Precision score calculated: {score} ({relevant_at_top}/{total_relevant} relevant at top)")
            
            return {
                "score": float(score),
                "metric": "context_precision",
                "description": "Evaluates the retriever's ability to rank relevant chunks higher than irrelevant ones",
                "relevant_at_top": relevant_at_top,
                "total_relevant": total_relevant,
                "context_relevance": result.get('context_relevance', [])
            }
        else:
            logging.warning("Failed to parse context precision JSON response")
            return {
                "score": 0.0,
                "metric": "context_precision",
                "error": "Failed to parse LLM response"
            }
        
    except Exception as e:
        logging.error(f"Error calculating Context Precision: {str(e)}")
        return {
            "score": 0.0,
            "metric": "context_precision",
            "error": str(e)
        }


async def calculate_insight_quality_batch(report_sections: Dict) -> Dict:
    """
    Calculate Insight Quality scores for ALL sections in a SINGLE API call.
    
    This optimized function evaluates the quality of all financial analysis sections
    at once, reducing API calls from N (one per section) to 1 (for entire report).
    
    Args:
        report_sections: Dict of section_name -> section_data with Analysis, Context, query
    
    Returns:
        Dict with insight quality scores for each section
    """
    try:
        from prompts.prompts_FA import EVALUATION_INSIGHT_QUALITY_BATCH_PROMPT
        
        logging.info(f"Calculating Insight Quality scores for {len(report_sections)} sections in single API call...")
        
        # Build sections data for batch evaluation
        sections_to_evaluate = []
        section_names_list = []
        
        for section_name, section_data in report_sections.items():
            # Extract data
            if isinstance(section_data, list) and len(section_data) > 0:
                section_item = section_data[0]
            elif isinstance(section_data, dict):
                section_item = section_data
            else:
                continue
            
            response = section_item.get('Analysis', '')
            if not response or response == 'Not Available':
                continue
            
            query = section_item.get('query', '')
            context = section_item.get('Context', [])
            
            # Add to batch
            section_names_list.append(section_name)
            sections_to_evaluate.append(f"""
### SECTION: {section_name}

**Query:** {query}

**Analysis:**
{response}

**Context (First 2 sources):**
{chr(10).join(context[:2]) if context else 'No context available'}

---
""")
        
        if not sections_to_evaluate:
            logging.warning("No sections to evaluate for Insight Quality")
            return {}
        
        # Build batch evaluation prompt
        all_sections_text = "\n".join(sections_to_evaluate)
        
        batch_prompt = EVALUATION_INSIGHT_QUALITY_BATCH_PROMPT.format(
            num_sections=len(sections_to_evaluate),
            all_sections=all_sections_text
        )
        
        # Single LLM API call for all sections
        response_text = call_llm_api(
            query=batch_prompt,
            model_name="gpt-5-mini-2025-08-07",
            temperature=0.0,
            max_tokens=6000  # Increased for multiple sections
        ).content
        
        # Parse JSON response
        import re
        json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
        if json_match:
            batch_results = json.loads(json_match.group())
        else:
            logging.warning("Failed to extract JSON from batch Insight Quality response")
            batch_results = {"sections": []}
        
        # Convert list results to dict keyed by section name
        results_dict = {}
        for section_result in batch_results.get('sections', []):
            section_name = section_result.get('section_name', '')
            if section_name:
                # Calculate average score
                avg_score = (
                    section_result.get('analytical_depth', {}).get('score', 0) +
                    section_result.get('reasoning_quality', {}).get('score', 0) +
                    section_result.get('business_usefulness', {}).get('score', 0) +
                    section_result.get('synthesis_quality', {}).get('score', 0)
                ) / 4.0
                
                section_result['average_score'] = avg_score
                section_result['metric'] = 'insight_quality'
                results_dict[section_name] = section_result
                
                logging.info(f"Insight Quality for {section_name}: {avg_score}/10")
        
        logging.info(f"Batch Insight Quality evaluation completed for {len(results_dict)} sections")
        return results_dict
        
    except Exception as e:
        logging.error(f"Error calculating batch Insight Quality: {str(e)}")
        return {}


async def evaluate_section(section_name: str, section_data: Dict) -> Dict:
    """
    Legacy function - not used in main workflow.
    Use evaluate_scores() instead which uses batch processing.
    
    This function is kept for backward compatibility but should not be called
    as it doesn't use the optimized batch processing.
    """
    logging.warning(f"evaluate_section() called for {section_name} - this is a legacy function")
    logging.warning("Use evaluate_scores() instead for optimized batch processing")
    
    # Return skipped to avoid errors
    return {"skipped": True, "reason": "Legacy function - use evaluate_scores() instead"}


# ─── MAIN EVALUATION PIPELINE ─────────────────────────────────────────

async def evaluate_report(report_path: str, client_folder: str) -> Dict:
    """
    Prepare evaluation data for a financial report.
    
    This function ONLY prepares the data (claims, context, queries).
    Actual metric calculation is done by evaluate_scores() using batch processing.
    
    Steps:
    1. Load the report
    2. Extract claims from Analysis sections
    3. Add context from original documents based on Citations
    4. Add query prompts for each section
    5. Save evaluation-ready data
    6. Call evaluate_scores() to calculate metrics using batch processing
    
    Args:
        report_path: Path to the report JSON file
        client_folder: Path to client session folder
    
    Returns:
        Dict: Evaluation results with all metrics (from evaluate_scores)
    """
    # Determine report type from filename
    report_filename = Path(report_path).name
    if "pnl" in report_filename.lower():
        report_type = "pnl"
    elif "bs" in report_filename.lower():
        report_type = "bs"
    elif "cf" in report_filename.lower():
        report_type = "cf"
    else:
        report_type = "unknown"
    
    logging.info(f"="*70)
    logging.info(f"PREPARING EVALUATION DATA FOR {report_type.upper()} REPORT")
    logging.info(f"="*70)
    
    # Step 1: Load the report
    with open(report_path, 'r', encoding='utf-8') as f:
        report_data = json.load(f)
    
    # Step 2: Extract claims
    logging.info("[1/4] Extracting claims from all sections...")
    report_data = claim_extractor(report_data, report_type)
    
    # Step 3: Add context from citations
    logging.info("[2/4] Adding context from original documents...")
    report_data = add_context_to_report(report_data, report_type, client_folder)
    
    # Step 4: Add query prompts
    logging.info("[3/4] Adding section prompts...")
    report_data = add_query_to_report(report_data, report_type)
    
    # Step 5: Save evaluation data
    logging.info("[4/4] Saving evaluation data...")
    output_path = save_evaluation_data(report_data, report_type, client_folder)
    
    logging.info(f"="*70)
    logging.info(f"EVALUATION DATA PREPARED: {output_path}")
    logging.info(f"="*70)
    
    # Step 6: Calculate metrics using batch processing
    logging.info("\nNow calculating metrics using batch processing...")
    evaluation_results = await evaluate_scores(output_path)
    
    # Add paths to results
    evaluation_results['report_path'] = report_path
    evaluation_results['evaluation_data_path'] = output_path
    
    return evaluation_results


# ─── TASK 4: EVALUATE SCORES FUNCTION ─────────────────────────────────

async def evaluate_scores(evaluation_data_path: str) -> Dict:
    """
    Calculate Faithfulness scores for evaluation data.
    
    This function takes the prepared evaluation data (with claims, context, and queries)
    and calculates Faithfulness metric efficiently using minimal API calls.
    
    Note: Context Precision and Insight Quality metrics have been removed.
    Only Faithfulness evaluation is performed.
    
    Args:
        evaluation_data_path: Path to evaluation data JSON file 
                             (pnl_evaluation_data.json, bs_evaluation_data.json, or cf_evaluation_data.json)
    
    Returns:
        Dict: Complete evaluation results with section-level and report-level scores,
              including claim-level validation
    
    Task 4.1: Calculate Faithfulness scores for each section
    Task 4.2: Calculate report-level scores (aggregated from sections)
    Task 4.3: Provide claim-level scores (Passed/Failed with remarks)
    Task 4.4: Use LLM-based Faithfulness evaluation
    Task 4.5: Save results in proper JSON format
    """
    from prompts.prompts_FA import EVALUATION_CLAIM_VALIDATION_PROMPT
    
    logging.info(f"Starting score evaluation for: {evaluation_data_path}")
    
    # Load evaluation data
    with open(evaluation_data_path, 'r', encoding='utf-8') as f:
        eval_data = json.load(f)
    
    report_content = eval_data.get('report', {})
    
    # Determine report type
    if 'pnl' in str(evaluation_data_path).lower():
        report_type = 'P&L'
    elif 'bs' in str(evaluation_data_path).lower():
        report_type = 'Balance Sheet'
    elif 'cf' in str(evaluation_data_path).lower():
        report_type = 'Cash Flow'
    else:
        report_type = 'Unknown'
    
    # Initialize results structure
    evaluation_results = {
        "report_type": report_type,
        "evaluation_data_path": evaluation_data_path,
        "section_scores": {},
        "report_level_scores": {}
    }
    
    # Step 1: Calculate Faithfulness metric for ALL sections in BATCH (2 API calls total)
    logging.info("="*70)
    logging.info("BATCH EVALUATION: Calculating Faithfulness metric in 2 API calls")
    logging.info("="*70)
    
    # 1. Faithfulness for all sections (1 API call)
    logging.info("[1/2] Calculating Faithfulness for all sections in single API call...")
    faithfulness_results = await calculate_faithfulness_batch(report_content)
    
    # 2. Claim Validation for all sections (1 API call)
    logging.info("[2/2] Validating all claims for all sections in single API call...")
    claim_validation_results = await validate_all_claims_batch(report_content)
    
    # FALLBACK: If batch validation returned empty, validate section by section
    if not claim_validation_results:
        logging.warning("Batch claim validation returned empty results. Falling back to section-by-section validation...")
        claim_validation_results = {}
        
        for section_name, section_data in report_content.items():
            # Extract data
            if isinstance(section_data, list) and len(section_data) > 0:
                section_item = section_data[0]
            elif isinstance(section_data, dict):
                section_item = section_data
            else:
                continue
            
            claims = section_item.get('claims', [])
            context = section_item.get('Context', [])
            
            if claims and context:
                logging.info(f"Validating {len(claims)} claims for {section_name}...")
                section_validation = await validate_claims_batch(claims, context, section_name)
                claim_validation_results[section_name] = section_validation
                logging.info(f"Section validation complete: {section_validation.get('passed', 0)}/{section_validation.get('total_claims', 0)} passed")
    else:
        logging.info(f"Batch claim validation successful for {len(claim_validation_results)} sections")
    
    logging.info("="*70)
    logging.info("BATCH EVALUATION COMPLETE: All metrics calculated")
    logging.info("="*70)
    
    # Step 2: Combine results for each section
    logging.info("Combining results for each section...")
    
    for section_name, section_data in report_content.items():
        try:
            # Extract data
            if isinstance(section_data, list) and len(section_data) > 0:
                section_item = section_data[0]
            elif isinstance(section_data, dict):
                section_item = section_data
            else:
                evaluation_results['section_scores'][section_name] = {"skipped": True, "reason": "Invalid format"}
                continue
            
            response = section_item.get('Analysis', '')
            if not response or response == 'Not Available':
                evaluation_results['section_scores'][section_name] = {"skipped": True, "reason": "No analysis"}
                continue
            
            # Combine all pre-calculated results
            section_scores = {
                "section_name": section_name,
                "num_claims": len(section_item.get('claims', [])),
                "num_contexts": len(section_item.get('Context', []))
            }
            
            # Add Faithfulness (ONLY metric we're using)
            if section_name in faithfulness_results:
                faith_result = faithfulness_results[section_name]
                section_scores['faithfulness'] = {
                    "score": faith_result.get('score', 0.0) * 100,
                    "percentage": f"{faith_result.get('score', 0.0) * 100:.1f}%",
                    "supported_claims": faith_result.get('supported_claims', 0),
                    "total_claims": faith_result.get('total_claims', 1)
                }
            
            # Add Claim Validation
            if section_name in claim_validation_results:
                section_scores['claim_validation'] = claim_validation_results[section_name]
            
            evaluation_results['section_scores'][section_name] = section_scores
            
        except Exception as e:
            logging.error(f"Error combining results for {section_name}: {str(e)}")
            evaluation_results['section_scores'][section_name] = {
                "error": str(e),
                "skipped": True
            }
    
    # Step 2: Calculate report-level scores (aggregate from sections)
    logging.info("Calculating report-level scores...")
    evaluation_results['report_level_scores'] = calculate_report_level_scores(
        evaluation_results['section_scores']
    )
    
    # Step 3: Save results
    output_path = Path(evaluation_data_path).parent / f"{report_type.lower().replace(' ', '_')}_scores.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(evaluation_results, f, indent=2, ensure_ascii=False)
    
    logging.info(f"Evaluation scores saved to: {output_path}")
    logging.info(f"Report-level Faithfulness: {evaluation_results['report_level_scores'].get('faithfulness', 0):.1f}%")
    logging.info(f"Total Claims Validated: {evaluation_results['report_level_scores'].get('claim_statistics', {}).get('total_claims', 0)}")
    logging.info(f"Claims Pass Rate: {evaluation_results['report_level_scores'].get('claim_statistics', {}).get('overall_pass_rate', '0%')}")
    
    return evaluation_results


async def evaluate_section_with_claims(section_name: str, section_data: Dict, insight_quality_result: Dict = None) -> Dict:
    """
    Evaluate a single section including claim-level validation.
    
    Args:
        section_name: Name of the section
        section_data: Section data with Analysis, claims, Context, query
        insight_quality_result: Pre-calculated Insight Quality result (from batch call)
    
    Returns:
        Dict with section scores and claim-level validation
    """
    logging.info(f"Evaluating section with claims: {section_name}")
    
    # Extract data
    if isinstance(section_data, list) and len(section_data) > 0:
        section_item = section_data[0]
    elif isinstance(section_data, dict):
        section_item = section_data
    else:
        return {"skipped": True, "reason": "Invalid section format"}
    
    query = section_item.get('query', '')
    context = section_item.get('Context', [])
    response = section_item.get('Analysis', '')
    claims = section_item.get('claims', [])
    
    # Skip if no analysis
    if not response or response == 'Not Available':
        return {"skipped": True, "reason": "No analysis available"}
    
    # Initialize results
    section_results = {
        "section_name": section_name,
        "num_claims": len(claims),
        "num_contexts": len(context)
    }
    
    # Calculate metrics
    try:
        # 1. Faithfulness (Custom LLM)
        if context and response:
            faithfulness_result = await calculate_faithfulness(query, context, response)
            section_results['faithfulness'] = {
                "score": faithfulness_result.get('score', 0.0) * 100,  # Convert to percentage
                "percentage": f"{faithfulness_result.get('score', 0.0) * 100:.1f}%"
            }
        
        # 2. Context Precision (Custom LLM)
        if context and response:
            context_precision_result = await calculate_context_precision(query, context, response)
            section_results['context_precision'] = {
                "score": context_precision_result.get('score', 0.0) * 100,  # Convert to percentage
                "percentage": f"{context_precision_result.get('score', 0.0) * 100:.1f}%"
            }
        
        # 3. Insight Quality (Pre-calculated from batch call)
        if insight_quality_result:
            section_results['insight_quality'] = {
                "score": insight_quality_result.get('average_score', 0.0) * 10,  # Convert to percentage
                "percentage": f"{insight_quality_result.get('average_score', 0.0) * 10:.1f}%",
                "dimensions": {
                    "analytical_depth": insight_quality_result.get('analytical_depth', {}),
                    "reasoning_quality": insight_quality_result.get('reasoning_quality', {}),
                    "business_usefulness": insight_quality_result.get('business_usefulness', {}),
                    "synthesis_quality": insight_quality_result.get('synthesis_quality', {})
                },
                "overall_assessment": insight_quality_result.get('overall_assessment', '')
            }
        
        # 4. Claim-level validation (single LLM call for all claims)
        if claims and context:
            claim_validation = await validate_claims_batch(claims, context, section_name)
            section_results['claim_validation'] = claim_validation
        
    except Exception as e:
        logging.error(f"Error calculating metrics for {section_name}: {str(e)}")
        section_results['error'] = str(e)
    
    return section_results


async def validate_all_claims_batch(report_sections: Dict) -> Dict:
    """
    Validate ALL claims from ALL sections in a SINGLE API call.
    
    Args:
        report_sections: Dict of section_name -> section_data with claims and Context
    
    Returns:
        Dict with validation results for each section
    """
    from prompts.prompts_FA import EVALUATION_ALL_CLAIMS_VALIDATION_PROMPT
    
    logging.info(f"Validating claims for {len(report_sections)} sections in single API call...")
    
    # Build sections data for batch validation
    sections_to_validate = []
    
    for section_name, section_data in report_sections.items():
        # Extract data
        if isinstance(section_data, list) and len(section_data) > 0:
            section_item = section_data[0]
        elif isinstance(section_data, dict):
            section_item = section_data
        else:
            continue
        
        claims = section_item.get('claims', [])
        context = section_item.get('Context', [])
        
        if not claims:
            continue
        
        # Build claims text
        claims_text = "\n".join([f"{i+1}. {claim}" for i, claim in enumerate(claims)])
        context_text = "\n\n---\n\n".join(context[:3]) if context else 'No context available'
        
        # Add to batch
        sections_to_validate.append(f"""
### SECTION: {section_name}

**Claims to Validate:**
{claims_text}

**Context:**
{context_text}

---
""")
    
    if not sections_to_validate:
        logging.warning("No sections with claims to validate")
        return {}
    
    # Build batch validation prompt
    all_sections_text = "\n".join(sections_to_validate)
    
    validation_prompt = EVALUATION_ALL_CLAIMS_VALIDATION_PROMPT.format(
        num_sections=len(sections_to_validate),
        all_sections=all_sections_text
    )
    
    try:
        # Determine max_tokens based on number of sections (Cash Flow needs more tokens)
        # Cash Flow has longer section names, so we increase the token limit
        max_tokens_for_validation = 12000 if len(sections_to_validate) >= 5 else 8000
        
        logging.info(f"Using max_tokens={max_tokens_for_validation} for {len(sections_to_validate)} sections")
        
        # Single LLM call for all claims from all sections
        response = call_llm_api(
            query=validation_prompt,
            model_name="gpt-5-mini-2025-08-07",
            temperature=0.0,
            max_tokens=max_tokens_for_validation
        ).content
        
        logging.info(f"Received claim validation response (first 500 chars): {response[:500]}...")
        
        # Parse JSON response
        import re
        json_match = re.search(r'\{.*\}', response, re.DOTALL)
        if json_match:
            try:
                validation_results = json.loads(json_match.group())
                logging.info(f"Successfully parsed claim validation JSON with {len(validation_results.get('sections', []))} sections")
            except json.JSONDecodeError as e:
                logging.error(f"JSON decode error in claim validation: {e}")
                logging.error(f"Problematic JSON (first 1000 chars): {json_match.group()[:1000]}...")
                validation_results = {"sections": []}
        else:
            logging.warning("Failed to extract JSON from all claims validation response")
            logging.warning(f"Full response (first 1000 chars): {response[:1000]}...")
            validation_results = {"sections": []}
        
        # Convert to dict keyed by section name
        results_dict = {}
        sections_list = validation_results.get('sections', [])
        
        if not sections_list:
            logging.error("No sections found in validation results!")
            logging.error(f"Validation results structure: {validation_results}")
        
        for section_result in sections_list:
            section_name = section_result.get('section_name', '')
            if section_name:
                claims_list = section_result.get('claims', [])
                passed_count = sum(1 for c in claims_list if c.get('status') == 'Passed')
                failed_count = sum(1 for c in claims_list if c.get('status') == 'Failed')
                total_claims = len(claims_list)
                
                results_dict[section_name] = {
                    "total_claims": total_claims,
                    "passed": passed_count,
                    "failed": failed_count,
                    "pass_rate": f"{(passed_count / total_claims * 100) if total_claims > 0 else 0:.1f}%",
                    "claims": claims_list
                }
                logging.info(f"Claim validation for {section_name}: {passed_count}/{total_claims} passed")
            else:
                logging.warning(f"Section result missing section_name: {section_result}")
        
        if results_dict:
            logging.info(f"Batch claim validation completed for {len(results_dict)} sections")
        else:
            logging.error("Batch claim validation returned NO results!")
        
        return results_dict
        
    except Exception as e:
        logging.error(f"Error validating all claims: {str(e)}")
        return {}


async def validate_claims_batch(claims: List[str], context: List[str], section_name: str) -> Dict:
    """Legacy function - kept for backward compatibility but not used in main workflow."""
    """
    Validate all claims in a single LLM API call.
    
    Args:
        claims: List of claims to validate
        context: List of context strings
        section_name: Name of the section
    
    Returns:
        Dict with validation results for each claim
    """
    from prompts.prompts_FA import EVALUATION_CLAIM_VALIDATION_PROMPT
    
    logging.info(f"Validating {len(claims)} claims for {section_name} in single API call...")
    
    # Build prompt for batch claim validation
    claims_text = "\n".join([f"{i+1}. {claim}" for i, claim in enumerate(claims)])
    context_text = "\n\n---\n\n".join(context[:5])  # Limit to first 5 contexts
    
    validation_prompt = EVALUATION_CLAIM_VALIDATION_PROMPT.format(
        section_name=section_name,
        claims=claims_text,
        context=context_text
    )
    
    try:
        # Single LLM call for all claims
        response = call_llm_api(
            query=validation_prompt,
            model_name="gpt-5-mini-2025-08-07",
            temperature=0.0,
            max_tokens=3000
        ).content
        
        # Parse JSON response
        import re
        json_match = re.search(r'\{.*\}', response, re.DOTALL)
        if json_match:
            validation_results = json.loads(json_match.group())
        else:
            logging.warning("Failed to parse claim validation JSON")
            validation_results = {"claims": []}
        
        # Calculate summary statistics
        claims_list = validation_results.get('claims', [])
        passed_count = sum(1 for c in claims_list if c.get('status') == 'Passed')
        failed_count = sum(1 for c in claims_list if c.get('status') == 'Failed')
        
        return {
            "total_claims": len(claims),
            "passed": passed_count,
            "failed": failed_count,
            "pass_rate": f"{(passed_count / len(claims) * 100) if claims else 0:.1f}%",
            "claims": claims_list
        }
        
    except Exception as e:
        logging.error(f"Error validating claims: {str(e)}")
        return {
            "total_claims": len(claims),
            "error": str(e),
            "claims": []
        }


def calculate_report_level_scores(section_scores: Dict) -> Dict:
    """
    Calculate report-level scores by aggregating section-level scores.
    
    Only calculates Faithfulness metric (Context Precision and Insight Quality removed).
    
    Args:
        section_scores: Dict of section-level scores
    
    Returns:
        Dict with report-level aggregate scores
    """
    logging.info("Calculating report-level aggregate scores (Faithfulness only)...")
    
    faithfulness_scores = []
    total_claims = 0
    passed_claims = 0
    failed_claims = 0
    
    for section_name, scores in section_scores.items():
        if scores.get('skipped') or 'error' in scores:
            continue
        
        # Collect Faithfulness scores
        if 'faithfulness' in scores:
            faithfulness_scores.append(scores['faithfulness']['score'])
        
        # Collect claim statistics
        if 'claim_validation' in scores:
            total_claims += scores['claim_validation'].get('total_claims', 0)
            passed_claims += scores['claim_validation'].get('passed', 0)
            failed_claims += scores['claim_validation'].get('failed', 0)
    
    # Calculate average faithfulness
    avg_faithfulness = sum(faithfulness_scores) / len(faithfulness_scores) if faithfulness_scores else 0.0
    
    return {
        "faithfulness": avg_faithfulness,
        "faithfulness_percentage": f"{avg_faithfulness:.1f}%",
        "total_sections_evaluated": len([s for s in section_scores.values() if not s.get('skipped')]),
        "total_sections_skipped": len([s for s in section_scores.values() if s.get('skipped')]),
        "claim_statistics": {
            "total_claims": total_claims,
            "passed_claims": passed_claims,
            "failed_claims": failed_claims,
            "overall_pass_rate": f"{(passed_claims / total_claims * 100) if total_claims > 0 else 0:.1f}%"
        }
    }


# ─── TESTING FUNCTION ─────────────────────────────────────────────────

def test_evaluation_pipeline():
    """
    Test function to verify the evaluation pipeline works correctly.
    """
    import asyncio
    
    # Example usage
    report_path = "./uploads/client_name/session_folder/pnl_final_draft.json"
    client_folder = "./uploads/client_name/session_folder"
    
    # Run the evaluation pipeline
    result = asyncio.run(evaluate_report(report_path, client_folder))
    
    print("Evaluation pipeline test completed!")
    print(f"Result keys: {result.keys()}")


def test_evaluate_scores():
    """
    Test function for evaluate_scores.
    """
    import asyncio
    
    # Example usage
    eval_data_path = "./uploads/client_name/session_folder/pnl_evaluation_data.json"
    
    # Run score evaluation
    result = asyncio.run(evaluate_scores(eval_data_path))
    
    print("Score evaluation test completed!")
    print(f"Report-level scores: {result['report_level_scores']}")


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Run test
    test_evaluation_pipeline()
