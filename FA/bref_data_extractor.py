"""
BREF Financial Data Extractor
Extracts P&L, Balance Sheet, and Cash Flow data from uploaded BREF document
"""

import asyncio
from typing import Dict, List
from config_defaults import (
    BREF_EXTRACTION_MODEL,
    BREF_EXTRACTION_TEMPERATURE,
    BREF_EXTRACTION_MAX_TOKENS
)

try:
    from bpce_api_setup.call_bpce_llm import call_llm_api
except ImportError:
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "bpce_api_setup"))
    from call_bpce_llm import call_llm_api


# Extraction prompts
PNL_EXTRACTION_PROMPT = """I am giving you a financial summary document. Your task is to only give me the Income Statement (P&L) data in a proper Markdown Tabular Format and return me only data for Income Statement without any change as it is mentioned in the document.
Note : If any value in a millions-denominated table reaches or exceeds 1,000 million, convert the value to billions.

Return ONLY the markdown table, no additional text or explanation."""

BS_EXTRACTION_PROMPT = """I am giving you a financial summary document. Your task is to only give me the Balance Sheet data in a proper Markdown Tabular Format and return me only data for Balance Sheet without any change as it is mentioned in the document.
Note : If any value in a millions-denominated table reaches or exceeds 1,000 million, convert the value to billions.

Return ONLY the markdown table, no additional text or explanation."""

CF_EXTRACTION_PROMPT = """I am giving you a financial summary document. Your task is to only give me the Cash Flow Statement data in a proper Markdown Tabular Format and return me only data for Cash Flow Statement without any change as it is mentioned in the document.
Note : If any value in a millions-denominated table reaches or exceeds 1,000 million, convert the value to billions.

Return ONLY the markdown table, no additional text or explanation."""


async def extract_single_statement(document_text: str, prompt: str, statement_type: str) -> Dict[str, str]:
    """Extract a single financial statement using LLM API"""
    try:
        full_prompt = f"{prompt}\n\nDocument:\n{document_text}"
        
        response = call_llm_api(
            query=full_prompt,
            model_name=BREF_EXTRACTION_MODEL,
            temperature=BREF_EXTRACTION_TEMPERATURE,
            max_tokens=BREF_EXTRACTION_MAX_TOKENS
        )
        response = getattr(response, "content", response)
        return {
            "type": statement_type,
            "data": response,
            "success": True
        }
    except Exception as e:
        return {
            "type": statement_type,
            "data": f"Error extracting {statement_type}: {str(e)}",
            "success": False
        }


async def extract_bref_financial_data(document_text: str) -> Dict[str, str]:
    """
    Extract P&L, Balance Sheet, and Cash Flow data in parallel from BREF document
    
    Args:
        document_text: The full text of the uploaded BREF document
        
    Returns:
        Dictionary with extracted data for each statement type
    """
    # Create tasks for parallel execution
    tasks = [
        extract_single_statement(document_text, PNL_EXTRACTION_PROMPT, "P&L"),
        extract_single_statement(document_text, BS_EXTRACTION_PROMPT, "Balance Sheet"),
        extract_single_statement(document_text, CF_EXTRACTION_PROMPT, "Cash Flow")
    ]
    
    # Execute all tasks in parallel
    results = await asyncio.gather(*tasks)
    
    # Organize results
    extracted_data = {
        "pnl": results[0]["data"] if results[0]["success"] else "Data not available",
        "bs": results[1]["data"] if results[1]["success"] else "Data not available",
        "cf": results[2]["data"] if results[2]["success"] else "Data not available"
    }
    
    return extracted_data


def extract_bref_data_sync(document_text: str) -> Dict[str, str]:
    """
    Synchronous wrapper for extract_bref_financial_data
    
    Args:
        document_text: The full text of the uploaded BREF document
        
    Returns:
        Dictionary with extracted data for each statement type
    """
    return asyncio.run(extract_bref_financial_data(document_text))
