"""
HKEX Extraction Cache Wrapper

Wraps the HKEX extraction function to use caching.
"""

import streamlit as st
from typing import Dict, List
from datetime import datetime


def extract_hkex_with_cache(
    pdf_path: str,
    pdf_bytes: bytes,
    company_name: str,
    target_year: int,
    selected_types: List[str],
    stitch_fn,
    provider: str,
    model_id: str
) -> Dict:
    """
    Extract HKEX financial statements with caching.
    
    Args:
        pdf_path: Path to PDF file
        pdf_bytes: PDF file bytes
        company_name: Company name
        target_year: Target fiscal year
        selected_types: List of statement types to extract
        stitch_fn: Image stitching function
        provider: LLM provider
        model_id: LLM model ID
    
    Returns:
        Dictionary with extraction results
    """
    from src.cache.cache_integration import extract_with_cache
    from src.extraction.parallel import extract_statements_parallel
    
    # Use cache wrapper
    result = extract_with_cache(
        pdf_bytes=pdf_bytes,
        company_name=company_name,
        target_year=target_year,
        selected_types=selected_types,
        extraction_function=extract_statements_parallel,
        # Pass additional arguments for extraction_function (as kwargs)
        pdf_path=pdf_path,
        statement_types=selected_types,
        stitch_fn=stitch_fn,
        provider=provider,
        model_id=model_id
    )
    
    return result
