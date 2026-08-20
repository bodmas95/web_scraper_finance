"""
Cache Helper Functions

Provides utility functions for checking cache status and managing cache in the UI.
"""

import streamlit as st
from typing import Optional, Dict
from datetime import datetime


def check_cache_exists(company_name: str, pdf_bytes: bytes, target_year: int) -> bool:
    """
    Check if cached extraction exists for this PDF.
    
    Args:
        company_name: Company name
        pdf_bytes: PDF file bytes
        target_year: Target fiscal year
    
    Returns:
        True if cache exists, False otherwise
    """
    if 'extraction_cache' not in st.session_state:
        return False
    
    cache = st.session_state.extraction_cache
    if cache is None:
        return False
    
    try:
        cached_results = cache.get(company_name, pdf_bytes, target_year)
        return cached_results is not None
    except Exception:
        return False


def get_cache_info(company_name: str, pdf_bytes: bytes, target_year: int) -> Optional[Dict]:
    """
    Get cache information for this PDF.
    
    Args:
        company_name: Company name
        pdf_bytes: PDF file bytes
        target_year: Target fiscal year
    
    Returns:
        Dictionary with cache info or None if not cached
    """
    if 'extraction_cache' not in st.session_state:
        return None
    
    cache = st.session_state.extraction_cache
    if cache is None:
        return None
    
    try:
        # Get cache entry details
        entry = cache.get_cache_entry_details(company_name, target_year)
        
        if entry:
            # Calculate time since last extraction
            updated_at = entry.get('updated_at')
            if updated_at:
                time_diff = datetime.utcnow() - updated_at
                hours_ago = int(time_diff.total_seconds() / 3600)
                minutes_ago = int((time_diff.total_seconds() % 3600) / 60)
                
                if hours_ago > 0:
                    time_str = f"{hours_ago}h ago"
                else:
                    time_str = f"{minutes_ago}m ago"
            else:
                time_str = "Unknown"
            
            return {
                'version': entry.get('version', 1),
                'extraction_count': entry.get('extraction_count', 1),
                'access_count': entry.get('access_count', 0),
                'last_extracted': time_str,
                'created_at': entry.get('created_at'),
                'updated_at': entry.get('updated_at')
            }
        
        return None
    
    except Exception as e:
        print(f"Error getting cache info: {e}")
        return None


def invalidate_cache_for_pdf(company_name: str, target_year: int):
    """
    Invalidate cache for a specific PDF.
    
    Args:
        company_name: Company name
        target_year: Target fiscal year
    """
    if 'extraction_cache' not in st.session_state:
        return
    
    cache = st.session_state.extraction_cache
    if cache is None:
        return
    
    try:
        cache.invalidate(company_name=company_name, target_year=target_year)
        st.success(f"✅ Cache cleared for {company_name} (Year: {target_year})")
    except Exception as e:
        st.error(f"❌ Error clearing cache: {e}")
