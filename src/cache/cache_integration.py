"""
Cache Integration Helper

Provides helper functions to integrate extraction cache into the UI workflow.
"""

import streamlit as st
from typing import Dict, List, Optional
from datetime import datetime
import logging
import os

# Setup debug logging
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_file = os.path.join(log_dir, f"cache_debug_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

logger.info("="*80)
logger.info("CACHE INTEGRATION MODULE LOADED")
logger.info(f"Debug log file: {log_file}")
logger.info("="*80)


def initialize_extraction_cache():
    """
    Initialize extraction cache in session state.
    Call this once at app startup.
    """
    logger.info("Initializing extraction cache...")
    
    if 'extraction_cache' not in st.session_state:
        try:
            logger.info("Cache not in session state, creating new instance...")
            
            from src.cache import get_mongo_client
            logger.info("Imported get_mongo_client")
            
            from src.cache.extraction_cache import ExtractionCache
            logger.info("Imported ExtractionCache")
            
            mongo_client = get_mongo_client()
            logger.info(f"Got MongoDB client: {mongo_client}")
            
            st.session_state.extraction_cache = ExtractionCache(mongo_client)
            logger.info("✅ Extraction cache initialized successfully")
            
            print("✅ Extraction cache initialized successfully")
            print(f"📝 Debug log: {log_file}")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize extraction cache: {e}", exc_info=True)
            print(f"⚠️ Warning: Could not initialize extraction cache: {e}")
            print(f"📝 Check debug log: {log_file}")
            
            # Show error in UI
            st.sidebar.error(f"⚠️ Cache initialization failed")
            st.sidebar.caption(f"Error: {str(e)}")
            st.sidebar.caption(f"Extraction will proceed without caching")
            
            st.session_state.extraction_cache = None
            st.session_state.cache_error = str(e)
    else:
        logger.info("Cache already initialized in session state")


def extract_with_cache(
    pdf_bytes: bytes,
    company_name: str,
    target_year: int,
    selected_types: List[str],
    extraction_function,
    **extraction_kwargs
) -> Dict:
    """
    Extract financial statements with caching.
    
    Args:
        pdf_bytes: PDF file bytes
        company_name: Company name
        target_year: Target fiscal year
        selected_types: List of statement types to extract
        extraction_function: Function to call for extraction (if cache miss)
        **extraction_kwargs: Additional arguments to pass to extraction_function
    
    Returns:
        Dictionary with extraction results
    
    Example:
        results = extract_with_cache(
            pdf_bytes=pdf_bytes,
            company_name="BYD Company Limited",
            target_year=2025,
            selected_types=["income_statement", "balance_sheet"],
            extraction_function=extract_statements_parallel,
            pdf_path=pdf_path,
            stitch_fn=stitch_images_vertical,
            provider=provider,
            model_id=model_id
        )
    """
    logger.info("="*80)
    logger.info("EXTRACT WITH CACHE CALLED")
    logger.info(f"Company: {company_name}")
    logger.info(f"Year: {target_year}")
    logger.info(f"Statement types: {selected_types}")
    logger.info(f"PDF size: {len(pdf_bytes):,} bytes")
    logger.info("="*80)
    
    # Initialize cache if not already done
    if 'extraction_cache' not in st.session_state:
        logger.info("Cache not in session state, initializing...")
        initialize_extraction_cache()
    else:
        logger.info("Cache already in session state")
    
    cache = st.session_state.extraction_cache
    
    if cache is None:
        # Cache not available - proceed with extraction
        logger.warning("Cache is None, proceeding without cache")
        
        # Show prominent warning to user
        st.warning(
            "⚠️ **Cache Not Available**\n\n"
            "Extraction will proceed without caching. This means:\n"
            "- Extraction will take 8-10 minutes\n"
            "- Results will NOT be cached for future use\n"
            "- Repeated extractions will take the same time\n\n"
            f"Reason: {st.session_state.get('cache_error', 'Unknown error')}"
        )
        st.caption(f"📝 Debug log: {log_file}")
        
        return extraction_function(**extraction_kwargs)
    
    # Step 1: Try to get from cache
    logger.info("Attempting to get from cache...")
    
    try:
        cached_results = cache.get(company_name, pdf_bytes, target_year)
        logger.info(f"Cache lookup result: {'HIT' if cached_results else 'MISS'}")
    except Exception as e:
        logger.error(f"Error during cache lookup: {e}", exc_info=True)
        cached_results = None
    
    if cached_results:
        logger.info(f"Cache HIT! Found {len(cached_results)} statements")
        # Cache HIT - filter for requested statement types
        filtered_results = {
            stmt_type: cached_results.get(stmt_type)
            for stmt_type in selected_types
            if stmt_type in cached_results
        }
        
        if filtered_results:
            logger.info(f"Returning {len(filtered_results)} filtered results from cache")
            st.success(f"✅ Loaded from cache! (Saved ~8-10 minutes)")
            st.info(f"📊 Cached statements: {', '.join(filtered_results.keys())}")
            st.caption(f"📝 Debug log: {log_file}")
            
            # Return in the same format as extraction_function
            return {
                "results": filtered_results,
                "manual_needed": [],
                "errors": {},
                "logs": {stmt: "Loaded from cache" for stmt in filtered_results.keys()}
            }
    
    # Step 2: Cache MISS - do extraction
    logger.info("Cache MISS - proceeding with extraction")
    logger.info(f"Calling extraction function: {extraction_function.__name__}")
    logger.info(f"Extraction kwargs: {list(extraction_kwargs.keys())}")
    
    st.info(f"🔄 Extracting from PDF... (This may take 8-10 minutes)")
    st.caption(f"💡 Results will be cached for future use")
    st.caption(f"📝 Debug log: {log_file}")
    
    # Call the extraction function
    try:
        extraction_result = extraction_function(**extraction_kwargs)
        logger.info(f"Extraction completed. Result keys: {list(extraction_result.keys()) if extraction_result else 'None'}")
    except Exception as e:
        logger.error(f"Extraction failed: {e}", exc_info=True)
        raise
    
    # Step 3: Cache the results (if extraction was successful)
    if extraction_result and extraction_result.get("results"):
        logger.info("Extraction successful, caching results...")
        logger.info(f"Results to cache: {list(extraction_result['results'].keys())}")
        
        try:
            cache.set(
                company_name=company_name,
                pdf_bytes=pdf_bytes,
                target_year=target_year,
                extraction_results=extraction_result["results"],
                metadata={
                    'extraction_time': datetime.utcnow().isoformat(),
                    'pdf_size': len(pdf_bytes),
                    'num_statements': len(extraction_result["results"]),
                    'statement_types': list(extraction_result["results"].keys())
                }
            )
            
            logger.info("✅ Results cached successfully")
            st.success("💾 Results cached for future use!")
            st.caption(f"📝 Debug log: {log_file}")
            
        except Exception as e:
            logger.error(f"Failed to cache results: {e}", exc_info=True)
            print(f"⚠️ Warning: Could not cache results: {e}")
            print(f"📝 Check debug log: {log_file}")
    else:
        logger.warning("Extraction result is empty or has no 'results' key, not caching")
    
    logger.info("Returning extraction result")
    logger.info("="*80)
    return extraction_result


def render_cache_management_ui():
    """
    Render cache management UI in sidebar.
    Call this in your sidebar to show cache statistics and management options.
    """
    if 'extraction_cache' not in st.session_state:
        initialize_extraction_cache()
    
    cache = st.session_state.extraction_cache
    
    if cache is None:
        st.sidebar.markdown("---")
        st.sidebar.error("❌ **Cache Status: OFFLINE**")
        st.sidebar.caption(
            f"Reason: {st.session_state.get('cache_error', 'Unknown error')[:100]}"
        )
        st.sidebar.caption(
            "Extractions will proceed without caching (8-10 min each)"
        )
        
        # Add retry button
        if st.sidebar.button("🔄 Retry Cache Initialization", use_container_width=True):
            if 'extraction_cache' in st.session_state:
                del st.session_state.extraction_cache
            if 'cache_error' in st.session_state:
                del st.session_state.cache_error
            st.rerun()
        
        return
    
    # Cache is available - show status
    st.sidebar.markdown("---")
    st.sidebar.success("✅ **Cache Status: ONLINE**")
    
    st.sidebar.subheader("🗄️ Cache Management")
    
    # Get cache statistics
    try:
        stats = cache.get_stats()
        
        # Display metrics
        col1, col2 = st.sidebar.columns(2)
        with col1:
            st.metric("Cached", stats.get('total_cached', 0))
        with col2:
            st.metric("Companies", stats.get('unique_companies', 0))
        
        st.sidebar.metric("Total Size", f"{stats.get('total_size_mb', 0)} MB")
        st.sidebar.metric("Total Accesses", stats.get('total_accesses', 0))
        
        # Show cached companies
        if stats.get('companies'):
            with st.sidebar.expander("📋 Cached Companies"):
                for company in stats['companies']:
                    st.write(f"• {company}")
        
        # Cache management buttons
        col_refresh, col_clear = st.sidebar.columns(2)
        
        with col_refresh:
            if st.button("🔄 Refresh", use_container_width=True):
                st.rerun()
        
        with col_clear:
            if st.button("🗑️ Clear All", use_container_width=True):
                cache.clear_all()
                st.sidebar.success("Cache cleared!")
                st.rerun()
        
        # Show most accessed entries
        if stats.get('most_accessed'):
            with st.sidebar.expander("🔥 Most Accessed"):
                for entry in stats['most_accessed']:
                    st.write(f"• {entry.get('company_name')} ({entry.get('target_year')}): {entry.get('access_count')} hits")
    
    except Exception as e:
        st.sidebar.error(f"Error loading cache stats: {e}")


def invalidate_cache_for_company(company_name: str, target_year: int = None):
    """
    Invalidate cache for a specific company.
    
    Args:
        company_name: Company name
        target_year: Specific year to invalidate (optional)
    """
    if 'extraction_cache' not in st.session_state:
        initialize_extraction_cache()
    
    cache = st.session_state.extraction_cache
    
    if cache:
        cache.invalidate(company_name=company_name, target_year=target_year)
        st.success(f"✅ Cache invalidated for {company_name}")
    else:
        st.warning("⚠️ Cache not available")


def get_cache_stats() -> Dict:
    """
    Get cache statistics.
    
    Returns:
        Dictionary with cache statistics
    """
    if 'extraction_cache' not in st.session_state:
        initialize_extraction_cache()
    
    cache = st.session_state.extraction_cache
    
    if cache:
        return cache.get_stats()
    else:
        return {}
