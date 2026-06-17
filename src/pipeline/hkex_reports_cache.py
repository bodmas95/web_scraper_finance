"""
MongoDB cache for HKEX annual reports.
Stores and retrieves annual reports to avoid repeated API calls.
"""

from datetime import datetime, timedelta
from typing import List, Dict, Optional
import streamlit as st


def get_mongo_client():
    """Get MongoDB client from cache."""
    try:
        from src.cache import get_mongo_client as _get_client
        return _get_client()
    except Exception as e:
        print(f"MongoDB connection error: {e}")
        return None


def get_cached_reports(stock_id: str) -> Optional[List[Dict]]:
    """
    Retrieve cached annual reports for a stock from MongoDB.
    
    Args:
        stock_id: HKEX stock code
        
    Returns:
        List of report dictionaries or None if not cached/expired
    """
    try:
        # Use the database from cache (which uses config.ini)
        from src.cache import get_mongo_db
        db = get_mongo_db()
        if not db:
            return None
        
        collection = db["hkex_reports_cache"]
        
        # Find cached reports for this stock
        cache_doc = collection.find_one({"stock_id": stock_id})
        
        if not cache_doc:
            return None
        
        # Check if cache is still valid (24 hours)
        cache_time = cache_doc.get("cached_at")
        if cache_time:
            age = datetime.now() - cache_time
            if age > timedelta(hours=24):
                # Cache expired
                return None
        
        reports = cache_doc.get("reports", [])
        print(f"✅ Retrieved {len(reports)} cached reports for stock {stock_id}")
        return reports
        
    except Exception as e:
        # Silently skip cache on errors (authorization, network, etc.)
        # Don't spam logs with verbose MongoDB errors
        return None


def cache_reports(stock_id: str, reports: List[Dict]) -> bool:
    """
    Cache annual reports for a stock in MongoDB.
    
    Args:
        stock_id: HKEX stock code
        reports: List of report dictionaries
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Use the database from cache (which uses config.ini)
        from src.cache import get_mongo_db
        db = get_mongo_db()
        if not db:
            return False
        
        collection = db["hkex_reports_cache"]
        
        # Upsert cache document
        cache_doc = {
            "stock_id": stock_id,
            "reports": reports,
            "cached_at": datetime.now(),
            "report_count": len(reports)
        }
        
        collection.update_one(
            {"stock_id": stock_id},
            {"$set": cache_doc},
            upsert=True
        )
        
        print(f"✅ Cached {len(reports)} reports for stock {stock_id}")
        return True
        
    except Exception as e:
        # Silently skip cache on errors (authorization, network, etc.)
        # Don't spam logs with verbose MongoDB errors
        return False


def clear_cache(stock_id: Optional[str] = None) -> bool:
    """
    Clear cached reports.
    
    Args:
        stock_id: If provided, clear only this stock's cache. Otherwise clear all.
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Use the database from cache (which uses config.ini)
        from src.cache import get_mongo_db
        db = get_mongo_db()
        if not db:
            return False
        
        collection = db["hkex_reports_cache"]
        
        if stock_id:
            result = collection.delete_one({"stock_id": stock_id})
            print(f"✅ Cleared cache for stock {stock_id}")
        else:
            result = collection.delete_many({})
            print(f"✅ Cleared all cached reports ({result.deleted_count} documents)")
        
        return True
        
    except Exception as e:
        # Silently skip on errors
        return False
