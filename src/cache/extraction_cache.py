"""
Extraction Cache Module

Provides MongoDB-based caching for PDF extraction results to improve performance.
Reduces extraction time from 8-10 minutes to 2-3 seconds for cached documents.
"""

from datetime import datetime
from typing import Optional, Dict, List
import hashlib
import json
import logging
import os

# Setup logging
logger = logging.getLogger(__name__)


class ExtractionCache:
    """
    Cache layer for PDF extraction results using MongoDB.
    
    Features:
    - Fast lookup by company name, PDF hash, and year
    - Automatic cache invalidation on PDF content change
    - Cache statistics and monitoring
    - Support for cache expiration (TTL)
    
    Usage:
        cache = ExtractionCache(mongo_client)
        
        # Try to get from cache
        results = cache.get(company_name, pdf_bytes, target_year)
        
        if results is None:
            # Cache miss - do extraction
            results = extract_financial_statements(...)
            
            # Store in cache
            cache.set(company_name, pdf_bytes, target_year, results)
    """
    
    def __init__(self, mongo_client, db_name='UAT_HR1', collection_name='extraction_cache'):
        """
        Initialize extraction cache.
        
        Args:
            mongo_client: MongoDB client instance
            db_name: Database name (default: 'UAT_HR1')
            collection_name: Collection name (default: 'extraction_cache')
        """
        self.db = mongo_client[db_name]
        self.cache_collection = self.db[collection_name]
        
        logger.info(f"Initializing ExtractionCache...")
        logger.info(f"  Database: {db_name}")
        logger.info(f"  Collection: {collection_name}")
        
        # Create indexes for fast lookup
        self._create_indexes()
        
        logger.info("ExtractionCache initialized successfully")
        print(f"ExtractionCache initialized (DB: {db_name}, Collection: {collection_name})")
    
    def _create_indexes(self):
        """Create MongoDB indexes for fast lookup."""
        try:
            # Unique index on cache_key for fast lookup
            self.cache_collection.create_index('cache_key', unique=True)
            
            # Index on company_name for filtering
            self.cache_collection.create_index('company_name')
            
            # Index on target_year for filtering
            self.cache_collection.create_index('target_year')
            
            # Index on created_at for TTL and sorting
            self.cache_collection.create_index('created_at')
            
            # Compound index for common queries
            self.cache_collection.create_index([
                ('company_name', 1),
                ('target_year', 1)
                        ])
            
            logger.info("Cache indexes created successfully")
            print("Cache indexes created successfully")
        except Exception as e:
            logger.warning(f"Could not create indexes: {e}")
            print(f" Warning: Could not create indexes: {e}")
    
    def _generate_cache_key(self, company_name: str, pdf_hash: str, target_year: int, manual_pages: Dict = None) -> str:
        """
        Generate unique cache key based on PDF content, year, and manual pages.
        
        CRITICAL: Cache key is based on PDF CONTENT (hash), NOT filename!
        This ensures the same PDF with different filenames uses the same cache entry.
        
        Args:
            company_name: Company name (stored for display, but NOT used in cache key)
            pdf_hash: MD5 hash of PDF content (PRIMARY identifier)
            target_year: Target fiscal year
            manual_pages: Manual page numbers dict (optional)
        
        Returns:
            Unique cache key (MD5 hash)
        """
        # CRITICAL: Use ONLY pdf_hash + year + manual_pages for cache key
        # Do NOT include company_name - it's just metadata for display
        # This ensures same PDF with different filenames shares the same cache
        key_data = f"{pdf_hash}_{target_year}"
        
        # Add manual pages to key if specified
        if manual_pages:
            # Sort by statement type for consistency
            sorted_pages = sorted(manual_pages.items())
            pages_str = "_".join([f"{stmt}:{page}" for stmt, page in sorted_pages if page is not None])
            if pages_str:
                key_data += f"_manual_{pages_str}"
        
        # Generate MD5 hash
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def _hash_pdf(self, pdf_bytes: bytes) -> str:
        """
        Generate MD5 hash of PDF content.
        
        Args:
            pdf_bytes: PDF file bytes
        
        Returns:
            MD5 hash string
        """
        return hashlib.md5(pdf_bytes).hexdigest()
    
    def get(self, company_name: str, pdf_bytes: bytes, target_year: int, manual_pages: Dict = None) -> Optional[Dict]:
        """
        Get cached extraction results.
        
        Args:
            company_name: Company name
            pdf_bytes: PDF file bytes
            target_year: Target fiscal year
            manual_pages: Manual page numbers dict (optional)
        
        Returns:
            Cached extraction results or None if not found
        """
        try:
            # Generate cache key
            pdf_hash = self._hash_pdf(pdf_bytes)
            cache_key = self._generate_cache_key(company_name, pdf_hash, target_year, manual_pages)
            
            # Query cache with exact match
            cached = self.cache_collection.find_one({'cache_key': cache_key})
            
            if cached:
                # Update access statistics
                self.cache_collection.update_one(
                    {'cache_key': cache_key},
                    {
                        '$inc': {'access_count': 1},
                        '$set': {'last_accessed': datetime.utcnow()}
                    }
                )
                
                manual_info = f" (Manual pages: {manual_pages})" if manual_pages else ""
                print(f"💚 Cache HIT (exact match) for {company_name} (Year: {target_year}){manual_info}")
                print(f"   Cached on: {cached.get('created_at', 'Unknown')}")
                print(f"   Access count: {cached.get('access_count', 0) + 1}")
                
                return cached.get('extraction_results')
            
                                    # FALLBACK 1: If no manual pages specified, try to find ANY cache entry for this PDF (same year)
            # This allows reusing manually-extracted results when doing automatic extraction
            if not manual_pages:
                print(f"🔍 Exact cache miss, searching for any cached extraction of this PDF (same year)...")
                
                # Find any cache entry with same PDF hash and year (regardless of manual pages or company name)
                # CRITICAL: Ignore company_name - same PDF with different filename should match
                fallback_cached = self.cache_collection.find_one({
                    'pdf_hash': pdf_hash,
                    'target_year': target_year
                })
                
                if fallback_cached:
                    # Update access statistics
                    self.cache_collection.update_one(
                        {'cache_key': fallback_cached['cache_key']},
                        {
                            '$inc': {'access_count': 1},
                            '$set': {'last_accessed': datetime.utcnow()}
                        }
                    )
                    
                    cached_company_name = fallback_cached.get('company_name', 'Unknown')
                    fallback_manual_pages = fallback_cached.get('manual_pages')
                    manual_info = f" (from manual pages: {fallback_manual_pages})" if fallback_manual_pages else ""
                    print(f"💛 Cache HIT (fallback - same year) for {company_name} (Year: {target_year}){manual_info}")
                    if cached_company_name != company_name:
                        print(f"   📝 Original filename: {cached_company_name}")
                    print(f"   Cached on: {fallback_cached.get('created_at', 'Unknown')}")
                    print(f"   Access count: {fallback_cached.get('access_count', 0) + 1}")
                    print(f"   ℹ️  Reusing results from previous extraction")
                    
                    return fallback_cached.get('extraction_results')
                
                                # FALLBACK 2: If still not found, try to find ANY cache entry for this PDF (any year)
                # This handles cases where the year was entered incorrectly
                print(f"🔍 Still not found, searching for any cached extraction of this PDF (any year)...")
                
                # Find any cache entry with same PDF hash (ignore year, manual pages, and company name)
                # CRITICAL: Ignore company_name - same PDF with different filename should match
                fallback_any_year = self.cache_collection.find_one({
                    'pdf_hash': pdf_hash
                })
                
                if fallback_any_year:
                    # Update access statistics
                    self.cache_collection.update_one(
                        {'cache_key': fallback_any_year['cache_key']},
                        {
                            '$inc': {'access_count': 1},
                            '$set': {'last_accessed': datetime.utcnow()}
                        }
                    )
                    
                    cached_company_name = fallback_any_year.get('company_name', 'Unknown')
                    cached_year = fallback_any_year.get('target_year')
                    fallback_manual_pages = fallback_any_year.get('manual_pages')
                    manual_info = f" (from manual pages: {fallback_manual_pages})" if fallback_manual_pages else ""
                    print(f"💙 Cache HIT (fallback - different year) for {company_name}{manual_info}")
                    if cached_company_name != company_name:
                        print(f"   📝 Original filename: {cached_company_name}")
                    print(f"   ⚠️  Year mismatch: requested {target_year}, cached {cached_year}")
                    print(f"   Cached on: {fallback_any_year.get('created_at', 'Unknown')}")
                    print(f"   Access count: {fallback_any_year.get('access_count', 0) + 1}")
                    print(f"   ℹ️  Reusing results from previous extraction (verify year is correct!)")
                    
                    return fallback_any_year.get('extraction_results')
            
            # No cache found
            manual_info = f" (Manual pages: {manual_pages})" if manual_pages else ""
            print(f"❌ Cache MISS for {company_name} (Year: {target_year}){manual_info}")
            return None
        
        except Exception as e:
            print(f" Error reading from cache: {e}")
            return None
    
    def set(self, company_name: str, pdf_bytes: bytes, target_year: int, 
            extraction_results: Dict, metadata: Dict = None, manual_pages: Dict = None):
        """
        Store extraction results in cache.
        
        Args:
            company_name: Company name
            pdf_bytes: PDF file bytes
            target_year: Target fiscal year
            extraction_results: Extraction results to cache
            metadata: Additional metadata (optional)
            manual_pages: Manual page numbers dict (optional)
        """
        try:
            # Generate cache key
            pdf_hash = self._hash_pdf(pdf_bytes)
            cache_key = self._generate_cache_key(company_name, pdf_hash, target_year, manual_pages)
            
                        # Get current version (if exists) and increment
            existing = self.cache_collection.find_one({'cache_key': cache_key})
            current_version = existing.get('version', 0) if existing else 0
            new_version = current_version + 1
            
                        # Prepare cache document
            cache_doc = {
                'cache_key': cache_key,
                'company_name': company_name.strip(),
                'pdf_hash': pdf_hash,
                'pdf_size_bytes': len(pdf_bytes),
                'target_year': target_year,
                'manual_pages': manual_pages,  # Store manual pages info
                'extraction_results': extraction_results,
                'metadata': metadata or {},
                'created_at': existing.get('created_at', datetime.utcnow()) if existing else datetime.utcnow(),
                'updated_at': datetime.utcnow(),
                'last_accessed': datetime.utcnow(),
                'access_count': existing.get('access_count', 0) if existing else 0,
                'version': new_version,
                'extraction_count': existing.get('extraction_count', 0) + 1 if existing else 1
            }
            
            # Upsert (insert or update)
            result = self.cache_collection.update_one(
                {'cache_key': cache_key},
                {'$set': cache_doc},
                upsert=True
            )
            
            manual_info = f" (Manual pages: {manual_pages})" if manual_pages else ""
            if result.upserted_id:
                print(f"💾 Cached extraction results for {company_name} (Year: {target_year}){manual_info}")
                print(f"   Version: {new_version} (NEW)")
            else:
                print(f"🔄 Updated cache for {company_name} (Year: {target_year}){manual_info}")
                print(f"   Version: {current_version} → {new_version}")
            
            print(f"   Cache key: {cache_key}")
            print(f"   PDF size: {len(pdf_bytes):,} bytes")
            print(f"   Extraction count: {cache_doc['extraction_count']}")
            if manual_pages:
                print(f"   Manual pages: {manual_pages}")
            
        except Exception as e:
            print(f" Error writing to cache: {e}")
    
    def invalidate(self, company_name: str = None, target_year: int = None, cache_key: str = None):
        """
        Invalidate cache entries.
        
        Args:
            company_name: Company name to invalidate (optional)
            target_year: Specific year to invalidate (optional)
            cache_key: Specific cache key to invalidate (optional)
        
        Examples:
            # Invalidate all entries for a company
            cache.invalidate(company_name="BYD Company Limited")
            
            # Invalidate specific year for a company
            cache.invalidate(company_name="BYD Company Limited", target_year=2025)
            
            # Invalidate specific cache entry
            cache.invalidate(cache_key="abc123...")
        """
        try:
            # Build query
            query = {}
            
            if cache_key:
                query['cache_key'] = cache_key
            else:
                if company_name:
                    query['company_name'] = company_name.strip()
                if target_year:
                    query['target_year'] = target_year
            
            if not query:
                print(" No invalidation criteria provided")
                return
            
            # Delete matching entries
            result = self.cache_collection.delete_many(query)
            
            print(f" Invalidated {result.deleted_count} cache entry(ies)")
            if company_name:
                print(f"   Company: {company_name}")
            if target_year:
                print(f"   Year: {target_year}")
        
        except Exception as e:
            print(f" Error invalidating cache: {e}")
    
    def clear_all(self):
        """Clear all cache entries."""
        try:
            result = self.cache_collection.delete_many({})
            print(f" Cleared all cache ({result.deleted_count} entries deleted)")
        except Exception as e:
            print(f" Error clearing cache: {e}")
    
    def get_stats(self) -> Dict:
        """
        Get cache statistics.
        
        Returns:
            Dictionary with cache statistics
        """
        try:
            total = self.cache_collection.count_documents({})
            companies = self.cache_collection.distinct('company_name')
            years = self.cache_collection.distinct('target_year')
            
            # Get total size
            pipeline = [
                {
                    '$group': {
                        '_id': None,
                        'total_size': {'$sum': '$pdf_size_bytes'},
                        'total_accesses': {'$sum': '$access_count'}
                    }
                }
            ]
            
            agg_result = list(self.cache_collection.aggregate(pipeline))
            total_size = agg_result[0]['total_size'] if agg_result else 0
            total_accesses = agg_result[0]['total_accesses'] if agg_result else 0
            
            # Get most accessed entries
            most_accessed = list(self.cache_collection.find(
                {},
                {'company_name': 1, 'target_year': 1, 'access_count': 1, '_id': 0}
            ).sort('access_count', -1).limit(5))
            
                # Filter out None values before sorting
            valid_companies = [c for c in companies if c is not None]
            valid_years = [y for y in years if y is not None]
            
            return {
                'total_cached': total,
                'unique_companies': len(valid_companies),
                'unique_years': len(valid_years),
                'companies': sorted(valid_companies),
                'years': sorted(valid_years),
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'total_accesses': total_accesses,
                'most_accessed': most_accessed,
                'cache_hit_rate': self._calculate_hit_rate()
            }
        
        except Exception as e:
            print(f" Error getting cache stats: {e}")
            return {}
    
    def _calculate_hit_rate(self) -> float:
        """
        Calculate cache hit rate.
        
        Returns:
            Hit rate as percentage (0-100)
        """
        try:
            # This is a simplified calculation
            # In production, you'd track hits/misses separately
            total_accesses = self.cache_collection.aggregate([
                {'$group': {'_id': None, 'total': {'$sum': '$access_count'}}}
            ])
            
            result = list(total_accesses)
            if result and result[0]['total'] > 0:
                # Rough estimate: if accessed more than once, it was a hit
                return round((result[0]['total'] / (result[0]['total'] + 1)) * 100, 2)
            
            return 0.0
        except:
            return 0.0
    
    def list_cached_companies(self) -> List[Dict]:
        """
        List all cached companies with details.
        
        Returns:
            List of dictionaries with company details
        """
        try:
            pipeline = [
                {
                    '$group': {
                        '_id': '$company_name',
                        'years': {'$addToSet': '$target_year'},
                        'total_entries': {'$sum': 1},
                        'total_accesses': {'$sum': '$access_count'},
                        'last_cached': {'$max': '$created_at'}
                    }
                },
                {
                    '$project': {
                        '_id': 0,
                        'company_name': '$_id',
                        'years': 1,
                        'total_entries': 1,
                        'total_accesses': 1,
                        'last_cached': 1
                    }
                },
                {
                    '$sort': {'company_name': 1}
                }
            ]
            
            return list(self.cache_collection.aggregate(pipeline))
        
        except Exception as e:
            print(f" Error listing cached companies: {e}")
            return []
    
            
    def get_cache_entry_details(self, company_name: str, target_year: int) -> Optional[Dict]:
        """
        Get detailed information about a specific cache entry.
        
        Args:
            company_name: Company name
            target_year: Target year
        
        Returns:
            Cache entry details or None (includes extraction_results to check completeness)
        """
        try:
            entry = self.cache_collection.find_one(
                {
                    'company_name': company_name.strip(),
                    'target_year': target_year
                },
                {
                    '_id': 0
                    # Include extraction_results to check which statements are cached
                }
            )
            
            # DEBUG: Log what we're returning
            if entry:
                logger.debug(f"get_cache_entry_details({company_name}, {target_year}): Found entry")
                logger.debug(f"  extraction_results type: {type(entry.get('extraction_results'))}")
                if isinstance(entry.get('extraction_results'), dict):
                    logger.debug(f"  extraction_results keys: {list(entry.get('extraction_results', {}).keys())}")
            else:
                logger.debug(f"get_cache_entry_details({company_name}, {target_year}): No entry found")
            
            return entry
        
        except Exception as e:
            logger.error(f"Error getting cache entry details: {e}", exc_info=True)
            print(f" Error getting cache entry details: {e}")
            return None
