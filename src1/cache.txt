"""
Global caching utilities.

Provides cached config loading, MongoDB connection pooling,
and LLM client caching to avoid repeated initialization overhead.
"""

import functools
import logging
import threading

_log = logging.getLogger(__name__)

#  Config cache 

_config_cache = None
_config_lock = threading.Lock()


def get_config():
    """Return a cached ConfigParser instance (thread-safe singleton)."""
    global _config_cache
    if _config_cache is not None:
        return _config_cache
    with _config_lock:
        if _config_cache is not None:
            return _config_cache
        from config.config import load_config
        _config_cache = load_config()
        return _config_cache


def clear_config_cache():
    """Force re-read of config.ini on next access."""
    global _config_cache
    with _config_lock:
        _config_cache = None


#  MongoDB connection pool 

_mongo_client = None
_mongo_lock = threading.Lock()


def get_mongo_client():
    """Return a cached MongoClient singleton (thread-safe)."""
    global _mongo_client
    if _mongo_client is not None:
        return _mongo_client
    with _mongo_lock:
        if _mongo_client is not None:
            return _mongo_client
        from pymongo import MongoClient
        from config.config import get_mongo_uri
        uri, _ = get_mongo_uri()
        _mongo_client = MongoClient(uri, maxPoolSize=20, minPoolSize=2)
        _log.info("MongoDB connection pool created (maxPoolSize=20)")
        return _mongo_client


def get_mongo_db():
    """Return the active database from the cached MongoClient."""
    from config.config import get_mongo_uri
    _, db_name = get_mongo_uri()
    return get_mongo_client()[db_name]


def close_mongo():
    """Close the MongoDB connection pool."""
    global _mongo_client
    with _mongo_lock:
        if _mongo_client:
            _mongo_client.close()
            _mongo_client = None


#  LLM client cache 

_llm_client_cache = {}
_llm_lock = threading.Lock()


def get_cached_llm_client(provider=None, model=None):
    """
    Return a cached LLM client for the given provider+model combination.
    Thread-safe. Avoids creating a new OpenAI/AzureOpenAI/Maia client per call.
    """
    from src.extraction.llm_client import get_client, LLM_PROVIDER, LLM_MODEL
    _provider = provider or LLM_PROVIDER
    _model = model or LLM_MODEL
    key = (_provider, _model)

    if key in _llm_client_cache:
        return _llm_client_cache[key]

    with _llm_lock:
        if key in _llm_client_cache:
            return _llm_client_cache[key]
        client = get_client(provider=provider, model=model)
        _llm_client_cache[key] = client
        _log.info("LLM client cached for provider=%s, model=%s", _provider, _model)
        return client


def clear_llm_cache():
    """Clear all cached LLM clients."""
    global _llm_client_cache
    with _llm_lock:
        _llm_client_cache.clear()
