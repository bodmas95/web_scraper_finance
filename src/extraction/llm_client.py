"""
LLM Client for PDF Extraction
Handles both OpenAI-compatible APIs (Gemma) and Maia API
Supports switching between providers via config.ini
"""

import threading
from openai import OpenAI, AzureOpenAI
from config.config import load_config
import logging

# Load configuration
_cfg = load_config()
LLM_PROVIDER = _cfg.get("LLM", "provider", fallback="openai").lower()  # openai, azure, gemma, or maia

# Client cache — avoids recreating OpenAI/AzureOpenAI clients on every call
_client_cache: dict = {}
_client_lock = threading.Lock()

# Select model based on provider
if LLM_PROVIDER == "maia":
    LLM_MODEL = _cfg.get("LLM", "maia_model", fallback="gpt-5.1-2025-11-13")
    MAIA_CREDENTIALS = _cfg.get("LLM", "maia_credentials", fallback="")
    LLM_URL = ""  # Not used for Maia
    LLM_API_KEY = ""  # Not used for Maia
else:
    LLM_MODEL = _cfg.get("LLM", "model")
    LLM_URL = _cfg.get("LLM", "url")
    LLM_API_KEY = _cfg.get("LLM", "api_key")
    LLM_API_VERSION = _cfg.get("LLM", "api_version", fallback="")
    MAIA_CREDENTIALS = ""

# Token usage accumulator — reset at the start of each extraction run
_usage = {"input": 0, "output": 0, "total": 0}


def reset_token_usage() -> None:
    """Reset token usage counters"""
    _usage["input"] = 0
    _usage["output"] = 0
    _usage["total"] = 0


def get_token_usage() -> dict:
    """Get current token usage"""
    return dict(_usage)


def track_usage(response) -> None:
    """Track token usage from an API response"""
    u = getattr(response, "usage", None)
    if u:
        _usage["input"] += getattr(u, "prompt_tokens", 0)
        _usage["output"] += getattr(u, "completion_tokens", 0)
        _usage["total"] += getattr(u, "total_tokens", 0)


def get_client(provider: str = None, model: str = None):
    """
    Get LLM client instance based on provider (cached, thread-safe).

    Args:
        provider: "maia", "azure", "openai", or "gemma" (optional, uses config if not provided)
        model: Model ID to use (optional, uses config if not provided)

    Returns:
        OpenAI-compatible client (either OpenAI, AzureOpenAI, or MaiaOpenAIAdapter)
    """
    _provider = provider or LLM_PROVIDER
    _model = model or LLM_MODEL
    cache_key = (_provider, _model)

    cached = _client_cache.get(cache_key)
    if cached is not None:
        return cached

    with _client_lock:
        cached = _client_cache.get(cache_key)
        if cached is not None:
            return cached
        client = _create_client(_provider, _model)
        _client_cache[cache_key] = client
        logging.info("LLM client cached for provider=%s model=%s", _provider, _model)
        return client


def _create_client(provider: str, model: str):
    """Create a new LLM client instance (not cached — use get_client instead)."""
    import httpx
    import os

    _provider = provider
    _model = model

    # Check provider type
    if _provider == "maia":
        # Use Maia API - reload credentials dynamically in case provider was changed
        _maia_creds = _cfg.get("LLM", "maia_credentials", fallback="")
        if not _maia_creds:
            logging.warning("Maia credentials not configured - falling back to Gemma")
            # Fall back to Gemma
            _provider = "openai"
            _model = LLM_MODEL if LLM_PROVIDER == "openai" else "gemma3-27b-it/"
        else:
            logging.info(f"Using Maia API with model: {_model}")
            from .maia_llm import get_maia_client
            
            # Get proxy configuration from environment or config.ini
            proxies = None
            proxy_url = None
            
            # Check environment variables first
            if os.getenv("https_proxy"):
                proxy_url = os.getenv("https_proxy")
            elif os.getenv("http_proxy"):
                proxy_url = os.getenv("http_proxy")
            else:
                # Check config.ini for proxy settings
                proxy_use = _cfg.get("PROXY", "proxy_use", fallback="none").lower()
                if proxy_use == "server":
                    # IP-based proxy (no authentication)
                    host = _cfg.get("PROXY", "server_host", fallback="")
                    port = _cfg.get("PROXY", "server_port", fallback="")
                    if host and port:
                        proxy_url = f"http://{host}:{port}"
                        logging.info(f"Using server proxy from config: {proxy_url}")
                elif proxy_use == "system":
                    # Corporate proxy (with authentication)
                    host = _cfg.get("PROXY", "corporate_host", fallback="")
                    port = _cfg.get("PROXY", "corporate_port", fallback="")
                    user = _cfg.get("PROXY", "corporate_username", fallback="")
                    pwd = _cfg.get("PROXY", "corporate_password", fallback="")
                    if host and port:
                        if user:
                            from urllib.parse import quote
                            proxy_url = f"http://{quote(user, safe='')}:{quote(pwd, safe='')}@{host}:{port}"
                        else:
                            proxy_url = f"http://{host}:{port}"
                        logging.info(f"Using corporate proxy from config: {host}:{port}")
            
            # IMPORTANT: Maia API is on .intranet domain - should NOT use proxy
            # Check if Maia endpoint is internal
            from .maia_llm import MAIA_API_ENDPOINT
            if ".intranet" in MAIA_API_ENDPOINT:
                logging.info("Maia API is on .intranet domain - bypassing proxy")
                
                # CRITICAL: Clear ALL proxy environment variables before creating Maia client
                # httpx picks up these env vars even when we say use_proxy=False
                old_proxy_env = {}
                for var in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 
                           'ALL_PROXY', 'all_proxy', 'NO_PROXY', 'no_proxy']:
                    old_proxy_env[var] = os.environ.pop(var, None)
                
                try:
                    # Disable SSL verification for internal domains
                    os.environ['PYTHONHTTPSVERIFY'] = '0'
                    os.environ['CURL_CA_BUNDLE'] = ''
                    os.environ['REQUESTS_CA_BUNDLE'] = ''
                    
                    client = get_maia_client(_maia_creds, _model, proxies=None, use_proxy=False)
                    return client
                finally:
                    # Restore proxy environment variables for other parts of the app
                    for var, val in old_proxy_env.items():
                        if val is not None:
                            os.environ[var] = val
                        else:
                            os.environ.pop(var, None)
            elif proxy_url:
                proxies = {
                    "http://": proxy_url,
                    "https://": proxy_url
                }
                return get_maia_client(_maia_creds, _model, proxies=proxies, use_proxy=True)
            else:
                return get_maia_client(_maia_creds, _model, proxies=None, use_proxy=True)
    
    # Use Azure OpenAI API
    if _provider == "azure":
        if not LLM_API_KEY:
            raise ValueError("LLM API key not configured in config.ini [LLM] section")
        if not LLM_URL:
            raise ValueError("LLM URL (Azure endpoint) not configured in config.ini [LLM] section")
        _api_version = LLM_API_VERSION
        if not _api_version:
            raise ValueError(
                "api_version not configured in config.ini [LLM] section. "
                "Required for Azure OpenAI (e.g. 2025-04-01-preview)"
            )
        logging.info(f"Using Azure OpenAI with deployment: {_model}, endpoint: {LLM_URL}")
        return AzureOpenAI(
            azure_endpoint=LLM_URL,
            api_key=LLM_API_KEY,
            api_version=_api_version,
        )

    # Use OpenAI-compatible API (Gemma, etc.) if provider is openai/gemma or after fallback
    if _provider == "openai" or _provider == "gemma":
        # Use OpenAI-compatible API (Gemma, etc.)
        if not LLM_API_KEY:
            raise ValueError("LLM API key not configured in config.ini [LLM] section")
        
        logging.info(f"Using OpenAI-compatible API with model: {_model}")
        
        # For internal .intranet servers, ALWAYS bypass proxy
        if ".intranet" in LLM_URL or "localhost" in LLM_URL or "127.0.0.1" in LLM_URL:
            # Temporarily clear ALL proxy environment variables
            old_env = {}
            for var in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "NO_PROXY", "no_proxy"):
                old_env[var] = os.environ.pop(var, None)

            try:
                # Create httpx client with explicit empty mounts (no proxy)
                http_client = httpx.Client(mounts={})
                client = OpenAI(base_url=LLM_URL, api_key=LLM_API_KEY, http_client=http_client)
                return client
            finally:
                # Restore environment variables
                for var, val in old_env.items():
                    if val is not None:
                        os.environ[var] = val

        # For external servers, use default behavior
        return OpenAI(base_url=LLM_URL, api_key=LLM_API_KEY)
