"""
LLM Client for PDF Extraction
Handles OpenAI API calls and token usage tracking
"""

from openai import OpenAI
from config.config import load_config

# Load configuration
_cfg = load_config()
LLM_MODEL = _cfg.get("LLM", "model")
LLM_URL = _cfg.get("LLM", "url")
LLM_API_KEY = _cfg.get("LLM", "api_key")

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


def get_client() -> OpenAI:
    """Get OpenAI client instance with proper proxy handling"""
    if not LLM_API_KEY:
        raise ValueError("LLM API key not configured in config.ini [LLM] section")
    
    import httpx
    import os
    
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
