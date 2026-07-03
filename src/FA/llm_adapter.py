"""
LLM Adapter for Financial Analysis
Bridges FA's call_llm_api interface with the project's config.ini settings.
Supports: azure, openai, gemma, maia providers.
"""

import sys
import os
import logging
from dataclasses import dataclass

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from config.config import load_config

_client_cache = {}


@dataclass
class LLMResponse:
    """Response wrapper matching the .content interface FA expects."""
    content: str


def _get_azure_client(cfg):
    if "azure" not in _client_cache:
        from openai import AzureOpenAI
        _client_cache["azure"] = AzureOpenAI(
            azure_endpoint=cfg.get("LLM", "url"),
            api_key=cfg.get("LLM", "api_key"),
            api_version=cfg.get("LLM", "api_version", fallback="2025-04-01-preview"),
        )
    return _client_cache["azure"]


def _get_openai_client(cfg):
    if "openai" not in _client_cache:
        from openai import OpenAI
        import httpx

        url = cfg.get("LLM", "url")
        api_key = cfg.get("LLM", "api_key")

        if ".intranet" in url or "localhost" in url or "127.0.0.1" in url:
            old_env = {}
            for var in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
                old_env[var] = os.environ.pop(var, None)
            try:
                http_client = httpx.Client(mounts={})
                _client_cache["openai"] = OpenAI(base_url=url, api_key=api_key, http_client=http_client)
            finally:
                for var, val in old_env.items():
                    if val is not None:
                        os.environ[var] = val
        else:
            _client_cache["openai"] = OpenAI(base_url=url, api_key=api_key)
    return _client_cache["openai"]


def _get_maia_client(cfg, model_name):
    from src.extraction.maia_llm import get_maia_client, MAIA_API_ENDPOINT

    credentials = cfg.get("LLM", "maia_credentials", fallback="")
    if not credentials:
        logging.warning("Maia credentials not configured, falling back to azure")
        return None

    old_proxy_env = {}
    if ".intranet" in MAIA_API_ENDPOINT:
        for var in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy',
                     'ALL_PROXY', 'all_proxy', 'NO_PROXY', 'no_proxy']:
            old_proxy_env[var] = os.environ.pop(var, None)

    try:
        client = get_maia_client(credentials, model_name, proxies=None, use_proxy=False)
        return client
    finally:
        for var, val in old_proxy_env.items():
            if val is not None:
                os.environ[var] = val


def call_llm_api(query: str, model_name: str = None,
                 temperature: float = 0.0, max_tokens: int = 4096) -> LLMResponse:
    """
    Unified LLM call function matching the bpce_api_setup interface.
    Routes to Azure/OpenAI/Gemma/Maia based on config.ini [LLM] provider.
    """
    cfg = load_config()
    provider = cfg.get("LLM", "provider", fallback="azure").lower()

    messages = [{"role": "user", "content": query}]

    if provider == "azure":
        client = _get_azure_client(cfg)
        # Azure requires the deployment name, not the model ID.
        # Always use the deployment name from config.ini regardless of
        # the UI-selected model_name (which is relevant for Maia/Gemma).
        deployment = cfg.get("LLM", "model", fallback="gpt-5.1")
        logging.info(f"[FA LLM] Azure provider → deployment={deployment}")
        # Newer Azure models (gpt-5.x) require max_completion_tokens
        resp = client.chat.completions.create(
            model=deployment,
            messages=messages,
            temperature=temperature,
            max_completion_tokens=max_tokens,
        )
        return LLMResponse(content=resp.choices[0].message.content)

    elif provider == "maia":
        model = model_name or cfg.get("LLM", "maia_model", fallback="gpt-5.1")
        logging.info(f"[FA LLM] Maia provider → model={model}")
        client = _get_maia_client(cfg, model)
        if client is None:
            deployment = cfg.get("LLM", "model", fallback="gpt-5.1")
            logging.warning(f"[FA LLM] Maia fallback to Azure → deployment={deployment}")
            client = _get_azure_client(cfg)
            model = deployment
        resp = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=temperature,
            max_completion_tokens=max_tokens,
        )
        return LLMResponse(content=resp.choices[0].message.content)

    else:  # openai, gemma (vLLM)
        client = _get_openai_client(cfg)
        model = model_name or cfg.get("LLM", "model")
        logging.info(f"[FA LLM] {provider} provider → model={model}")
        resp = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        return LLMResponse(content=resp.choices[0].message.content)
