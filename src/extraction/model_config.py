"""
Model Configuration for PDF Extraction
Provides available models and selection logic
"""

# Available Maia models
MAIA_MODELS = {
    "gpt-5.5-2026-04-24": "GPT-5.5 (Latest, Recommended)",
    "claude-sonnet-4-5": "Claude Sonnet 4.5",
    "claude-opus-4-6": "Claude Opus 4.6",
    "gemini-2.5-pro": "Gemini 2.5 Pro",
    "gpt-4o-2024-08-06": "GPT-4o",
}

# Available Gemma models (OpenAI-compatible)
GEMMA_MODELS = {
    "gemma3-27b-it/": "Gemma 3 27B",
}

def get_all_models():
    """
    Get all available models from both providers.
    
    Returns:
        dict: {model_id: display_name}
    """
    all_models = {}
    
    # Add Maia models with provider prefix (internal use only)
    for model_id, display_name in MAIA_MODELS.items():
        all_models[f"maia:{model_id}"] = display_name
    
    # Add Gemma models with provider prefix (internal use only)
    for model_id, display_name in GEMMA_MODELS.items():
        all_models[f"gemma:{model_id}"] = display_name
    
    return all_models


def parse_model_selection(selection: str):
    """
    Parse model selection string into provider and model_id.
    
    Args:
        selection: String in format "provider:model_id"
    
    Returns:
        tuple: (provider, model_id)
    """
    if ":" in selection:
        provider, model_id = selection.split(":", 1)
        return provider, model_id
    else:
        # Fallback for backward compatibility
        return "gemma", selection


def get_default_model():
    """Get the default model selection."""
    from config.config import load_config
    
    _cfg = load_config()
    provider = _cfg.get("LLM", "provider", fallback="openai").lower()
    
    if provider == "maia":
        model = _cfg.get("LLM", "maia_model", fallback="gpt-5.1-2025-11-13")
        return f"maia:{model}"
    else:
        model = _cfg.get("LLM", "model", fallback="gemma3-27b-it/")
        return f"gemma:{model}"


def render_model_selector(key_prefix=""):
    """
    Render a Streamlit selectbox for model selection.
    
    Args:
        key_prefix: Prefix for the selectbox key to avoid conflicts
    
    Returns:
        tuple: (provider, model_id) selected by user
    """
    import streamlit as st
    from config.config import load_config
    
    # Check if Maia credentials are configured
    _cfg = load_config()
    maia_credentials = _cfg.get("LLM", "maia_credentials", fallback="")
    has_maia_credentials = bool(maia_credentials and ":" in maia_credentials)
    
    all_models = get_all_models()
    
    # Filter out Maia models if credentials not configured
    if not has_maia_credentials:
        all_models = {k: v for k, v in all_models.items() if not k.startswith("maia:")}
        if not all_models:
            # Fallback to Gemma if no models available
            all_models = {"gemma:gemma3-27b-it/": "Gemma 3 27B"}
    
    default_selection = get_default_model()
    
    # Find index of default model
    model_list = list(all_models.keys())
    try:
        default_index = model_list.index(default_selection)
    except ValueError:
        default_index = 0
    
    selected = st.selectbox(
        "Select AI Model",
        options=model_list,
        format_func=lambda x: all_models[x],
        index=default_index,
        key=f"{key_prefix}_model_selector" if key_prefix else "model_selector",
        help="Choose the AI model for extraction."
    )
    
    # Show info message if Maia credentials not configured
    if not has_maia_credentials:
        st.info("ℹ️ To use Maia models (GPT-5.1, Claude, Gemini), add credentials to config.ini: `maia_credentials = CLIENT_ID:CLIENT_SECRET`")
    
    return parse_model_selection(selected)
