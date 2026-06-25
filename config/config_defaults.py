"""
============================================================
  DEFAULT CONFIGURATION SETTINGS
============================================================
  Centralized configuration for LLM settings and defaults
============================================================
"""
# ─── LLM MODEL OPTIONS ─────────────────────────────────────
LLM_OPTIONS = [
    "claude-sonnet-4-5",
    "gemini-2.5-pro",
    "gpt-4o-mini-2024-07-18",
    "gpt-5-mini-2025-08-07",
    "gpt-5.5-2026-04-24",
    "claude-haiku-4-5",
    "claude-opus-4-6"

]
## New models 
# llm_model = "claude-opus-4-6"
# llm_model = "gpt-5.5-2026-04-24"

# ─── DEFAULT LLM SETTINGS ──────────────────────────────────

# BREF Configuration Defaults
BREF_DEFAULT_TEMPERATURE = 0.4
BREF_DEFAULT_MAX_TOKENS = 3500
# BREF_DEFAULT_LLM_MODEL = "gemini-2.5-pro"
BREF_DEFAULT_LLM_MODEL = "gpt-5.5-2026-04-24"
# BREF_DEFAULT_LLM_MODEL = "claude-opus-4-6"


# FA (Financial Analysis) Configuration Defaults
FA_DEFAULT_TEMPERATURE = 0.4
FA_DEFAULT_MAX_TOKENS =10000
# FA_DEFAULT_LLM_MODEL = "gemini-2.5-pro"
FA_DEFAULT_LLM_MODEL = "gpt-5.5-2026-04-24"
# FA_DEFAULT_LLM_MODEL = "claude-opus-4-6"

# Combined/Final Report Configuration Defaults (Sr. Analyst)
COMBINED_DEFAULT_TEMPERATURE = 0.4
COMBINED_DEFAULT_MAX_TOKENS = 10000
# COMBINED_DEFAULT_LLM_MODEL = "gemini-2.5-pro"
COMBINED_DEFAULT_LLM_MODEL = "gpt-5.5-2026-04-24"
# COMBINED_DEFAULT_LLM_MODEL = "claude-opus-4-6"

# Summarizer Configuration Defaults (same as Sr. Analyst)
SUMMARIZER_DEFAULT_TEMPERATURE = 0.4
SUMMARIZER_DEFAULT_MAX_TOKENS = 3000
# SUMMARIZER_DEFAULT_LLM_MODEL = "gemini-2.5-pro"
SUMMARIZER_DEFAULT_LLM_MODEL = "gpt-5.5-2026-04-24"
# SUMMARIZER_DEFAULT_LLM_MODEL = "claude-opus-4-6"

# ─── SLIDER RANGES ─────────────────────────────────────────

# Temperature Range
TEMPERATURE_MIN = 0.0
TEMPERATURE_MAX = 2.0
TEMPERATURE_STEP = 0.1

# Token Ranges
BREF_TOKENS_MIN = 1000
BREF_TOKENS_MAX = 5000
BREF_TOKENS_STEP = 100

FA_TOKENS_MIN = 1000
FA_TOKENS_MAX = 10000
FA_TOKENS_STEP = 100

COMBINED_TOKENS_MIN = 1000
COMBINED_TOKENS_MAX = 30000
COMBINED_TOKENS_STEP = 100

# Summarizer Token Range (same as Combined)
SUMMARIZER_TOKENS_MIN = 1000
SUMMARIZER_TOKENS_MAX = 30000
SUMMARIZER_TOKENS_STEP = 100

# ─── CLIENT LIST ───────────────────────────────────────────
CLIENT_LIST = ["CTFJ", "NextEra", "OVH"]

# ─── DIRECTORY PATHS ───────────────────────────────────────
BASE_UPLOAD_DIR = "./uploads"
PARSED_DOCS_DIR = "./parsedDocs"

# ─── RAGAS EVALUATION SETTINGS ─────────────────────────────
RAGAS_DEFAULT_LLM_MODEL = "gpt-4o-mini"
RAGAS_DEFAULT_TEMPERATURE = 0.0
RAGAS_DEFAULT_MAX_TOKENS = 2000

# Claim-Level Faithfulness Evaluation Settings
CLAIM_EXTRACTION_MODEL = "gpt-5-mini-2025-08-07"  # Model for extracting claims from response
CLAIM_VERIFICATION_MODEL = "gpt-5-mini-2025-08-07"  # Model for verifying claims against context
CLAIM_EXTRACTION_TEMPERATURE = 0.0  # Temperature for claim extraction (0.0 for deterministic)
CLAIM_VERIFICATION_TEMPERATURE = 0.0  # Temperature for claim verification (0.0 for deterministic)
CLAIM_EXTRACTION_MAX_TOKENS = 4000  # Max tokens for claim extraction
CLAIM_VERIFICATION_MAX_TOKENS = 3000  # Max tokens for claim verification

# ─── EVALUATION SCORE SETTINGS ─────────────────────────────
# Model for Insight Quality evaluation (LLM-as-judge)
INSIGHT_QUALITY_MODEL = "gpt-5-mini-2025-08-07"
INSIGHT_QUALITY_TEMPERATURE = 0.0
INSIGHT_QUALITY_MAX_TOKENS = 1500

# Model for batch claim validation
CLAIM_VALIDATION_MODEL = "gpt-5-mini-2025-08-07"
CLAIM_VALIDATION_TEMPERATURE = 0.0
CLAIM_VALIDATION_MAX_TOKENS = 3000

# ─── BREF DATA EXTRACTION SETTINGS ─────────────────────────
# Settings for extracting financial data tables from BREF document
BREF_EXTRACTION_MODEL = "gpt-5-mini-2025-08-07"
BREF_EXTRACTION_TEMPERATURE = 0.3
BREF_EXTRACTION_MAX_TOKENS = 1000

# ─── EVALUATOR AGENT SETTINGS ──────────────────────────────
# Fixed settings for all evaluator agents (NOT configurable from UI)
EVALUATOR_MODEL = "gpt-5-mini-2025-08-07"
EVALUATOR_TEMPERATURE = 0.0  # Deterministic for consistent evaluation
EVALUATOR_MAX_TOKENS = 200
