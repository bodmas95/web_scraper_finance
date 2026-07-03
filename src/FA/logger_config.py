"""
============================================================
  LOGGING CONFIGURATION
============================================================
  Configures logging to write to both console and file
============================================================
"""

import logging
import sys
import os
import warnings
from datetime import datetime
from pathlib import Path


# Custom filter to suppress specific warnings
class SuppressSpecificWarnings(logging.Filter):
    """Filter to suppress specific warning messages"""
    
    def __init__(self):
        super().__init__()
        # List of warning patterns to suppress
        self.suppress_patterns = [
            "missing ScriptRunContext",
            "ThreadPoolExecutor",
            "This warning can be ignored when running in bare mode",
            "Thread '",
            "ScriptRunContext",
            "bare mode"
        ]
    
    def filter(self, record):
        """Return False to suppress the log record, True to allow it"""
        # Check if any suppress pattern is in the log message
        message = record.getMessage()
        for pattern in self.suppress_patterns:
            if pattern in message:
                return False  # Suppress this log
        return True  # Allow this log


def suppress_streamlit_warnings():
    """
    Suppress Streamlit-specific warnings at the Python warnings level
    """
    # Suppress specific warning categories
    warnings.filterwarnings('ignore', message='.*ScriptRunContext.*')
    warnings.filterwarnings('ignore', message='.*ThreadPoolExecutor.*')
    warnings.filterwarnings('ignore', message='.*bare mode.*')
    warnings.filterwarnings('ignore', message='.*Thread.*missing.*')
    
    # Also suppress at the root logger level for any Streamlit loggers
    logging.getLogger('streamlit').setLevel(logging.ERROR)
    logging.getLogger('streamlit.runtime').setLevel(logging.ERROR)
    logging.getLogger('streamlit.runtime.scriptrunner').setLevel(logging.ERROR)
    logging.getLogger('streamlit.runtime.scriptrunner.script_run_context').setLevel(logging.ERROR)
    
    # Add filter to root logger to catch any that slip through
    root_logger = logging.getLogger()
    root_logger.addFilter(SuppressSpecificWarnings())


def setup_logger(session_folder: str, client_name: str) -> logging.Logger:
    """
    Setup session-specific logger that writes to both console and session-specific file
    
    Args:
        session_folder: Path to session folder
        client_name: Client name for log file
    
    Returns:
        Configured logger instance
    """
    # Suppress Streamlit warnings first
    suppress_streamlit_warnings()
    
    # Create log filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"{client_name}_{timestamp}.log"
    log_filepath = os.path.join(session_folder, log_filename)
    
    # Ensure session folder exists
    os.makedirs(session_folder, exist_ok=True)
    
    # Create SESSION-SPECIFIC logger using session_folder as unique identifier
    # This ensures each user session has its own logger instance
    safe_folder = session_folder.replace("/", "_").replace("\\", "_")
    logger_name = f'financial_analysis_{safe_folder}'
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    
    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Create formatters
    # File gets VERY detailed logs with all information
    detailed_formatter = logging.Formatter(
        fmt='%(asctime)s | %(levelname)-8s | %(funcName)-30s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console gets simpler logs for readability
    simple_formatter = logging.Formatter(
        fmt='%(levelname)-8s | %(message)s'
    )
    
    # File handler - detailed logs
    file_handler = logging.FileHandler(log_filepath, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(detailed_formatter)
    # Add filter to suppress specific warnings in log file too
    file_handler.addFilter(SuppressSpecificWarnings())
    logger.addHandler(file_handler)
    
    # Console handler - simple logs (use UTF-8 to avoid emoji encoding errors on Windows)
    import io
    _utf8_stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    console_handler = logging.StreamHandler(_utf8_stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(simple_formatter)
    # Add filter to suppress specific warnings
    console_handler.addFilter(SuppressSpecificWarnings())
    logger.addHandler(console_handler)
    
    # Log session start
    logger.info("="*90)
    logger.info(f"SESSION STARTED: {client_name}")
    logger.info(f"Log file: {log_filepath}")
    logger.info(f"Session folder: {session_folder}")
    logger.info("="*90)
    
    return logger


def get_logger(session_folder: str = None) -> logging.Logger:
    """
    Get the existing logger instance for a specific session
    
    Args:
        session_folder: Session folder path (optional, uses default if not provided)
    
    Returns:
        Logger instance
    """
    if session_folder:
        safe_folder = session_folder.replace("/", "_").replace("\\", "_")
        logger_name = f'financial_analysis_{safe_folder}'
        return logging.getLogger(logger_name)
    else:
        return logging.getLogger('financial_analysis')


def log_section_start(title: str):
    """Log a section start with visual separator"""
    logger = get_logger()
    logger.info("")
    logger.info("="*90)
    logger.info(f"  {title}")
    logger.info("="*90)


def log_section_end(title: str):
    """Log a section end with visual separator"""
    logger = get_logger()
    logger.info("="*90)
    logger.info(f"  {title} - COMPLETED")
    logger.info("="*90)
    logger.info("")


def log_step(step_name: str, details: str = ""):
    """Log a step with optional details"""
    logger = get_logger()
    if details:
        logger.info(f"✓ {step_name}: {details}")
    else:
        logger.info(f"✓ {step_name}")


def log_error(error_msg: str, exception: Exception = None):
    """Log an error with optional exception details"""
    logger = get_logger()
    logger.error(f"✗ ERROR: {error_msg}")
    if exception:
        logger.error(f"  Exception: {str(exception)}")
        logger.error(f"  Type: {type(exception).__name__}")


def log_warning(warning_msg: str):
    """Log a warning"""
    logger = get_logger()
    logger.warning(f"⚠ WARNING: {warning_msg}")


def log_config(config_dict: dict, title: str = "Configuration"):
    """Log configuration details"""
    logger = get_logger()
    logger.info(f"{title}:")
    for key, value in config_dict.items():
        logger.info(f"  {key}: {value}")


def log_file_saved(filepath: str, description: str = "File"):
    """Log when a file is saved"""
    logger = get_logger()
    logger.info(f"💾 {description} saved: {filepath}")


def log_agent_start(agent_name: str, agent_number: int = None):
    """Log when an agent starts"""
    logger = get_logger()
    if agent_number:
        logger.info(f"🤖 {agent_name} #{agent_number} - Starting")
    else:
        logger.info(f"🤖 {agent_name} - Starting")


def log_agent_complete(agent_name: str, agent_number: int = None):
    """Log when an agent completes"""
    logger = get_logger()
    if agent_number:
        logger.info(f"✅ {agent_name} #{agent_number} - Completed")
    else:
        logger.info(f"✅ {agent_name} - Completed")


def log_llm_call(model: str, temperature: float, max_tokens: int):
    """Log LLM API call details"""
    logger = get_logger()
    logger.info(f"🔄 LLM API Call: model={model}, temp={temperature}, max_tokens={max_tokens}")


def log_parallel_execution(num_agents: int, agent_type: str):
    """Log parallel execution details"""
    logger = get_logger()
    logger.info(f"⚡ Parallel Execution: {num_agents} {agent_type} agents running simultaneously")


def log_chunk_info(num_chunks: int, analysis_type: str = None):
    """Log chunk information"""
    logger = get_logger()
    if analysis_type:
        logger.info(f"📦 Chunks for {analysis_type}: {num_chunks}")
    else:
        logger.info(f"📦 Total chunks created: {num_chunks}")


def log_validation_result(chunk_index: int, accepted: bool, score: int = None, feedback: str = None, issues: list = None):
    """Log validation result with detailed information"""
    logger = get_logger()
    status = "✅ ACCEPTED" if accepted else "❌ REJECTED"
    
    if score is not None:
        logger.info(f"📊 Validation - Chunk {chunk_index}: {status} (Score: {score})")
    else:
        logger.info(f"📊 Validation - Chunk {chunk_index}: {status}")
    
    # Log additional details for rejected reports
    if not accepted:
        if feedback:
            logger.warning(f"  → Rejection Reason: {feedback}")
        if issues:
            logger.warning(f"  → Issues Found: {', '.join(issues)}")


def log_graph_structure(graph_type: str, num_analysts: int, num_evaluators: int):
    """Log the graph structure being built"""
    logger = get_logger()
    logger.info(f"")
    logger.info(f"{'='*90}")
    logger.info(f"GRAPH STRUCTURE: {graph_type.upper()}")
    logger.info(f"{'='*90}")
    logger.info(f"  Total Nodes: {num_analysts + num_evaluators + 3}")
    logger.info(f"  - Start Node: 1")
    logger.info(f"  - Analyst Sub-Agents: {num_analysts} (parallel)")
    logger.info(f"  - Analyst Aggregator: 1")
    logger.info(f"  - Evaluator Sub-Agents: {num_evaluators} (parallel)")
    logger.info(f"  - Evaluator Aggregator: 1")
    logger.info(f"  - Senior Analyst: 1")
    logger.info(f"{'='*90}")
    logger.info(f"")


def log_state_update(field_name: str, value_summary: str):
    """Log when state is updated"""
    logger = get_logger()
    logger.info(f"📝 State Update: {field_name} = {value_summary}")


def log_node_execution(node_name: str, status: str = "starting"):
    """Log node execution"""
    logger = get_logger()
    if status == "starting":
        logger.info(f"")
        logger.info(f"{'─'*90}")
        logger.info(f"▶ NODE STARTING: {node_name}")
        logger.info(f"{'─'*90}")
    elif status == "completed":
        logger.info(f"{'─'*90}")
        logger.info(f"✓ NODE COMPLETED: {node_name}")
        logger.info(f"{'─'*90}")
        logger.info(f"")
    elif status == "failed":
        logger.info(f"{'─'*90}")
        logger.info(f"✗ NODE FAILED: {node_name}")
        logger.info(f"{'─'*90}")
        logger.info(f"")


def log_parallel_execution(message: str):
    """Log parallel execution info"""
    logger = get_logger()
    logger.info(message)


def log_chunk_info(num_chunks: int, analysis_type: str):
    """Log chunk information"""
    logger = get_logger()
    logger.info(f"Chunk Info: {num_chunks} chunks for {analysis_type} analysis")


def log_rejection_details(chunk_index: int, validation_result: dict):
    """Log detailed rejection information"""
    logger = get_logger()
    logger.warning(f"")
    logger.warning(f"{'='*90}")
    logger.warning(f"REPORT REJECTION DETAILS - Chunk {chunk_index}")
    logger.warning(f"{'='*90}")
    logger.warning(f"Score: {validation_result.get('score', 'N/A')}")
    logger.warning(f"Feedback: {validation_result.get('feedback', 'No feedback provided')}")
    
    if 'issues' in validation_result and validation_result['issues']:
        logger.warning(f"Issues:")
        for issue in validation_result['issues']:
            logger.warning(f"  - {issue}")
    
    if 'missing_sections' in validation_result and validation_result['missing_sections']:
        logger.warning(f"Missing Sections:")
        for section in validation_result['missing_sections']:
            logger.warning(f"  - {section}")
    
    logger.warning(f"{'='*90}")
    logger.warning(f"")


def log_retry_attempt(chunk_index: int, retry_count: int, max_retries: int, reason: str):
    """Log retry attempt information"""
    logger = get_logger()
    logger.warning(f"🔄 RETRY - Chunk {chunk_index}: Attempt {retry_count}/{max_retries}")
    logger.warning(f"  Reason: {reason}")


def close_logger():
    """Close all logger handlers"""
    logger = get_logger()
    logger.info("")
    logger.info("="*90)
    logger.info("SESSION ENDED")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info("="*90)
    logger.info("")
    
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)
