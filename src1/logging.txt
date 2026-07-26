"""
Common logging configuration for the financial data ingestion pipeline.

Usage (in any module):
    from src.logging import get_logger
    logger = get_logger(__name__)

    logger.info("Starting pipeline for %s", company_name)
    logger.warning("No data found for FY%s", year)
    logger.error("Download failed: %s", exc)

Configuration (config.ini [LOGGING]):
    level        - root log level: DEBUG, INFO, WARNING, ERROR  (default: INFO)
    log_dir      - directory for rotating log files             (default: logs)
    max_bytes    - max size per log file in bytes               (default: 10485760 = 10 MB)
    backup_count - number of rotated files to keep              (default: 5)

Both a console handler (stdout) and a rotating file handler are attached to
the root logger on first call. Subsequent calls to get_logger() return the
standard named logger without re-configuring handlers.
"""

import logging
import logging.handlers
import sys
import warnings
from pathlib import Path
from configparser import ConfigParser

#  lazy init guard: configure root logger only once 
_CONFIGURED = False

_DEFAULT_LEVEL        = "INFO"
_DEFAULT_LOG_DIR      = "logs"
_DEFAULT_MAX_BYTES    = 10 * 1024 * 1024   # 10 MB
_DEFAULT_BACKUP_COUNT = 5

_FMT     = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
_DATEFMT = "%Y-%m-%d %H:%M:%S"


def _load_log_cfg() -> dict:
    """Read [LOGGING] from config/logging_config.ini. Returns empty dict if the section is absent."""
    try:
        from config.config import get_section
        return get_section("LOGGING")
    except (KeyError, Exception):
        # Fallback to reading from logging_config.ini directly
        try:
            config_path = Path(__file__).parent.parent / "config" / "logging_config.ini"
            if config_path.exists():
                parser = ConfigParser()
                parser.read(config_path)
                if parser.has_section("LOGGING"):
                    return dict(parser.items("LOGGING"))
        except Exception:
            pass
        return {}


def _load_library_log_levels() -> dict:
    """Read [LIBRARY_LOGGING] from config/logging_config.ini."""
    try:
        config_path = Path(__file__).parent.parent / "config" / "logging_config.ini"
        if config_path.exists():
            parser = ConfigParser()
            parser.read(config_path)
            if parser.has_section("LIBRARY_LOGGING"):
                return dict(parser.items("LIBRARY_LOGGING"))
    except Exception:
        pass
    return {}


def _load_app_log_levels() -> dict:
    """Read [APP_LOGGING] from config/logging_config.ini."""
    try:
        config_path = Path(__file__).parent.parent / "config" / "logging_config.ini"
        if config_path.exists():
            parser = ConfigParser()
            parser.read(config_path)
            if parser.has_section("APP_LOGGING"):
                return dict(parser.items("APP_LOGGING"))
    except Exception:
        pass
    return {}


def _suppress_third_party_loggers() -> None:
    """Suppress verbose logging from third-party libraries."""
    # Load library log levels from config
    library_levels = _load_library_log_levels()
    
    # Default suppressions (can be overridden by config)
    default_suppressions = {
        'pymongo': 'WARNING',
        'pymongo.topology': 'WARNING',
        'pymongo.serverSelection': 'WARNING',
        'pymongo.connection': 'WARNING',
        'streamlit': 'ERROR',
        'streamlit.runtime': 'ERROR',
        'streamlit.runtime.scriptrunner': 'ERROR',
        'streamlit.runtime.scriptrunner.script_run_context': 'ERROR',
        'urllib3': 'WARNING',
        'httpx': 'WARNING',
        'requests': 'WARNING',
        'pdfminer': 'ERROR',
        'pdfplumber': 'WARNING',
        'matplotlib': 'WARNING',
        'PIL': 'WARNING',
    }
    
    # Merge config with defaults (config takes precedence)
    suppressions = {**default_suppressions, **library_levels}
    
    # Apply suppressions
    for logger_name, level_str in suppressions.items():
        level = getattr(logging, level_str.upper(), logging.WARNING)
        logging.getLogger(logger_name).setLevel(level)
    
    # Suppress Python warnings for Streamlit
    warnings.filterwarnings('ignore', message='.*ScriptRunContext.*')
    warnings.filterwarnings('ignore', message='.*ThreadPoolExecutor.*')
    warnings.filterwarnings('ignore', message='.*bare mode.*')
    warnings.filterwarnings('ignore', category=UserWarning, module='pdfminer')


def _configure_app_loggers() -> None:
    """Configure application-specific logger levels."""
    app_levels = _load_app_log_levels()
    
    for logger_name, level_str in app_levels.items():
        level = getattr(logging, level_str.upper(), logging.INFO)
        logging.getLogger(logger_name).setLevel(level)


def _configure_root_logger() -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return

    cfg          = _load_log_cfg()
    level_str    = cfg.get("level",        _DEFAULT_LEVEL).upper()
    log_dir      = cfg.get("log_dir",      _DEFAULT_LOG_DIR)
    max_bytes    = int(cfg.get("max_bytes",    _DEFAULT_MAX_BYTES))
    backup_count = int(cfg.get("backup_count", _DEFAULT_BACKUP_COUNT))

    level     = getattr(logging, level_str, logging.INFO)
    formatter = logging.Formatter(_FMT, datefmt=_DATEFMT)

    #  console handler (stdout) 
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(level)

    #  rotating file handler 
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    file_handler = logging.handlers.RotatingFileHandler(
        log_path / "pipeline.log",
        maxBytes    = max_bytes,
        backupCount = backup_count,
        encoding    = "utf-8",
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level)

        #  configure root logger 
    root = logging.getLogger()
    root.setLevel(level)
    # Guard against duplicate handlers (e.g. pytest re-importing)
    if not any(isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
               for h in root.handlers):
        root.addHandler(console_handler)
    if not any(isinstance(h, logging.handlers.RotatingFileHandler) for h in root.handlers):
        root.addHandler(file_handler)

    #  suppress third-party verbose logging 
    _suppress_third_party_loggers()
    
    #  configure application-specific loggers 
    _configure_app_loggers()

    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """
    Return a named logger, configuring the root logger on first call.

    Args:
        name: typically __name__ of the calling module, or a descriptive
              label like "ovh.crawler" or "hkex.pipeline".
    """
    _configure_root_logger()
    return logging.getLogger(name)
