import os
import configparser

# ---------------------------------------------------------------------------
# Config file location
# ---------------------------------------------------------------------------

BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, "config.ini")


# ---------------------------------------------------------------------------
# Functions (used by OVH pipeline via: from config.config import ...)
# ---------------------------------------------------------------------------

def load_config() -> configparser.ConfigParser:
    """Return a fresh ConfigParser instance reading config.ini."""
    cfg = configparser.ConfigParser(interpolation=None)
    cfg.read(CONFIG_PATH)
    return cfg


def get_section(section: str) -> dict:
    """
    Return all key-value pairs for a config section as a plain dict.
    Returns an empty dict if the section does not exist.
    """
    if config.has_section(section):
        return dict(config[section])
    return {}


def get_proxy_config() -> dict | None:
    """
    Return proxy settings as a dict, or None if proxy_use = none.

    proxy_use = none              -> None
    proxy_use = system/corporate  -> corporate proxy (NTLM) credentials
    proxy_use = server            -> IP-based proxy (no auth)
    """
    if PROXY_USE in ("system", "corporate"):
        return {
            "host":     config.get("PROXY", "corporate_host",     fallback=""),
            "port":     config.get("PROXY", "corporate_port",     fallback="8080"),
            "username": config.get("PROXY", "corporate_username", fallback=""),
            "password": config.get("PROXY", "corporate_password", fallback=""),
        }
    if PROXY_USE == "server":
        return {
            "host":     config.get("PROXY", "system_host",     fallback=""),
            "port":     config.get("PROXY", "system_port",     fallback="3125"),
            "username": config.get("PROXY", "server_username", fallback=""),
            "password": config.get("PROXY", "server_password", fallback=""),
        }
    return None


def get_env_section():
    """Return the active environment config section (SectionProxy)."""
    env_name = config.get("env", "name", fallback="uat")
    return config[env_name] if config.has_section(env_name) else {}


def get_mongo_uri() -> tuple[str, str]:
    """
    Build and return (mongo_uri, database_name) from the active env section.
    Omits credentials from the URI when mongo_username is blank.
    """
    env_name = config.get("env", "name", fallback="uat")
    env      = config[env_name] if config.has_section(env_name) else {}

    host     = env.get("mongo_host",                   "localhost")
    port     = env.get("mongo_port",                   "27017")
    username = env.get("mongo_username",               "").strip()
    password = env.get("mongo_password",               "").strip()
    database = env.get("mongo_database",               "")
    auth_db  = env.get("mongo_authentication_database","admin")

    if username:
        uri = (
            f"mongodb://{username}:{password}@{host}:{port}/{database}"
            f"?retryWrites=false&serverSelectionTimeoutMS=5000&connectTimeoutMS=10000"
            f"&authSource={auth_db}&authMechanism=SCRAM-SHA-256"
        )
    else:
        uri = (
            f"mongodb://{host}:{port}/{database}"
            f"?retryWrites=false&serverSelectionTimeoutMS=5000&connectTimeoutMS=10000"
        )
    return uri, database


# ---------------------------------------------------------------------------
# Module-level config object
# (used by HKEX pipeline via direct config.get(...) calls)
# ---------------------------------------------------------------------------

config = load_config()

PROXY_USE = config.get("PROXY", "proxy_use", fallback="none").strip().lower()

# ---------------------------------------------------------------------------
# Module-level constants
# (imported directly by HKEX pipeline and main.py)
# ---------------------------------------------------------------------------

# MongoDB
try:
    MONGO_URI, MONGO_DATABASE = get_mongo_uri()
except Exception:
    MONGO_URI, MONGO_DATABASE = "", ""

# HKEX URLs
try:
    BASE_URL   = config.get("HKEX", "base_url",   fallback="")
    SEARCH_URL = config.get("HKEX", "search_url", fallback="")
except Exception:
    BASE_URL = SEARCH_URL = ""

# HTTP headers
try:
    USER_AGENT   = config.get("HEADERS", "user_agent",   fallback="Mozilla/5.0")
    CONTENT_TYPE = config.get("HEADERS", "content_type", fallback="application/x-www-form-urlencoded")
    HEADERS = {
        "User-Agent":   USER_AGENT,
        "Content-Type": CONTENT_TYPE,
    }
except Exception:
    USER_AGENT   = "Mozilla/5.0"
    CONTENT_TYPE = "application/x-www-form-urlencoded"
    HEADERS = {"User-Agent": USER_AGENT, "Content-Type": CONTENT_TYPE}

# Download directory (HKEX)
try:
    DOWNLOAD_DIR = config.get("HKEX", "download_dir", fallback="hkex_pdfs")
except Exception:
    DOWNLOAD_DIR = "hkex_pdfs"

# Proxies
try:
    PROXIES = {
        "HTTP":  config.get("PROXIES", "HTTP_PROXY",  fallback=""),
        "HTTPS": config.get("PROXIES", "HTTPS_PROXY", fallback=""),
    }
except Exception:
    PROXIES = {"HTTP": "", "HTTPS": ""}
