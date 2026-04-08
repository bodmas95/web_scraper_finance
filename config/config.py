import configparser
from pathlib import Path

_CONFIG_PATH = Path(__file__).parent.parent / "config.ini"


def load_config() -> configparser.ConfigParser:
    config = configparser.ConfigParser(interpolation=None)
    config.read(_CONFIG_PATH)
    return config


def get_section(section: str) -> dict:
    """Return all key-value pairs for a config section as a plain dict."""
    config = load_config()
    if section not in config:
        raise KeyError(f"Section [{section}] not found in config.ini")
    return dict(config[section])


def get_proxy_config() -> dict | None:
    """
    Return proxy settings as a dict, or None if proxy_use = none.

    proxy_use = none              → None
    proxy_use = system/corporate  → corporate proxy (NTLM) credentials
    proxy_use = server            → IP-based proxy (no auth)
    """
    config   = load_config()
    proxy_use = config.get("PROXY", "proxy_use", fallback="none").strip().lower()

    if proxy_use in ("system", "corporate"):
        return {
            "host":     config.get("PROXY", "corporate_host",     fallback=""),
            "port":     config.get("PROXY", "corporate_port",     fallback="8080"),
            "username": config.get("PROXY", "corporate_username", fallback=""),
            "password": config.get("PROXY", "corporate_password", fallback=""),
        }
    if proxy_use == "server":
        return {
            "host":     config.get("PROXY", "system_host",     fallback=""),
            "port":     config.get("PROXY", "system_port",     fallback="3125"),
            "username": config.get("PROXY", "server_username", fallback=""),
            "password": config.get("PROXY", "server_password", fallback=""),
        }
    return None


def get_mongo_uri() -> tuple[str, str]:
    """Build and return (mongo_uri, database_name) from the active env section."""
    config   = load_config()
    env_name = config.get("env", "name")
    env      = config[env_name]

    host     = env.get("mongo_host", "localhost")
    port     = env.get("mongo_port", "27017")
    username = env.get("mongo_username", "").strip()
    password = env.get("mongo_password", "").strip()
    database = env.get("mongo_database", "")
    auth_db  = env.get("mongo_authentication_database", "admin")

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
# Module-level constants — used by HKEX pipeline imports
# ---------------------------------------------------------------------------

try:
    MONGO_URI, MONGO_DATABASE = get_mongo_uri()
except Exception:
    MONGO_URI, MONGO_DATABASE = "", ""

try:
    DOWNLOAD_DIR = get_section("HKEX").get("download_dir", "hkex_pdfs")
except Exception:
    DOWNLOAD_DIR = "hkex_pdfs"
