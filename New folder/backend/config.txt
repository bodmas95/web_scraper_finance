from pydantic_settings import BaseSettings
from typing import Optional
import os

os.environ.setdefault("ENV_FILE", os.path.join(os.path.dirname(__file__), "..", ".env"))


class Settings(BaseSettings):
    # Proxy
    PROXY_MODE: str = "none"
    PROXY_SERVER_HOST: str = ""
    PROXY_SERVER_PORT: int = 3125
    PROXY_CORPORATE_HOST: str = ""
    PROXY_CORPORATE_PORT: int = 8080
    PROXY_CORPORATE_USERNAME: str = ""
    PROXY_CORPORATE_PASSWORD: str = ""

    # MongoDB
    MONGO_HOST: str = "localhost"
    MONGO_PORT: int = 27017
    MONGO_USERNAME: str = ""
    MONGO_PASSWORD: str = ""
    MONGO_DATABASE: str = "UAT_HR1"
    MONGO_AUTH_DB: str = "admin"

    # LLM
    LLM_PROVIDER: str = "azure"
    LLM_MODEL: str = "gpt-5.1"
    LLM_URL: str = ""
    LLM_API_KEY: str = ""
    LLM_API_VERSION: str = "2025-04-01-preview"
    LLM_TEMPERATURE: float = 0

    # Maia
    MAIA_URL: str = "https://api.ai.uat.intranet.groupebpce.fr"
    MAIA_CREDENTIALS: str = ""
    MAIA_MODEL: str = ""

    # PDF
    PDF_CONFIDENCE_THRESHOLD: float = 0.7
    PDF_DEFAULT_METHOD: str = "text"

    # Logging
    LOG_LEVEL: str = "INFO"

    model_config = {"env_file": os.environ.get("ENV_FILE", "../.env"), "extra": "ignore"}

    @property
    def mongo_uri(self) -> str:
        if self.MONGO_USERNAME:
            return (
                f"mongodb://{self.MONGO_USERNAME}:{self.MONGO_PASSWORD}"
                f"@{self.MONGO_HOST}:{self.MONGO_PORT}"
                f"/{self.MONGO_DATABASE}?authSource={self.MONGO_AUTH_DB}"
            )
        return f"mongodb://{self.MONGO_HOST}:{self.MONGO_PORT}/{self.MONGO_DATABASE}"

    @property
    def http_proxy_url(self) -> Optional[str]:
        if self.PROXY_MODE == "server":
            return f"http://{self.PROXY_SERVER_HOST}:{self.PROXY_SERVER_PORT}"
        if self.PROXY_MODE == "system":
            creds = f"{self.PROXY_CORPORATE_USERNAME}:{self.PROXY_CORPORATE_PASSWORD}@"
            return f"http://{creds}{self.PROXY_CORPORATE_HOST}:{self.PROXY_CORPORATE_PORT}"
        return None


settings = Settings()
