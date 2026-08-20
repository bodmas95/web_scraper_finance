import base64
import time
from typing import Any, List, Optional

import httpx
from langchain_core.callbacks import CallbackManagerForLLMRun
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage
from langchain_core.outputs import ChatResult, ChatGeneration
from langchain_openai import AzureChatOpenAI

from config import settings


class MaiaLLM(BaseChatModel):
    """LangChain-compatible wrapper around the MAIA generative AI API."""

    base_url: str = ""
    credentials: str = ""
    model_name: str = ""
    temperature: float = 0

    _access_token: Optional[str] = None
    _token_expiry: float = 0

    @property
    def _llm_type(self) -> str:
        return "maia"

    def _get_auth_header(self) -> str:
        """Base64-encode the client_id:client_secret credentials."""
        return base64.b64encode(self.credentials.encode()).decode()

    def _fetch_token_sync(self, client: httpx.Client) -> str:
        """Fetch an OAuth2 access token synchronously, caching until expiry."""
        now = time.time()
        if self._access_token and now < self._token_expiry:
            return self._access_token

        resp = client.post(
            f"{self.base_url}/oauth2/token",
            headers={
                "Authorization": f"Basic {self._get_auth_header()}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={"grant_type": "client_credentials"},
        )
        resp.raise_for_status()
        data = resp.json()
        self._access_token = data["access_token"]
        self._token_expiry = now + data.get("expires_in", 3600) - 60
        return self._access_token

    async def _fetch_token_async(self, client: httpx.AsyncClient) -> str:
        """Fetch an OAuth2 access token asynchronously, caching until expiry."""
        now = time.time()
        if self._access_token and now < self._token_expiry:
            return self._access_token

        resp = await client.post(
            f"{self.base_url}/oauth2/token",
            headers={
                "Authorization": f"Basic {self._get_auth_header()}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={"grant_type": "client_credentials"},
        )
        resp.raise_for_status()
        data = resp.json()
        self._access_token = data["access_token"]
        self._token_expiry = now + data.get("expires_in", 3600) - 60
        return self._access_token

    def _build_payload(self, messages: List[BaseMessage]) -> dict:
        """Build the MAIA API request payload from LangChain messages."""
        # Combine all message content into a single prompt string.
        parts = []
        for msg in messages:
            content = msg.content if isinstance(msg.content, str) else str(msg.content)
            parts.append(content)
        prompt = "\n".join(parts)

        return {
            "model": self.model_name,
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": self.temperature,
            },
        }

    def _parse_response(self, data: dict) -> str:
        """Extract the generated text from a MAIA API response."""
        try:
            return data["candidates"][0]["text"]
        except (KeyError, IndexError, TypeError):
            # Fallback: try nested content/parts structure
            try:
                return data["candidates"][0]["content"]["parts"][0]["text"]
            except (KeyError, IndexError, TypeError):
                raise ValueError(f"Unexpected MAIA response format: {data}")

    def _generate(
        self,
        messages: List[BaseMessage],
        stop: Optional[List[str]] = None,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
        **kwargs: Any,
    ) -> ChatResult:
        """Synchronous generation — calls the MAIA API via httpx."""
        # No proxy for .intranet domains
        with httpx.Client(verify=False, timeout=120) as client:
            token = self._fetch_token_sync(client)
            payload = self._build_payload(messages)

            resp = client.post(
                f"{self.base_url}/generativeAI/v2/texts",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )
            resp.raise_for_status()
            text = self._parse_response(resp.json())

        message = AIMessage(content=text)
        return ChatResult(generations=[ChatGeneration(message=message)])

    async def _agenerate(
        self,
        messages: List[BaseMessage],
        stop: Optional[List[str]] = None,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
        **kwargs: Any,
    ) -> ChatResult:
        """Asynchronous generation — calls the MAIA API via httpx.AsyncClient."""
        # No proxy for .intranet domains
        async with httpx.AsyncClient(verify=False, timeout=120) as client:
            token = await self._fetch_token_async(client)
            payload = self._build_payload(messages)

            resp = await client.post(
                f"{self.base_url}/generativeAI/v2/texts",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )
            resp.raise_for_status()
            text = self._parse_response(resp.json())

        message = AIMessage(content=text)
        return ChatResult(generations=[ChatGeneration(message=message)])


def get_llm():
    """Return the LLM instance based on LLM_PROVIDER setting ("azure" or "maia")."""
    if settings.LLM_PROVIDER == "maia":
        return MaiaLLM(
            base_url=settings.MAIA_URL,
            credentials=settings.MAIA_CREDENTIALS,
            model_name=settings.MAIA_MODEL,
            temperature=settings.LLM_TEMPERATURE,
        )

    # Default: Azure AI Foundry via LangChain
    kwargs = {
        "azure_deployment": settings.LLM_MODEL,
        "azure_endpoint": settings.LLM_URL,
        "api_key": settings.LLM_API_KEY,
        "api_version": settings.LLM_API_VERSION,
        "temperature": settings.LLM_TEMPERATURE,
    }

    proxy_url = settings.http_proxy_url
    if proxy_url:
        http_client = httpx.Client(proxy=proxy_url, verify=False)
        async_http_client = httpx.AsyncClient(proxy=proxy_url, verify=False)
        kwargs["http_client"] = http_client
        kwargs["http_async_client"] = async_http_client

    return AzureChatOpenAI(**kwargs)
