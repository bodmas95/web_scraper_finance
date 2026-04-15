"""
Unified HTTP client.

All crawlers and parsers import this module instead of calling requests directly.
The backend is controlled by proxy_use in config.ini [PROXY]:

    proxy_use = none    Direct requests.get() with no proxy
    proxy_use = server  requests.get() routed through an IP-based proxy
                        (system_host:system_port, no authentication)
    proxy_use = system  pycurl with NTLM authentication through the corporate proxy
                        (host:port with user/pass from config.ini [PROXY])
"""

import json as _json
from io import BytesIO
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlencode

import requests
from requests import HTTPError

from config.config import get_section, get_proxy_config
proxy= get_proxy_config()

if proxy:
    proxies = {
        "http": f"http://{proxy['host']}:{proxy['port']}",
        "https": f"https://{proxy['host']}:{proxy['port']}",
    }
else:
    proxies = None

_PROXY_CFG = get_section("PROXY")

PROXY_USE  = _PROXY_CFG.get("proxy_use", "none").strip().lower()

# system-mode (NTLM via pycurl) — reads the renamed corporate_* keys
PROXY_HOST = _PROXY_CFG.get("corporate_host", "")
PROXY_PORT = int(_PROXY_CFG.get("corporate_port", "8080") or "8080")
PROXY_USER = _PROXY_CFG.get("corporate_username", "")
PROXY_PASS = _PROXY_CFG.get("corporate_password", "")

# system-mode (IP-based, no auth)
_SYS_HOST  = _PROXY_CFG.get("system_host", "")
_SYS_PORT  = _PROXY_CFG.get("system_port", "")


# ---------------------------------------------------------------------------
# Unified response object
# ---------------------------------------------------------------------------

class HttpResponse:
    """
    Minimal drop-in for requests.Response.
    Supports: .status_code, .content, .text, .encoding, .json(),
              .raise_for_status(), .iter_content(chunk_size)
    """

    def __init__(self, status_code: int, content: bytes, headers: dict = None):
        self.status_code = status_code
        self.content     = content
        self.headers     = headers or {}
        self.encoding    = "utf-8"

    @property
    def text(self) -> str:
        return self.content.decode(self.encoding, errors="replace")

    def json(self):
        return _json.loads(self.content)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise HTTPError(f"HTTP {self.status_code}")

    def iter_content(self, chunk_size: int = 8192):
        for i in range(0, len(self.content), chunk_size):
            yield self.content[i : i + chunk_size]


# ---------------------------------------------------------------------------
# none / system backend (requests)
# ---------------------------------------------------------------------------

def _system_proxies() -> dict:
    if _SYS_HOST and _SYS_PORT:
        proxy_url = f"http://{_SYS_HOST}:{_SYS_PORT}"
    else:
        proxy_url = f"http://{PROXY_HOST}:{PROXY_PORT}"
    return {"http": proxy_url, "https": proxy_url}


def _requests_get(
    url: str,
    headers: dict = None,
    params: dict = None,
    timeout: int = 30,
) -> HttpResponse:
    # proxy_use=server → IP-based proxy via requests (no auth)
    proxies = _system_proxies() if PROXY_USE == "server" else None
    resp = requests.get(
        url, headers=headers, params=params, timeout=timeout, proxies=proxies
    )
    return HttpResponse(resp.status_code, resp.content, dict(resp.headers))


# ---------------------------------------------------------------------------
# server backend (pycurl + NTLM)
# ---------------------------------------------------------------------------

def proxy_request(
    method: str,
    url: str,
    headers: Optional[Dict[str, str]] = None,
    json: Optional[Any] = None,
    data: Optional[Any] = None,
    timeout: int = 600,
) -> Tuple[int, Dict[str, str], bytes]:
    """
    HTTP request through corporate NTLM proxy using pycurl.

    Returns (status_code, response_headers_dict, response_body_bytes).
    On pycurl error returns (500, {}, b"PyCURL Error") instead of raising.
    """
    try:
        import pycurl
    except ImportError:
        raise RuntimeError(
            "proxy_use=server requires pycurl. Install: pip install pycurl"
        )

    method      = method.upper()
    buf_body    = BytesIO()
    buf_headers = BytesIO()
    c = pycurl.Curl()
    try:
        c.setopt(pycurl.URL, url)

        if PROXY_HOST and PROXY_PORT:
            c.setopt(pycurl.PROXY, PROXY_HOST)
            c.setopt(pycurl.PROXYPORT, PROXY_PORT)
            if PROXY_USER and PROXY_PASS:
                c.setopt(pycurl.PROXYUSERNAME, PROXY_USER)
                c.setopt(pycurl.PROXYPASSWORD, PROXY_PASS)
                c.setopt(pycurl.PROXYAUTH, pycurl.HTTPAUTH_NTLM)

        c.setopt(pycurl.SSL_VERIFYPEER, 0)
        c.setopt(pycurl.SSL_VERIFYHOST, 0)
        c.setopt(pycurl.CONNECTTIMEOUT, min(timeout, 60))
        c.setopt(pycurl.TIMEOUT, timeout)

        if method == "GET":
            c.setopt(pycurl.HTTPGET, 1)
        elif method == "POST":
            c.setopt(pycurl.POST, 1)
        else:
            c.setopt(pycurl.CUSTOMREQUEST, method)

        body_bytes      = None
        request_headers = headers.copy() if headers else {}

        if json is not None:
            body_bytes = _json.dumps(json).encode("utf-8")
            request_headers.setdefault("Content-Type", "application/json")
        elif data is not None:
            if isinstance(data, bytes):
                body_bytes = data
            elif isinstance(data, str):
                body_bytes = data.encode("utf-8")
            elif isinstance(data, dict):
                body_bytes = urlencode(data).encode("utf-8")
                request_headers.setdefault(
                    "Content-Type", "application/x-www-form-urlencoded"
                )
            else:
                raise TypeError("data must be bytes, str, or dict")

        if body_bytes is not None:
            c.setopt(pycurl.POSTFIELDS, body_bytes)

        request_headers.setdefault(
            "User-Agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/87.0.4280.66 Safari/537.36",
        )
        header_list = [f"{k}: {v}" for k, v in request_headers.items()]
        if header_list:
            c.setopt(pycurl.HTTPHEADER, header_list)

        c.setopt(pycurl.WRITEDATA, buf_body)
        c.setopt(pycurl.HEADERFUNCTION, buf_headers.write)
        c.perform()

        status      = c.getinfo(pycurl.RESPONSE_CODE)
        raw_headers = buf_headers.getvalue().decode("iso-8859-1", errors="replace")
        body_out    = buf_body.getvalue()

        resp_headers: Dict[str, str] = {}
        for line in raw_headers.split("\r\n"):
            if ":" in line:
                name, value = line.split(":", 1)
                resp_headers[name.strip()] = value.strip()

        return status, resp_headers, body_out

    except pycurl.error as e:
        from src.logging import get_logger as _get_logger
        _get_logger(__name__).error("pycurl error: %s", e)
        return 500, {}, b"PyCURL Error"
    finally:
        c.close()


def _pycurl_get(
    url: str,
    headers: dict = None,
    params: dict = None,
    timeout: int = 30,
) -> HttpResponse:
    if params:
        url = f"{url}?{urlencode(params)}"
    status, resp_headers, body = proxy_request("GET", url, headers=headers, timeout=timeout)
    return HttpResponse(status, body, resp_headers)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get(
    url: str,
    headers: dict = None,
    params: dict = None,
    timeout: int = 30,
) -> HttpResponse:
    """
    Perform an HTTP GET request using the backend from config.ini [PROXY] proxy_use.

    proxy_use = none   → direct requests (no proxy)
    proxy_use = server → requests through IP-based proxy (system_host:system_port, no auth)
    proxy_use = system → pycurl through NTLM corporate proxy (host:port with user/pass)

    Returns an HttpResponse with .status_code, .text, .content, .json(),
    .raise_for_status(), and .iter_content(chunk_size).
    """
    if PROXY_USE == "system":
        return _pycurl_get(url, headers=headers, params=params, timeout=timeout)
    return _requests_get(url, headers=headers, params=params, timeout=timeout)


def post(
    url: str,
    headers: dict = None,
    json: Any = None,
    data: Any = None,
    timeout: int = 30,
) -> HttpResponse:
    """
    Perform an HTTP POST request using the backend from config.ini [PROXY] proxy_use.
    """
    if PROXY_USE == "system":
        status, resp_headers, body = proxy_request(
            "POST", url, headers=headers, json=json, data=data, timeout=timeout
        )
        return HttpResponse(status, body, resp_headers)
    proxies = _system_proxies() if PROXY_USE == "server" else None
    resp = requests.post(
        url, headers=headers, json=json, data=data, timeout=timeout, proxies=proxies
    )
    return HttpResponse(resp.status_code, resp.content, dict(resp.headers))
