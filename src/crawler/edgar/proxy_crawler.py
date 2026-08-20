"""
SEC EDGAR Crawler with NTLM Proxy Support
Uses curl subprocess for proxy authentication
"""

import subprocess
import json
from typing import Optional, Dict, Any
from edgar import Company, set_identity
import edgar.httprequests as hr
import edgar.entity.submissions as subs
import edgar.sgml.sgml_common as sgml_common
import edgar.attachments as attachments
from src.crawler.edgar.crawler import EdgarCrawler


class FakeResponse:
    """Mock response object for curl-based requests"""
    
    def __init__(self, url, content: bytes, status_code: int = 200):
        self.url = url
        self._content = content
        self.status_code = status_code
    
    @property
    def content(self):
        return self._content
    
    @property
    def text(self):
        return self._content.decode("utf-8", errors="replace")
    
    def json(self):
        return json.loads(self.text)
    
    def iter_lines(self):
        for line in self._content.splitlines():
            yield line
    
    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code} for {self.url}")
    
    def close(self):
        pass


class ProxyEdgarCrawler(EdgarCrawler):
    """
    EDGAR crawler with NTLM proxy support using curl subprocess.
    Extends EdgarCrawler with proxy patching.
    """
    
    def __init__(self, config):
        """
        Initialize proxy crawler with curl-based HTTP requests.
        
        Args:
            config: Configuration object with:
                - identity: SEC identity string
                - proxy_url: Proxy URL (e.g., http://proxyusers.hk.intranet:8080)
                - proxy_user: Proxy username (e.g., cib.net/jverma)
                - proxy_password: Proxy password
                - user_agent: User agent string
        """
        self.config = config
        self.proxy_url = getattr(config, 'proxy_url', '')
        self.proxy_user = getattr(config, 'proxy_user', '')
        self.proxy_password = getattr(config, 'proxy_password', '')
        self.user_agent = getattr(config, 'user_agent', config.identity)
        
        # Patch edgar HTTP functions before calling parent init
        if self.proxy_url:
            self._patch_edgar_http()
        
        # Initialize parent class
        super().__init__(config)
    
    def _curl_fetch(self, url: str, as_text=False) -> bytes:
        """
        Fetch URL using curl with NTLM proxy authentication.
        
        Args:
            url: URL to fetch
            as_text: If True, return decoded text instead of bytes
            
        Returns:
            Response content as bytes or text
        """
        cmd = [
            "curl",
            "--silent",
            "--show-error",
            "--location",
            "--ssl-no-revoke",
            "--proxy-ntlm",
            "-U", f"{self.proxy_user}:{self.proxy_password}",
            "--proxy", self.proxy_url,
            "-A", self.user_agent,
            url,
        ]
        
        try:
            res = subprocess.run(cmd, capture_output=True, timeout=60)
            
            if res.returncode != 0:
                raise RuntimeError(
                    f"Curl failed for {url}\n"
                    f"returncode={res.returncode}\n"
                    f"stderr={res.stderr.decode(errors='replace')}"
                )
            
            return res.stdout.decode("utf-8", errors="replace") if as_text else res.stdout
            
        except subprocess.TimeoutExpired:
            raise RuntimeError(f"Curl timeout for {url}")
        except Exception as e:
            raise RuntimeError(f"Curl error for {url}: {e}")
    
    def _patched_get_with_retry(self, url, *args, **kwargs):
        """Patched version of edgar's get_with_retry using curl"""
        content = self._curl_fetch(url, as_text=False)
        return FakeResponse(url, content, status_code=200)
    
    def _patched_download_file(self, url, as_text=None, path=None):
        """Patched version of edgar's download_file using curl"""
        content = self._curl_fetch(url, as_text=False)
        
        if path:
            with open(path, "wb") as f:
                f.write(content)
            return path
        
        if as_text:
            return content.decode("utf-8", errors="replace")
        
        return content
    
    def _patched_download_json(self, url):
        """Patched version of edgar's download_json using curl"""
        return json.loads(self._curl_fetch(url, as_text=True))
    
    def _patched_stream_with_retry(self, url, *args, **kwargs):
        """Patched version of edgar's stream_with_retry using curl"""
        content = self._curl_fetch(url, as_text=False)
        yield FakeResponse(url, content, status_code=200)
    
    def _patch_edgar_http(self):
        """
        Patch edgar library's HTTP functions to use curl-based proxy requests.
        This replaces httpx-based requests with curl subprocess calls.
        """
        print(f"Patching edgar HTTP functions for proxy: {self.proxy_url}")
        
        # Patch package-level functions in edgar.httprequests
        hr.get_with_retry = self._patched_get_with_retry
        hr.download_file = self._patched_download_file
        hr.download_json = self._patched_download_json
        hr.stream_with_retry = self._patched_stream_with_retry
        
        # Patch directly imported references in other modules
        subs.download_json = self._patched_download_json
        sgml_common.stream_with_retry = self._patched_stream_with_retry
        attachments.get_with_retry = self._patched_get_with_retry
        
        print("Edgar HTTP functions patched successfully")
    
    def fetch_company_financials(self, ticker: str, year: int = None) -> Optional[Dict[str, Any]]:
        """
        Fetch company financial statements using proxy-enabled requests.
        
        Args:
            ticker: Stock ticker symbol
            year: Fiscal year (optional)
            
        Returns:
            Dictionary with company info and financial statements
        """
        print(f"\nFetching {ticker} via proxy: {self.proxy_url}")
        print(f"User: {self.proxy_user}")
        
        # Call parent class method which now uses patched HTTP functions
        return super().fetch_company_financials(ticker, year)


def create_proxy_crawler(config):
    """
    Factory function to create appropriate crawler based on proxy configuration.
    
    Args:
        config: Configuration object
        
    Returns:
        ProxyEdgarCrawler if proxy is configured, otherwise EdgarCrawler
    """
    proxy_url = getattr(config, 'proxy_url', '')
    
    if proxy_url:
        print(f"Creating proxy-enabled EDGAR crawler")
        return ProxyEdgarCrawler(config)
    else:
        print(f"Creating standard EDGAR crawler (no proxy)")
        return EdgarCrawler(config)
