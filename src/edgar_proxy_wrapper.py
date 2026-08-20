"""
EDGAR API Proxy Wrapper
Patches the edgar-tool library to work with corporate NTLM proxy
"""
import subprocess
import json
import configparser
import os

def setup_edgar_with_proxy():
    """
    Setup EDGAR library to work with corporate proxy using NTLM authentication.
    Reads proxy settings from config.ini and patches edgar library functions.
    """
    # Read config.ini
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    # Check if proxy is enabled
    proxy_use = config.get('PROXY', 'proxy_use', fallback='none')
    
    if proxy_use == 'none':
        print("📡 EDGAR: Direct connection (no proxy)")
        from edgar import set_identity
        identity = config.get('EDGAR', 'identity', fallback='User user@example.com')
        set_identity(identity)
        return
    
    elif proxy_use == 'server':
        # IP-based proxy (no authentication) - Patch httpx to use proxy
        proxy_host = config.get('PROXY', 'server_host')
        proxy_port = config.get('PROXY', 'server_port')
        PROXY = f"http://{proxy_host}:{proxy_port}"
        USER = None
        PASSWORD = None
        print(f"📡 EDGAR: Using server proxy {PROXY} (no auth)")
        
        # Set environment variables (some libraries check these)
        import os
        os.environ['HTTP_PROXY'] = PROXY
        os.environ['HTTPS_PROXY'] = PROXY
        os.environ['http_proxy'] = PROXY
        os.environ['https_proxy'] = PROXY
        
                # Patch httpx.Client to always use proxy
        import httpx
        
        print(f"  Patching httpx.Client.__init__ to use proxy...")
        _original_client_init = httpx.Client.__init__
        
        def patched_client_init(self, *args, **kwargs):
            # Force proxy configuration
            if 'proxy' not in kwargs and 'proxies' not in kwargs:
                kwargs['proxy'] = PROXY
            if 'verify' not in kwargs:
                kwargs['verify'] = False  # Disable SSL verification
            print(f"  Creating httpx.Client with proxy={kwargs.get('proxy')}, verify={kwargs.get('verify')}")
            _original_client_init(self, *args, **kwargs)
        
        httpx.Client.__init__ = patched_client_init
        
        # Also patch any existing clients in edgar module
        try:
            import edgar.httprequests as hr
            print(f"  Checking for existing edgar.httprequests.client...")
            # Force recreate the client with proxy
            if hasattr(hr, 'client'):
                print(f"  Found existing client, recreating with proxy...")
                hr.client = httpx.Client(proxy=PROXY, verify=False)
            else:
                print(f"  No existing client found")
        except Exception as e:
            print(f"  Error patching edgar.httprequests: {e}")
        
        from edgar import set_identity
        identity = config.get('EDGAR', 'identity', fallback='User user@example.com')
        set_identity(identity)
        
                
        print(f"✅ EDGAR configured with server proxy (httpx patched)")
        print(f"   Proxy: {PROXY}")
        print(f"   User-Agent: {identity}")
        print(f"   SSL Verification: Disabled")
        return  # Exit early - no need to patch with curl
        
    elif proxy_use == 'system':
        # NTLM proxy (with authentication)
        proxy_host = config.get('PROXY', 'corporate_host')
        proxy_port = config.get('PROXY', 'corporate_port')
        USER = config.get('PROXY', 'corporate_username')
        PASSWORD = config.get('PROXY', 'corporate_password')
        PROXY = f"http://{proxy_host}:{proxy_port}"
        print(f"📡 EDGAR: Using corporate proxy {PROXY} with NTLM auth")
    
    else:
        raise ValueError(f"Invalid proxy_use value: {proxy_use}")
    
    # Get user agent
    UA = config.get('EDGAR', 'identity', fallback='User user@example.com')
    
    # Define curl-based fetch function
    def curl_fetch(url: str, as_text=False):
        """Fetch URL using curl with NTLM proxy authentication"""
        cmd = [
            "curl",
            "--silent",
            "--show-error",
            "--location",
            "--ssl-no-revoke",
        ]
        
        # Add proxy authentication if using NTLM
        if proxy_use == 'system' and USER and PASSWORD:
            cmd.extend([
                "--proxy-ntlm",
                "-U", f"{USER}:{PASSWORD}",
            ])
        
        # Add proxy
        cmd.extend([
            "--proxy", PROXY,
            "-A", UA,
            url,
        ])
        
        res = subprocess.run(cmd, capture_output=True)
        if res.returncode != 0:
            raise RuntimeError(
                f"Curl failed for {url}\n"
                f"returncode={res.returncode}\n"
                f"stderr={res.stderr.decode(errors='replace')}"
            )
        return res.stdout.decode("utf-8", errors="replace") if as_text else res.stdout
    
    # Define FakeResponse class to mimic requests.Response
    class FakeResponse:
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
    
    # Define patched functions
    def patched_get_with_retry(url, *args, **kwargs):
        content = curl_fetch(url, as_text=False)
        return FakeResponse(url, content, status_code=200)
    
    def patched_download_file(url, as_text=None, path=None):
        content = curl_fetch(url, as_text=False)
        if path:
            with open(path, "wb") as f:
                f.write(content)
            return path
        if as_text:
            return content.decode("utf-8", errors="replace")
        return content
    
    def patched_download_json(url):
        return json.loads(curl_fetch(url, as_text=True))
    
    def patched_stream_with_retry(url, *args, **kwargs):
        content = curl_fetch(url, as_text=False)
        yield FakeResponse(url, content, status_code=200)
    
    # Import edgar modules and patch them
    import edgar.httprequests as hr
    import edgar.entity.submissions as subs
    import edgar.sgml.sgml_common as sgml_common
    import edgar.attachments as attachments
    from edgar import set_identity
    
    # Patch package-level functions
    hr.get_with_retry = patched_get_with_retry
    hr.download_file = patched_download_file
    hr.download_json = patched_download_json
    hr.stream_with_retry = patched_stream_with_retry
    
    # Patch directly imported references in other modules
    subs.download_json = patched_download_json
    sgml_common.stream_with_retry = patched_stream_with_retry
    attachments.get_with_retry = patched_get_with_retry
    
    # Set identity
    set_identity(UA)
    
    print(f"✅ EDGAR library patched successfully with proxy support")
    print(f"   User-Agent: {UA}")


# Example usage
# if __name__ == "__main__":
#     # Setup proxy BEFORE importing Company
#     setup_edgar_with_proxy()
    
#     # Now import and use Company
#     from edgar import Company
    
#     # Test with Apple
#     print("\n🔍 Testing EDGAR API with Apple (AAPL)...")
#     company = Company('AAPL')
    
#     print(f"\n📊 Company: {company.name}")
#     print(f"   CIK: {company.cik}")
#     print(f"   Shares Outstanding: {company.shares_outstanding:,.0f}")
#     print(f"   Public Float: ${company.public_float:,.0f}")
    
#     print("\n📈 Fetching financial statements...")
#     income_stmt = company.income_statement()
#     print(f"   Income Statement: {len(income_stmt)} rows")
    
#     balance_sheet = company.balance_sheet()
#     print(f"   Balance Sheet: {len(balance_sheet)} rows")
    
#     cash_flow = company.cashflow_statement()
#     print(f"   Cash Flow: {len(cash_flow)} rows")
    
#     print("\n✅ EDGAR API test successful!")
