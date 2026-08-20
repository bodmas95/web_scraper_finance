class ProxyBase:
    """
    DEPRECATED: This class is kept for backward compatibility only.
    
    Proxy configuration should be controlled via config.ini [PROXY] section
    and http_client module. Do not use this class for new code.
    
    For EDGAR, proxy is configured via httpx client patching in streamlit_app.py
    """
    def __init__(self, http_proxy: str = None, https_proxy: str = None):
        self.http_proxy = http_proxy or ""
        self.https_proxy = https_proxy or ""

    def set_proxy(self):
        """
        DEPRECATED: Do not use. Proxy should be controlled via config.ini.
        This method does nothing now.
        """
        # Do nothing - proxy is controlled via config.ini and http_client
        pass
