import os

class ProxyBase:
    def __init__(self, http_proxy: str, https_proxy: str):
        self.http_proxy = http_proxy
        self.https_proxy = https_proxy

    def set_proxy(self):
        os.environ["HTTP_PROXY"] = self.http_proxy
        os.environ["HTTPS_PROXY"] = self.https_proxy
        os.environ["http_proxy"] = self.http_proxy
        os.environ["https_proxy"] = self.https_proxy