from src.http_client import get, post

class BaseCrawler:
    def __init__(self, headers):
        self.headers = headers

    def get(self, url, headers=None, params=None, timeout=30):
        response = get(
            url=url,
            headers=headers,
            params=params,
            timeout=timeout
        )
        response.raise_for_status()
        return response

    def post(self, url, headers=None, json=None, data=None, timeout=30):
        response = post(
            url=url,
            headers=headers,
            json=json,
            data=data,
            timeout=timeout
        )
        response.raise_for_status()
        return response