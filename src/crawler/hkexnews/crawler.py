from src.crawler.base import BaseCrawler
#from src.crawler.proxy_base import proxy_request
from config.config import HEADERS, PROXIES
from src import http_client


class HKEXCrawler(BaseCrawler):
    """Crawler for fetching HKEX data using source configuration from MongoDB."""

    def __init__(self):
        super().__init__(HEADERS)

    def fetch_data(
        self,
        source_url: str,
        source_filters: dict,
        stock_id: str,
        start_date: str,
        end_date: str
    ) -> bytes:
        """
        Fetch HKEX search result page.

        Args:
            source_url: URL from sources collection.
            source_filters: Base filters from sources collection.
            stock_id: HKEX stock id from companies collecion.
            start_date: start date in YYYYMMDD format.
            end_date: End date in YYYYMMDD format.

        Returns:
            Raw HTML response.
        """
        #payload = dict(source_filters)
        payload = {
            "lang": source_filters.get("language", "EN"),
            "category": "0",
            "market": "SEHK",
            "searchType": "1",
            "documentType": "",
            "t1code": "40000",
            "t2Gcode": "-2",
            "t2code": "40100",
            "stockId": stock_id,
            "from": start_date,
            "to": end_date,
            "MB-Daterange": "0",
            "title": ""
        }

        response = self.post(url=source_url, data=payload)
        body = response.content
        return body