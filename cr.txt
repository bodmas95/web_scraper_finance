"""
OVH crawler - fetches financial PDFs and news articles from OVHcloud's
investor-relations and newsroom pages.

All URLs and user-agent strings are read from config.ini [OVH] / [HEADERS].
No files are saved here; the caller (pipeline) handles persistence.
"""

import re
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from config.config import get_section
from src import http_client
from src.logging import get_logger

logger = get_logger(__name__)

_HEADERS_CFG = get_section("HEADERS")

_HEADERS = {
    "User-Agent": _HEADERS_CFG.get("user_agent", "Mozilla/5.0"),
}

_DOC_TYPES = [
    "press_release",
    "presentation",
    "consolidated_financial_statements",
]


class OVHCrawler:
    """
    Fetches financial PDFs and news articles from OVHcloud public pages.

    Args:
        company  : company name (informational, used in log messages)
        url      : base URL to crawl (from the source record)
        year     : fiscal year to filter on, or None to fetch all available years
        filter   : optional dict of additional filters (from source.filters)
        metadata : optional dict passed through to returned records
    """

    def __init__(
        self,
        company: str,
        url: str,
        year: int | None = None,
        filter: dict | None = None,
        metadata: dict | None = None,
    ):
        self.company  = company
        self.url      = url
        self.year     = year
        self.filter   = filter or {}
        self.metadata = metadata or {}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get(self, url: str, params: dict = None) -> http_client.HttpResponse:
        resp = http_client.get(url, headers=_HEADERS, params=params, timeout=30)
        resp.raise_for_status()
        resp.encoding = "utf-8"
        return resp

    def _classify_report(self, row_label: str) -> tuple[str, str]:
        """Return (report_type, short_name) based on the row label text."""
        label = row_label.lower()
        if "annual" in label:
            return "annual_results", "annual_results"
        if "q1" in label:
            return "interim_report", "Q1_revenue"
        if "q3" in label:
            return "interim_report", "Q3_revenue"
        if "half" in label:
            return "interim_report", "half_year_results"
        return "other", label.replace(" ", "_")

    # ------------------------------------------------------------------
    # Financial PDFs
    # ------------------------------------------------------------------

    def _parse_pdf_links(self, html: str) -> list[dict]:
        """
        Parse the financial results page and return a list of report entries.

        Each entry:
        {
            "fiscal_year": "FY2025",
            "year": 2025,
            "report_type": "annual_results",
            "report_name": "annual_results",
            "documents": {
                "press_release": {"url": "...", "filename": "..."},
                ...
            }
        }
        """
        soup    = BeautifulSoup(html, "html.parser")
        results = []

        for fy_text_node in soup.find_all(string=re.compile(r"FY\d{4}")):
            raw = fy_text_node.get_text() if hasattr(fy_text_node, "get_text") else str(fy_text_node)
            fy_match = re.search(r"FY(\d{4})", raw)
            if not fy_match:
                continue
            fiscal_year = int(fy_match.group(1))

            if self.year and fiscal_year != self.year:
                continue

            heading = fy_text_node.find_parent() or fy_text_node

            table = None
            for sibling in heading.find_all_next():
                if sibling.name == "table":
                    table = sibling
                    break
                if sibling.string and re.search(r"FY\d{4}", str(sibling.string)):
                    break

            if table is None:
                continue

            for row in table.find_all("tr"):
                cells = row.find_all(["td", "th"])
                if not cells:
                    continue

                row_label   = cells[0].get_text(strip=True)
                report_type, short_name = self._classify_report(row_label)
                if report_type == "other":
                    continue

                pdf_urls = []
                for cell in cells[1:4]:
                    link = cell.find("a", href=True)
                    if link and "pdf" in link.get_text(strip=True).lower():
                        pdf_urls.append(urljoin(self.url, link["href"]))
                    else:
                        pdf_urls.append(None)

                while len(pdf_urls) < 3:
                    pdf_urls.append(None)

                documents = {}
                for doc_type, pdf_url in zip(_DOC_TYPES, pdf_urls):
                    if not pdf_url:
                        continue
                    if report_type == "interim_report":
                        filename = f"FY{fiscal_year}_interim_{short_name}_{doc_type}.pdf"
                    else:
                        filename = f"FY{fiscal_year}_{short_name}_{doc_type}.pdf"
                    documents[doc_type] = {"url": pdf_url, "filename": filename}

                if documents:
                    if report_type == "interim_report":
                        filing_id = f"FY{fiscal_year}_{short_name}_interim_report"
                    else:
                        filing_id = f"FY{fiscal_year}_{short_name}"
                    results.append({
                        "fiscal_year":      f"FY{fiscal_year}",
                        "year":             fiscal_year,
                        "report_type":      report_type,
                        "report_name":      short_name,
                        "source_filing_id": filing_id,
                        "documents":        documents,
                    })

        return results

    def fetch_pdfs(self) -> list[dict]:
        """
        Fetch the financial results page and download all matching PDFs.

        Returns a flat list of file dicts:
        {
            "url":         "...",
            "filename":    "FY2025_annual_results_press_release.pdf",
            "bytes":       b"...",
            "report_type": "annual_results",
            "fiscal_year": "FY2025",
            "doc_type":    "press_release",
        }
        """
        logger.info("Fetching financial results page: %s", self.url)
        html    = self._get(self.url).text
        entries = self._parse_pdf_links(html)
        logger.info("Found %d report entries", len(entries))

        files = []
        for entry in entries:
            for doc_type, doc in entry["documents"].items():
                pdf_url  = doc["url"]
                filename = doc["filename"]
                logger.debug("Downloading %s", filename)
                try:
                    resp = http_client.get(pdf_url, headers=_HEADERS, timeout=60)
                    resp.raise_for_status()
                    data = b"".join(resp.iter_content(chunk_size=8192))
                    files.append({
                        "url":              pdf_url,
                        "filename":         filename,
                        "bytes":            data,
                        "report_type":      entry["report_type"],
                        "report_name":      entry["report_name"],
                        "fiscal_year":      entry["fiscal_year"],
                        "doc_type":         doc_type,
                        "source_filing_id": entry["source_filing_id"],
                    })
                    logger.info("Downloaded %s (%d bytes)", filename, len(data))
                except requests.RequestException as exc:
                    logger.error("Failed to download %s: %s", filename, exc)

        return files

    # ------------------------------------------------------------------
    # News articles
    # ------------------------------------------------------------------

    def _parse_news_listing(self, html: str, year_strs: set[str]) -> list[dict]:
        soup     = BeautifulSoup(html, "html.parser")
        articles = []
        seen     = set()

        for a_tag in soup.find_all("a", href=re.compile(r"/en/newsroom/news/[^?]")):
            href = a_tag["href"]
            if href in seen:
                continue
            seen.add(href)

            parts = a_tag.get_text(separator="|", strip=True).split("|")
            if len(parts) < 2:
                continue

            date_str = parts[0].strip()
            title    = parts[1].strip() if len(parts) > 1 else ""
            summary  = parts[2].strip() if len(parts) > 2 else ""

            date_match = re.search(r"(\d{2}/\d{2}/(\d{4}))", date_str)
            if not date_match:
                continue
            if date_match.group(2) not in year_strs:
                continue

            raw_date = date_match.group(1)           # "MM/DD/YYYY"
            parts    = raw_date.split("/")
            iso_date = f"{parts[2]}-{parts[0]}-{parts[1]}" if len(parts) == 3 else raw_date
            slug     = re.sub(r"[^a-z0-9]+", "-", title.lower())[:60].strip("-")
            articles.append({
                "date":             raw_date,
                "date_raw":         date_str,
                "title":            title,
                "summary":          summary,
                "url":              href,
                "content":          None,
                "source_filing_id": f"news_{iso_date}_{slug}",
            })

        return articles

    def _fetch_article_content(self, url: str) -> str:
        resp = self._get(url)
        soup = BeautifulSoup(resp.text, "html.parser")

        for tag in soup(["nav", "header", "footer", "script", "style", "noscript"]):
            tag.decompose()

        main_div = (
            soup.find("div", class_="dialog-off-canvas-main-canvas")
            or soup.find("main")
            or soup
        )
        text  = main_div.get_text(separator="\n", strip=True)
        lines = text.split("\n")

        start_idx = 0
        end_idx   = len(lines)

        for i, line in enumerate(lines):
            if re.search(r"(Mon|Tue|Wed|Thu|Fri|Sat|Sun), \d{2}/\d{2}/\d{4}", line):
                start_idx = i + 1
                while start_idx < len(lines) and (
                    re.match(r"^\d{4}$", lines[start_idx].strip())
                    or lines[start_idx].strip() in ("Press Release", "News", "Press release")
                ):
                    start_idx += 1
                break

        for i in range(len(lines) - 1, start_idx, -1):
            if "Back to top" in lines[i] or lines[i].strip() == "Partager":
                end_idx = i
                break

        return "\n".join(l.strip() for l in lines[start_idx:end_idx] if l.strip())

    def fetch_news(self, years: list[int] | None = None) -> list[dict]:
        """
        Fetch news articles from OVHcloud's newsroom.

        Args:
            years: list of calendar years to include; defaults to [self.year]
                   if self.year is set, otherwise the current year.

        Returns a list of article dicts, each with keys:
            date, date_raw, title, summary, url, content
        """
        if years is None:
            years = [self.year] if self.year else [2025, 2026]

        news_url  = self.url
        year_strs = {str(y) for y in years}

        params = {f"year[{y}]": str(y) for y in years}
        params["article_type[News]"] = "News"

        logger.info("Fetching news listing for years %s", years)
        resp = http_client.get(news_url, headers=_HEADERS, params=params, timeout=30)
        resp.raise_for_status()
        articles = self._parse_news_listing(resp.text, year_strs)
        logger.info("Found %d articles", len(articles))

        for i, article in enumerate(articles):
            logger.info("Fetching article %d/%d: %s", i + 1, len(articles), article["title"][:70])
            try:
                article["content"] = self._fetch_article_content(article["url"])
            except requests.RequestException as exc:
                logger.error("Failed to fetch article content: %s", exc)
                article["content"] = ""
            time.sleep(0.5)

        return articles
