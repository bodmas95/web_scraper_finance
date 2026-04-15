#(company name, url, year, filters, metadata)
import re
from bs4 import BeautifulSoup
from config.config import BASE_URL


class HKEXParser:
    """Parser for extracting structured report data from HKEX HTML."""

    @staticmethod
    def extract_year(value: str) -> int | None:
        """Extract fiscal year from string if available."""
        match = re.search(r"(20\d{2})", value)
        return int(match.group(1)) if match else None

    @staticmethod
    def infer_report_type(value: str) -> str:
        """Infer report type from text."""
        lower_value = value.lower()
        if "annual" in lower_value:
            return "annual"
        if "interim" in lower_value:
            return "interim"
        if "quarter" in lower_value:
            return "quarter"
        if "results" in lower_value:
            return "results"
        return "unknown"

    def extract_reports(self, html: bytes) -> list[dict]:
        """
        Extract structured report items from HKEX result page.

        Returns:
            List of dicts with title, url, report_type, fiscal_year, reporting_id.
        """
        soup = BeautifulSoup(html, "html.parser")
        reports = []
        for anchor in soup.find_all("a", href=True):
            href = anchor["href"]
            if ".pdf" not in href.lower():
                continue
            full_url = BASE_URL + href
            filename = href.split("/")[-1]
            title = anchor.get_text(strip=True) or filename
            print(title)

            # Extract year
            year_match = re.search(r"(20\d{2})", title)
            fiscal_year = int(year_match.group(1)) if year_match else None

            # Infer report type
            lower_title = title.lower().strip()

            if "annual" in lower_title:
                report_type = "annual"
            elif "interim" in lower_title:
                report_type = "interim"
            elif "quarter" in lower_title:
                report_type = "quarter"
            elif "results" in lower_title:
                report_type = "results"
            else:
                report_type = "unknown"

            # Create unique reporting id
            reporting_id = f"{fiscal_year}_{report_type}_{filename}"

            #fiscal_year = self.extract_year(title) or self.extract_year(filename)
            #report_type = self.infer_report_type(title) if title else self.infer_report_type(filename)
            #reporting_id = f"{fiscal_year}_{report_type}_{filename}"
            reports.append(
                {
                    "title": title,
                    "url": full_url,
                    "filename": filename,
                    "reportType": report_type,
                    "fiscalYear": fiscal_year,
                    "reportingId": reporting_id,
                }
            )

        return reports