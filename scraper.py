"""
OVHcloud Financial Results & News Scraper

Scrapes:
1. Annual results PDFs (Press Release, Presentation, Consolidated Financial Statements)
2. Interim report PDFs (Q1 Revenue, Q3 Revenue - Press Release only)
3. News articles (2025-2026) in text format

From https://corporate.ovhcloud.com/en/investor-relations/financial-results/
and  https://corporate.ovhcloud.com/en/newsroom/news
"""

import json
import os
import re
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

FINANCIAL_RESULTS_URL = "https://corporate.ovhcloud.com/en/investor-relations/financial-results/"
NEWS_URL = "https://corporate.ovhcloud.com/en/newsroom/news"
DOWNLOAD_DIR = "pdfs"
NEWS_DIR = "news"
FINANCIAL_OUTPUT = "financial_results.json"
NEWS_OUTPUT = "news_articles.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

DOC_TYPES = ["press_release", "presentation", "consolidated_financial_statements"]


def fetch_page(url: str) -> str:
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    resp.encoding = "utf-8"
    return resp.text


# ---------------------------------------------------------------------------
# PART 1: Financial Results (Annual + Interim)
# ---------------------------------------------------------------------------

def classify_report(row_label: str) -> tuple[str, str]:
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


def parse_financial_results(html: str) -> list[dict]:
    """Parse the financial results page and extract PDF links for all report types."""
    soup = BeautifulSoup(html, "html.parser")
    results = []

    fy_headings = soup.find_all(string=re.compile(r"FY\d{4}"))

    for fy_text_node in fy_headings:
        fy_match = re.search(
            r"FY(\d{4})",
            fy_text_node.get_text() if hasattr(fy_text_node, "get_text") else str(fy_text_node),
        )
        if not fy_match:
            continue
        fiscal_year = int(fy_match.group(1))

        heading = fy_text_node.find_parent()
        if heading is None:
            heading = fy_text_node

        # Find the table following this heading
        table = None
        for sibling in heading.find_all_next():
            if sibling.name == "table":
                table = sibling
                break
            if sibling.string and re.search(r"FY\d{4}", str(sibling.string)):
                break

        if table is None:
            continue

        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            if not cells:
                continue

            row_label = cells[0].get_text(strip=True)
            report_type, short_name = classify_report(row_label)

            if report_type == "other":
                continue

            # Extract PDF links from the data columns
            pdf_links = []
            for cell in cells[1:4]:
                link = cell.find("a", href=True)
                if link and "pdf" in link.get_text(strip=True).lower():
                    pdf_links.append(urljoin(FINANCIAL_RESULTS_URL, link["href"]))
                else:
                    pdf_links.append(None)

            while len(pdf_links) < 3:
                pdf_links.append(None)

            entry = {
                "fiscal_year": f"FY{fiscal_year}",
                "year": fiscal_year,
                "report_type": report_type,
                "report_name": short_name,
                "documents": {},
            }

            for doc_type, url in zip(DOC_TYPES, pdf_links):
                if url:
                    entry["documents"][doc_type] = {
                        "url": url,
                        "local_path": None,
                    }

            if entry["documents"]:
                results.append(entry)

    return results


def download_pdfs(results: list[dict]) -> None:
    """Download all PDFs with naming: FY{year}_{report_type}_{report_name}_{doc_type}.pdf"""
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    for entry in results:
        fy = entry["fiscal_year"]
        report_name = entry["report_name"]
        report_type = entry["report_type"]

        for doc_type, doc_info in entry["documents"].items():
            url = doc_info["url"]

            # Build descriptive filename
            # e.g. FY2025_interim_Q3_revenue_press_release.pdf
            #      FY2025_annual_results_presentation.pdf
            if report_type == "interim_report":
                filename = f"{fy}_interim_{report_name}_{doc_type}.pdf"
            else:
                filename = f"{fy}_{report_name}_{doc_type}.pdf"

            filepath = os.path.join(DOWNLOAD_DIR, filename)

            if os.path.exists(filepath):
                doc_info["local_path"] = filepath
                print(f"  [SKIP] {filepath} (already exists)")
                continue

            print(f"  Downloading {fy} {report_name} - {doc_type}...")
            try:
                resp = requests.get(url, headers=HEADERS, timeout=60, stream=True)
                resp.raise_for_status()
                with open(filepath, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)
                doc_info["local_path"] = filepath
                print(f"    -> Saved to {filepath}")
            except requests.RequestException as e:
                print(f"    -> FAILED: {e}")


# ---------------------------------------------------------------------------
# PART 2: News Articles
# ---------------------------------------------------------------------------

def fetch_news_listing(years: list[int]) -> list[dict]:
    """Fetch the news listing page filtered by years and article_type=News."""
    params = {}
    for y in years:
        params[f"year[{y}]"] = str(y)
    params["article_type[News]"] = "News"

    resp = requests.get(NEWS_URL, headers=HEADERS, params=params, timeout=30)
    resp.raise_for_status()
    resp.encoding = "utf-8"
    soup = BeautifulSoup(resp.text, "html.parser")

    articles = []
    seen = set()
    year_strs = {str(y) for y in years}

    for a_tag in soup.find_all("a", href=re.compile(r"/en/newsroom/news/[^?]")):
        href = a_tag["href"]
        if href in seen:
            continue
        seen.add(href)

        text_parts = a_tag.get_text(separator="|", strip=True).split("|")
        if len(text_parts) < 2:
            continue

        date_str = text_parts[0].strip()
        title = text_parts[1].strip() if len(text_parts) > 1 else ""
        summary = text_parts[2].strip() if len(text_parts) > 2 else ""

        # Parse date: "Mon, 02/09/2026 - 12:00"
        date_match = re.search(r"(\d{2}/\d{2}/(\d{4}))", date_str)
        if not date_match:
            continue

        # Filter: only keep articles from requested years
        article_year = date_match.group(2)
        if article_year not in year_strs:
            continue

        articles.append({
            "date": date_match.group(1),
            "date_raw": date_str,
            "title": title,
            "summary": summary,
            "url": href,
            "content": None,
        })

    return articles


def fetch_article_content(url: str) -> str:
    """Fetch full article text from an article detail page."""
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    resp.encoding = "utf-8"
    soup = BeautifulSoup(resp.text, "html.parser")

    # Remove non-content elements
    for tag in soup(["nav", "header", "footer", "script", "style", "noscript"]):
        tag.decompose()

    # Find the main content container
    main_div = soup.find("div", class_="dialog-off-canvas-main-canvas")
    if not main_div:
        main_div = soup.find("main") or soup

    text = main_div.get_text(separator="\n", strip=True)

    # Extract content between the date line and "Back to top"
    # The article body starts after the date/tags line
    lines = text.split("\n")
    start_idx = 0
    end_idx = len(lines)

    # Find the start: after the date line (e.g., "Mon, 02/09/2026 - 12:00")
    for i, line in enumerate(lines):
        if re.search(r"(Mon|Tue|Wed|Thu|Fri|Sat|Sun), \d{2}/\d{2}/\d{4}", line):
            # Skip date and year/category tags that follow
            start_idx = i + 1
            # Skip tag lines like "2026", "Press Release", "News" that follow the date
            while start_idx < len(lines) and (
                re.match(r"^\d{4}$", lines[start_idx].strip())
                or lines[start_idx].strip() in ("Press Release", "News", "Press release")
            ):
                start_idx += 1
            break

    # Find the end: "Back to top" or "Partager"
    for i in range(len(lines) - 1, start_idx, -1):
        if "Back to top" in lines[i] or lines[i].strip() == "Partager":
            end_idx = i
            break

    content_lines = [l.strip() for l in lines[start_idx:end_idx] if l.strip()]
    return "\n".join(content_lines)


def save_news_articles(articles: list[dict]) -> None:
    """Save each news article as a separate .txt file and the index as JSON."""
    os.makedirs(NEWS_DIR, exist_ok=True)

    for article in articles:
        # Create filename from date and title slug
        date_parts = article["date"].split("/")  # MM/DD/YYYY
        date_prefix = f"{date_parts[2]}-{date_parts[0]}-{date_parts[1]}"
        slug = re.sub(r"[^a-z0-9]+", "-", article["title"].lower())[:60].strip("-")
        filename = f"{date_prefix}_{slug}.txt"
        filepath = os.path.join(NEWS_DIR, filename)

        content = article.get("content") or ""
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(f"Title: {article['title']}\n")
            f.write(f"Date: {article['date_raw']}\n")
            f.write(f"URL: {article['url']}\n")
            f.write(f"{'=' * 80}\n\n")
            f.write(content)

        article["local_path"] = filepath
        print(f"  Saved: {filepath}")


def save_metadata(data: list[dict], output_file: str) -> None:
    """Save structured metadata as JSON."""
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)
    print(f"\nMetadata saved to {output_file}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    # --- Financial Results ---
    print("=" * 60)
    print("PART 1: Financial Results (Annual + Interim)")
    print("=" * 60)

    print(f"\nFetching {FINANCIAL_RESULTS_URL} ...")
    html = fetch_page(FINANCIAL_RESULTS_URL)

    print("Parsing financial results...")
    financial_results = parse_financial_results(html)

    if not financial_results:
        print("No financial results found.")
    else:
        annual = [r for r in financial_results if r["report_type"] == "annual_results"]
        interim = [r for r in financial_results if r["report_type"] == "interim_report"]
        print(f"\nFound {len(annual)} annual result(s) and {len(interim)} interim report(s):")
        for entry in financial_results:
            docs = ", ".join(entry["documents"].keys())
            print(f"  {entry['fiscal_year']} [{entry['report_name']}]: {docs}")

        print("\nDownloading PDFs...")
        download_pdfs(financial_results)
        save_metadata(financial_results, FINANCIAL_OUTPUT)

    # --- News ---
    print("\n" + "=" * 60)
    print("PART 2: News Articles (2025-2026)")
    print("=" * 60)

    print("\nFetching news listing for 2025 & 2026...")
    articles = fetch_news_listing([2025, 2026])
    print(f"Found {len(articles)} news article(s)")

    print("\nFetching full article content...")
    for i, article in enumerate(articles):
        print(f"  [{i+1}/{len(articles)}] {article['title'][:70]}...")
        try:
            article["content"] = fetch_article_content(article["url"])
        except requests.RequestException as e:
            print(f"    -> FAILED: {e}")
            article["content"] = ""
        # Be polite with request rate
        time.sleep(0.5)

    print("\nSaving news articles as text files...")
    save_news_articles(articles)

    # Save JSON without the full content (it's in the .txt files)
    articles_meta = []
    for a in articles:
        meta = {k: v for k, v in a.items() if k != "content"}
        articles_meta.append(meta)
    save_metadata(articles_meta, NEWS_OUTPUT)

    print("\n" + "=" * 60)
    print("ALL DONE!")
    print("=" * 60)
    print(f"\nPDFs downloaded to: {DOWNLOAD_DIR}/")
    print(f"News articles saved to: {NEWS_DIR}/")
    print(f"Financial metadata: {FINANCIAL_OUTPUT}")
    print(f"News metadata: {NEWS_OUTPUT}")
    print(f"\nMongoDB import commands:")
    print(f"  mongoimport --db ovhcloud --collection financial_results --jsonArray --file {FINANCIAL_OUTPUT}")
    print(f"  mongoimport --db ovhcloud --collection news_articles --jsonArray --file {NEWS_OUTPUT}")


if __name__ == "__main__":
    main()
