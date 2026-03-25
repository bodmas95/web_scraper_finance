"""
MongoDB ingestion script for OVHcloud financial results and news articles.

Reads the structured JSON output from scraper.py and inserts it into MongoDB.
Optionally stores PDF binary content for full document storage.
"""

import json
import os
import sys

from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("MONGO_DB", "ovhcloud")
FINANCIAL_FILE = "financial_results.json"
NEWS_FILE = "news_articles.json"
NEWS_DIR = "news"


def load_json(filepath: str) -> list[dict]:
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def ingest_financial_results(embed_pdfs: bool = False) -> None:
    """Insert or update financial results in MongoDB."""
    if not os.path.exists(FINANCIAL_FILE):
        print(f"  {FINANCIAL_FILE} not found. Run scraper.py first.")
        return

    records = load_json(FINANCIAL_FILE)
    print(f"  Loaded {len(records)} financial result record(s)")

    client = MongoClient(MONGO_URI)
    collection = client[DB_NAME]["financial_results"]

    for record in records:
        if embed_pdfs:
            from bson import Binary
            for doc_info in record.get("documents", {}).values():
                local_path = doc_info.get("local_path")
                if local_path and os.path.exists(local_path):
                    with open(local_path, "rb") as f:
                        doc_info["pdf_content"] = Binary(f.read())
                    doc_info["file_size_bytes"] = os.path.getsize(local_path)

        # Upsert by fiscal_year + report_name to avoid duplicates
        filter_key = {
            "fiscal_year": record["fiscal_year"],
            "report_name": record["report_name"],
        }
        result = collection.update_one(filter_key, {"$set": record}, upsert=True)
        action = "Updated" if result.modified_count else "Inserted"
        print(f"    {action}: {record['fiscal_year']} - {record['report_name']}")

    print(f"  Total documents in financial_results: {collection.count_documents({})}")
    client.close()


def ingest_news_articles() -> None:
    """Insert or update news articles in MongoDB, including full text content."""
    if not os.path.exists(NEWS_FILE):
        print(f"  {NEWS_FILE} not found. Run scraper.py first.")
        return

    records = load_json(NEWS_FILE)
    print(f"  Loaded {len(records)} news article record(s)")

    client = MongoClient(MONGO_URI)
    collection = client[DB_NAME]["news_articles"]

    for record in records:
        # Load full text content from the .txt file
        local_path = record.get("local_path")
        if local_path and os.path.exists(local_path):
            with open(local_path, "r", encoding="utf-8") as f:
                # Skip the header (title, date, url, separator)
                lines = f.readlines()
                content_start = 0
                for i, line in enumerate(lines):
                    if line.startswith("=" * 10):
                        content_start = i + 1
                        break
                record["content"] = "".join(lines[content_start:]).strip()

        result = collection.update_one(
            {"url": record["url"]},
            {"$set": record},
            upsert=True,
        )
        action = "Updated" if result.modified_count else "Inserted"
        print(f"    {action}: {record['title'][:60]}")

    print(f"  Total documents in news_articles: {collection.count_documents({})}")
    client.close()


def main():
    embed_pdfs = "--embed-pdfs" in sys.argv

    print("=" * 60)
    print("Ingesting Financial Results...")
    print("=" * 60)
    if embed_pdfs:
        print("  (with embedded PDF binary content)")
    ingest_financial_results(embed_pdfs=embed_pdfs)

    print("\n" + "=" * 60)
    print("Ingesting News Articles...")
    print("=" * 60)
    ingest_news_articles()

    print("\nDone!")


if __name__ == "__main__":
    main()
