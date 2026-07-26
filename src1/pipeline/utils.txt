"""
Local filesystem utilities for the OVH pipeline.

All code that saves files to disk lives here.
The pipeline calls these after obtaining data from the crawler or parser
so that saving logic stays separate from fetching logic.
"""

import json
from pathlib import Path


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def save_bytes(data: bytes, filepath: Path) -> None:
    """Write binary content (PDF, Excel, etc.) to disk."""
    ensure_dir(filepath.parent)
    filepath.write_bytes(data)


def save_text(text: str, filepath: Path) -> None:
    """Write UTF-8 text to disk."""
    ensure_dir(filepath.parent)
    filepath.write_text(text, encoding="utf-8")


def save_json(data: dict | list, filepath: Path) -> None:
    """Write a JSON-serialisable object to disk."""
    ensure_dir(filepath.parent)
    filepath.write_text(
        json.dumps(data, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


def build_article_text(article: dict) -> str:
    """Format a news article dict into a plain-text string ready for storage."""
    lines = [
        f"Title: {article.get('title', '')}",
        f"Date:  {article.get('date_raw', article.get('date', ''))}",
        f"URL:   {article.get('url', '')}",
        "=" * 80,
        "",
        article.get("content", ""),
    ]
    return "\n".join(lines)


def article_filename(article: dict) -> str:
    """
    Build a filesystem-safe filename for a news article.
    Format: YYYY-MM-DD_slug.txt
    """
    import re
    date = article.get("date", "")
    parts = date.split("/")
    if len(parts) == 3:
        date_prefix = f"{parts[2]}-{parts[0]}-{parts[1]}"
    else:
        date_prefix = "unknown-date"
    slug = re.sub(r"[^a-z0-9]+", "-", article.get("title", "").lower())[:60].strip("-")
    return f"{date_prefix}_{slug}.txt"
