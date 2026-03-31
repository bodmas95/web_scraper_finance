# Project Understanding: Financial Data Ingestion Pipeline

## What This Project Does

This is a multi-company financial data ingestion pipeline. It fetches financial documents and structured data from public sources (company websites, regulatory APIs), stores files in MongoDB GridFS, and writes structured metadata records to MongoDB collections for downstream consumption.

Currently supported companies: **OVHcloud** and **HKEX** (stub).

---

## How to Run

```bash
# Single company
python -m src.main --company ovh
python -m src.main --company hkex

# All companies
python -m src.main --company all
```

The pipeline reads from `companies` and `sources` collections in MongoDB, dispatches each source to the appropriate handler, and writes results to `reports`, `ingestionLogs`, and GridFS.

---

## Folder Structure

```
ovhcloud_package/
├── config.ini                        All settings: URLs, MongoDB, proxy, logging
├── config/
│   └── config.py                     load_config(), get_section(), get_mongo_uri()
├── src/
│   ├── main.py                       Entry point — argparse, DB connect, dispatch loop
│   ├── logging.py                    Shared logging module (console + rotating file)
│   ├── http_client.py                Unified HTTP GET (none / server / system proxy)
│   ├── utils.py                      (empty — reserved)
│   ├── crawler/
│   │   ├── ovh/
│   │   │   └── crawler.py            OVHCrawler — fetch_pdfs() and fetch_news()
│   │   └── hkex/
│   │       └── crawler.py            HKEX crawler (pre-existing)
│   ├── parser/
│   │   ├── ovh/
│   │   │   └── parser.py             XBRL parser — run() → dict of produced file paths
│   │   └── hkex/
│   │       └── parser.py             HKEX parser (pre-existing)
│   └── pipeline/
│       ├── db.py                     MongoDBClient context manager
│       ├── db_utils.py               MongoDB and GridFS read/write helpers
│       ├── utils.py                  Local filesystem helpers
│       ├── ovh/
│       │   ├── company_web_pipeline.py   WEB + NEWS handler for OVH
│       │   └── company_api_pipeline.py   API (XBRL) handler for OVH
│       └── hkex/
│           ├── company_web_pipeline.py   Stub
│           └── company_api_pipeline.py   Stub
└── ovhcloud_filings/                 Local file cache (PDFs, XBRL files, news)
    ├── FY2025/
    ├── FY2024/
    ├── ...
    └── news/
```

---

## config.ini Sections

| Section | Purpose |
|---|---|
| `[env]` | Sets the active environment name (e.g. `uat`) — `get_mongo_uri()` reads from that section |
| `[uat]` | MongoDB credentials: host, port, username, password, database, auth_source |
| `[OVH]` | LEI, API base URL, investor-relations URL, newsroom URL, download dir, output filename |
| `[HKEX]` | Base URL and download dir for HKEX |
| `[HEADERS]` | Browser User-Agent for web crawling |
| `[PROXY]` | Proxy mode and credentials (see proxy section below) |
| `[LOGGING]` | Log level, log directory, max file size, backup count |

---

## Proxy Settings (`config.ini [PROXY]`)

```ini
proxy_use = none    # Direct requests — no proxy
proxy_use = server  # IP-based proxy via requests (system_host:system_port, no auth)
proxy_use = system  # NTLM corporate proxy via pycurl (host:port with user/pass)
```

**`proxy_use = server`** → `requests` library uses `system_host` / `system_port` as a plain HTTP proxy. No authentication required. Used when behind a simple IP-based network proxy.

**`proxy_use = system`** → `pycurl` library connects through the corporate proxy at `host:port` using NTLM authentication (`user` / `pass`). Used when behind a corporate intranet proxy that requires Windows domain credentials.

The backend is selected automatically at startup. All crawlers and parsers call `src.http_client.get()` and never import `requests` or `pycurl` directly.

---

## Logging (`src/logging.py`)

A shared logging module, works for all companies and all pipeline stages.

```python
from src.logging import get_logger
logger = get_logger(__name__)   # use in any module

logger.info("Pipeline started for %s", company_name)
logger.warning("No data for FY%s", year)
logger.error("Download failed: %s", exc, exc_info=True)
```

On first call, `get_logger()` configures the root logger with two handlers:
- **Console** — writes to stdout, level from config
- **Rotating file** — writes to `logs/pipeline.log`, rotates at 10 MB, keeps 5 backups

Log format:
```
2026-04-01 12:00:00 | INFO     | src.crawler.ovh.crawler | Fetching financial results page: ...
```

Level and paths are configurable in `config.ini [LOGGING]`. The module is safe to import from multiple places — it configures root logger only once.

---

## MongoDB Collections

The pipeline reads from two collections and writes to three:

| Collection | R/W | Purpose |
|---|---|---|
| `companies` | Read | Company identity (name, aliases, LEI, sector) |
| `sources` | Read | What to fetch: URL, sourceType, filters, exchange |
| `reports` | Write | One document per ingested filing or article |
| `ingestionLogs` | Write | One document per pipeline run (running → success/error) |
| `fs.files` / `fs.chunks` | Write | GridFS binary storage for all files |

### `reports` Document Schema

```json
{
  "_id": ObjectId,
  "companyId": "...",
  "sourceId": "...",
  "exchange": "CORPORATE_WEBSITE",
  "source": "OVH_INVESTOR_RELATIONS",
  "sourceFilingId": "FY2025_annual_results",
  "reportType": "annual_results",
  "fiscalYear": "FY2025",
  "status": "active",
  "files": [
    {
      "fileId": "GridFS ObjectId",
      "format": "PDF",
      "language": "en",
      "url": "https://...",
      "downloadStatus": "success",
      "downloadedAt": "2026-04-01T..."
    }
  ],
  "createdAt": ISODate,
  "updatedAt": ISODate
}
```

### `ingestionLogs` Document Schema

```json
{
  "_id": ObjectId,
  "sourceId": "...",
  "companyId": "...",
  "result": {
    "status": "success",
    "errorCode": null,
    "errorMessage": null
  },
  "report": {
    "fiscalYear": "FY2025",
    "reportType": "annual_results",
    "exchange": "CORPORATE_WEBSITE",
    "urls": ["https://..."]
  },
  "files": [
    {
      "fileId": "GridFS ObjectId",
      "format": "PDF",
      "fileName": "FY2025_annual_results_press_release.pdf",
      "url": "https://...",
      "mimeType": "application/pdf",
      "sizeBytes": 1234567,
      "downloadStatus": "success",
      "downloadErrorMessage": null
    }
  ],
  "parse": {
    "status": "pending",
    "parser": null,
    "parsedAt": null,
    "errorMessage": null
  },
  "createdAt": ISODate,
  "updatedAt": ISODate
}
```

---

## `sourceFilingId` — How It Is Generated

`sourceFilingId` uniquely identifies a **filing event** (not an individual file). Multiple files belonging to the same event (press release + presentation + financial statements) share the same `sourceFilingId`.

### WEB (PDF) Sources

Built in `OVHCrawler._parse_pdf_links()` from fiscal year and report type:

| Filing event | `sourceFilingId` |
|---|---|
| Annual results | `FY2025_annual_results` |
| Q1 revenue (interim) | `FY2026_Q1_revenue_interim_report` |
| Q3 revenue (interim) | `FY2025_Q3_revenue_interim_report` |
| Half-year results (interim) | `FY2025_half_year_results_interim_report` |

Formula:
- Annual: `{fiscal_year}_{report_name}`
- Interim: `{fiscal_year}_{report_name}_interim_report`

### NEWS Sources

Built in `OVHCrawler._parse_news_listing()` from the article date and title:

```
news_2026-01-15_ovhcloud-launches-new-service
```

Formula: `news_{YYYY-MM-DD}_{title-slug-60-chars}`

### API (XBRL) Sources

| File set | `sourceFilingId` |
|---|---|
| Per-year XBRL files | `FY2025_XBRL` |
| API filings listing JSON | `None` |
| Consolidated Excel | `None` |

---

## Source Types and Pipeline Routing

| `sourceType` in DB | URL pattern | Pipeline called | What it does |
|---|---|---|---|
| `WEB` | investor-relations URL | `run_web_pipeline()` | Downloads financial PDFs, saves to GridFS |
| `NEWS` | any | `run_news_pipeline()` | Fetches news articles, stores text in GridFS |
| `WEB` | URL contains `newsroom`/`/news/` | `run_news_pipeline()` | Auto-detected: routes to news even if stored as WEB |
| `API` | filings.xbrl.org | `run()` in company_api_pipeline | Runs XBRL parser, ingests all produced files |

The auto-detection for `WEB` + newsroom URL handles the common case where the source document was saved with `sourceType=WEB` instead of `NEWS` in the database.

---

## OVH WEB/NEWS Pipeline (`company_web_pipeline.py`)

### `run_web_pipeline(db, company, source)`

1. Inserts an ingestion log with `status=running`
2. Instantiates `OVHCrawler` with the source URL
3. Calls `crawler.fetch_pdfs()` → list of file dicts
4. For each PDF:
   - Saves locally to `ovhcloud_filings/{fiscal_year}/{filename}`
   - Stores in GridFS → `file_id`
   - Inserts a `reports` document with `sourceFilingId` derived from the filing event
5. Updates ingestion log with `status=success` and file records

### `run_news_pipeline(db, company, source)`

1. Inserts an ingestion log with `status=running`
2. Instantiates `OVHCrawler`
3. Calls `crawler.fetch_news(years=...)` → list of article dicts
4. For each article:
   - Formats as plain text (Title / Date / URL / content)
   - Saves locally to `ovhcloud_filings/news/{YYYY-MM-DD_slug}.txt`
   - Stores UTF-8 text in GridFS → `file_id`
   - Inserts a `reports` document with `sourceFilingId = news_{date}_{slug}`
5. Updates ingestion log with `status=success`

---

## OVH API Pipeline (`company_api_pipeline.py`)

Runs the full XBRL extraction and ingests every produced file:

| File | GridFS stored name | Report type |
|---|---|---|
| `api_filings.json` | `api_filings.json` | `api_filings_listing` |
| `ixbrlviewer.html` | `FY2025_ixbrlviewer.html` | `xbrl_filing` |
| `viewer_data.json` | `FY2025_viewer_data.json` | `xbrl_filing` |
| `report.json` | `FY2025_report.json` | `xbrl_filing` |
| `ovhcloud_complete_financials.xlsx` | `ovhcloud_complete_financials.xlsx` | `Financial_Components` |

The parser (`src/parser/ovh/parser.py`) returns a dict:
```python
{
    "excel":       "/abs/path/ovhcloud_complete_financials.xlsx",
    "api_listing": "/abs/path/ovhcloud_filings/api_filings.json",
    "per_year": {
        "FY2025": {
            "viewer_html": "/abs/path/ovhcloud_filings/FY2025/ixbrlviewer.html",
            "viewer_json": "/abs/path/ovhcloud_filings/FY2025/viewer_data.json",
            "oim_json":    "/abs/path/ovhcloud_filings/FY2025/report.json",
        },
        ...
    }
}
```

---

## OVHCrawler (`crawler/ovh/crawler.py`)

Instantiated with `(company, url, year=None, filter=None, metadata=None)`.

### `fetch_pdfs()` return dict (per file)

```python
{
    "url":              "https://...",
    "filename":         "FY2025_annual_results_press_release.pdf",
    "bytes":            b"...",
    "report_type":      "annual_results",       # or "interim_report"
    "report_name":      "annual_results",       # or "Q1_revenue", "half_year_results", etc.
    "fiscal_year":      "FY2025",
    "doc_type":         "press_release",        # or "presentation" / "consolidated_financial_statements"
    "source_filing_id": "FY2025_annual_results",
}
```

### `fetch_news()` return dict (per article)

```python
{
    "date":             "04/01/2026",
    "date_raw":         "Wed, 04/01/2026",
    "title":            "OVHcloud announces ...",
    "summary":          "...",
    "url":              "/en/newsroom/news/...",
    "content":          "Full article text...",
    "source_filing_id": "news_2026-04-01_ovhcloud-announces-...",
}
```

---

## XBRL Parser (`parser/ovh/parser.py`)

The parser fetches and processes OVHcloud's ESEF filings from `filings.xbrl.org`.

### Processing Pipeline

```
api_discover()           Query the XBRL registry for all OVHcloud filings
discover_fiscal_years()  Build year configs dynamically from API results
download_viewer_data()   Download ixbrlviewer.html (contains embedded JSON)
extract_labels()         Pull FR + EN label for every XBRL concept
parse_all_facts()        Extract every numeric value from the JSON
pyxbrl_enrich_labels()   Fill missing labels from the IFRS taxonomy (optional)
build_table()            Filter → classify → deduplicate → order facts per year
write_excel()            Write the formatted .xlsx workbook
```

### Classification Logic

Facts are classified by keyword matching on the XBRL concept name. No hardcoded concept list — new concepts are captured automatically.

**Balance sheet** (instant-period facts):
- Liabilities: `liabilit`, `payable`, `borrowing`, `equity`, `provision`, `deferredtaxliabilit`
- Assets: `asset`, `goodwill`, `intangible`, `rightofuse`, `receivable`, `cashandcash`, `propertyplant`

**Income statement** (duration-period facts):
- `revenue`, `profit`, `loss`, `income`, `expense`, `depreciation`, `ebitda`, `taxexpense`

**Cash flow** (checked before income statement):
- `cashflows`, `proceedsfrom`, `paymentsto`, `adjustmentsfor`, `interestpaid`, `increasedecrease`

### Excel Output

| Sheet | Contents |
|---|---|
| Income Statement | Revenue → EBITDA → Net profit, all fiscal years |
| Cash Flow | Operating / investing / financing activities |
| Assets | Non-current → current assets, balance sheet order |
| Liabilities | Equity → non-current → current liabilities, balance sheet order |

Columns: French label | English label | FY2025 (k€) | FY2024 (k€) | ... | Var (k€) | Var (%) | XBRL concept

---

## `db_utils.py` Reference

| Function | Purpose |
|---|---|
| `get_company(db, name)` | Find company by name or alias (case-insensitive) |
| `get_sources_for_company(db, company_id, source_type)` | Return active sources, optional type filter |
| `insert_report(db, doc)` | Insert report, auto-set createdAt/updatedAt, return `_id` |
| `update_report(db, report_id, fields)` | Update fields, bump updatedAt |
| `insert_ingestion_log(db, doc)` | Insert log document, return `_id` |
| `update_ingestion_log(db, log_id, fields)` | Update log fields (mark success or error) |
| `save_bytes_to_gridfs(db, data, filename, metadata)` | Store binary file (PDF, Excel, HTML) |
| `save_text_to_gridfs(db, text, filename, metadata)` | Store UTF-8 text (news articles) |

---

## `pipeline/utils.py` Reference

| Function | Purpose |
|---|---|
| `save_bytes(data, filepath)` | Write binary to disk (creates parent dirs) |
| `save_text(text, filepath)` | Write UTF-8 text to disk |
| `save_json(data, filepath)` | Write JSON with indent=2 |
| `build_article_text(article)` | Format article dict to plain text block |
| `article_filename(article)` | Build `YYYY-MM-DD_slug.txt` from article dict |

---

## Adding a New Company

1. Add a config section in `config.ini` (e.g. `[MYCO]`)
2. Create `src/crawler/myco/crawler.py` with `fetch_pdfs()` / `fetch_news()`
3. Create `src/pipeline/myco/company_web_pipeline.py` and `company_api_pipeline.py`
4. Register the company in `src/main.py`:
   - Add to `KNOWN_COMPANIES`
   - Add a branch in `_get_pipeline()` for `key[0] == "myco"`
5. Add company and source documents to MongoDB
6. Import `from src.logging import get_logger` in every new module

---

## Data Source for OVHcloud

| Source | What it provides |
|---|---|
| `https://corporate.ovhcloud.com/en/investor-relations/financial-results/` | Financial PDFs (press releases, presentations, financial statements) |
| `https://corporate.ovhcloud.com/en/newsroom/news` | Press release news articles |
| `https://filings.xbrl.org` | ESEF XBRL structured filings (machine-readable financial data) |

OVHcloud's LEI: `9695001J8OSOVX4TP939`

XBRL data is in ESEF format (European Single Electronic Format), the mandatory EU regulatory filing format. All data is publicly available. The viewer HTML embeds a JSON blob containing both bilingual labels and all numeric facts.
