"""
XBRL Workstream UI Component.
Fetches filings from the XBRL API, parses viewer_data.json,
builds IFRS financial-statement views, and offers consolidated downloads.
"""

import io
import json
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from config.config import get_section as _get_section
from src import http_client
from src.pipeline.db import MongoDBClient
from src.parser.ovh import parser as ovh_parser
from src.parser.xbrl import parser as xbrl_parser


# ==============================================================================
# SESSION STATE
# ==============================================================================

def initialize_xbrl_state():
    defaults = {
        "filings": [],
        "selected_filing": None,
        "financial_data": {},
        "consolidated_data": None,
        "show_individual_filing": False,
        "api_base": None,
        "filing_metadata": {},
        "ovh_sources": [],
        "selected_source": None,
        "show_filings": False,
        "raw_api_data": None,
        "all_facts": [],          # Flat list of all XBRL facts
        "concept_map": {},        # {sheet_type: {label: concept}}
        "parsed_labels": set(),   # Set of parsed FY labels
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


# ==============================================================================
# OVH HELPER FUNCTIONS
# ==============================================================================

def save_raw_api_data_to_mongodb(lei, api_base, filings_data):
    """Save raw API data to MongoDB reports collection"""
    try:
        with MongoDBClient() as client:
            # Create a document for raw API data
            raw_data_doc = {
                "lei": lei,
                "apiBase": api_base,
                "dataType": "raw_api_filings",
                "filings": filings_data,
            }

            # Check if already exists
            existing = client.db.reports.find_one({
                "lei": lei,
                "dataType": "raw_api_filings"
            })

            if existing:
                # Update existing
                client.db.reports.update_one(
                    {"_id": existing["_id"]},
                    {"$set": {
                        "filings": filings_data,
                        "updatedAt": datetime.utcnow()
                    }}
                )
                return str(existing["_id"])
            else:
                # Insert new
                result = client.db.reports.insert_one(raw_data_doc)
                return str(result.inserted_id)
    except Exception as e:
        st.error(f"Error saving raw API data to MongoDB: {str(e)}")
        return None


def load_raw_api_data_from_mongodb(lei):
    """Load raw API data from MongoDB"""
    try:
        with MongoDBClient() as client:
            doc = client.db.reports.find_one({
                "lei": lei,
                "dataType": "raw_api_filings"
            })
            if doc:
                return doc.get("filings", [])
            return None
    except Exception as e:
        st.error(f"Error loading raw API data from MongoDB: {str(e)}")
        return None


def load_filings_from_api(lei, api_base):
    """Load filings list from API, save locally and to MongoDB. Returns list of filing dicts."""
    try:
        _OVH_CFG = _get_section("OVH")
        download_dir = Path(_OVH_CFG.get("download_dir", "ovh_filings"))
        download_dir.mkdir(parents=True, exist_ok=True)
        local_file = download_dir / f"filings_{lei}.json"

        ua = _OVH_CFG.get("user_agent", "XBRL-Research/1.0 research@example.com")
        headers = {"User-Agent": ua, "Accept": "application/json,*/*"}

        filings = xbrl_parser.fetch_filings(lei, api_base, headers=headers)

        if filings:
            # Save locally
            local_file.write_text(
                json.dumps(filings, ensure_ascii=False, indent=2), encoding="utf-8"
            )

        return filings
    except Exception as e:
        st.error(f"Error loading filings: {str(e)}")
        return []


def save_xbrl_json_to_mongodb(lei, filing_id, period_end, json_bytes: bytes):
    """Save viewer_data.json bytes to MongoDB GridFS. Returns GridFS file_id str or None."""
    try:
        with MongoDBClient() as client:
            fy_label = f"FY{period_end[:4]}" if period_end else "UNKNOWN"
            # Remove old copy if exists
            existing = client.db.reports.find_one({
                "lei": lei,
                "filingId": filing_id,
                "dataType": "xbrl_viewer_json",
            })
            if existing:
                old_fid = existing.get("gridfsFileId")
                if old_fid:
                    try:
                        from bson import ObjectId
                        client.fs.delete(ObjectId(old_fid))
                    except Exception:
                        pass
                client.db.reports.delete_one({"_id": existing["_id"]})

            file_id = client.save_bytes_to_gridfs(
                json_bytes,
                filename=f"{filing_id}_{period_end}_viewer_data.json",
                metadata={
                    "lei": lei,
                    "filingId": filing_id,
                    "periodEnd": period_end,
                    "fiscalYear": fy_label,
                    "contentType": "application/json",
                    "fileType": "xbrl_viewer_json",
                },
            )
            client.db.reports.insert_one({
                "lei": lei,
                "filingId": filing_id,
                "periodEnd": period_end,
                "fiscalYear": fy_label,
                "dataType": "xbrl_viewer_json",
                "gridfsFileId": file_id,
                "createdAt": datetime.utcnow(),
            })
            return file_id
    except Exception as e:
        st.warning(f"Could not save XBRL JSON to MongoDB: {e}")
        return None


def load_xbrl_json_from_mongodb(lei, filing_id) -> bytes:
    """Load viewer_data.json bytes from MongoDB GridFS. Returns bytes or None."""
    try:
        with MongoDBClient() as client:
            doc = client.db.reports.find_one({
                "lei": lei,
                "filingId": filing_id,
                "dataType": "xbrl_viewer_json",
            })
            if not doc:
                return None
            from bson import ObjectId
            grid_out = client.fs.get(ObjectId(doc["gridfsFileId"]))
            return grid_out.read()
    except Exception:
        return None


def save_viewer_data_to_mongodb(lei, filing_id, period_end, viewer_json_path, report_html_path=None):
    """Save viewer_data.json and report_doc.html to MongoDB GridFS"""
    try:
        with MongoDBClient() as client:
            fy_label = f"FY{period_end[:4]}" if period_end else "UNKNOWN"

            # Save report_doc.html to GridFS if provided
            html_file_id = None
            if report_html_path and Path(report_html_path).exists():
                with open(report_html_path, 'rb') as f:
                    html_bytes = f.read()
                html_file_id = client.save_bytes_to_gridfs(
                    html_bytes,
                    filename=f"{filing_id}_{period_end}_report.html",
                    metadata={
                        "lei": lei,
                        "filingId": filing_id,
                        "periodEnd": period_end,
                        "fiscalYear": fy_label,
                        "contentType": "text/html",
                        "fileType": "report_html"
                    }
                )

            # Save viewer_data.json to GridFS
            viewer_file_id = None
            if viewer_json_path and Path(viewer_json_path).exists():
                with open(viewer_json_path, 'rb') as f:
                    viewer_bytes = f.read()
                viewer_file_id = client.save_bytes_to_gridfs(
                    viewer_bytes,
                    filename=f"{filing_id}_{period_end}_viewer_data.json",
                    metadata={
                        "lei": lei,
                        "filingId": filing_id,
                        "periodEnd": period_end,
                        "fiscalYear": fy_label,
                        "contentType": "application/json",
                        "fileType": "viewer_data"
                    }
                )

            # Create or update report document
            report_doc = {
                "lei": lei,
                "filingId": filing_id,
                "periodEnd": period_end,
                "fiscalYear": fy_label,
                "dataType": "xbrl_source_files",
                "reportHtmlFileId": html_file_id,
                "viewerDataFileId": viewer_file_id,
                "createdAt": datetime.utcnow(),
                "updatedAt": datetime.utcnow()
            }

            # Check if already exists
            existing = client.db.reports.find_one({
                "lei": lei,
                "filingId": filing_id,
                "dataType": "xbrl_source_files"
            })

            if existing:
                # Update existing
                client.db.reports.update_one(
                    {"_id": existing["_id"]},
                    {"$set": {
                        "reportHtmlFileId": html_file_id,
                        "viewerDataFileId": viewer_file_id,
                        "updatedAt": datetime.utcnow()
                    }}
                )
                return str(existing["_id"])
            else:
                result = client.db.reports.insert_one(report_doc)
                return str(result.inserted_id)
    except Exception as e:
        st.error(f"Error saving viewer data to MongoDB: {str(e)}")
        import traceback
        st.error(traceback.format_exc())
        return None


def save_parsed_data_to_mongodb(lei, filing_id, period_end, tables, report_path=None):
    """Save parsed financial data to MongoDB reports collection and GridFS"""
    try:
        with MongoDBClient() as client:
            # Save PDF to GridFS if provided
            pdf_file_id = None
            if report_path and Path(report_path).exists():
                with open(report_path, 'rb') as f:
                    pdf_bytes = f.read()
                pdf_file_id = client.save_bytes_to_gridfs(
                    pdf_bytes,
                    filename=f"{filing_id}_{period_end}.pdf",
                    metadata={
                        "lei": lei,
                        "filingId": filing_id,
                        "periodEnd": period_end,
                        "contentType": "application/pdf"
                    }
                )

            # Save parsed tables as JSON to GridFS
            tables_json = json.dumps(tables, ensure_ascii=False, indent=2)
            tables_file_id = client.save_text_to_gridfs(
                tables_json,
                filename=f"{filing_id}_{period_end}_tables.json",
                metadata={
                    "lei": lei,
                    "filingId": filing_id,
                    "periodEnd": period_end,
                    "contentType": "application/json"
                }
            )

            # Create report document
            fy_label = f"FY{period_end[:4]}" if period_end else "UNKNOWN"
            report_doc = {
                "lei": lei,
                "filingId": filing_id,
                "periodEnd": period_end,
                "fiscalYear": fy_label,
                "dataType": "parsed_financial_tables",
                "pdfFileId": pdf_file_id,
                "tablesFileId": tables_file_id,
                "tableNames": list(tables.keys()),
                "createdAt": datetime.utcnow(),
                "updatedAt": datetime.utcnow()
            }

            # Check if already exists
            existing = client.db.reports.find_one({
                "lei": lei,
                "filingId": filing_id,
                "dataType": "parsed_financial_tables"
            })

            if existing:
                # Update existing
                client.db.reports.update_one(
                    {"_id": existing["_id"]},
                    {"$set": {
                        "pdfFileId": pdf_file_id,
                        "tablesFileId": tables_file_id,
                        "tableNames": list(tables.keys()),
                        "updatedAt": datetime.utcnow()
                    }}
                )
                return str(existing["_id"])
            else:
                # Insert new
                result = client.db.reports.insert_one(report_doc)
                return str(result.inserted_id)
    except Exception as e:
        st.error(f"Error saving parsed data to MongoDB: {str(e)}")
        import traceback
        st.error(traceback.format_exc())
        return None


def load_parsed_data_from_mongodb(lei, filing_id):
    """Load parsed financial data from MongoDB"""
    try:
        with MongoDBClient() as client:
            # Find the report document
            report = client.db.reports.find_one({
                "lei": lei,
                "filingId": filing_id,
                "dataType": "parsed_financial_tables"
            })

            if not report:
                return None

            # Get tables from GridFS
            tables_file_id = report.get("tablesFileId")
            if not tables_file_id:
                return None

            # Read from GridFS
            from bson import ObjectId
            tables_data = client.fs.get(ObjectId(tables_file_id)).read()
            tables = json.loads(tables_data.decode('utf-8'))

            return tables
    except Exception as e:
        st.error(f"Error loading parsed data from MongoDB: {str(e)}")
        import traceback
        st.error(traceback.format_exc())
        return None


def parse_filing_data(filing, lei, api_base, silent=False):
    """Parse a specific OVH filing and extract financial tables

    Args:
        filing: Filing dictionary
        lei: LEI identifier
        api_base: API base URL
        silent: If True, don't show info messages (for batch consolidation)

    Returns:
        tuple: (tables, xbrl_facts) or (None, None) on error
    """
    try:
        filing_id = filing.get('_id', 'N/A')
        period_end = filing.get('period_end', '')

        # Set global variables for parser
        ovh_parser.LEI = lei
        ovh_parser.API_BASE = api_base

        # Get OVH config
        _OVH_CFG = _get_section("OVH")
        download_dir = Path(_OVH_CFG.get("download_dir"))

        # Create fiscal year directory
        fy_label = f"FY{period_end[:4]}" if period_end else "UNKNOWN"
        fy_dir = download_dir / fy_label
        fy_dir.mkdir(parents=True, exist_ok=True)

        # Download report
        if not silent:
            st.info(f"Downloading report for {fy_label}...")
        report_path = ovh_parser.download_report(filing, fy_dir)

        if not report_path:
            if not silent:
                st.error(f"No report available for {fy_label}")
            return None, None

        # Extract tables (WITHOUT XBRL concepts yet)
        if not silent:
            st.info(f"Extracting financial tables for {fy_label}...")
        tables = ovh_parser.extract_section_tables(report_path, fy_label)

        if not tables:
            if not silent:
                st.warning(f"No tables found for {fy_label}")
            return None, None

        # Normalize and add English labels (NO XBRL concepts yet)
        for tbl_name in tables:
            tables[tbl_name] = ovh_parser._detect_unit_and_normalize(tables[tbl_name])
            tables[tbl_name] = ovh_parser._add_english_column(tables[tbl_name])

        # Download XBRL facts
        if not silent:
            st.info(f"Downloading XBRL facts for {fy_label}...")
        json_path = ovh_parser.download_xbrl_json(filing, fy_dir)

        xbrl_facts = []
        if json_path:
            xbrl_facts = ovh_parser.parse_xbrl_facts(json_path, fy_label)
            if not silent:
                st.success(f"✅ Extracted {len(xbrl_facts)} XBRL facts")
        else:
            if not silent:
                st.warning("⚠ Could not download XBRL facts")

        if not silent:
            st.success(f"✅ {fy_label}: {len(tables)} statement types parsed")

        return tables, xbrl_facts

    except Exception as e:
        if not silent:
            st.error(f"Error parsing filing: {str(e)}")
            import traceback
            st.error(traceback.format_exc())
        return None, None


def save_raw_xbrl_to_mongodb(lei: str, filing_id: str, period_end: str, fy_label: str, raw_data: dict):
    """Save raw XBRL JSON (facts dict) to MongoDB reports collection as a queryable document."""
    try:
        with MongoDBClient() as client:
            doc = {
                "lei": lei,
                "filingId": filing_id,
                "periodEnd": period_end,
                "fiscalYear": fy_label,
                "dataType": "xbrl_raw_json",
                "rawData": raw_data,
                "savedAt": datetime.utcnow(),
            }
            existing = client.db.reports.find_one({
                "lei": lei,
                "filingId": filing_id,
                "dataType": "xbrl_raw_json",
            })
            if existing:
                client.db.reports.update_one(
                    {"_id": existing["_id"]},
                    {"$set": {"rawData": raw_data, "savedAt": datetime.utcnow()}},
                )
                return str(existing["_id"])
            else:
                result = client.db.reports.insert_one(doc)
                return str(result.inserted_id)
    except Exception as e:
        st.warning(f"Could not save raw XBRL JSON to MongoDB: {e}")
        return None


def _load_viewer_labels(filing: dict, api_base: str, fy_dir: Path, headers: dict, silent: bool = False):
    """
    Load ixbrlviewer.html (cache locally) and extract concept labels.
    Returns {concept_short: (fr_label, en_label)} or empty dict on failure.
    """
    viewer_url = filing.get("viewer_url", "")
    if not viewer_url:
        return {}

    viewer_local = fy_dir / "ixbrlviewer.html"

    viewer_content = None
    if viewer_local.exists():
        try:
            viewer_content = viewer_local.read_text(encoding="utf-8", errors="replace")
        except Exception:
            pass

    if viewer_content is None:
        try:
            full_url = api_base + viewer_url if not viewer_url.startswith("http") else viewer_url
            from src import http_client as _hc
            resp = _hc.get(full_url, headers={**headers, "Accept": "text/html,*/*"}, timeout=60)
            resp.raise_for_status()
            viewer_content = resp.content.decode("utf-8", errors="replace")
            fy_dir.mkdir(parents=True, exist_ok=True)
            viewer_local.write_text(viewer_content, encoding="utf-8")
            if not silent:
                st.success(f"Saved ixbrlviewer.html locally: {viewer_local}")
        except Exception as e:
            if not silent:
                st.warning(f"Could not fetch ixbrlviewer.html: {e}")
            return {}

    labels = xbrl_parser.extract_labels_from_ixbrl_viewer(viewer_content)
    return labels


def _save_labeled_json_locally(fy_dir: Path, lei: str, filing_id: str, fy_label: str,
                               facts: list, labels_dict: dict):
    """
    Save a JSON file locally with concepts, their labels, and fact values.
    File: {fy_dir}/xbrl_facts_labeled.json
    """
    try:
        from src.parser.xbrl import parser as _xp
        labeled = {}
        for fact in facts:
            cs = fact.get("concept_short", "")
            cf = fact.get("concept_full", "")
            if not cs:
                continue
            fr, en = labels_dict.get(cs, _xp.IFRS_CONCEPT_LABELS.get(cs, (cs, cs)))
            key = cs
            if key not in labeled:
                labeled[key] = {
                    "concept": cf,
                    "concept_short": cs,
                    "fr_label": fr,
                    "en_label": en,
                    "facts": [],
                }
            labeled[key]["facts"].append({
                "period": f"{fact.get('period_start', '')}/{fact.get('period_end', '')}" if fact.get("period_start") else fact.get("period_end", ""),
                "fy_year": fact.get("fy_year", ""),
                "value": fact.get("value_numeric"),
                "unit": fact.get("unit", ""),
                "decimals": fact.get("decimals", ""),
            })

        output = {
            "lei": lei,
            "filing_id": filing_id,
            "fy_label": fy_label,
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "total_concepts": len(labeled),
            "concepts": labeled,
        }
        out_path = fy_dir / "xbrl_facts_labeled.json"
        out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
        return str(out_path)
    except Exception as e:
        st.warning(f"Could not save labeled JSON: {e}")
        return None


def parse_xbrl_filing(filing: dict, lei: str, api_base: str, silent: bool = False, company_name: str = ""):
    """
    Parse a filing using the general XBRL-only approach (no HTML download).

    Cache strategy:
      1. Local file  -> {download_dir}/{fy_label}/viewer_data.json
      2. MongoDB GridFS
      3. Download from API (then save locally + to MongoDB)

    Also:
      - Downloads ixbrlviewer.html to extract official EN/FR labels
      - Saves raw JSON to MongoDB as a queryable document
      - Saves xbrl_facts_labeled.json locally

    Returns:
        (statements_dict, facts_list)
        statements_dict: {statement_type: pd.DataFrame}  columns = FR | EN | Concept | FY... columns
        facts_list: raw list of fact dicts
    """
    try:
        filing_id  = filing.get("_id", "")
        period_end = filing.get("period_end", "")
        fy_label   = f"FY{period_end[:4]}" if period_end else "UNKNOWN"

        _OVH_CFG   = _get_section("OVH")
        ua         = _OVH_CFG.get("user_agent", "XBRL-Research/1.0 research@example.com")
        headers    = {"User-Agent": ua, "Accept": "application/json,*/*"}
        download_dir = Path(_OVH_CFG.get("download_dir", "xbrl_filings"))
        # Use company name as directory (fallback to LEI if name not provided)
        raw_name   = company_name.strip() if company_name and company_name.strip() else (lei or "unknown")
        company_slug = re.sub(r'[\\/:*?"<>|]', "_", raw_name).strip()
        fy_dir     = download_dir / company_slug / fy_label
        local_path = fy_dir / "viewer_data.json"

        json_bytes = None

        # 1. Check local cache
        if local_path.exists():
            if not silent:
                st.info(f"Loading {fy_label} from local cache...")
            json_bytes = local_path.read_bytes()

        # 2. Check MongoDB GridFS
        if json_bytes is None and filing_id:
            if not silent:
                st.info(f"Checking MongoDB for {fy_label}...")
            json_bytes = load_xbrl_json_from_mongodb(lei, filing_id)
            if json_bytes:
                # Save locally so it's available as a file too
                fy_dir.mkdir(parents=True, exist_ok=True)
                local_path.write_bytes(json_bytes)
                if not silent:
                    st.info(f"Loaded {fy_label} from MongoDB cache — saved locally: {local_path}")

        # 3. Download from API
        if json_bytes is None:
            if not silent:
                st.info(f"Downloading XBRL data for {fy_label} from API...")
            from src import http_client as _hc
            json_url = filing.get("json_url", "")
            if not json_url:
                if not silent:
                    st.error(f"No json_url in filing for {fy_label}")
                return None, None
            full_url = api_base + json_url if not json_url.startswith("http") else json_url
            resp = _hc.get(full_url, headers=headers, timeout=120)
            resp.raise_for_status()
            json_bytes = resp.content

            # Save locally
            fy_dir.mkdir(parents=True, exist_ok=True)
            local_path.write_bytes(json_bytes)
            if not silent:
                st.success(f"Saved viewer_data.json locally: {local_path}")

            # Save to MongoDB GridFS
            save_xbrl_json_to_mongodb(lei, filing_id, period_end, json_bytes)
            if not silent:
                st.success(f"Saved viewer_data.json to MongoDB GridFS")

        # Parse raw JSON
        import json as _json
        data = _json.loads(json_bytes.decode("utf-8", errors="replace"))
        facts_raw = data.get("facts", {})

        # Save raw JSON to MongoDB as a queryable document (always, on every parse)
        save_raw_xbrl_to_mongodb(lei, filing_id, period_end, fy_label, data)
        if not silent:
            st.success(f"Saved raw XBRL JSON to MongoDB (dataType: xbrl_raw_json)")

        # Load labels from ixbrlviewer.html
        fy_dir.mkdir(parents=True, exist_ok=True)
        labels_dict = _load_viewer_labels(filing, api_base, fy_dir, headers, silent=silent)
        if labels_dict and not silent:
            st.success(f"Loaded {len(labels_dict)} concept labels from ixbrlviewer.html")

        facts = []
        for _fid, fact in facts_raw.items():
            dims    = fact.get("dimensions", {})
            concept = dims.get("concept", "")
            period  = dims.get("period", "")
            unit    = dims.get("unit", "")
            raw_val = fact.get("value", "")
            decimals = fact.get("decimals", "")
            if not concept:
                continue
            p_type, p_start, p_end, fy_year = xbrl_parser._parse_period(period)
            numeric = xbrl_parser._to_numeric(raw_val)
            facts.append({
                "concept_full":  concept,
                "concept_short": xbrl_parser._concept_short(concept),
                "period_type":   p_type,
                "period_start":  p_start,
                "period_end":    p_end,
                "fy_year":       fy_year,
                "unit":          unit,
                "value_raw":     raw_val,
                "value_numeric": numeric,
                "decimals":      decimals,
            })

        if not facts:
            if not silent:
                st.warning(f"No XBRL facts found for {fy_label}")
            return None, None

        if not silent:
            st.success(f"{len(facts)} facts extracted for {fy_label}")

        # Save labeled JSON locally
        labeled_path = _save_labeled_json_locally(fy_dir, lei, filing_id, fy_label, facts, labels_dict)
        if labeled_path and not silent:
            st.success(f"Saved labeled facts JSON: {labeled_path}")

                        # Build view with all years from this filing (current + comparative)
        # Extract company key from company name for custom mapping
        company_key = None
        if company_name:
            # Convert company name to lowercase key (e.g., "VINCI" -> "vinci", "Vinci" -> "vinci")
            company_key = company_name.lower().strip()
            if not silent:
                st.info(f"Using company key for custom mapping: '{company_key}'")

        statements = xbrl_parser.build_filing_view(facts, labels_dict=labels_dict or None, company_key=company_key)

        if not statements:
            if not silent:
                st.warning(f"No known IFRS concepts matched for {fy_label}")
            return None, facts

        if not silent:
            st.success(f"Built {len(statements)} statement(s) for {fy_label}")

        # Save parsed statements as JSON locally
        try:
            statements_out = {
                "lei": lei,
                "filing_id": filing_id,
                "fy_label": fy_label,
                "generated_at": datetime.utcnow().isoformat(),
                "statements": {
                    stmt_type: df.to_dict(orient="records")
                    for stmt_type, df in statements.items()
                    if df is not None and not df.empty
                },
            }
            stmts_path = fy_dir / "parsed_statements.json"
            stmts_path.write_text(
                json.dumps(statements_out, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            if not silent:
                st.success(f"Saved parsed statements JSON: {stmts_path}")
        except Exception as _e:
            if not silent:
                st.warning(f"Could not save parsed_statements.json: {_e}")

        return statements, facts

    except Exception as e:
        if not silent:
            st.error(f"Error parsing XBRL filing: {e}")
            import traceback
            st.error(traceback.format_exc())
        return None, None


# ==============================================================================
# MAIN UI
# ==============================================================================

def render_xbrl_section(company, lei):
    """Render the full XBRL workstream UI.

    Parameters
    ----------
    company : dict
        The selected company document (must contain at least ``name``).
    lei : str
        The LEI identifier for the company.
    """

    company_type = st.session_state.get("company_type")

    # ------------------------------------------------------------------
    # Block 1 — Load Filings from API
    # ------------------------------------------------------------------
    if company_type == 'XBRL':
        company_display_name = company.get('name', 'XBRL')
        st.header(f"{company_display_name} Data Source")

        # Sources are already loaded automatically when company is selected
        st.session_state.ovh_sources = st.session_state.company_sources

                # Find API source and show button
        api_source = None
        for source in st.session_state.ovh_sources:
            if source.get('sourceType') == 'API':
                api_source = source
                break

        if api_source:
            col_load, col_clear = st.columns([3, 1])
            with col_load:
                load_btn = st.button("Load Filings from API", type="primary", use_container_width=True)
            with col_clear:
                clear_cache_btn = st.button("\U0001f5d1️ Clear Cache", use_container_width=True, help="Clear all cached data and force re-parsing")

            if clear_cache_btn:
                # Clear session state
                st.session_state.financial_data = {}
                st.session_state.all_facts = []
                st.session_state.concept_map = {}
                st.session_state.parsed_labels = set()
                st.session_state.consolidated_data = None
                st.session_state.show_individual_filing = False

                # Clear local cache files
                try:
                    from pathlib import Path
                    _OVH_CFG = _get_section("OVH")
                    download_dir = Path(_OVH_CFG.get("download_dir", "ovhcloud_filings"))
                    company_name = st.session_state.selected_company.get("name", "")
                    if company_name:
                        import re
                        company_slug = re.sub(r'[\\/:*?"<>|]', "_", company_name).strip()
                        company_dir = download_dir / company_slug
                        if company_dir.exists():
                            # Delete only parsed_statements.json files, keep raw XBRL data
                            for parsed_file in company_dir.rglob("parsed_statements.json"):
                                parsed_file.unlink()
                                st.success(f"Deleted: {parsed_file}")
                except Exception as e:
                    st.warning(f"Could not clear local cache: {e}")

                st.success("✅ Cache cleared! Click 'Load Filings from API' to reload.")
                st.rerun()

            if load_btn:
                api_base = api_source.get('sourceUrl', 'https://filings.xbrl.org')
                st.session_state.api_base = api_base
                st.session_state.selected_source = api_source
                st.session_state.show_filings = True

                # First, try to load from MongoDB
                cached_filings = load_raw_api_data_from_mongodb(lei)

                if cached_filings:
                    st.info("Loaded filings from MongoDB cache")
                    st.session_state.filings = cached_filings
                    st.session_state.raw_api_data = cached_filings
                else:
                    # Load from API and save to MongoDB
                    with st.spinner("Loading OVH filings from API..."):
                        ovh_parser.LEI = lei
                        ovh_parser.API_BASE = api_base
                        filings = load_filings_from_api(lei, api_base)

                    if filings:
                        # Save to MongoDB
                        save_raw_api_data_to_mongodb(lei, api_base, filings)
                        st.session_state.filings = filings
                        st.session_state.raw_api_data = filings
                        st.success(f"Loaded and saved {len(filings)} filings to MongoDB")
                    else:
                        st.warning("No filings found")

                if st.session_state.filings:
                    st.rerun()
        else:
            st.info("No API source available for this company")

    # ------------------------------------------------------------------
    # Block 2 — Filing display + individual filing parse
    # ------------------------------------------------------------------
    if st.session_state.filings and st.session_state.company_type == 'XBRL' and st.session_state.show_filings:
        st.markdown("---")
        st.header("Available Filings")

        # Create a DataFrame for filings (without Report Available column)
        filings_data = []
        for filing in st.session_state.filings:
            filings_data.append({
                "Period End": filing.get('period_end', 'N/A'),
                "Errors": filing.get('error_count', 0),
                "Filing ID": filing.get('_id', 'N/A')
            })

        filings_df = pd.DataFrame(filings_data)
        st.dataframe(filings_df, width='stretch', hide_index=True)

        # Filing selection
        st.markdown("### Select a Filing to View Details")

        filing_options = [f"Period: {f.get('period_end', 'N/A')} (ID: {f.get('_id', 'N/A')[:8]}...)"
                          for f in st.session_state.filings]

        selected_filing_name = st.selectbox(
            "Choose a filing",
            options=filing_options,
            key="filing_selector"
        )

        selected_filing_idx = filing_options.index(selected_filing_name)
        selected_filing = st.session_state.filings[selected_filing_idx]
        st.session_state.selected_filing = selected_filing

        # Parse filing button
        col1, col2 = st.columns([1, 4])

        with col1:
            if st.button("Parse Filing", type="primary"):
                if st.session_state.lei and st.session_state.api_base:
                    with st.spinner("Parsing XBRL data..."):
                        _cname = st.session_state.selected_company.get("name", "") if st.session_state.selected_company else ""
                        statements, xbrl_facts = parse_xbrl_filing(
                            selected_filing,
                            st.session_state.lei,
                            st.session_state.api_base,
                            company_name=_cname,
                        )

                    if statements:
                        pe = selected_filing.get('period_end', '')
                        fy_label = f"FY{pe[:4]}" if pe else "UNKNOWN"

                        if fy_label not in st.session_state.parsed_labels:
                            # Store DataFrames keyed by FY label
                            st.session_state.financial_data[fy_label] = statements

                            # Store raw facts (tagged with fy_label) - ENRICH WITH LABELS
                            if xbrl_facts:
                                # Load labels from xbrl_facts_labeled.json if available
                                from pathlib import Path as PathLib
                                import re as re_module
                                _OVH_CFG = _get_section("OVH")
                                download_dir = PathLib(_OVH_CFG.get("download_dir", "xbrl_filings"))
                                _cname = st.session_state.selected_company.get("name", "") if st.session_state.selected_company else ""
                                company_slug = re_module.sub(r'[\\/:*?"<>|]', "_", _cname).strip() if _cname else (st.session_state.lei or "unknown")
                                labeled_json_path = download_dir / company_slug / fy_label / "xbrl_facts_labeled.json"

                                labels_map = {}  # {concept_short: (fr_label, en_label)}
                                if labeled_json_path.exists():
                                    try:
                                        labeled_data = json.loads(labeled_json_path.read_text(encoding="utf-8"))
                                        concepts_dict = labeled_data.get("concepts", {})
                                        for concept_short, concept_data in concepts_dict.items():
                                            fr_label = concept_data.get("fr_label", "")
                                            en_label = concept_data.get("en_label", "")
                                            labels_map[concept_short] = (fr_label, en_label)
                                        st.info(f"Loaded {len(labels_map)} concept labels from {labeled_json_path.name}")
                                    except Exception as e:
                                        st.warning(f"Could not load labels from {labeled_json_path.name}: {e}")
                                else:
                                    st.warning(f"Label file not found: {labeled_json_path}")

                                # Enrich facts with labels
                                tagged = []
                                for f in xbrl_facts:
                                    concept_short = f.get("concept_short", "")
                                    concept_full = f.get("concept_full", "")

                                    # Try to get labels from map first
                                    fr_label, en_label = labels_map.get(concept_short, ("", ""))

                                    # If no labels found and it's a company-specific concept, use concept_short as fallback
                                    if not fr_label and not en_label and ":" in concept_full:
                                        namespace = concept_full.split(":")[0]
                                        if namespace.lower() not in ["ifrs-full", "ifrs"]:
                                            # Company-specific concept without label - use concept_short as label
                                            fr_label = concept_short
                                            en_label = concept_short

                                    enriched_fact = {
                                        **f,
                                        "fy_label": fy_label,
                                        "fr_label": fr_label,
                                        "en_label": en_label,
                                    }
                                    tagged.append(enriched_fact)

                                st.session_state.all_facts = [
                                    f for f in st.session_state.all_facts
                                    if f.get("fy_label") != fy_label
                                ]
                                st.session_state.all_facts.extend(tagged)

                            st.session_state.parsed_labels.add(fy_label)
                            st.session_state.filing_metadata[fy_label] = f"{fy_label} (from {pe} filing)"

                        st.session_state.show_individual_filing = True
                        st.success(f"Parsed {fy_label}: {', '.join(statements.keys())}")
                        st.rerun()
                    else:
                        st.error("Failed to parse filing — no XBRL facts matched known concepts")
                else:
                    st.error("LEI or API Base URL not set")

        # Display financial data ONLY if user clicked "Parse Filing" button
        period_end = selected_filing.get('period_end', '')
        fy_label = f"FY{period_end[:4]}" if period_end else "UNKNOWN"

        if st.session_state.show_individual_filing and fy_label in st.session_state.financial_data:
            st.markdown("---")
            st.header("Financial Statements")

            statements = st.session_state.financial_data[fy_label]
            tab_names = list(statements.keys())
            tabs = st.tabs(tab_names)

            for tab, stmt_type in zip(tabs, tab_names):
                with tab:
                    df = statements[stmt_type]
                    if df is not None and not df.empty:
                        # Detect unit from first numeric value column
                        val_cols = [c for c in df.columns if c not in ("French Label", "English Label", "Concept")]
                        unit_label = "€ millions"
                        if val_cols:
                            facts_for_unit = [
                                f for f in st.session_state.all_facts
                                if f.get("fy_label") == fy_label and f.get("value_numeric") is not None
                            ]
                            if facts_for_unit:
                                sample_decimals = facts_for_unit[0].get("decimals", "")
                                unit_label = xbrl_parser.get_value_unit_label(sample_decimals)
                        st.caption(f"Values in {unit_label}")
                        st.dataframe(df, width="stretch", height=min(600, 40 + 35 * len(df)), hide_index=True)
                        # Per-sheet Excel download
                        _buf = io.BytesIO()
                        with pd.ExcelWriter(_buf, engine="openpyxl") as _w:
                            df.to_excel(_w, sheet_name=stmt_type[:31], index=False)
                        st.download_button(
                            label=f"Download {stmt_type} as Excel",
                            data=_buf.getvalue(),
                            file_name=f"{stmt_type.replace(' ', '_')}_{fy_label}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key=f"dl_single_{stmt_type}",
                        )
                    else:
                        st.info(f"No data available for {stmt_type}")

            # Download all statements in one Excel workbook
            st.markdown("---")

            # Two-column layout for download buttons
            dl_col1, dl_col2 = st.columns(2)

            with dl_col1:
                _all_excel = xbrl_parser.generate_excel_bytes(statements)
                st.download_button(
                    label=f"Download All Statements ({fy_label}) as Excel",
                    data=_all_excel,
                    file_name=f"Financial_Statements_{fy_label}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="dl_single_all_excel",
                    type="primary",
                    use_container_width=True,
                )

            with dl_col2:
                # Download XBRL facts for this specific year
                fy_facts = [f for f in st.session_state.all_facts if f.get("fy_label") == fy_label]
                if fy_facts:
                    try:
                        # Get company name for filename
                        company_name = st.session_state.selected_company.get("name", "") if st.session_state.selected_company else ""
                        # Clean company name for filename (remove special characters)
                        import re as re_module
                        company_slug = re_module.sub(r'[\\/:*?"<>|\s]', "_", company_name).strip() if company_name else "company"

                        facts_excel = xbrl_parser.create_xbrl_facts_excel(fy_facts)
                        st.download_button(
                            label=f"Download XBRL Concepts & Facts ({fy_label}) as Excel",
                            data=facts_excel,
                            file_name=f"xbrl_facts_{company_slug}_{fy_label}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="dl_single_facts_excel",
                            type="secondary",
                            use_container_width=True,
                        )
                    except Exception as _exc:
                        st.warning(f"Could not build XBRL facts Excel: {_exc}")
                else:
                    st.info("No raw XBRL facts available for this year.")

    # ------------------------------------------------------------------
    # Block 3 — Consolidate All Filings
    # ------------------------------------------------------------------
    if st.session_state.filings and st.session_state.company_type == 'XBRL' and st.session_state.show_filings:
        st.markdown("---")
        st.header("Consolidate All Filings")

        # Show info about available filings
        total_count = len(st.session_state.filings)
        parsed_count = len(st.session_state.financial_data)

        st.info(f"{total_count} filings available. Currently {parsed_count} parsed. Click 'Consolidate Data' to parse and consolidate all filings.")

        col1, col2 = st.columns([1, 4])

        with col1:
            if st.button("Consolidate Data", type="primary", width="stretch"):
                if not st.session_state.lei or not st.session_state.api_base:
                    st.error("LEI or API Base URL not set")
                else:
                    # Hide individual filing display when consolidating
                    st.session_state.show_individual_filing = False

                    # Check which filings need parsing
                    unparsed_filings = []
                    for filing in st.session_state.filings:
                        pe = filing.get('period_end', '')
                        fy_label = f"FY{pe[:4]}" if pe else "UNKNOWN"
                        if fy_label not in st.session_state.parsed_labels:
                            unparsed_filings.append(filing)

                    # Parse only unparsed filings (silently)
                    if unparsed_filings:
                        progress = st.progress(0, text="Starting...")
                        for i, filing in enumerate(unparsed_filings):
                            pe = filing.get('period_end', '')
                            fy_label = f"FY{pe[:4]}" if pe else "UNKNOWN"

                            progress.progress(i / len(unparsed_filings), text=f"Parsing {fy_label}...")

                            _cname_batch = st.session_state.selected_company.get("name", "") if st.session_state.selected_company else ""
                            statements, xbrl_facts = parse_xbrl_filing(
                                filing,
                                st.session_state.lei,
                                st.session_state.api_base,
                                silent=True,
                                company_name=_cname_batch,
                            )

                            if statements and fy_label not in st.session_state.parsed_labels:
                                st.session_state.financial_data[fy_label] = statements
                                if xbrl_facts:
                                    # Load labels from xbrl_facts_labeled.json if available
                                    from pathlib import Path as PathLib
                                    import re as re_module
                                    _OVH_CFG = _get_section("OVH")
                                    download_dir = PathLib(_OVH_CFG.get("download_dir", "xbrl_filings"))
                                    _cname_batch = st.session_state.selected_company.get("name", "") if st.session_state.selected_company else ""
                                    company_slug = re_module.sub(r'[\\/:*?"<>|]', "_", _cname_batch).strip() if _cname_batch else (st.session_state.lei or "unknown")
                                    labeled_json_path = download_dir / company_slug / fy_label / "xbrl_facts_labeled.json"

                                    labels_map = {}  # {concept_short: (fr_label, en_label)}
                                    if labeled_json_path.exists():
                                        try:
                                            labeled_data = json.loads(labeled_json_path.read_text(encoding="utf-8"))
                                            concepts_dict = labeled_data.get("concepts", {})
                                            for concept_short, concept_data in concepts_dict.items():
                                                fr_label = concept_data.get("fr_label", "")
                                                en_label = concept_data.get("en_label", "")
                                                labels_map[concept_short] = (fr_label, en_label)
                                        except Exception as e:
                                            pass  # Silently fail during batch processing

                                    # Enrich facts with labels
                                    tagged = []
                                    for f in xbrl_facts:
                                        concept_short = f.get("concept_short", "")
                                        concept_full = f.get("concept_full", "")

                                        # Try to get labels from map first
                                        fr_label, en_label = labels_map.get(concept_short, ("", ""))

                                        # If no labels found and it's a company-specific concept, use concept_short as fallback
                                        if not fr_label and not en_label and ":" in concept_full:
                                            namespace = concept_full.split(":")[0]
                                            if namespace.lower() not in ["ifrs-full", "ifrs"]:
                                                # Company-specific concept without label - use concept_short as label
                                                fr_label = concept_short
                                                en_label = concept_short

                                        enriched_fact = {
                                            **f,
                                            "fy_label": fy_label,
                                            "fr_label": fr_label,
                                            "en_label": en_label,
                                        }
                                        tagged.append(enriched_fact)

                                    st.session_state.all_facts = [
                                        f for f in st.session_state.all_facts
                                        if f.get("fy_label") != fy_label
                                    ]
                                    st.session_state.all_facts.extend(tagged)
                                st.session_state.parsed_labels.add(fy_label)
                                st.session_state.filing_metadata[fy_label] = f"{fy_label} (from {pe} filing)"

                        progress.progress(1.0, text="Done!")

                    # Build consolidated view across all parsed FYs
                    all_facts_by_fy = {}
                    for fy_lbl in st.session_state.parsed_labels:
                        all_facts_by_fy[fy_lbl] = [
                            f for f in st.session_state.all_facts
                            if f.get("fy_label") == fy_lbl
                        ]

                    consolidated = xbrl_parser.build_consolidated(all_facts_by_fy)
                    st.session_state.consolidated_data = consolidated

                    st.success(f"Consolidated {len(st.session_state.parsed_labels)} fiscal year(s)")
                    st.rerun()

    # ------------------------------------------------------------------
    # Block 4 — Consolidated Financial Statements display
    # ------------------------------------------------------------------
    if (st.session_state.get("company_type") == "XBRL"
            and st.session_state.get("consolidated_data")):

        st.markdown("---")
        st.markdown("### Consolidated Financial Statements — All Years")

        consolidated_data = st.session_state.consolidated_data
        stmt_types = [s for s in xbrl_parser.STATEMENT_TYPES if s in consolidated_data]

        if stmt_types:
            tabs = st.tabs(stmt_types)
            for tab, stmt_type in zip(tabs, stmt_types):
                with tab:
                    df = consolidated_data[stmt_type]
                    if df is not None and not df.empty:
                        fy_cols = [c for c in df.columns if c not in ("French Label", "English Label", "Concept")]
                        # Detect unit label from parsed facts
                        _unit_label_cons = "€ millions"
                        _all_facts_flat = [f for fy_facts in st.session_state.get("all_facts_by_fy", {}).values() for f in fy_facts if f.get("value_numeric") is not None]
                        if not _all_facts_flat and st.session_state.get("all_facts"):
                            _all_facts_flat = [f for f in st.session_state.all_facts if f.get("value_numeric") is not None]
                        if _all_facts_flat:
                            _unit_label_cons = xbrl_parser.get_value_unit_label(_all_facts_flat[0].get("decimals", ""))
                        st.caption(f"{len(df)} concepts · {len(fy_cols)} year(s): {', '.join(fy_cols)} · Values in {_unit_label_cons}")
                        st.dataframe(df, width="stretch", height=500, hide_index=True)
                        _cons_buf = io.BytesIO()
                        with pd.ExcelWriter(_cons_buf, engine="openpyxl") as _cw:
                            df.to_excel(_cw, sheet_name=stmt_type[:31], index=False)
                        st.download_button(
                            label=f"Download {stmt_type} (All Years) as Excel",
                            data=_cons_buf.getvalue(),
                            file_name=f"{stmt_type.replace(' ', '_')}_consolidated_{datetime.now().strftime('%Y%m%d')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key=f"dl_cons_{stmt_type}",
                        )
                    else:
                        st.info(f"No data available for {stmt_type}")

            # Download row -- Statements Excel + XBRL Facts Excel side by side
            st.markdown("---")
            dl_col1, dl_col2 = st.columns(2)

            with dl_col1:
                excel_bytes = xbrl_parser.generate_excel_bytes(consolidated_data)
                st.download_button(
                    label="Download All Statements as Excel",
                    data=excel_bytes,
                    file_name=f"xbrl_consolidated_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type="primary",
                    width="stretch",
                )

            with dl_col2:
                all_facts = st.session_state.get("all_facts", [])
                if all_facts:
                    try:
                        # Get company name for filename
                        company_name = st.session_state.selected_company.get("name", "") if st.session_state.selected_company else ""
                        # Clean company name for filename (remove special characters)
                        import re as re_module
                        company_slug = re_module.sub(r'[\\/:*?"<>|\s]', "_", company_name).strip() if company_name else "company"

                        facts_excel = xbrl_parser.create_xbrl_facts_excel(all_facts)
                        st.download_button(
                            label="Download XBRL Concepts & Facts Excel",
                            data=facts_excel,
                            file_name=f"xbrl_facts_{company_slug}_consolidated_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            type="secondary",
                            width="stretch",
                        )
                    except Exception as _exc:
                        st.warning(f"Could not build XBRL facts Excel: {_exc}")
                else:
                    st.info("No raw XBRL facts available — parse filings first.")

    # ------------------------------------------------------------------
    # Block 5 — BREF Mapping Section
    # ------------------------------------------------------------------
    if (st.session_state.get("company_type") == "XBRL"
            and st.session_state.get("consolidated_data")):
        
        # Import BREF mapping UI
        try:
            from src.components.brefmap_multi_ui import render_multi_statement_mapping
            BREF_MAPPING_AVAILABLE = True
        except ImportError:
            BREF_MAPPING_AVAILABLE = False
        
        if BREF_MAPPING_AVAILABLE:
            st.markdown("---")
            
            # Convert consolidated XBRL data to extraction results format
            # The consolidated_data has structure: {statement_type: DataFrame}
            # We need to convert to: {statement_type: {"rows": [...], "target_year": ..., "company": ...}}
            
            consolidated_data = st.session_state.consolidated_data
            
            # Determine target year from column names (get the most recent year)
            all_year_cols = []
            for stmt_type, df in consolidated_data.items():
                if df is not None and not df.empty:
                    # Get year columns (exclude label and concept columns)
                    year_cols = [c for c in df.columns if c not in ("French Label", "English Label", "Concept")]
                    all_year_cols.extend(year_cols)
            
            # Extract years from column names
            # For balance sheet: columns are dates like "31-Dec-2024"
            # For income/cash flow: columns are like "FY2024"
            years = []
            for col in all_year_cols:
                if col.startswith("FY"):
                    try:
                        years.append(int(col.replace("FY", "")))
                    except ValueError:
                        pass
                elif "-" in col:  # Date format like "31-Dec-2024"
                    try:
                        year = int(col.split("-")[-1])
                        years.append(year)
                    except (ValueError, IndexError):
                        pass
            
            target_year = max(years) if years else datetime.now().year
            company_name = st.session_state.selected_company.get("name", "") if st.session_state.selected_company else ""
            
            # Map XBRL statement types to BREF statement types
            xbrl_to_bref_map = {
                "Income Statement": "income_statement",
                "Assets": "balance_sheet",  # Assets part of balance sheet
                "Liabilities": "balance_sheet",  # Liabilities part of balance sheet
                "Cash Flow": "cash_flow",
            }
            
                        # XBRL companies are European (EMEA), so use EMEA region
            # EMEA uses the same field codes as APAC (Q-prefix codes)
            # Set session state so brefmap_multi_ui can read it
            st.session_state.selected_region = "EMEA"
            
            # Convert consolidated data to extraction results format
            extraction_results = {}
            
            for stmt_type, df in consolidated_data.items():
                if df is None or df.empty:
                    continue
                
                bref_stmt_type = xbrl_to_bref_map.get(stmt_type)
                if not bref_stmt_type:
                    continue
                
                # For balance sheet, we need to combine Assets and Liabilities
                if bref_stmt_type == "balance_sheet":
                    if bref_stmt_type not in extraction_results:
                        extraction_results[bref_stmt_type] = {
                            "rows": [],
                            "target_year": target_year,
                            "company": company_name,
                        }
                else:
                    extraction_results[bref_stmt_type] = {
                        "rows": [],
                        "target_year": target_year,
                        "company": company_name,
                    }
                
                # Convert DataFrame rows to extraction format
                # Each row should have: {"label": ..., "2024": ..., "2023": ..., etc.}
                for _, row in df.iterrows():
                    # Use English Label as the primary label
                    label = row.get("English Label", row.get("French Label", ""))
                    if not label:
                        continue
                    
                    row_dict = {"label": label}
                    
                    # Add all year columns
                    for col in df.columns:
                        if col in ("French Label", "English Label", "Concept"):
                            continue
                        
                        # Extract year from column name
                        year_str = None
                        if col.startswith("FY"):
                            year_str = col.replace("FY", "")
                        elif "-" in col:  # Date format
                            try:
                                year_str = col.split("-")[-1]
                            except IndexError:
                                continue
                        
                        if year_str:
                            # Convert value to numeric if possible
                            value = row.get(col, "")
                            if value and value != "":
                                # Remove commas and convert to float
                                try:
                                    if isinstance(value, str):
                                        value = value.replace(",", "")
                                        value = float(value)
                                except (ValueError, AttributeError):
                                    pass
                            row_dict[year_str] = value
                    
                    extraction_results[bref_stmt_type]["rows"].append(row_dict)
            
            # Render BREF mapping UI
            if extraction_results:
                render_multi_statement_mapping(
                    extraction_results=extraction_results,
                    key_prefix="xbrl"
                )
            else:
                st.info("No data available for BREF mapping. Please consolidate financial statements first.")
        else:
            st.markdown("---")
            st.warning("BREF mapping module not available. Please ensure all dependencies are installed.")
