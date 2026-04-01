"""
OVHcloud Full Financial Statement Extractor
===========================================
Extracts complete financial tables with French and English labels for all
available fiscal years discovered dynamically from the XBRL filing API.

KEY DESIGN:
  Zero hardcoded concept names - everything auto-extracted
  French labels from viewer JSON (labels.ns0.fr / labels.std.fr)
  English labels from viewer JSON (labels.ns0.en / labels.std.en)
  All 4 tables: Income Statement, Cash Flow, Assets, Liabilities
  Balance Sheet split into Assets tab and Liabilities tab
  All available fiscal years discovered at runtime - no hardcoded year list
  XlsxWriter: professional formatting, alternating rows, frozen panes

HOW LABELS WORK IN VIEWER JSON:
  The ixbrlviewer.html contains a JSON blob with a "concepts" section:
  {
    "ifrs-full:Revenue": {
      "labels": {
        "ns0": {"en": "Total revenue",  "fr": "Total des produits"},
        "std": {"en": "Revenue",        "fr": "Produits des activites ordinaires"}
      }
    },
    "ovhgroupe:CurrentEbitda": {
      "labels": {
        "ns0": {"en": "Current EBITDA", "fr": "EBITDA courant"}
      }
    }
  }
  We prefer "ns0" labels (OVHcloud's own labels) over "std" (IFRS taxonomy).

Install:
    pip install py-xbrl requests pandas openpyxl xlsxwriter beautifulsoup4 lxml

Run:
    python ovhcloud_api_extractor.py
    python ovhcloud_api_extractor.py --debug
"""

import sys, json, re, time, requests, warnings
from src import http_client
from pathlib import Path
from datetime import datetime, timedelta

try:
    import pandas as pd
except ImportError:
    print("Run: pip install pandas openpyxl xlsxwriter requests beautifulsoup4 lxml")
    sys.exit(1)

try:
    from bs4 import BeautifulSoup
    try:
        from bs4 import XMLParsedAsHTMLWarning
        warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
    except ImportError:
        pass
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False
    print("beautifulsoup4 not installed - install with: pip install beautifulsoup4 lxml")

try:
    from xbrl.instance import XbrlInstance, NumericFact
    from xbrl.cache import HttpCache
    HAS_PYXBRL = True
    print("py-xbrl available")
except ImportError:
    HAS_PYXBRL = False
    print("py-xbrl not installed - using raw JSON parser")
    print("Install: pip install py-xbrl")

from config.config import get_section as _get_section
_OVH_CFG = _get_section("OVH")

DEBUG        = "--debug" in sys.argv
OUTPUT       = "ovhcloud_complete_financials.xlsx"
DOWNLOAD_DIR = "/opt/data/raw"
LEI          = "9695001J8OSOVX4TP939"
API_BASE     = "https://filings.xbrl.org"
HEADERS      = {
    "User-Agent": _OVH_CFG.get("user_agent", "OVHcloud-XBRL/2.0 research@example.com"),
    "Accept":     "application/json,*/*",
}


# ============================================================================
#  STEP 1 - API Discovery
# ============================================================================

def api_discover(lei: str, save_path: Path = None) -> list[dict]:
    url = f"{API_BASE}/api/filings"
    print(f"\nGET {url}")
    print(f"filter[entity.identifier]={lei}")
    try:
        r = http_client.get(url, params={"filter[entity.identifier]": lei,
                                          "page[size]": 50},
                            headers=HEADERS, timeout=30)
        r.raise_for_status()
        data    = r.json()
        filings = data.get("data", [])
        total   = data.get("meta", {}).get("count", "?")
        print(f"{len(filings)} filing(s) returned (total on filings.xbrl.org: {total})")
        attrs = []
        for f in filings:
            a = dict(f.get("attributes", {}))
            a["_id"] = f.get("id", "")
            attrs.append(a)
        attrs.sort(key=lambda x: x.get("period_end", ""), reverse=True)
        print()
        for a in attrs:
            j = "json: yes" if a.get("json_url")    else "json: no"
            v = "viewer: yes" if a.get("viewer_url") else "viewer: no"
            print(f"  period={a.get('period_end')}  {j}  {v}  errors={a.get('error_count', 0)}")
        if save_path and attrs:
            save_path.parent.mkdir(parents=True, exist_ok=True)
            save_path.write_text(
                json.dumps(attrs, indent=2, ensure_ascii=False), encoding="utf-8"
            )
            print(f"Saved API listing: {save_path}")
        return attrs
    except Exception as e:
        print(f"API error: {e}")
        return []


def make_fy_config(period_end_str: str) -> dict:
    """Build a fiscal year config dict from a period_end date string."""
    pe   = datetime.strptime(period_end_str, "%Y-%m-%d")
    bs   = pe + timedelta(days=1)
    return {
        "year":       str(pe.year),
        "period_end": period_end_str,
        "bs_instant": bs.strftime("%Y-%m-%d"),
    }


def discover_fiscal_years(filings: list[dict]) -> dict:
    """
    Build a TARGET_FYS dict from whatever fiscal years exist in the API response.
    Returns { "FY2025": {...}, "FY2024": {...}, "FY2023": {...}, ... }
    sorted most-recent first.
    """
    configs = {}
    for f in filings:
        pe = f.get("period_end", "")
        if not pe:
            continue
        fy_label = f"FY{pe[:4]}"
        if fy_label not in configs:
            configs[fy_label] = make_fy_config(pe)
    return dict(sorted(configs.items(), reverse=True))


def pick_filing(filings: list[dict], year: str) -> dict | None:
    for f in filings:
        if f.get("period_end", "").startswith(year):
            return f
    return None


# ============================================================================
#  STEP 2 - Download viewer HTML and extract labels and facts
# ============================================================================

def download_viewer_data(viewer_url: str, save_dir: Path | None = None) -> dict | None:
    """
    Download ixbrlviewer.html and extract the full embedded JSON blob.

    If save_dir is provided the raw HTML is written to:
        <save_dir>/ixbrlviewer.html

    The embedded viewer JSON (the parsed data this function returns) is also
    written separately as:
        <save_dir>/viewer_data.json

    Both files are skipped if they already exist on disk, so re-running the
    script does not re-download files that are already present.
    """
    if not HAS_BS4:
        print("BeautifulSoup not installed")
        return None

    full      = API_BASE + viewer_url
    fname     = viewer_url.split("/")[-1]
    html_path = save_dir / "ixbrlviewer.html"       if save_dir else None
    json_path = save_dir / "viewer_data.json"       if save_dir else None

    # Load from disk if already downloaded
    if html_path and html_path.exists():
        print(f"\nUsing cached {html_path}")
        raw_bytes = html_path.read_bytes()
    else:
        print(f"\nDownloading {fname} ...")
        try:
            r = http_client.get(full, headers=HEADERS, timeout=120)
            r.raise_for_status()
            raw_bytes = r.content
            print(f"{len(raw_bytes)/1024:.0f} KB downloaded")
            if html_path:
                html_path.write_bytes(raw_bytes)
                print(f"Saved HTML: {html_path}")
        except Exception as e:
            print(f"Error: {e}")
            return None

    # Load parsed viewer JSON from disk if already extracted
    if json_path and json_path.exists():
        print(f"Using cached {json_path}")
        try:
            return json.loads(json_path.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"Cache read error, re-parsing: {e}")

    try:
        soup   = BeautifulSoup(raw_bytes, "lxml")
        script = soup.find("script", {"type": "application/x.ixbrl-viewer+json"})
        if not (script and script.string):
            print("No viewer JSON found in HTML")
            return None
        data = json.loads(script.string)
        print(f"JSON parsed: {len(script.string)/1024:.0f} KB")
        if json_path:
            json_path.write_text(
                json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"Saved viewer JSON: {json_path}")
        return data
    except Exception as e:
        print(f"Parse error: {e}")
        return None


def download_oim_json(json_url: str, save_dir: Path | None = None) -> dict | None:
    """
    Download the OIM xBRL-JSON facts file.

    If save_dir is provided the file is written to:
        <save_dir>/report.json

    The file is skipped if it already exists on disk.
    """
    full      = API_BASE + json_url
    fname     = json_url.split("/")[-1]
    save_path = save_dir / "report.json" if save_dir else None

    if save_path and save_path.exists():
        print(f"\nUsing cached {save_path}")
        try:
            return json.loads(save_path.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"Cache read error, re-downloading: {e}")

    print(f"\nDownloading {fname} ...")
    try:
        r = http_client.get(full, headers=HEADERS, timeout=120)
        r.raise_for_status()
        data = r.json()
        print(f"{len(r.content)/1024:.0f} KB downloaded")
        if save_path:
            save_path.write_text(
                json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"Saved OIM JSON: {save_path}")
        return data
    except Exception as e:
        print(f"OIM JSON error: {e}")
        return None


# ============================================================================
#  STEP 3 - Extract French and English labels from viewer JSON
# ============================================================================

def extract_labels(viewer_data: dict) -> dict[str, dict]:
    """
    Returns {concept: {"fr": ..., "en": ...}} for all concepts in viewer JSON.

    The viewer JSON contains a "concepts" section where each entry has a "labels"
    dict with two possible keys:
      "ns0" - OVHcloud's own custom labels (preferred)
      "std"  - IFRS standard taxonomy labels (fallback)
    Both contain "fr" and "en" sub-keys.

    Priority order: ns0 first, then std, then any other key present.
    If only one language is available, it is used for both FR and EN.
    """
    if not viewer_data:
        return {}

    def _find_concepts(obj, depth=0) -> dict:
        if depth > 8:
            return {}
        if isinstance(obj, dict):
            if "concepts" in obj and isinstance(obj["concepts"], dict) \
               and len(obj["concepts"]) > 5:
                return obj["concepts"]
            for v in obj.values():
                r = _find_concepts(v, depth + 1)
                if r:
                    return r
        elif isinstance(obj, list):
            for item in obj[:5]:
                r = _find_concepts(item, depth + 1)
                if r:
                    return r
        return {}

    concepts_raw = _find_concepts(viewer_data)
    if not concepts_raw:
        return {}

    label_map = {}
    for concept_key, meta in concepts_raw.items():
        if not isinstance(meta, dict):
            continue
        labels = meta.get("labels", {})
        if not labels:
            continue

        fr_label = ""
        en_label = ""

        for priority_key in ["ns0", "std"] + [k for k in labels if k not in ("ns0", "std")]:
            lv = labels.get(priority_key, {})
            if not isinstance(lv, dict):
                continue
            if not fr_label and lv.get("fr"):
                fr_label = lv["fr"].strip()
            if not en_label and lv.get("en"):
                en_label = lv["en"].strip()
            if fr_label and en_label:
                break

        if fr_label or en_label:
            label_map[concept_key] = {
                "fr": fr_label or en_label,
                "en": en_label or fr_label,
            }

    both = sum(1 for v in label_map.values() if v["fr"] and v["en"])
    print(f"{len(label_map)} bilingual labels extracted ({both} with both FR+EN)")
    return label_map


# ============================================================================
#  STEP 4 - Parse all facts from viewer JSON
# ============================================================================

def parse_all_facts(viewer_data: dict) -> pd.DataFrame:
    """
    Parse all numeric facts from viewer JSON into a DataFrame.

    Viewer JSON facts format (Arelle iXBRL viewer):
    facts = {
      "fc_173975": {
        "a": {
          "c": "ifrs-full:Revenue",        - concept name
          "p": "2024-09-01/2025-09-01",    - period (duration) or "2025-08-31" (instant)
          "u": "iso4217:EUR"               - unit
        },
        "v": "1084600000",                 - value in raw euros (always)
        "d": -5                            - decimal precision indicator, NOT a scale factor
      }
    }

    Values are always in raw euros. Divided by 1000 to convert to k-euros.
    The "d" field only describes precision for display; it does not change the value.
    """

    def _find_facts(obj, depth=0) -> dict:
        if depth > 8:
            return {}
        if isinstance(obj, dict):
            if "facts" in obj and isinstance(obj["facts"], dict) \
               and len(obj["facts"]) > 5:
                sample = list(obj["facts"].values())[:3]
                if any(isinstance(f, dict) and ("a" in f or "v" in f) for f in sample):
                    return obj["facts"]
            for v in obj.values():
                r = _find_facts(v, depth + 1)
                if r:
                    return r
        elif isinstance(obj, list):
            for item in obj[:5]:
                r = _find_facts(item, depth + 1)
                if r:
                    return r
        return {}

    facts_raw = _find_facts(viewer_data)
    if not facts_raw:
        print("No facts found in viewer JSON")
        return pd.DataFrame()

    records = []
    for fid, fact in facts_raw.items():
        if not isinstance(fact, dict):
            continue
        attrs   = fact.get("a", {})
        if not isinstance(attrs, dict):
            continue
        concept = attrs.get("c", "")
        period  = str(attrs.get("p", ""))
        unit    = str(attrs.get("u", ""))
        raw_val = fact.get("v")

        if not concept or raw_val is None:
            continue

        try:
            value_euros = float(str(raw_val).replace(",", ""))
        except (ValueError, TypeError):
            continue

        value_ke = value_euros / 1_000
        ptype    = "instant" if "/" not in period else "duration"

        records.append({
            "concept":     concept,
            "value_ke":    value_ke,
            "period":      period,
            "period_type": ptype,
            "unit":        unit,
        })

    df = pd.DataFrame(records)
    if df.empty:
        print("No numeric facts parsed")
        return df

    print(f"{len(df)} facts ({df['concept'].nunique()} unique concepts)")

    if DEBUG:
        print("\nALL FACTS:")
        for concept, grp in df.groupby("concept"):
            for _, row in grp.iterrows():
                print(f"  {row['value_ke']:>14,.1f} k€  period={row['period']:<28}  {concept}")
        print()

    return df


# ============================================================================
#  STEP 5 - Classify facts into financial statements
# ============================================================================

# Presentation order for balance sheet rows, confirmed from OVHcloud's published PDF.
# The number assigned to each concept controls sort order in the Excel sheet.
# Non-current assets: 10-88, Current assets: 100-148, Total: 200+

ASSET_ORDER = {
    "ifrs-full:Goodwill":                                                          10,
    "ifrs-full:IntangibleAssetsOtherThanGoodwill":                                 20,
    "ifrs-full:PropertyPlantAndEquipment":                                         30,
    "ifrs-full:RightofuseAssets":                                                  40,
    "ifrs-full:RightofuseAssetsThatDoNotMeetDefinitionOfInvestmentProperty":       40,
    "ifrs-full:NoncurrentDerivativeFinancialAssets":                               50,
    "ifrs-full:OtherNoncurrentReceivables":                                        60,
    "ifrs-full:OtherNoncurrentAssets":                                             65,
    "ifrs-full:OtherNoncurrentFinancialAssets":                                    70,
    "ifrs-full:DeferredTaxAssets":                                                 80,
    "ifrs-full:NoncurrentAssets":                                                  88,
    "ovhgroupe:CurrentTradesReceivablesAndContractAssets":                        100,
    "ifrs-full:TradeAndOtherCurrentReceivables":                                  100,
    "ovhgroupe:OtherReceivablesAndCurrentAssets":                                 110,
    "ifrs-full:OtherCurrentAssets":                                               110,
    "ifrs-full:CurrentTaxAssetsCurrent":                                          120,
    "ifrs-full:CurrentTaxAssets":                                                 120,
    "ifrs-full:CurrentDerivativeFinancialAssets":                                 130,
    "ifrs-full:CashAndCashEquivalents":                                           140,
    "ifrs-full:BalancesWithBanks":                                                140,
    "ifrs-full:CashAndCashEquivalentsIfDifferentFromStatementOfFinancialPosition":142,
    "ifrs-full:CurrentAssets":                                                    148,
    "ifrs-full:Assets":                                                           200,
    "ifrs-full:EquityAndLiabilities":                                             200,
}

# Equity: 10-48, Non-current liabilities: 100-168, Current liabilities: 200-268, Total: 300

LIABILITY_ORDER = {
    "ifrs-full:IssuedCapital":                                                     10,
    "ifrs-full:SharePremium":                                                      20,
    "ovhgroupe:ReservesAndRetainedEarnings":                                       30,
    "ifrs-full:Reserves":                                                          30,
    "ifrs-full:RetainedEarningsProfitLossForReportingPeriod":                      40,
    "ifrs-full:ProfitLoss":                                                        42,
    "ifrs-full:Equity":                                                            48,
    "ifrs-full:LongtermBorrowings":                                               100,
    "ifrs-full:NoncurrentLeaseLiabilities":                                       110,
    "ifrs-full:NoncurrentPortionOfNoncurrentLeaseLiabilities":                    110,
    "ifrs-full:NoncurrentDerivativeFinancialLiabilities":                         120,
    "ifrs-full:OtherNoncurrentFinancialLiabilities":                              130,
    "ifrs-full:NoncurrentProvisions":                                             140,
    "ifrs-full:DeferredTaxLiabilities":                                           150,
    "ifrs-full:OtherNoncurrentLiabilities":                                       160,
    "ifrs-full:NoncurrentLiabilities":                                            168,
    "ifrs-full:CurrentBorrowingsAndCurrentPortionOfNoncurrentBorrowings":         200,
    "ifrs-full:CurrentLeaseLiabilities":                                          210,
    "ifrs-full:CurrentPortionOfNoncurrentLeaseLiabilities":                       210,
    "ifrs-full:CurrentProvisions":                                                220,
    "ifrs-full:TradeAndOtherCurrentPayablesToTradeSuppliers":                     230,
    "ifrs-full:TradeAndOtherCurrentPayables":                                     230,
    "ifrs-full:CurrentTaxLiabilitiesCurrent":                                     240,
    "ifrs-full:CurrentTaxLiabilities":                                            240,
    "ifrs-full:CurrentDerivativeFinancialLiabilities":                            250,
    "ifrs-full:OtherCurrentLiabilities":                                          260,
    "ifrs-full:CurrentLiabilities":                                               268,
    "ifrs-full:EquityAndLiabilities":                                             300,
}

CF_KEYWORDS = [
    "cashflows", "cashflow", "proceedsfrom", "paymentsto", "paymentsof",
    "adjustmentsfor", "interestpaid", "incometaxespaid",
    "effectofexchange", "increasedecrease", "receiptsdisbursements",
    "payments", "receipts", "disbursements",
]

IS_KEYWORDS = [
    "revenue", "profit", "loss", "income", "expense", "benefit",
    "depreciation", "amortisation", "amortization", "impairment",
    "financecost", "financeincomes", "interestexpense", "taxexpense",
    "comprehensive", "ebitda", "operatingincome",
]


def classify(concept: str, period_type: str) -> str:
    """
    Classify a fact into: Income Statement | Cash Flow | Assets | Liabilities | Other

    The fundamental rule:
      - Instant period (a single date)  => balance sheet item
      - Duration period (a date range)  => flow item (income statement or cash flow)

    For balance sheet items the concept local name is matched against keyword lists.
    Liabilities are checked before assets because some names overlap (e.g. "tax").

    For flow items, cash flow keywords are checked first; anything that matches
    income statement keywords is assigned to Income Statement. OVHcloud-specific
    concepts (prefixed ovhgroupe:) have their own keyword sets.
    """
    local = concept.split(":")[-1].lower()

    if period_type == "instant":
        liab_kw = [
            "liabilit", "payable", "payables",
            "debt", "borrowing", "longtermborrowings", "currentborrowings",
            "provision", "provisions",
            "equity", "issuedcapital", "sharepremium",
            "reserves", "retainedearnings", "profitloss",
            "retainedearningsprofitloss",
            "derivativefinancialliabilit",
            "currenttaxliabilit",
            "deferredtaxliabilit",
            "othercurrentliabilit",
            "othernoncurrentliabilit",
            "othernoncurrentfinancialliabilit",
            "equityandliabilities",
            "noncurrentliabilities",
            "currentliabilities",
        ]
        asset_kw = [
            "asset", "assets",
            "goodwill",
            "intangible",
            "property", "propertyplant",
            "rightofuse", "rightofuseasset",
            "receivable", "receivables",
            "cashandcash", "cashequivalents",
            "deferredtaxasset",
            "othernoncurrentreceivable",
            "noncurrentfinancialasset",
            "derivativefinancialasset",
            "currenttaxasset",
            "noncurrentassets",
            "currentassets",
            "balanceswithbanks",
        ]

        if any(k in local for k in liab_kw):
            return "Liabilities"
        if any(k in local for k in asset_kw):
            return "Assets"

        if "ovhgroupe" in concept.lower():
            if any(k in local for k in ["receivable", "tradereceivable", "currenttrade"]):
                return "Assets"
            if any(k in local for k in ["reserves", "retained"]):
                return "Liabilities"
            return "Assets"

        return "Assets"

    if any(k in local for k in CF_KEYWORDS):
        return "Cash Flow"
    if any(k in local for k in IS_KEYWORDS):
        return "Income Statement"

    if "ovhgroupe" in concept.lower():
        if any(k in local for k in ["ebitda", "operating", "income", "revenue", "expense",
                                     "financial", "netfinancial"]):
            return "Income Statement"
        if any(k in local for k in ["cash", "payments", "proceeds", "receipts",
                                     "loans", "advances", "guarantee", "transaction"]):
            return "Cash Flow"

    return "Other"


def period_to_fy(period: str) -> str | None:
    """
    Map a period string to a fiscal year label.

    OVH fiscal year ends August 31 each year.

    Duration  "2024-09-01/2025-08-31"  -> end date Aug 31 2025  -> FY2025
    Instant   "2025-08-31"             -> Aug 31 2025            -> FY2025
    Instant   "2025-09-01"             -> bs_instant for FY2025  -> FY2025
                                          (subtract 1 day to reach Aug 31)

    Returns None if the period string cannot be parsed.
    """
    period = period.strip()
    end    = period.split("/")[1] if "/" in period else period
    try:
        d = datetime.strptime(end[:10], "%Y-%m-%d")
    except ValueError:
        return None
    if d.month == 9 and d.day == 1:
        d = d - timedelta(days=1)
    return f"FY{d.year}"


def group_facts_by_year(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Split a facts DataFrame (which may contain multiple years because each
    filing includes current-year AND prior-year comparative data) into a dict
    keyed by fiscal year label.

    e.g. the FY2025 filing contains FY2025 and FY2024 facts side by side.
    This function separates them into {"FY2025": df25, "FY2024": df24}.
    """
    if df.empty:
        return {}
    df = df.copy()
    df["_fy"] = df["period"].apply(period_to_fy)
    df = df[df["_fy"].notna()]
    groups = {}
    for fy_label, sub in df.groupby("_fy"):
        groups[fy_label] = sub.drop(columns=["_fy"]).reset_index(drop=True)
    return groups


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate rows from a facts DataFrame.

    Step 1: Drop exact duplicates where concept + period + value_ke are identical.
    Step 2: For the same concept + period with different values (which happens when
            a total is reported alongside segment breakdowns), keep the row with the
            largest absolute value - that is the consolidated total.
    """
    if df.empty:
        return df

    df = df.drop_duplicates(subset=["concept", "period", "value_ke"]).copy()

    result = []
    for (concept, period), grp in df.groupby(["concept", "period"]):
        if len(grp) == 1:
            result.append(grp.iloc[0])
        else:
            vals = pd.to_numeric(grp["value_ke"], errors="coerce")
            idx  = vals.abs().idxmax()
            result.append(grp.loc[idx])

    return pd.DataFrame(result).reset_index(drop=True)


def build_table(df_year: pd.DataFrame,
                label_map: dict,
                statement: str) -> pd.DataFrame:
    """
    Build a complete financial statement table for one fiscal year.

    df_year is already scoped to a single fiscal year by group_facts_by_year().

    Steps:
    1. classify()       - assign each row to Income Statement / Cash Flow /
                          Assets / Liabilities based on period_type and concept name
    2. Statement filter - keep rows for the requested statement; for Assets and
                          Liabilities run a secondary keyword pass for any rows
                          that the classifier could not place clearly
    3. deduplicate()    - remove exact copies and keep the largest value when the
                          same concept+period appears under multiple dimensions
    4. Label attachment - map concept -> French and English label; fall back to
                          CamelCase splitting if no label is found
    5. Zero filter      - drop rows with abs(value_ke) < 0.01
    6. Ordering         - ASSET_ORDER / LIABILITY_ORDER for balance sheet tabs;
                          sort by absolute value for Income Statement / Cash Flow
    7. Section tagging  - tag each row as non-current / current / equity / total
                          so write_excel() can insert section-header rows

    Returns DataFrame: fr_label, en_label, concept, value_ke, period, section
    """
    df = df_year.copy()
    if df.empty:
        return pd.DataFrame()

    df["statement"] = df.apply(
        lambda r: classify(r["concept"], r["period_type"]), axis=1)

    if statement == "Assets":
        sub = df[df["statement"] == "Assets"].copy()
        bs_unsorted = df[(df["statement"] == "Balance Sheet") &
                         (df["period_type"] == "instant")]
        if not bs_unsorted.empty:
            liab_local = ["liabilit", "payable", "equity", "capital", "premium",
                          "reserves", "profitloss", "provision", "borrowing"]
            mask = ~bs_unsorted["concept"].str.split(":").str[-1].str.lower().apply(
                lambda x: any(k in x for k in liab_local))
            sub = pd.concat([sub, bs_unsorted[mask]], ignore_index=True)

    elif statement == "Liabilities":
        sub = df[df["statement"] == "Liabilities"].copy()
        bs_unsorted = df[(df["statement"] == "Balance Sheet") &
                         (df["period_type"] == "instant")]
        if not bs_unsorted.empty:
            liab_local = ["liabilit", "payable", "equity", "capital", "premium",
                          "reserves", "profitloss", "provision", "borrowing"]
            mask = bs_unsorted["concept"].str.split(":").str[-1].str.lower().apply(
                lambda x: any(k in x for k in liab_local))
            sub = pd.concat([sub, bs_unsorted[mask]], ignore_index=True)

    else:
        sub = df[df["statement"] == statement].copy()

    if sub.empty:
        return pd.DataFrame()

    sub = deduplicate(sub)
    sub = sub.copy()

    sub["fr_label"] = sub["concept"].map(lambda c: label_map.get(c, {}).get("fr", ""))
    sub["en_label"] = sub["concept"].map(lambda c: label_map.get(c, {}).get("en", ""))

    def _fallback_label(row):
        local = row["concept"].split(":")[-1]
        return re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', local)

    mask = sub["fr_label"] == ""
    sub.loc[mask, "fr_label"] = sub[mask].apply(_fallback_label, axis=1)
    mask = sub["en_label"] == ""
    sub.loc[mask, "en_label"] = sub[mask].apply(_fallback_label, axis=1)

    sub = sub[sub["value_ke"].abs() >= 0.01].copy()

    if statement == "Assets":
        order_map = ASSET_ORDER
    elif statement == "Liabilities":
        order_map = LIABILITY_ORDER
    else:
        order_map = {}

    if order_map:
        def _pres_sort(row):
            pos = order_map.get(row["concept"])
            if pos is not None:
                return (pos, 0)
            return (55, -abs(row["value_ke"]))
        sub["_sk"] = sub.apply(_pres_sort, axis=1)
        sub = sub.sort_values("_sk").drop(columns=["_sk"])
    else:
        sub["_abs"] = sub["value_ke"].abs()
        sub = sub.sort_values("_abs", ascending=False).drop(columns=["_abs"])

    def _section_of(row):
        c = row["concept"]
        if statement == "Assets":
            o = ASSET_ORDER.get(c, 55)
            if o < 88:  return "non-current"
            if o < 200: return "current"
            return "total"
        elif statement == "Liabilities":
            o = LIABILITY_ORDER.get(c, 55)
            if o < 48:  return "equity"
            if o < 168: return "non-current"
            if o < 300: return "current"
            return "total"
        return "line"

    sub["section"] = sub.apply(_section_of, axis=1)
    return sub[["fr_label", "en_label", "concept", "value_ke", "period", "section"]].reset_index(drop=True)


# ============================================================================
#  STEP 6 - py-xbrl label enrichment
# ============================================================================

def pyxbrl_enrich_labels(json_url: str, label_map: dict,
                          cache_dir: str = ".xbrl_cache") -> dict:
    """
    Use py-xbrl to resolve IFRS taxonomy labels for concepts still missing
    French or English labels after extract_labels().
    Operates only on concepts not already present in label_map.
    """
    if not HAS_PYXBRL or not json_url:
        return label_map

    full_url = API_BASE + json_url
    print(f"\n[py-xbrl] Enriching labels from taxonomy ...")
    try:
        cache    = HttpCache(cache_dir)
        instance = XbrlInstance.create_from_url(full_url, cache)

        count = 0
        for fact in instance.facts:
            if not isinstance(fact, NumericFact):
                continue
            prefix = fact.concept.prefix or ""
            local  = str(fact.concept.name)
            key    = f"{prefix}:{local}" if prefix else local
            if key in label_map:
                continue

            lbs = {}
            try:
                lbs = fact.concept.labels
            except Exception:
                pass

            fr = (lbs.get("fr", {}).get("standard") or
                  lbs.get("fr", {}).get("label", ""))
            en = (lbs.get("en", {}).get("standard") or
                  lbs.get("en", {}).get("label", ""))

            if en or fr:
                label_map[key] = {"fr": fr or en, "en": en or fr}
                count += 1

        print(f"py-xbrl added {count} additional labels")
    except Exception as e:
        print(f"py-xbrl enrichment failed: {e}")

    return label_map


# ============================================================================
#  STEP 7 - Write Excel with XlsxWriter
# ============================================================================

SHEET_STYLES = {
    "Income Statement": {"hdr_bg": "#1A4080", "alt_bg": "#EDF3FC"},
    "Cash Flow":        {"hdr_bg": "#145A32", "alt_bg": "#E9F7EF"},
    "Assets":           {"hdr_bg": "#6E4B00", "alt_bg": "#FEF9E7"},
    "Liabilities":      {"hdr_bg": "#4A235A", "alt_bg": "#F5EEF8"},
}


def write_excel(all_tables: dict, output: str):
    """
    Write all financial statements to a formatted Excel workbook.

    all_tables structure:
      {
        "Income Statement": {"FY2025": df, "FY2024": df, "FY2023": df, ...},
        "Cash Flow":        {"FY2025": df, ...},
        "Assets":           {"FY2025": df, ...},
        "Liabilities":      {"FY2025": df, ...},
      }

    Column layout per sheet (N = number of fiscal years):
      Col 0      : French label
      Col 1      : English label
      Col 2..N+1 : Year values, most recent first (one column per year)
      Col N+2    : XBRL concept identifier

    No YoY change columns are written - only the raw values from each filing.
    """
    try:
        import xlsxwriter
    except ImportError:
        print("\nxlsxwriter not found - install: pip install xlsxwriter")
        print("Falling back to openpyxl ...")
        _write_openpyxl(all_tables, output)
        return

    print(f"\nWriting {output} ...")
    wb = xlsxwriter.Workbook(output, {"nan_inf_to_errors": True})

    def F(**kw):
        d = {"font_name": "Arial", "font_size": 10, "valign": "vcenter"}
        d.update(kw)
        return wb.add_format(d)

    num_fmt = "#,##0;(#,##0);\"-\""

    # Cover sheet
    cov = wb.add_worksheet("Overview")
    cov.hide_gridlines(2)
    cov.set_column("A:A", 32)
    cov.set_column("B:G", 22)
    cov.set_row(0, 48)
    cov.merge_range("A1:G1",
        "OVHcloud (OVH Groupe SA) - Complete ESEF Financial Statements",
        F(bold=True, font_size=17, font_color="#FFFFFF", bg_color="#0D1B2A",
          align="center", valign="vcenter"))
    cov.set_row(1, 20)
    cov.merge_range("A2:G2",
        f"Source: filings.xbrl.org REST API  |  ESEF / IFRS  |  Amounts in k€  |  "
        f"{datetime.now():%Y-%m-%d %H:%M}",
        F(italic=True, font_size=9, font_color="#CCCCCC", bg_color="#0D1B2A",
          align="center"))

    cov.set_row(3, 18)
    cov.write("A4", "Data Source and Method",
        F(bold=True, font_size=11, font_color="#0D1B2A", bottom=2))
    cov.merge_range("B4:G4", "", F(bottom=2))

    info_rows = [
        ("Company",         "OVHcloud / OVH Groupe SA"),
        ("Stock Exchange",  "Euronext Paris (OVH)"),
        ("LEI",             LEI),
        ("Fiscal Year End", "31 August each year"),
        ("Currency",        "Euros (EUR) - amounts in k€ (thousands)"),
        ("API",             f"GET {API_BASE}/api/filings?filter[entity.identifier]={LEI}"),
        ("Data File",       "ixbrlviewer.html embedded JSON / OIM xBRL-JSON fallback"),
        ("Label Source",    "French + English from viewer JSON concepts.labels.ns0/std"),
        ("Parser",          "Raw viewer JSON + py-xbrl taxonomy enrichment"),
        ("Negatives",       "Outflows shown in (parentheses)"),
        ("Coverage",        "All facts extracted automatically - no hardcoded concept list"),
    ]
    k_fmt = F(bold=True, font_color="#1A4080", bg_color="#F0F4FF", border=1)
    v_fmt = F(font_color="#333333", bg_color="#FFFFFF", border=1)
    for ri, (k, v) in enumerate(info_rows, 5):
        cov.set_row(ri, 15)
        cov.write(ri, 0, k, k_fmt)
        cov.merge_range(ri, 1, ri, 6, v, v_fmt)

    cov.set_row(17, 22)
    cov.write(17, 0, "Financial Statement Sheets",
        F(bold=True, font_size=11, font_color="#FFFFFF", bg_color="#1A4080", border=1))
    cov.merge_range(17, 1, 17, 6, "", F(bg_color="#1A4080", border=1))

    for ri, (stmt, style) in enumerate(SHEET_STYLES.items(), 18):
        stmt_years = all_tables.get(stmt, {})
        counts     = {fy: len(df) for fy, df in stmt_years.items()}
        summary    = "  |  ".join(
            f"{fy}: {n} lines" for fy, n in sorted(counts.items(), reverse=True))
        cov.set_row(ri, 18)
        cov.write(ri, 0, stmt,
            F(bold=True, font_color="#FFFFFF", bg_color=style["hdr_bg"],
              border=1, indent=1))
        cov.merge_range(ri, 1, ri, 6, summary,
            F(font_color="#FFFFFF", bg_color=style["hdr_bg"], border=1, indent=1))

    # One sheet per financial statement
    for stmt, style in SHEET_STYLES.items():
        hdr_bg = style["hdr_bg"]
        alt_bg = style["alt_bg"]

        ws = wb.add_worksheet(stmt)
        ws.hide_gridlines(2)

        stmt_data = all_tables.get(stmt, {})
        years     = sorted(stmt_data.keys(), reverse=True)

        # Column layout: FR label | EN label | year cols... | XBRL concept
        n_years     = len(years)
        concept_col = 2 + n_years

        ws.freeze_panes(4, 0)
        ws.set_column(0, 0, 50)
        ws.set_column(1, 1, 50)
        for ci in range(2, 2 + n_years):
            ws.set_column(ci, ci, 17)
        ws.set_column(concept_col, concept_col, 52)

        ws.set_row(0, 36)
        ws.merge_range(0, 0, 0, concept_col,
            f"OVHcloud - {stmt}  |  IFRS  |  k€  |  Source: filings.xbrl.org",
            F(bold=True, font_size=14, font_color="#FFFFFF",
              bg_color="#0D1B2A", align="left", indent=2, valign="vcenter"))

        ws.set_row(1, 16)
        ws.merge_range(1, 0, 1, concept_col,
            f"All XBRL-tagged line items extracted automatically  "
            f"LEI: {LEI}  |  Amounts in thousands of euros (k€)  |  Outflows in (parentheses)",
            F(italic=True, font_size=8, font_color="#AAAAAA",
              bg_color="#0D1B2A", align="left", indent=2))

        ws.set_row(2, 6)
        ws.merge_range(2, 0, 2, concept_col, "", F(bg_color=hdr_bg))

        ws.set_row(3, 24)
        hdr_f = F(bold=True, font_color="#FFFFFF", bg_color=hdr_bg,
                  align="center", border=1, text_wrap=True, font_size=10)

        col_headers  = ["Libelle (Francais)", "Label (English)"]
        col_headers += [f"{y} (k€)" for y in years]
        col_headers += ["Concept XBRL"]

        for ci, h in enumerate(col_headers):
            ws.write(3, ci, h, hdr_f)

        if not years or all(stmt_data.get(y, pd.DataFrame()).empty for y in years):
            ws.merge_range(4, 0, 4, concept_col,
                f"No {stmt} data found - check API connectivity",
                F(font_color="#CC0000", bold=True))
            continue

        # Build unified concept list: union of all years, preserving labels
        seen = {}
        for fy in years:
            df = stmt_data.get(fy, pd.DataFrame())
            if not df.empty:
                for _, r in df.iterrows():
                    if r["concept"] not in seen:
                        seen[r["concept"]] = {"fr": r["fr_label"], "en": r["en_label"]}

        val_maps = {}
        for fy in years:
            df = stmt_data.get(fy, pd.DataFrame())
            val_maps[fy] = dict(zip(df["concept"], df["value_ke"])) if not df.empty else {}

        # Sort: concepts present in most recent year first (by magnitude), then older years
        def _sort_key(c):
            v = val_maps[years[0]].get(c) if years else None
            if v is not None:
                return (0, -abs(v))
            for fy in years[1:]:
                v = val_maps[fy].get(c)
                if v is not None:
                    return (1, -abs(v))
            return (2, 0)

        ordered = sorted(seen.keys(), key=_sort_key)

        SECTION_HDRS = {
            ("Assets",      "non-current"): "Non-current Assets / Actif non courant",
            ("Assets",      "current"):     "Current Assets / Actif courant",
            ("Assets",      "total"):       "TOTAL ASSETS / TOTAL ACTIF",
            ("Liabilities", "equity"):      "Equity / Capitaux propres",
            ("Liabilities", "non-current"): "Non-current Liabilities / Passif non courant",
            ("Liabilities", "current"):     "Current Liabilities / Passif courant",
            ("Liabilities", "total"):       "TOTAL EQUITY AND LIABILITIES / TOTAL PASSIF ET CAPITAUX PROPRES",
        }

        last_section = None
        actual_row   = 4

        for concept in ordered:
            sec = "line"
            for fy in years:
                df = stmt_data.get(fy, pd.DataFrame())
                if (df is not None and not df.empty
                        and "section" in df.columns
                        and concept in df["concept"].values):
                    sec = df.loc[df["concept"] == concept, "section"].values[0]
                    break

            if stmt in ("Assets", "Liabilities") and sec != last_section \
               and sec not in ("line", ""):
                hdr_text = SECTION_HDRS.get((stmt, sec), "")
                if hdr_text:
                    is_total = (sec == "total")
                    sec_bg   = hdr_bg if is_total else "#2F6096"
                    ws.set_row(actual_row, 22)
                    ws.merge_range(actual_row, 0, actual_row, concept_col, hdr_text,
                        F(bold=True, font_color="#FFFFFF", bg_color=sec_bg,
                          border=1, indent=1, font_size=10 if is_total else 9,
                          top=2, bottom=2))
                    actual_row += 1
                last_section = sec

            yr_vals = [val_maps[fy].get(concept) for fy in years]
            fr_lbl  = seen[concept]["fr"]
            en_lbl  = seen[concept]["en"]

            is_bold = sec in ("total",) or any(
                k in concept.split(":")[-1].lower()
                for k in ["noncurrentassets", "currentassets",
                           "noncurrentliabilities", "currentliabilities",
                           "equityandliabilities", "assets", "equity"])

            alt = (actual_row % 2 == 0)
            bg  = "#E8F4E8" if is_bold else (alt_bg if alt else "#FFFFFF")

            ws.set_row(actual_row, 18)
            ws.write(actual_row, 0, fr_lbl,
                F(bg_color=bg, border=1, indent=2 if not is_bold else 1,
                  text_wrap=True, font_color="#0D1B2A", bold=is_bold))
            ws.write(actual_row, 1, en_lbl,
                F(bg_color=bg, border=1, indent=2 if not is_bold else 1,
                  text_wrap=True, font_color="#444444", italic=True,
                  font_size=9, bold=is_bold))

            nf = F(bg_color=bg, border=1, align="right", num_format=num_fmt, bold=is_bold)
            for ci, v in enumerate(yr_vals, 2):
                ws.write(actual_row, ci, round(v) if v is not None else None, nf)

            ws.write(actual_row, concept_col, concept,
                F(bg_color="#F2F2F2", border=1, font_color="#AAAAAA", font_size=8))
            actual_row += 1

        print(f"Sheet: {stmt}  ({len(ordered)} line items, {len(years)} years)")

    wb.close()
    print(f"\nSaved: {output}")


def _write_openpyxl(all_tables: dict, output: str):
    """Fallback writer used when xlsxwriter is not installed."""
    from openpyxl import Workbook as OWB
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter as gcl

    wb  = OWB()
    cov = wb.active
    cov.title  = "Overview"
    cov["A1"]  = "OVHcloud Complete Financial Statements"
    cov["A2"]  = f"Source: filings.xbrl.org API | py-xbrl | {datetime.now():%Y-%m-%d}"

    def _border():
        s = Side(style="thin", color="AAAAAA")
        return Border(left=s, right=s, top=s, bottom=s)

    for stmt in ["Income Statement", "Cash Flow", "Assets", "Liabilities"]:
        ws    = wb.create_sheet(stmt)
        years = sorted(all_tables.get(stmt, {}).keys(), reverse=True)

        hdrs  = ["Libelle (Francais)", "Label (English)"]
        hdrs += [f"{y} (k€)" for y in years]
        hdrs += ["Concept XBRL"]

        for ci, h in enumerate(hdrs, 1):
            c           = ws.cell(1, ci, h)
            c.font      = Font(name="Arial", bold=True, color="FFFFFF", size=10)
            c.fill      = PatternFill("solid", fgColor="1A4080")
            c.border    = _border()
            c.alignment = Alignment(horizontal="center", vertical="center")

        stmt_data = all_tables.get(stmt, {})
        seen      = {}
        for fy in years:
            df = stmt_data.get(fy, pd.DataFrame())
            if not df.empty:
                for _, r in df.iterrows():
                    if r["concept"] not in seen:
                        seen[r["concept"]] = {"fr": r["fr_label"], "en": r["en_label"]}

        val_maps = {}
        for fy in years:
            df = stmt_data.get(fy, pd.DataFrame())
            val_maps[fy] = dict(zip(df["concept"], df["value_ke"])) if not df.empty else {}

        for ri, concept in enumerate(seen, 2):
            yr_vals = [val_maps[fy].get(concept) for fy in years]
            row     = [seen[concept]["fr"], seen[concept]["en"]]
            row    += [round(v) if v is not None else None for v in yr_vals]
            row    += [concept]

            for ci, val in enumerate(row, 1):
                c           = ws.cell(ri, ci, val)
                c.border    = _border()
                c.font      = Font(name="Arial", size=9)
                if ci >= 3:
                    c.alignment = Alignment(horizontal="right")

        ws.column_dimensions["A"].width = 48
        ws.column_dimensions["B"].width = 48
        for i in range(len(years)):
            ws.column_dimensions[gcl(3 + i)].width = 18
        ws.column_dimensions[gcl(3 + len(years))].width = 48
        ws.freeze_panes = "A2"

    wb.save(output)
    print(f"\nSaved (openpyxl): {output}")


# ============================================================================
#  MAIN
# ============================================================================

def main():
    print("OVHcloud Complete Financial Extractor")
    print("=" * 40)

    root_dir = Path(DOWNLOAD_DIR)
    root_dir.mkdir(exist_ok=True)

    all_filings = api_discover(LEI, save_path=root_dir / "api_filings.json")
    if not all_filings:
        print("\n[FATAL] No filings found. Check network connection.")
        sys.exit(1)

    TARGET_FYS = discover_fiscal_years(all_filings)
    if not TARGET_FYS:
        print("\n[FATAL] No fiscal years discovered from filings.")
        sys.exit(1)

    print(f"\nFiscal year filings found: {', '.join(TARGET_FYS.keys())}")
    print("Each filing contains current-year AND prior-year comparative data.")
    print(f"\nDownload directory: {root_dir.resolve()}")

    # master_facts: {fy_label: DataFrame}
    # Each filing contributes two years of data. Facts from multiple filings
    # are merged and deduplicated so the primary filing for a year takes
    # precedence (largest absolute value wins in deduplicate()).
    master_facts = {}
    label_master = {}

    for fy_label, fy_cfg in TARGET_FYS.items():
        print(f"\nProcessing filing {fy_label}  ({fy_cfg['period_end']})")
        print("-" * 40)

        filing = pick_filing(all_filings, fy_cfg["year"])
        if not filing:
            print(f"No filing found for {fy_label}")
            continue

        fy_dir = root_dir / fy_label
        fy_dir.mkdir(exist_ok=True)
        print(f"Filing directory: {fy_dir}")

        viewer_url = filing.get("viewer_url", "")
        json_url   = filing.get("json_url", "")

        labels = {}
        facts  = pd.DataFrame()

        if viewer_url:
            viewer_data = download_viewer_data(viewer_url, save_dir=fy_dir)
            if viewer_data:
                labels = extract_labels(viewer_data)
                facts  = parse_all_facts(viewer_data)

        if facts.empty and json_url:
            print(f"\n[fallback] Trying OIM JSON ...")
            oim = download_oim_json(json_url, save_dir=fy_dir)
            if oim:
                facts_raw = oim.get("facts", {})
                records   = []
                for fid, f in (facts_raw.items() if isinstance(facts_raw, dict) else []):
                    dims = f.get("dimensions", {})
                    c    = dims.get("concept", "")
                    p    = str(dims.get("period", ""))
                    v    = f.get("value")
                    if c and v is not None:
                        try:
                            records.append({
                                "concept":     c,
                                "value_ke":    float(v) / 1000,
                                "period":      p,
                                "period_type": "instant" if "/" not in p else "duration",
                            })
                        except Exception:
                            pass
                if records:
                    facts = pd.DataFrame(records)
                    print(f"OIM JSON: {len(facts)} facts")

        if json_url:
            if not (fy_dir / "report.json").exists():
                download_oim_json(json_url, save_dir=fy_dir)
            labels = pyxbrl_enrich_labels(json_url, labels)

        # Merge labels into master map
        for k, v in labels.items():
            if k not in label_master:
                label_master[k] = v
            else:
                if not label_master[k]["fr"] and v["fr"]:
                    label_master[k]["fr"] = v["fr"]
                if not label_master[k]["en"] and v["en"]:
                    label_master[k]["en"] = v["en"]

        if facts.empty:
            print(f"No facts extracted for filing {fy_label}")
            time.sleep(0.5)
            continue

        # Split all facts in this filing by the year each fact belongs to.
        # A FY2025 filing contains both FY2025 and FY2024 comparative facts.
        year_groups = group_facts_by_year(facts)
        years_in_filing = sorted(year_groups.keys(), reverse=True)
        print(f"Years found in this filing: {', '.join(years_in_filing)}")

        for yr_label, yr_df in year_groups.items():
            if yr_label in master_facts:
                # Append - deduplicate will resolve duplicates later
                master_facts[yr_label] = pd.concat(
                    [master_facts[yr_label], yr_df], ignore_index=True)
            else:
                master_facts[yr_label] = yr_df.copy()

        print(f"Filing summary: {len(facts)} total facts  |  {len(labels)} labels")
        time.sleep(0.5)

    if not master_facts:
        print("\n[FATAL] No facts collected from any filing.")
        sys.exit(1)

    # Deduplicate merged facts for each year
    print(f"\nDeduplicating facts per year ...")
    for yr_label in master_facts:
        before = len(master_facts[yr_label])
        master_facts[yr_label] = deduplicate(master_facts[yr_label])
        after  = len(master_facts[yr_label])
        print(f"  {yr_label}: {before} -> {after} rows after dedup")

    all_years_sorted = sorted(master_facts.keys(), reverse=True)
    print(f"\nYears with data: {', '.join(all_years_sorted)}")

    print(f"\nBuilding financial statement tables")
    print("-" * 40)

    all_tables = {}
    for stmt in ["Income Statement", "Cash Flow", "Assets", "Liabilities"]:
        print(f"\n[BUILD] {stmt}")
        stmt_data = {}
        for yr_label in all_years_sorted:
            yr_facts = master_facts[yr_label]
            table    = build_table(yr_facts, label_master, stmt)
            stmt_data[yr_label] = table
            print(f"  {yr_label}: {len(table)} rows")

            if DEBUG and not table.empty:
                print(table[["fr_label", "en_label", "value_ke"]].to_string())

        all_tables[stmt] = stmt_data

    try:
        write_excel(all_tables, OUTPUT)
    except PermissionError:
        alt = OUTPUT.replace(".xlsx", "_new.xlsx")
        print(f"\n{OUTPUT} is open in Excel - saving as {alt}")
        write_excel(all_tables, alt)

    print(f"\nRESULTS SUMMARY")
    print("=" * 40)
    for stmt in ["Income Statement", "Cash Flow", "Assets", "Liabilities"]:
        parts = [f"{fy}: {len(all_tables.get(stmt, {}).get(fy, pd.DataFrame()))}"
                 for fy in all_years_sorted]
        print(f"  {stmt:<22}  {'  |  '.join(parts)}")
    print(f"\n  Output: {OUTPUT}\n")


def run(year: int | None = None, lei: str | None = None, api_base: str | None = None) -> dict:
    """
    Callable entry point for the pipeline.

    Args:
        year:     fiscal year to restrict to (e.g. 2025), or None for all years.
        lei:      LEI identifier from the source document's filters field.
        api_base: XBRL API base URL from the source document's sourceUrl field.

    Returns:
        Dict with paths to every file produced:
        {
            "excel":       "/abs/path/ovhcloud_complete_financials.xlsx" or None,
            "api_listing": "/abs/path/ovhcloud_filings/api_filings.json" or None,
            "per_year": {
                "FY2025": {
                    "viewer_html": "/abs/path/.../ixbrlviewer.html",
                    "viewer_json": "/abs/path/.../viewer_data.json",
                    "oim_json":    "/abs/path/.../report.json",
                },
                ...
            },
        }
    """
    global LEI, API_BASE
    if lei:
        LEI = lei
    if api_base:
        API_BASE = api_base
    main()

    root_dir = Path(DOWNLOAD_DIR)
    result: dict = {
        "excel":       str(Path(OUTPUT).resolve()) if Path(OUTPUT).exists() else None,
        "api_listing": None,
        "per_year":    {},
    }

    api_path = root_dir / "api_filings.json"
    if api_path.exists():
        result["api_listing"] = str(api_path.resolve())

    if root_dir.exists():
        for fy_dir in sorted(root_dir.iterdir()):
            if not fy_dir.is_dir() or not fy_dir.name.startswith("FY"):
                continue
            fy_files = {}
            for fname, key in [
                ("ixbrlviewer.html", "viewer_html"),
                ("viewer_data.json", "viewer_json"),
                ("report.json",      "oim_json"),
            ]:
                p = fy_dir / fname
                if p.exists():
                    fy_files[key] = str(p.resolve())
            if fy_files:
                result["per_year"][fy_dir.name] = fy_files

    return result


if __name__ == "__main__":
    main()
