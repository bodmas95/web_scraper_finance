"""
DIAGNOSTIC SCRIPT — Inspect actual JSON structure of ixbrlviewer.html
Run this first to understand the data structure, then v6 will parse it correctly.

Usage:  python diagnose_viewer.py
Output: prints JSON structure + saves raw JSON to viewer_data.json
"""
import sys, re, json, requests, warnings
try:
    from bs4 import XMLParsedAsHTMLWarning
    warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
except ImportError:
    pass
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "OVHcloud-XBRL-Research/diag research@example.com"}
URL = ("https://filings.xbrl.org/9695001J8OSOVX4TP939/2025-08-31"
       "/ESEF/FR/0/ovhgroupe-2025-08-31-0-fr/reports/ixbrlviewer.html")

print(f"Downloading {URL} …")
r = requests.get(URL, headers=HEADERS, timeout=120)
print(f"HTTP {r.status_code}  size={len(r.content)/1024:.0f} KB")
html = r.content

# ── Find ALL script tags ──────────────────────────────────────────────────
soup = BeautifulSoup(html, "lxml")
scripts = soup.find_all("script")
print(f"\nFound {len(scripts)} <script> tags total")

for i, s in enumerate(scripts):
    stype  = s.get("type","(no type)")
    ssrc   = s.get("src","")
    slen   = len(s.string or "")
    print(f"  [{i:2d}] type={stype!r:45s} src={ssrc[:60]!r:62s} inner_len={slen:,}")

# ── Extract the large script tag ─────────────────────────────────────────
print("\n── Largest script tags (inner content > 1000 chars) ──")
big_scripts = [(i, s) for i, s in enumerate(scripts)
               if len(s.string or "") > 1000]
for i, s in big_scripts:
    content = (s.string or "").strip()
    print(f"\n  Script [{i}]  length={len(content)/1024:.0f} KB")
    print(f"  First 300 chars: {repr(content[:300])}")
    print(f"  Last  200 chars: {repr(content[-200:])}")

# ── Try to parse as JSON and show top-level keys ──────────────────────────
print("\n── Attempting JSON parse of each large script ──")
for i, s in big_scripts:
    content = (s.string or "").strip()
    # Try direct parse
    for attempt, text in [("direct", content),
                           ("strip prefix", re.sub(r'^[^{[]+', '', content, count=1))]:
        try:
            data = json.loads(text)
            print(f"\n  ✓ Script [{i}] parsed as JSON ({attempt})")
            print(f"    Type: {type(data).__name__}")
            if isinstance(data, dict):
                print(f"    Top-level keys: {list(data.keys())[:20]}")
                # Recurse one level
                for k, v in list(data.items())[:10]:
                    vtype = type(v).__name__
                    vlen  = len(v) if isinstance(v, (dict,list,str)) else "n/a"
                    sample = ""
                    if isinstance(v, dict):
                        sample = str(list(v.keys())[:5])
                    elif isinstance(v, list) and v:
                        sample = str(v[0])[:100]
                    elif isinstance(v, str):
                        sample = v[:100]
                    print(f"      '{k}': {vtype}  len={vlen}  sample={sample!r}")
            elif isinstance(data, list):
                print(f"    List length: {len(data)}")
                if data:
                    print(f"    First item type: {type(data[0]).__name__}")
                    if isinstance(data[0], dict):
                        print(f"    First item keys: {list(data[0].keys())[:10]}")

            # Save the parsed JSON for inspection
            with open(f"viewer_data_script{i}.json", "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            print(f"    → Saved to viewer_data_script{i}.json")
            break
        except json.JSONDecodeError as e:
            print(f"  ✗ Script [{i}] JSON parse failed ({attempt}): {e}")

# ── Also search for 'facts' anywhere in the raw text ─────────────────────
text = html.decode("utf-8", errors="replace")
print("\n── Searching for 'facts' key in raw HTML ──")
matches = [m.start() for m in re.finditer(r'"facts"', text)]
print(f"  Found 'facts' at {len(matches)} position(s)")
for pos in matches[:5]:
    print(f"  Position {pos}: ...{text[max(0,pos-50):pos+100]}...")

# Search for known XBRL concept patterns
print("\n── Searching for IFRS concept names ──")
ifrs_matches = re.findall(r'ifrs-full:[A-Za-z]+', text)
unique_ifrs = sorted(set(ifrs_matches))
print(f"  Found {len(unique_ifrs)} unique ifrs-full concepts")
for c in unique_ifrs[:30]:
    print(f"    {c}")

# Search for financial values we know (1084600 = Revenue in k€)
print("\n── Searching for known values ──")
for val in ["1084600", "1084.6", "991800", "261900", "354400", "3900"]:
    count = text.count(val)
    if count:
        idx = text.index(val)
        print(f"  Found '{val}' ({count}x): …{text[max(0,idx-80):idx+80]}…")
    else:
        print(f"  NOT found: '{val}'")

print("\nDone. Check the .json file(s) saved above for full structure.")
