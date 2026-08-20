"""
BREF Mapper v1 — Three-Pass LLM Mapping Engine
===============================================

Maps BREF template fields to annual report extraction rows using a sequential
four-stage pipeline. Each stage only receives fields that the previous stage
could not resolve.

Pipeline stages
---------------
PASS 1 — Value Match (LLM)
    For every BREF field that has a reference_value (previous-year value from
    the template), ask the LLM to find the one extraction row whose prior-year
    value matches within 1 % tolerance.
        LLM returns "mapped"    → verify the pick, accept or self-correct.
        LLM returns "ambiguous" → resolve locally by choosing the candidate
                                   whose ref-year value is closest to the
                                   template value (lowest hierarchy level as
                                   tiebreaker).
        LLM returns "no_match"  → forward to Pass 1.1.

PASS 1.1 — Sign-Inversion Check (no LLM)
    Pure numeric scan.  For fields that Pass 1 could not match, check whether
    any unused extraction row matches when ALL reference-year template values
    are negated (e.g. BREF stores NCI income as -431, EDGAR reports +431).
    Qualification requires ≥ 1 non-zero verified year, scaled up to 2 when the
    extraction row itself has ≥ 2 non-zero values across the reference years
    (prevents false positives on sparse / zero-heavy rows like I38).
    Accepted matches set sign_flipped=True and negate target_value before output.

PASS 2 — Alias Match (LLM)
    For fields still unmatched, the LLM receives each field's label, known
    aliases (from field_mappings.py), and ALL reference-year values.  The LLM
    must match BOTH by name AND verify the prior-year values agree.
    Post-LLM, we re-run _verify_prev_years() ourselves (normal sign first,
    inverted sign second) to catch any remaining sign-convention mismatches.

PASS 3 — Self-Reasoning / Derived Match (LLM)
    For fields still unmatched, the LLM reasons from scratch over ALL available
    years in the extraction, applying five structured steps:
      1. Identify source row(s)
      2. Verify derivation across ALL reference years
      3. Handle sign conventions
      4. List component rows and arithmetic for combinations
      5. State the final formula
    Post-LLM, _verify_prev_years() is applied again for a final sanity check.

Duplicate guard
---------------
Each extraction row label may be matched to at most ONE BREF field across all
passes.  The shared `used_labels` set tracks claimed rows.

Output shape per field
-----------------------
{
  "label":               str,          # BREF field code + description
  "reference_value":     float|None,   # previous-year value from template
  "all_ref_values":      {year: val},  # all prior years from template
  "sheet_name":          str,
  "row_num":             int,
  "matched_label":       str|None,     # extraction row label that was matched
  "target_value":        float|None,   # value to write for target_year
  "status":              str,          # mapped / mapped_alias / mapped_derived /
                                       # no_match / no_ref_values / calculated
  "reason":              str,
  "pass":                int,          # 0-3
  "year_values":         {year: val},  # all years from the matched extraction row
  "extracted_ref_value": float|None,   # prior-year value found in extraction
  "confidence":          str|None,     # high / medium / low  (Pass 2/3 LLM)
  "formula":             str|None,     # Pass 3 formula
  "components":          list|None,    # Pass 3 component row labels
  "sign_flipped":        bool|None,    # True if sign was inverted to match
  "years_verified":      dict|None,    # Pass 3 year-by-year verification
}
"""

# ═══════════════════════════════════════════════════════════════════════════════
# Imports
# ═══════════════════════════════════════════════════════════════════════════════

import json
import re
import sys
import time
import traceback
from typing import Any

from src.extraction.llm_client import get_client, track_usage
from src.mapping_v1.config import BATCH_SIZE, BATCH_SIZE_PASS3, VALUE_MATCH_TOLERANCE as TOLERANCE

# Newline constant — avoids issues with the write_file tool splitting literal backslash-n.
NL = "\n"


# ═══════════════════════════════════════════════════════════════════════════════
# Numeric helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _to_float(value: Any) -> float | None:
    """Convert a cell/string value to float; return None if not numeric."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.replace(",", "").strip())
        except ValueError:
            return None
    return None


def _values_match(a: Any, b: Any, tolerance: float = TOLERANCE) -> bool:
    """
    Return True if two values agree within the given relative tolerance.
    Both 0 → True.  Either None → False.
    """
    try:
        fa, fb = float(a), float(b)
        if fa == 0 and fb == 0:
            return True
        return abs(fa - fb) / max(abs(fa), abs(fb)) <= tolerance
    except (TypeError, ValueError, ZeroDivisionError):
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# Row / column helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _col_date_rank(col_name: str) -> tuple:
    """
    Sort key for a column name.  When a row has multiple columns for the same
    calendar year (e.g. "2024-06-30" and "2024-12-31"), we prefer the one with
    the latest date so we always pick the fiscal year-end value.

    Returns (year, month, day) for descending sort comparison.
    """
    m = re.search(r"(20\d{2})[^\d]?(\d{2})?[^\d]?(\d{2})?", str(col_name))
    if not m:
        return (0, 0, 0)
    return (
        int(m.group(1)),
        int(m.group(2)) if m.group(2) else 0,
        int(m.group(3)) if m.group(3) else 0,
    )


def _best_col_for_year(row: dict, target_year: int) -> tuple:
    """
    Among all columns in a row that belong to `target_year`, pick the one with
    the latest date (year-end preferred over mid-year).

    Returns (col_name, float_value) or (None, None) if no match.
    """
    skip = {"label", "parent", "parent_abstract_concept", "Currency", "Unit", "level"}
    candidates = []
    for col_name, col_value in row.items():
        if col_name in skip:
            continue
        m = re.search(r"(20\d{2})", str(col_name))
        if not m or int(m.group(1)) != target_year:
            continue
        fval = _to_float(col_value)
        if fval is None:
            continue
        candidates.append((_col_date_rank(col_name), col_name, fval))
    if not candidates:
        return None, None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1], candidates[0][2]


def _all_year_values(row: dict) -> dict:
    """
    Return {year_str: value} for every calendar year present in the row.
    When multiple columns share the same year, the LATEST date wins.
    """
    skip = {"label", "parent", "parent_abstract_concept", "Currency", "Unit", "level"}
    by_year: dict[int, list] = {}
    for col_name, col_value in row.items():
        if col_name in skip:
            continue
        m = re.search(r"(20\d{2})", str(col_name))
        if not m:
            continue
        year = int(m.group(1))
        fval = _to_float(col_value)
        if fval is None:
            continue
        by_year.setdefault(year, []).append((_col_date_rank(col_name), fval))
    return {
        str(yr): sorted(vals, key=lambda x: x[0], reverse=True)[0][1]
        for yr, vals in by_year.items()
    }


def _extract_year_values(row: dict, ref_year: int, target_year: int) -> tuple:
    """Return (ref_year_value, target_year_value), preferring year-end dates."""
    _, ref_val    = _best_col_for_year(row, ref_year)
    _, target_val = _best_col_for_year(row, target_year)
    return ref_val, target_val


def _build_row_lookup(rows: list) -> dict:
    """
    Build a case-insensitive label → row dict so LLM-returned labels can be
    resolved back to the original extraction row.

    Indexed by:
      - bare label                        ("Cash and cash equivalents")
      - parent > label                    ("AssetsCurrent > Cash and cash equivalents")
      - label stripped of parent prefix   (in case LLM drops the parent)
    """
    lookup: dict[str, dict] = {}
    for row in rows:
        label  = row.get("label", "")
        parent = row.get("parent", "")
        lookup[label.lower().strip()] = row
        if parent:
            lookup[(parent + " > " + label).lower().strip()] = row
        for sep in (" > ", "> ", " >"):
            if sep in label:
                lookup[label.split(sep, 1)[-1].lower().strip()] = row
    return lookup


# ═══════════════════════════════════════════════════════════════════════════════
# Prompt-context formatters
# ═══════════════════════════════════════════════════════════════════════════════

def _rows_to_text(rows: list, ref_year: int, target_year: int) -> str:
    """
    Format extraction rows as human-readable text for Pass 1 / Pass 2 prompts.
    Each line: [parent > ] label | prev=YYYY:value | curr=YYYY:value
    """
    lines = []
    for row in rows:
        label  = row.get("label", "")
        parent = row.get("parent", "")
        ref_v, tgt_v = _extract_year_values(row, ref_year, target_year)
        context = (parent + " > " + label) if parent else label
        lines.append(
            context +
            " | prev=" + str(ref_year) + ":" + (str(ref_v) if ref_v is not None else "-") +
            " | curr=" + str(target_year) + ":" + (str(tgt_v) if tgt_v is not None else "-")
        )
    return NL.join(lines)


def _rows_to_text_all_years(rows: list) -> str:
    """
    Format extraction rows with ALL available year values for Pass 3 prompts.
    Each line: [parent > ] label | YYYY:value | YYYY:value | ...
    """
    lines = []
    for row in rows:
        label   = row.get("label", "")
        parent  = row.get("parent", "")
        years   = _all_year_values(row)
        context = (parent + " > " + label) if parent else label
        yr_str  = " | ".join(str(y) + ":" + str(v) for y, v in sorted(years.items()))
        lines.append(context + " | " + yr_str)
    return NL.join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# LLM call + JSON handling
# ═══════════════════════════════════════════════════════════════════════════════

def _llm_call(prompt: str, provider: Any, model: Any) -> str:
    """
    Send a prompt to the configured LLM and return the raw response string.

    Streams the response so large outputs do not time out.  Prints progress
    dots every 3 seconds.  Falls back to a retry without stream_options if the
    provider (e.g. Maia) does not support it.
    """
    print(NL + "[mapping_v1] LLM call — provider=" + str(provider) + " model=" + str(model))
    client = get_client(provider=provider, model=model)

    call_kwargs = dict(
        model=model or "gemma3-27b-it/",
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        response_format={"type": "json_object"},
        stream=True,
    )
    # stream_options (includes token usage in the stream) is OpenAI-native only.
    # Maia proxy does not support it — omit to avoid 400 errors.
    if provider != "maia":
        call_kwargs["stream_options"] = {"include_usage": True}

    try:
        stream = client.chat.completions.create(**call_kwargs)
    except Exception as e:
        print("[mapping_v1] LLM error — retrying without stream_options: " + str(e))
        call_kwargs.pop("stream_options", None)
        stream = client.chat.completions.create(**call_kwargs)

    full      = ""
    last_ping = time.time()
    for chunk in stream:
        delta = chunk.choices[0].delta.content if chunk.choices else ""
        if delta:
            full += delta
            sys.stdout.write(delta)
            sys.stdout.flush()
            if time.time() - last_ping >= 3:
                print(NL + "[waiting… " + str(len(full)) + " chars]", flush=True)
                last_ping = time.time()
        if hasattr(chunk, "usage") and chunk.usage:
            try:
                track_usage(chunk)
            except Exception:
                pass

    print(NL + "[Done — " + str(len(full)) + " chars]" + NL, flush=True)
    return full


def _clean_json_string(raw: str) -> str:
    """
    Replace unescaped control characters (newlines, carriage returns, tabs)
    that appear inside JSON string values with their escaped equivalents.

    LLMs sometimes include literal newlines inside 'reason' or 'formula'
    strings, which causes json.loads() to raise "Expecting ',' delimiter".
    """
    BACKSLASH = chr(92)
    result      = []
    in_string   = False
    escape_next = False
    for ch in raw:
        if escape_next:
            result.append(ch)
            escape_next = False
            continue
        if ch == BACKSLASH:
            escape_next = True
            result.append(ch)
            continue
        if ch == '"':
            in_string = not in_string
            result.append(ch)
            continue
        if in_string and ch in ("\n", "\r", "\t"):
            result.append("\\n" if ch == "\n" else ("\\r" if ch == "\r" else "\\t"))
            continue
        result.append(ch)
    return "".join(result)


def _repair_truncated_json(raw: str) -> str:
    """
    Best-effort repair of a JSON response that was cut off mid-stream because
    the LLM hit its output token limit.

    Strategy: walk the string tracking open brackets / braces and whether we
    are inside a string literal.  When we reach the end with unclosed
    containers, close them in reverse order so json.loads() can parse whatever
    complete fields were emitted before truncation.
    """
    BACKSLASH   = chr(92)
    in_string   = False
    escape_next = False
    depth_stack = []  # tracks '{' or '[' open containers

    for ch in raw:
        if escape_next:
            escape_next = False
            continue
        if ch == BACKSLASH and in_string:
            escape_next = True
            continue
        if ch == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch in ("{", "["):
            depth_stack.append(ch)
        elif ch in ("}", "]"):
            if depth_stack:
                depth_stack.pop()

    if not depth_stack:
        return raw  # Not truncated — return as-is.

    repaired = raw
    if in_string:
        repaired += '"'                          # close the open string
    repaired = repaired.rstrip().rstrip(",")     # drop trailing comma if any
    for opener in reversed(depth_stack):
        repaired += "}" if opener == "{" else "]"
    return repaired


def _parse_json(raw: str) -> dict:
    """
    Parse LLM JSON output with three progressive fallback attempts:

    1. Parse as-is — works for well-formed responses.
    2. Clean control characters — fixes literal newlines inside string values
       (common in 'reason' or 'formula' fields).
    3. Repair + clean — closes unclosed brackets for truncated responses.
    """
    raw = raw.strip()
    # Strip markdown code fences the LLM sometimes wraps around JSON.
    if raw.startswith("```"):
        raw = re.sub(r"^```[a-z]*\n?", "", raw)
        raw = re.sub(r"\n?```$", "", raw.strip())

    # Attempt 1: parse as-is.
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    # Attempt 2: clean unescaped control characters, then parse.
    try:
        return json.loads(_clean_json_string(raw))
    except json.JSONDecodeError:
        pass

    # Attempt 3: repair truncation on the cleaned string, then parse.
    cleaned  = _clean_json_string(raw)
    repaired = _repair_truncated_json(cleaned)
    print("[mapper] JSON malformed — repair attempted (" + str(len(raw)) + " → " + str(len(repaired)) + " chars)")
    try:
        return json.loads(repaired)
    except json.JSONDecodeError:
        pass

    # Attempt 4: mid-string structural corruption (e.g. "1,234" unescaped comma
    # inside a numeric string, or LLM-injected literal quotes inside a value).
    # Extract every individually-valid {...} object from the array and re-assemble.
    # We lose the one malformed entry but the rest of the batch succeeds.
    print("[mapper] JSON still invalid after repair — attempting object-level extraction")
    valid_objects = []
    depth = 0
    start = None
    for i, ch in enumerate(cleaned):
        if ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start is not None:
                fragment = cleaned[start:i + 1]
                try:
                    obj = json.loads(fragment)
                    valid_objects.append(obj)
                except json.JSONDecodeError:
                    # Try cleaning the fragment individually
                    try:
                        obj = json.loads(_clean_json_string(fragment))
                        valid_objects.append(obj)
                    except json.JSONDecodeError:
                        print("[mapper] Skipping malformed object at char " + str(start) + "–" + str(i))
                start = None

    if valid_objects:
        print("[mapper] Recovered " + str(len(valid_objects)) + " valid object(s) via extraction")
        # Wrap in {"mappings": [...]} or return as list depending on context
        # The callers always expect either {"mappings": [...]} or a list
        if isinstance(valid_objects[0], dict) and "label" in valid_objects[0]:
            # Pass 1/2 style: array of field objects — wrap in list
            return valid_objects
        # Pass 3 style: {"mappings": [...]}
        return {"mappings": valid_objects}

    raise json.JSONDecodeError(
        "All JSON parse attempts failed. Raw response (first 500 chars): " + raw[:500],
        raw, 0
    )


# ═══════════════════════════════════════════════════════════════════════════════
# LLM Prompts
# ═══════════════════════════════════════════════════════════════════════════════

_PASS1_PROMPT = (
    "You are a financial data analyst performing a value-based lookup." + NL + NL +
    "Extracted rows from the annual report:" + NL +
    "(format:  [parent > ] label | prev={ref_year}:value | curr={target_year}:value)" + NL + NL +
    "{rows_text}" + NL + NL +
    "BREF fields to match (JSON):" + NL +
    "{fields_json}" + NL + NL +
    "Task:" + NL +
    "For each BREF field, find the ONE extraction row whose previous-year value" + NL +
    "numerically matches the field's reference_value within a 1% tolerance." + NL + NL +
    "Rules:" + NL +
    '1. Exactly one match   -> status "mapped",    fill matched_label and current_value.' + NL +
    '2. Zero matches        -> status "no_match",   matched_label null, current_value null.' + NL +
    '3. Two or more matches -> status "ambiguous",  matched_label null, current_value null.' + NL +
    "4. Each extraction row may be proposed for at most ONE field in this batch." + NL +
    "5. Preserve the sign of current_value exactly as it appears in the data." + NL + NL +
    "Respond ONLY with valid JSON:" + NL +
    '{{"results": [{{"field_label": "I30 | Sales (turnover)", "matched_label": "Revenue", "current_value": 5000, "status": "mapped"}}, ...]}}'
)

_PASS2_PROMPT = (
    "You are a financial data analyst matching BREF template fields to annual report" + NL +
    "rows using field names, known aliases, AND previous-year value verification." + NL + NL +
    "Extracted rows from the annual report:" + NL +
    "(format:  [parent > ] label | prev={ref_year}:value | curr={target_year}:value)" + NL + NL +
    "{rows_text}" + NL + NL +
    "BREF fields to match (JSON — each field includes aliases AND all previous-year reference values):" + NL +
    "{fields_json}" + NL + NL +
    "Task: For each BREF field, find the single extraction row that BOTH:" + NL +
    "  a) has a label matching the field name or one of its aliases, AND" + NL +
    "  b) has previous-year value(s) that agree with reference_values within 2%." + NL + NL +
    "Rules:" + NL +
    "1. ALIAS PRIORITY — if a row label appears in the aliases list, prefer it." + NL +
    "2. VALUE VERIFICATION IS MANDATORY — a name match alone is NOT enough." + NL +
    "   If the matched row prev-year value does NOT agree within 2%, set no_match." + NL +
    "3. If reference_values has multiple years, ALL must agree." + NL +
    "4. Each extraction row may be used for at most ONE field in this batch." + NL +
    "5. Preserve the sign of current_value exactly as it appears in the data." + NL + NL +
    'Confidence: "high"   = alias/name + value verified across all years.' + NL +
    '           "medium" = concept match + value verified.' + NL +
    '           "low"    = name only, no value verification (rejected automatically).' + NL + NL +
    "Respond ONLY with valid JSON:" + NL +
    '{{"results": [{{"field_label": "L22 | Short-term debt", "matched_label": "Current portion of LT debt", "current_value": 4200, "status": "mapped_alias", "confidence": "high", "reason": "Alias match verified: row 2022=1758 matches ref 1758"}}, ...]}}' + NL
)

_PASS3_PROMPT = (
    "You are a senior financial analyst populating a BREF template." + NL +
    "Some fields could not be matched. You must figure them out yourself." + NL + NL +
    "Extracted rows — ALL available years shown:" + NL +
    "(format:  [parent > ] label | YYYY:value | YYYY:value | ...)" + NL + NL +
    "{rows_text_all_years}" + NL + NL +
    "BREF fields that still need mapping (JSON — includes ALL previous-year reference values):" + NL +
    "{fields_json}" + NL + NL +
    "For each field, reason through these five steps:" + NL +
    "  1. IDENTIFY SOURCE — which row(s) produce the correct value?" + NL +
    "  2. VERIFY ACROSS ALL PREVIOUS YEARS (MANDATORY):" + NL +
    "     The derivation MUST reproduce ALL reference_values for ALL available previous years." + NL +
    "     Example: if reference_values: 2021→1361, 2022→1758, your formula must give" + NL +
    "     ~1361 for 2021 AND ~1758 for 2022. If either fails, set no_match." + NL +
    "  3. SIGN CONVENTIONS — are any rows stored positive but should be negated?" + NL +
    "  4. COMBINATIONS — if multiple rows are needed, list all components + arithmetic." + NL +
    "  5. FINAL FORMULA — state the exact formula used." + NL + NL +
    'Confidence: "high"   = derivation verified across ALL reference years.' + NL +
    '           "medium" = verified for most years, one minor discrepancy.' + NL +
    '           "low"    = best guess, not fully verified (rejected automatically).' + NL +
    'If derivation cannot be verified against reference_values, use status "no_match".' + NL + NL +
    "Respond ONLY with valid JSON:" + NL +
    '{{"results": [{{"field_label": "I32 | Other financial", "matched_label": "Other income", "current_value": -10, "status": "mapped_derived", "confidence": "high", "components": ["OtherExpenses", "OtherIncome"], "sign_flipped": true, "formula": "-(OtherExpenses)+OtherIncome", "years_verified": {{{{"2021": -120, "2022": -141}}}}, "reason": "..."}}, ...]}}'
)


# ═══════════════════════════════════════════════════════════════════════════════
# Verification helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _verify_prev_years(matched_row: dict, all_ref_values: dict, tolerance: float = 0.02) -> tuple:
    """
    Verify that a matched extraction row agrees with ALL available previous-year
    reference values from the BREF template.

    Two-stage sign check
    --------------------
    Stage 1 — Normal sign:
        Check whether each prior year's extraction value matches the template
        value within tolerance.  Pass if ALL available years agree.

    Stage 2 — Inverted sign (only if Stage 1 fails):
        Some sources (e.g. SEC EDGAR) report NCI income as positive while the
        BREF convention stores it as negative.  Re-check with -template_value.
        If ALL years pass with the inverted sign, return sign_flipped=True so
        the caller negates target_value before writing.

    Args:
        matched_row:    The extraction row dict (label + year columns).
        all_ref_values: {year_str: template_value} e.g. {"2022": -431, "2023": 41}
        tolerance:      Relative tolerance for numeric comparison (default 2%).

    Returns:
        (passed: bool, sign_flipped: bool, detail: str)
    """
    if not all_ref_values:
        return True, False, "No previous years to verify."

    checks = []
    for yr_str, ref_val in all_ref_values.items():
        if ref_val is None:
            continue
        _, row_val = _best_col_for_year(matched_row, int(yr_str))
        if row_val is None:
            continue  # Year not in this row — not a failure, just skip.
        checks.append((yr_str, ref_val, row_val, _values_match(ref_val, row_val, tolerance)))

    if not checks:
        return True, False, "No comparable years found in row."

    # Stage 1: normal sign.
    if all(c[3] for c in checks):
        detail = " | ".join(
            str(c[0]) + ": template=" + str(c[1]) + " row=" + str(c[2]) + " OK"
            for c in checks
        )
        return True, False, detail

    # Stage 2: inverted sign across ALL years.
    inv_checks = [
        (yr_str, ref_val, row_val, _values_match(-ref_val, row_val, tolerance))
        for yr_str, ref_val, row_val, _ in checks
    ]
    if all(c[3] for c in inv_checks):
        detail = " | ".join(
            str(c[0]) + ": template=" + str(c[1]) + " row=" + str(c[2]) + " OK (sign inverted)"
            for c in inv_checks
        )
        return True, True, detail

    # Both normal and inverted failed across ALL years.
    # Last resort: check if just the MOST RECENT ref year agrees (normal sign).
    # This handles cases where a line item's composition changed in older years
    # (e.g. I32 for AES: 2023 matches perfectly but 2022 used a different source).
    # Accept with partial=True so callers can flag as medium confidence.
    most_recent = max(checks, key=lambda c: c[0])
    if _values_match(most_recent[1], most_recent[2], tolerance):
        detail = (
            "PARTIAL — most recent year " + str(most_recent[0]) +
            ": template=" + str(most_recent[1]) + " row=" + str(most_recent[2]) + " OK" +
            " | older years differ (composition may have changed)"
        )
        return True, False, detail

    # All checks failed completely.
    detail = " | ".join(
        str(c[0]) + ": template=" + str(c[1]) + " row=" + str(c[2]) + " FAIL"
        for c in checks
    )
    return False, False, detail


def _build_empty_result(field: dict, status: str, reason: str, pass_num: int) -> dict:
    """Return a result dict with no matched value and the given status/reason."""
    return {
        **field,
        "matched_label":       None,
        "target_value":        None,
        "status":              status,
        "reason":              reason,
        "pass":                pass_num,
        "year_values":         {},
        "extracted_ref_value": None,
        "confidence":          None,
        "formula":             None,
        "components":          None,
        "sign_flipped":        None,
        "years_verified":      None,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Field metadata lookups  (field_mappings.py)
# ═══════════════════════════════════════════════════════════════════════════════

def _get_aliases(field_label: str, region: str = "US") -> list:
    """
    Look up known aliases for a BREF field label from field_mappings.py.
    Returns an empty list if no aliases are defined or the lookup fails.
    Never raises — failures degrade gracefully to Pass 3.
    """
    try:
        from src.mapping.field_mappings import get_field_mappings
        for stmt_fields in get_field_mappings(region).values():
            if field_label in stmt_fields:
                return stmt_fields[field_label].get("aliases", [])
    except Exception:
        pass
    return []


# Fields permanently excluded from both mapping and calculation.
# These are OCI / comprehensive-income rows that sit below the net-profit line
# and are never populated by the mapping or calculation pipeline.
IGNORED_FIELDS: frozenset = frozenset({
    "I75 | Comprehensive income, net of income taxes",
    "I77 | +/- Income (loss) attributable to Non-controlling interests",
    "I78 | Comprehensive income attributable to owners of the Group",
})


def _is_ignored(field_label: str) -> bool:
    """Return True if the field should be silently skipped in mapping and calculation."""
    clean = field_label.lstrip("* \u00a0")
    return clean in IGNORED_FIELDS or any(clean.startswith(ig.split(" | ")[0] + " |") for ig in IGNORED_FIELDS)


def _is_calculated(field_label: str, region: str = "US") -> tuple:
    """
    Check whether a BREF field is marked as calculated (formula-derived)
    or is in the permanent ignore list.
    Returns (is_calculated: bool, formula_string: str).
    Such fields are skipped entirely — no LLM call, no formula evaluation.
    """
    if _is_ignored(field_label):
        return True, ""
    try:
        from src.mapping.field_mappings import get_field_mappings
        for stmt_fields in get_field_mappings(region).values():
            if field_label in stmt_fields:
                fd = stmt_fields[field_label]
                if fd.get("is_calculated", False):
                    return True, fd.get("calculation") or ""
    except Exception:
        pass
    return False, ""


# ═══════════════════════════════════════════════════════════════════════════════
# Pass 1 — Value Match (LLM)
# ═══════════════════════════════════════════════════════════════════════════════

def _pass1_batch(
    batch: list, rows: list, ref_year: int, target_year: int,
    provider: Any, model: Any, used_labels: set,
) -> list:
    """
    Run Pass 1 (value-based LLM match) for a batch of BREF fields.

    After receiving the LLM response we apply two local corrections:

    Correction 1 — Verify the LLM's pick:
        The LLM may return "mapped" with the wrong row (e.g. when two rows share
        the same reference-year value but differ in target year — B150 "Cash and
        cash equivalents" vs the Parent Company sub-row).  We re-verify by
        checking that matched_row's ref-year value actually matches
        reference_value.  If not, we scan all rows ourselves and pick the best
        match (closest value, lowest hierarchy level as tiebreaker).

    Correction 2 — Ambiguous resolver:
        When the LLM returns "ambiguous" (multiple rows match the ref value),
        we resolve locally: filter to candidates whose ref-year value is
        verified, then pick the one at the lowest hierarchy level.
    """
    rows_text   = _rows_to_text(rows, ref_year, target_year)
    fields_json = json.dumps(
        [{"field_label": f["label"], "reference_value": f["reference_value"]} for f in batch],
        indent=2,
    )
    prompt = _PASS1_PROMPT.format(
        ref_year=ref_year, target_year=target_year,
        rows_text=rows_text, fields_json=fields_json,
    )
    print(NL + "  [Pass 1] " + str(len(batch)) + " fields → LLM")
    try:
        raw = _llm_call(prompt, provider, model)
    except Exception as e:
        print("[mapping_v1] Pass 1 LLM call failed: " + str(e))
        traceback.print_exc()
        return [_build_empty_result(f, "no_match", "LLM call failed: " + str(e), 1) for f in batch]

    data       = _parse_json(raw)
    llm_res    = {r["field_label"]: r for r in data.get("results", [])}
    row_lookup = _build_row_lookup(rows)
    results    = []

    for field in batch:
        label               = field["label"]
        r                   = llm_res.get(label, {})
        status              = r.get("status", "no_match")
        year_values         = {}
        extracted_ref_value = None
        sign_flipped        = False
        matched             = None
        cur_val             = None
        reason              = "No value match found."

        if status == "mapped":
            matched = r.get("matched_label")
            if matched and matched.lower().strip() in used_labels:
                results.append(_build_empty_result(field, "no_match", "Duplicate: row already used.", 1))
                continue

            if matched:
                matched_row         = row_lookup.get(matched.lower().strip(), {})
                year_values         = _all_year_values(matched_row)
                extracted_ref_value = year_values.get(str(ref_year))
                _, row_ref_val      = _best_col_for_year(matched_row, ref_year)
                tmpl_ref            = field.get("reference_value")

                # Verify the LLM's pick: its ref-year value must match the
                # template reference_value.  If not, self-correct by scanning.
                if tmpl_ref is not None and not _values_match(tmpl_ref, row_ref_val):
                    correct = [
                        row for row in rows
                        if row.get("label", "").lower().strip() not in used_labels
                        and _values_match(tmpl_ref, _best_col_for_year(row, ref_year)[1])
                    ]
                    if correct:
                        correct.sort(key=lambda row: (
                            abs(float(_best_col_for_year(row, ref_year)[1] or 0) - float(tmpl_ref)),
                            int(row.get("level", 99)) if str(row.get("level", 99)).isdigit() else 99,
                        ))
                        matched_row         = correct[0]
                        matched             = matched_row.get("label", "")
                        year_values         = _all_year_values(matched_row)
                        extracted_ref_value = year_values.get(str(ref_year))
                        reason              = "Value match (self-corrected): '" + matched + "' prev-year ~= " + str(tmpl_ref)
                    else:
                        # No correct row found — forward to Pass 1.1.
                        results.append(_build_empty_result(field, "no_match", "No value match found.", 1))
                        continue
                else:
                    reason = "Value match: '" + str(matched) + "' prev-year ~= " + str(field["reference_value"])

                used_labels.add(matched.lower().strip())
                _, cur_val = _extract_year_values(matched_row, ref_year, target_year)
            else:
                cur_val = None

        elif status == "ambiguous":
            # LLM found multiple rows with the same reference-year value.
            # Resolve locally: prefer rows whose ref-year value is verified,
            # then pick the lowest hierarchy level (most summary row).
            tmpl_ref   = field.get("reference_value")
            candidates = [
                row for row in rows
                if _values_match(tmpl_ref, _best_col_for_year(row, ref_year)[1])
                and row.get("label", "").lower().strip() not in used_labels
            ]
            if candidates:
                verified = [r for r in candidates if _values_match(tmpl_ref, _best_col_for_year(r, ref_year)[1])]
                pool     = verified if verified else candidates
                pool.sort(key=lambda r: (
                    int(r.get("level", 99)) if str(r.get("level", 99)).isdigit() else 99
                ))
                matched_row         = pool[0]
                matched             = matched_row.get("label", "")
                used_labels.add(matched.lower().strip())
                year_values         = _all_year_values(matched_row)
                extracted_ref_value = year_values.get(str(ref_year))
                _, cur_val          = _extract_year_values(matched_row, ref_year, target_year)
                status              = "mapped"
                reason              = "Ambiguous resolved: ref-year verified — picked '" + matched + "'."
            else:
                reason = "Ambiguous: multiple rows match with differing target values."

        results.append({
            **field,
            "matched_label":       matched,
            "target_value":        cur_val,
            "status":              status,
            "reason":              reason,
            "pass":                1,
            "year_values":         year_values,
            "extracted_ref_value": extracted_ref_value,
            "confidence":          None,
            "formula":             None,
            "components":          None,
            "sign_flipped":        True if sign_flipped else None,
            "years_verified":      None,
        })

    return results


# ═══════════════════════════════════════════════════════════════════════════════
# Pass 1.1 — Sign-Inversion Check (no LLM)
# ═══════════════════════════════════════════════════════════════════════════════

def _pass1_sign_check(
    no_match_fields: list, rows: list, ref_year: int, target_year: int, used_labels: set,
    region: str = "US", statement_type: str = "",
) -> tuple:
    """
    Pass 1.1 — pure numeric sign-inversion scan.

    Some financial sources (e.g. SEC EDGAR) report values with the opposite
    sign to the BREF convention.  For example:
      - BREF template stores NCI income as -431 (negative = deducted from group)
      - EDGAR reports it as  +431 (positive = income attributable to NCI)

    For each unmatched field, scan all unused extraction rows and check whether
    ALL available reference-year template values match when negated.

    Qualification threshold
    -----------------------
    We require at least `min_required` non-zero verified years, where:
        min_required = min(2, nonzero_values_available_in_row)

    This means:
      - If the extraction row has ≥ 2 non-zero ref-year values → require 2 to
        confirm the inversion (prevents false positives).
      - If the row only has 1 non-zero ref-year value (e.g. I38 where 2022=0
        in extraction) → require just 1 (avoids silently dropping valid matches).

    Accepted matches
    ----------------
    sign_flipped=True is stored in the result and target_value is negated
    (extraction is opposite to BREF → negate to restore BREF convention).
    
    EMEA Cashflow Special Handling
    -------------------------------
    For EMEA region cashflow statement:
      - ICF38 and ICF49: Apply sign change (set sign_flipped=True)
      - All other fields: Detect sign mismatch but DON'T change the sign
        (set sign_mismatch_detected=True for orange highlighting in UI)

    Returns (sign_mapped: list, still_no_match: list).
    """
    sign_mapped    = []
    still_no_match = []

    for field in no_match_fields:
        all_refs = field.get("all_ref_values", {str(ref_year): field.get("reference_value")})
        if not all_refs:
            still_no_match.append(field)
            continue

        inv_candidates = []
        for row in rows:
            if row.get("label", "").lower().strip() in used_labels:
                continue

            ok_for_all       = True
            nonzero_verified = 0

            for yr_str, rv in all_refs.items():
                if rv is None:
                    continue
                _, row_val = _best_col_for_year(row, int(yr_str))
                if row_val is None:
                    continue
                if not _values_match(-rv, row_val):
                    ok_for_all = False
                    break
                if rv != 0:
                    nonzero_verified += 1

            if not ok_for_all:
                continue

            # Compute how many non-zero ref-year values exist in THIS row
            # to calibrate the minimum required for confidence.
            nonzero_in_row = sum(
                1 for yr_str in all_refs
                if _best_col_for_year(row, int(yr_str))[1] not in (None, 0, 0.0)
            )
            min_required = min(2, nonzero_in_row) if nonzero_in_row > 0 else 2

            if nonzero_verified >= min_required and nonzero_verified >= 1:
                inv_candidates.append(row)

        if not inv_candidates:
            still_no_match.append(field)
            continue

        # When multiple rows qualify, prefer the summary-level row (no parent).
        no_parent = [r for r in inv_candidates if not r.get("parent", "")]
        best = no_parent[0] if no_parent else inv_candidates[0]

        matched = best.get("label", "")
        used_labels.add(matched.lower().strip())
        raw_year_values = _all_year_values(best)
        _, cur_val = _extract_year_values(best, ref_year, target_year)
        
        # Extract field code for EMEA cashflow special handling
        field_label = field.get("label", "")
        field_code = field_label.split(" |")[0].strip() if " |" in field_label else field_label.strip()
        
                # EMEA Cashflow Special Handling:
        # - ICF38 and ICF49: Apply sign change (current behavior)
        # - All other fields: Detect mismatch but don't change sign
        should_flip_sign = True
        sign_mismatch_detected = False
        
        if region == "EMEA" and statement_type == "cash_flow":
            if field_code in ("ICF38", "ICF49"):
                # These fields ALWAYS get sign flipped
                should_flip_sign = True
            else:
                # Other fields: detect mismatch but don't flip
                should_flip_sign = False
                sign_mismatch_detected = True
        
        # Apply sign flip conditionally
        if should_flip_sign:
            # Negate ALL values — extraction sign is opposite to BREF convention.
            # Correct at source so UI, Excel, and Pass 4 all see consistent values.
            year_values         = {yr: -v for yr, v in raw_year_values.items()}
            extracted_ref_value = year_values.get(str(ref_year))
            if cur_val is not None:
                cur_val = -cur_val
            reason_text = "Sign-inverted match (Pass 1.1): '" + matched + "' — all ref years match with negated template values."
        else:
            # Don't flip sign - keep original values for analyst review
            year_values         = raw_year_values
            extracted_ref_value = year_values.get(str(ref_year))
            reason_text = "Sign mismatch detected (Pass 1.1): '" + matched + "' — values match with opposite signs. Analyst should review."

        sign_mapped.append({
            **field,
            "matched_label":       matched,
            "target_value":        cur_val,
            "status":              "mapped",
            "reason":              reason_text,
            "pass":                1,
            "year_values":         year_values,
            "extracted_ref_value": extracted_ref_value,
            "confidence":          None,
            "formula":             None,
            "components":          None,
            "sign_flipped":        True if should_flip_sign else None,
            "sign_mismatch_detected": True if sign_mismatch_detected else None,
            "years_verified":      None,
        })

    return sign_mapped, still_no_match


# ═══════════════════════════════════════════════════════════════════════════════
# Pass 2 — Alias Match (LLM + post-verification)
# ═══════════════════════════════════════════════════════════════════════════════

def _pass2_batch(
    batch: list, rows: list, ref_year: int, target_year: int,
    provider: Any, model: Any, used_labels: set,
) -> list:
    """
    Run Pass 2 (alias-based LLM match) for a batch of unmatched fields.

    The LLM receives each field's label, all known aliases, and all reference-
    year template values.  It must match BOTH by name AND verify the values.

    Post-LLM verification
    ---------------------
    We run _verify_prev_years() ourselves on every accepted match:
    - Normal sign check first.
    - If that fails, inverted sign check (handles sign-convention mismatches
      that the LLM may not detect, e.g. I23 NCI income).
    - If sign_flipped=True, negate target_value before storing.

    Low-confidence LLM results are rejected immediately (forward to Pass 3).
    """
    rows_text   = _rows_to_text(rows, ref_year, target_year)
    fields_json = json.dumps(
        [{
            "field_label":      f["label"],
            "aliases":          _get_aliases(f["label"]),
            "reference_values": f.get("all_ref_values", {str(ref_year): f.get("reference_value")}),
        } for f in batch],
        indent=2,
    )
    prompt = _PASS2_PROMPT.format(
        ref_year=ref_year, target_year=target_year,
        rows_text=rows_text, fields_json=fields_json,
    )
    print(NL + "  [Pass 2] " + str(len(batch)) + " fields → LLM")
    raw        = _llm_call(prompt, provider, model)
    data       = _parse_json(raw)
    llm_res    = {r["field_label"]: r for r in data.get("results", [])}
    row_lookup = _build_row_lookup(rows)
    results    = []

    for field in batch:
        label  = field["label"]
        r      = llm_res.get(label, {})
        status = r.get("status", "no_match")
        year_values         = {}
        extracted_ref_value = None

        if status == "mapped_alias":
            matched    = r.get("matched_label")
            confidence = r.get("confidence", "low")

            # Reject low-confidence alias matches immediately.
            if confidence == "low":
                results.append(_build_empty_result(field, "no_match", "Alias rejected: low confidence.", 2))
                continue

            if matched and matched.lower().strip() in used_labels:
                results.append(_build_empty_result(field, "no_match", "Duplicate: row already used.", 2))
                continue

            if matched:
                matched_row = row_lookup.get(matched.lower().strip(), {})
                if not matched_row:
                    results.append(_build_empty_result(field, "no_match",
                        "Alias rejected: LLM label '" + str(matched) + "' not found in extraction rows.", 2))
                    continue

                # Post-LLM value verification (includes sign-inversion check).
                all_refs = field.get("all_ref_values", {str(ref_year): field.get("reference_value")})
                val_ok, sign_flipped, val_detail = _verify_prev_years(matched_row, all_refs)
                if not val_ok:
                    results.append(_build_empty_result(field, "no_match",
                        "Alias rejected: name matched '" + str(matched) + "' but prev-year values disagree. " + val_detail, 2))
                    continue

                used_labels.add(matched.lower().strip())
                raw_year_values     = _all_year_values(matched_row)
                _, cur_val          = _extract_year_values(matched_row, ref_year, target_year)
                if sign_flipped:
                    year_values         = {yr: -v for yr, v in raw_year_values.items()}
                    extracted_ref_value = year_values.get(str(ref_year))
                    if cur_val is not None:
                        cur_val = -cur_val
                else:
                    year_values         = raw_year_values
                    extracted_ref_value = year_values.get(str(ref_year))
                reason = r.get("reason", "Alias match: '" + str(matched) + "' [" + str(confidence) + "] verified: " + val_detail)
            else:
                cur_val = None
                reason  = r.get("reason", "Alias match returned no label.")
        else:
            matched = None; cur_val = None; reason = "No alias match found."; status = "no_match"
            sign_flipped = False

        results.append({
            **field,
            "matched_label":       matched,
            "target_value":        cur_val,
            "status":              status,
            "reason":              reason,
            "confidence":          r.get("confidence"),
            "pass":                2,
            "year_values":         year_values,
            "extracted_ref_value": extracted_ref_value,
            "formula":             None,
            "components":          None,
            "sign_flipped":        True if sign_flipped else None,
            "years_verified":      None,
        })

    return results


# ═══════════════════════════════════════════════════════════════════════════════
# Pass 3 — Self-Reasoning / Derived Match (LLM + post-verification)
# ═══════════════════════════════════════════════════════════════════════════════

def _pass3_batch(
    batch: list, rows: list, ref_year: int, target_year: int,
    provider: Any, model: Any, used_labels: set,
) -> list:
    """
    Run Pass 3 (self-reasoning LLM match) for a batch of still-unmatched fields.

    Pass 3 receives ALL available years in the extraction rows so the LLM can
    verify its derivation across multiple historical years.  Batch size is kept
    small (BATCH_SIZE_PASS3) because the all-years context makes both input and
    output significantly larger than Pass 1 / Pass 2.

    Post-LLM verification
    ---------------------
    Same _verify_prev_years() check as Pass 2 — normal sign first, inverted if
    needed.  Low-confidence LLM results are rejected.
    """
    rows_text_all = _rows_to_text_all_years(rows)
    fields_json   = json.dumps(
        [{
            "field_label":      f["label"],
            "reference_values": f.get("all_ref_values", {str(ref_year): f.get("reference_value")}),
        } for f in batch],
        indent=2,
    )
    prompt     = _PASS3_PROMPT.format(rows_text_all_years=rows_text_all, fields_json=fields_json)
    print(NL + "  [Pass 3] " + str(len(batch)) + " fields → LLM")
    raw        = _llm_call(prompt, provider, model)
    data       = _parse_json(raw)
    llm_res    = {r["field_label"]: r for r in data.get("results", [])}
    row_lookup = _build_row_lookup(rows)
    results    = []

    for field in batch:
        label  = field["label"]
        r      = llm_res.get(label, {})
        status = r.get("status", "no_match")
        year_values         = {}
        extracted_ref_value = None

        if status == "mapped_derived":
            matched    = r.get("matched_label")
            confidence = r.get("confidence", "low")

            if confidence == "low":
                results.append(_build_empty_result(field, "no_match", "Derived rejected: low confidence.", 3))
                continue

            if matched and matched.lower().strip() in used_labels:
                results.append(_build_empty_result(field, "no_match", "Duplicate: row already used.", 3))
                continue

            if matched:
                matched_row = row_lookup.get(matched.lower().strip(), {})

                if not matched_row:
                    # Attempt multi-row arithmetic: LLM may return an expression
                    # like "Other expense + Other income" as matched_label when
                    # the derived value comes from combining multiple source rows.
                    # Parse it into (label, operator) pairs, look up each row,
                    # and compute the combined value for ref and target years.
                    components_llm = r.get("components", [])
                    expr_label     = matched  # e.g. "Other expense + Other income"

                    # Build list of (row, operator) from components or expression
                    comp_rows = []
                    # Try components list first (more reliable)
                    if components_llm:
                        for i, comp in enumerate(components_llm):
                            comp_row = row_lookup.get(str(comp).lower().strip(), {})
                            if comp_row:
                                op = "+" if i == 0 else "-"  # default; formula overrides
                                comp_rows.append((comp_row, op))
                    # Fall back: parse expression string for +/- operators
                    if not comp_rows and any(op in expr_label for op in ("+", "-")):
                        import re as _re
                        tokens = _re.split(r"(\s*[+\-]\s*)", expr_label)
                        op = "+"
                        for tok in tokens:
                            tok = tok.strip()
                            if tok in ("+", "-"):
                                op = tok
                                continue
                            if not tok:
                                continue
                            cr = row_lookup.get(tok.lower(), {})
                            if cr:
                                comp_rows.append((cr, op))

                    if not comp_rows:
                        results.append(_build_empty_result(field, "no_match",
                            "Derived rejected: LLM label '" + str(matched) + "' not found in extraction rows.", 3))
                        continue

                    # Compute combined year_values by summing/subtracting all component rows
                    combined_years: dict = {}
                    for cr, op in comp_rows:
                        for yr_str, val in _all_year_values(cr).items():
                            prev = combined_years.get(yr_str, 0.0)
                            combined_years[yr_str] = prev + val if op == "+" else prev - val
                        used_labels.add(cr.get("label", "").lower().strip())

                    # Build a synthetic row dict for _verify_prev_years
                    matched_row = dict(combined_years)  # keys are year strings like "2023"
                    # Rename keys to column-like names so _best_col_for_year works
                    matched_row = {yr + "-12-31": val for yr, val in combined_years.items()}

                    year_values         = combined_years
                    extracted_ref_value = combined_years.get(str(ref_year))
                    cur_val             = combined_years.get(str(target_year))

                    # Verify the computed combined value against template ref years
                    all_refs = field.get("all_ref_values", {str(ref_year): field.get("reference_value")})
                    val_ok, sign_flipped, val_detail = _verify_prev_years(matched_row, all_refs)
                    if not val_ok:
                        results.append(_build_empty_result(field, "no_match",
                            "Derived rejected: combined '" + str(matched) + "' prev-year values disagree. " + val_detail, 3))
                        continue
                    if sign_flipped and cur_val is not None:
                        cur_val = -cur_val

                    formula    = r.get("formula", expr_label)
                    reason     = (
                        "Derived (multi-row): formula='" + str(formula) + "' | " +
                        "components=" + str([c.get("label", "") for c, _ in comp_rows]) + " | " +
                        "verified: " + val_detail
                    )
                    results.append({
                        **field,
                        "matched_label":       matched,
                        "target_value":        cur_val,
                        "status":              "mapped_derived",
                        "reason":              reason,
                        "confidence":          r.get("confidence"),
                        "formula":             formula,
                        "components":          [c.get("label", "") for c, _ in comp_rows],
                        "sign_flipped":        True if sign_flipped else r.get("sign_flipped"),
                        "years_verified":      r.get("years_verified"),
                        "pass":                3,
                        "year_values":         year_values,
                        "extracted_ref_value": extracted_ref_value,
                    })
                    continue

                all_refs = field.get("all_ref_values", {str(ref_year): field.get("reference_value")})
                val_ok, sign_flipped, val_detail = _verify_prev_years(matched_row, all_refs)
                if not val_ok:
                    results.append(_build_empty_result(field, "no_match",
                        "Derived rejected: '" + str(matched) + "' prev-year values disagree. " + val_detail, 3))
                    continue

                used_labels.add(matched.lower().strip())
                raw_year_values     = _all_year_values(matched_row)
                _, cur_val          = _extract_year_values(matched_row, ref_year, target_year)
                if sign_flipped:
                    year_values         = {yr: -v for yr, v in raw_year_values.items()}
                    extracted_ref_value = year_values.get(str(ref_year))
                    if cur_val is not None:
                        cur_val = -cur_val
                else:
                    year_values         = raw_year_values
                    extracted_ref_value = year_values.get(str(ref_year))
            else:
                cur_val      = None
                sign_flipped = False

            formula = r.get("formula", "")
            reason  = (
                "Derived: formula='" + str(formula) + "' | " +
                "components=" + str(r.get("components", [])) + " | " +
                "confidence=" + str(confidence)
            )
        else:
            matched      = None
            cur_val      = None
            sign_flipped = False
            reason       = "No derivation found after self-reasoning."
            status       = "no_match"

        results.append({
            **field,
            "matched_label":       matched,
            "target_value":        cur_val,
            "status":              status,
            "reason":              reason,
            "confidence":          r.get("confidence"),
            "formula":             r.get("formula"),
            "components":          r.get("components"),
            "sign_flipped":        True if sign_flipped else r.get("sign_flipped"),
            "years_verified":      r.get("years_verified"),
            "pass":                3,
            "year_values":         year_values,
            "extracted_ref_value": extracted_ref_value,
        })

    return results


# ═══════════════════════════════════════════════════════════════════════════════
# Pass 4 — Calculated Fields (no LLM)
# ═══════════════════════════════════════════════════════════════════════════════

def _pass4_direct_copy(results: list, ref_year: int) -> list:
    """
    Pass 4.0 — Direct copy for calculated fields whose reference-year value
    matches an already-mapped field's extracted reference-year value.

    Why this is needed
    ------------------
    Some calculated fields in the BREF template are identical to a mapped field
    for a given company (e.g. I1 | Revenue = I30 | Sales (turnover) for AES Corp
    because AES has no other revenue streams).  In these cases the formula-based
    Pass 4 would fail because I1's formula might be I30+I31+... and I31 is
    no_match.  But the previous-year values prove they are the same row — we can
    copy the target_value directly with full confidence.

    Algorithm
    ---------
    For each calculated field that has a non-None reference_value (from the BREF
    template), scan all mapped fields.  A mapped field qualifies as a direct-copy
    source if:
      1. Its extracted_ref_value matches the calculated field's reference_value
         within 1% tolerance (proves they come from the same source row).
      2. ALL additional ref years in all_ref_values also match (multi-year check
         for extra confidence — prevents false positives).
      3. It has a non-None target_value to copy.

    If exactly one source qualifies → copy its target_value.
    If multiple qualify and they all agree on target_value → copy from the first.
    If multiple qualify but disagree → skip (ambiguous, let formula handle it).

    Returns updated results list.
    """
    MAPPED_STATUSES = {"mapped", "mapped_alias", "mapped_derived", "calculated_ok"}

    # Build lookup of mapped fields: label → result
    mapped_fields = [r for r in results if r.get("status") in MAPPED_STATUSES
                     and r.get("target_value") is not None
                     and r.get("extracted_ref_value") is not None]

    calc_fields = [r for r in results if r.get("status") == "calculated"
                   and r.get("reference_value") is not None]

    if not calc_fields or not mapped_fields:
        return results

    copied = 0
    for calc in calc_fields:
        tmpl_ref  = calc.get("reference_value")
        all_refs  = calc.get("all_ref_values", {str(ref_year): tmpl_ref})

        candidates = []
        for src in mapped_fields:
            ext_ref = src.get("extracted_ref_value")
            if not _values_match(tmpl_ref, ext_ref):
                continue  # Primary ref year doesn't match — skip.

            # Multi-year check: all available ref years must agree.
            multi_ok = True
            for yr_str, rv in all_refs.items():
                if rv is None or yr_str == str(ref_year):
                    continue
                src_yr_val = src.get("year_values", {}).get(yr_str)
                if src_yr_val is None:
                    continue  # Year not in source — not a failure, just skip.
                if not _values_match(rv, src_yr_val):
                    multi_ok = False
                    break

            if multi_ok:
                candidates.append(src)

        if not candidates:
            continue

        # Check candidates agree on target_value
        tgt_vals = [c.get("target_value") for c in candidates if c.get("target_value") is not None]
        if not tgt_vals:
            continue

        if len(set(round(v, 2) for v in tgt_vals)) > 1:
            # Multiple candidates disagree on target — skip (ambiguous)
            calc["reason"] = (
                "Calculated field — direct copy ambiguous: " +
                str(len(candidates)) + " candidates disagree on target value."
            )
            continue

        # All candidates agree — copy from the first
        source = candidates[0]
        calc["target_value"]        = source["target_value"]
        calc["matched_label"]       = source.get("matched_label", source.get("label", ""))
        calc["extracted_ref_value"] = source.get("extracted_ref_value")
        calc["year_values"]         = source.get("year_values", {})
        calc["status"]              = "calculated_ok"
        calc["reason"]              = (
            "Direct copy (Pass 4.0): ref-year value matches '" +
            str(source["label"]) + "' → target_value=" + str(source["target_value"])
        )
        copied += 1
        print("  [copy] " + calc["label"] + " ← " + source["label"] +
              " (target=" + str(source["target_value"]) + ")")

    print("  Pass 4.0: " + str(copied) + " direct copies")
    return results


def _pass4_calculate(results: list, statement_type: str, region: str, ref_year: int, target_year: int) -> list:
    """
    Pass 4 — evaluate formula-derived BREF fields using the target_values
    produced by Passes 1–3.  No LLM is involved — pure arithmetic.

    Qualification rule
    ------------------
    A calculated field is ONLY evaluated if ALL component fields in its formula:
      1. Were successfully mapped (status in: mapped, mapped_alias, mapped_derived)
      2. Have a verified reference-year value — i.e. extracted_ref_value is not
         None (meaning the mapper found and read the row) and target_value is not
         None.

    If any component fails either check, the field is left with target_value=None
    and a descriptive reason is written so the analyst knows exactly which
    component was missing or unverified.

    Cascading dependencies
    ----------------------
    Some formulas reference other calculated fields (e.g. I24 = I37 + I38, where
    I37 itself is *I37 = I21 - I35 + I36).  We iterate up to 10 times so that
    computed values become available as inputs for downstream formulas.

    Args:
        results:        Merged result list from _finalise().
        statement_type: "income_statement", "balance_sheet", or "cash_flow".
        region:         "US", "APAC", or "EMEA".

    Returns:
        Updated results list with calculated fields populated where possible.
    """
    try:
        from src.mapping.bref_calculated import parse_calculation_formula
        from src.mapping.field_mappings import get_field_mappings
    except Exception as e:
        print("[Pass 4] Could not import calculation modules: " + str(e))
        return results

    # Build a lookup: field_code → result dict  (e.g. "I30" → result_dict)
    # Also covers calculated fields once they get a target_value this pass.
    MAPPED_STATUSES = {"mapped", "mapped_alias", "mapped_derived", "calculated_ok"}

    def _code(label: str) -> str:
        """Extract field code from 'I30 | Sales (turnover)' → 'I30'."""
        return label.split(" |")[0].strip().lstrip("*")

    # Index results by field code for fast lookup
    by_code: dict[str, dict] = {}
    for r in results:
        by_code[_code(r["label"])] = r

    # Get formula definitions from field_mappings
    try:
        stmt_fields = get_field_mappings(region).get(statement_type, {})
    except Exception as e:
        print("[Pass 4] Could not load field mappings: " + str(e))
        return results

    # Collect calculated fields that still need a value
    calc_results = [r for r in results if r.get("status") == "calculated"]

    print(NL + "=" * 60)
    print("PASS 4 — Calculated Fields  (" + str(len(calc_results)) + " fields)")
    print("=" * 60)
    for dbg in calc_results:
        print("  [queued] " + dbg["label"] + " | ref=" + str(dbg.get("reference_value")) +
              " | formula=" + str(dbg.get("formula") or dbg.get("_formula", "?")))

    MAX_ITERATIONS = 10
    for iteration in range(MAX_ITERATIONS):
        computed_this_iteration = 0

        for result in calc_results:
            if result.get("target_value") is not None:
                continue  # Already computed in a previous iteration

            label   = result["label"]
            code    = _code(label)

            # Look up formula from field_mappings
            field_def = None
            for fk, fd in stmt_fields.items():
                if fk == label or _code(fk) == code:
                    field_def = fd
                    break

            if not field_def:
                result["reason"] = "Calculated field — no formula definition found in field_mappings."
                continue

            formula = field_def.get("calculation", "")
            if not formula:
                result["reason"] = "Calculated field — formula is empty."
                continue

            # Parse formula into component field codes
            try:
                operations = parse_calculation_formula(formula)
            except Exception as e:
                result["reason"] = "Calculated field — formula parse error: " + str(e)
                continue

            # Collect component values — treat missing/unverified as 0.
            # We still attempt the calculation and verify against ref years.
            # If the zeroed result matches, it means the missing component is
            # genuinely 0 for this company (e.g. I3-I4 where I4 is blank).
            values    = {}
            zeroed    = []  # components treated as 0 (missing or unverified)

            for comp_code, operator in operations:
                comp_result = by_code.get(comp_code)
                tv          = None

                if comp_result is not None:
                    comp_status = comp_result.get("status", "")
                    if comp_status in MAPPED_STATUSES or comp_status == "calculated_ok":
                        tv = comp_result.get("target_value")

                if tv is None:
                    # Component absent or unresolved — treat as 0
                    values[comp_code] = 0.0
                    zeroed.append(comp_code)
                else:
                    values[comp_code] = float(tv)

            # Evaluate formula
            try:
                calc_value = 0.0
                for comp_code, operator in operations:
                    v = values[comp_code]
                    if operator == "+":
                        calc_value += v
                    elif operator == "-":
                        calc_value -= v
                    elif operator == "*":
                        calc_value *= v
                    elif operator == "/":
                        if v == 0:
                            raise ZeroDivisionError("Division by zero in " + formula)
                        calc_value /= v

                # Verify result against ALL previous-year reference values.
                # This confirms that zeroing missing components is correct.
                all_refs  = result.get("all_ref_values", {str(ref_year): result.get("reference_value")})
                ref_match    = True
                needs_negate = False
                if all_refs:
                    for yr_str, tmpl_val in all_refs.items():
                        if tmpl_val is None:
                            continue

                        # Skip years where NO component has extraction data.
                        # Years like 2020/2021 may be in the template but absent
                        # from extraction — comparing 0 vs template would wrongly fail.
                        any_comp_has_yr = any(
                            by_code.get(comp_code) is not None and
                            by_code[comp_code].get("year_values", {}).get(yr_str) is not None
                            for comp_code, _ in operations
                        )
                        if not any_comp_has_yr:
                            continue  # No extraction data for this year — skip.

                        # Re-compute formula for this ref year using component year values.
                        # Apply sign correction: year_values stores raw extraction values,
                        # but some components may have been sign-corrected (force-positive
                        # or sign-flipped).  Detect via comparing extracted_ref_value to
                        # the component's own reference_value — if signs differ, negate.
                        ref_calc = 0.0
                        for comp_code, op in operations:
                            comp_result = by_code.get(comp_code)
                            comp_yr_val = 0.0
                            if comp_result is not None:
                                raw_yr = comp_result.get("year_values", {}).get(yr_str)
                                if raw_yr is not None:
                                    comp_yr_val = float(raw_yr)
                                    # Detect sign correction: if the component's own
                                    # target_value sign differs from raw year_values sign
                                    # (due to force-positive or sign-flip), negate.
                                    comp_tv  = comp_result.get("target_value")
                                    comp_raw = comp_result.get("year_values", {}).get(
                                        str(target_year), comp_tv
                                    )
                                    if (comp_tv is not None and comp_raw is not None and
                                            comp_tv != 0 and comp_raw != 0 and
                                            (comp_tv > 0) != (comp_raw > 0)):
                                        comp_yr_val = -comp_yr_val
                                else:
                                    # Component has no year_values for this year (e.g.
                                    # calculated_ok fields have no extraction rows).
                                    # Fall back to all_ref_values (template values).
                                    comp_yr_val = float(
                                        comp_result.get("all_ref_values", {}).get(yr_str, 0) or 0
                                    )
                            if op == "+":
                                ref_calc += comp_yr_val
                            elif op == "-":
                                ref_calc -= comp_yr_val
                            elif op == "*":
                                ref_calc *= comp_yr_val
                        if not _values_match(tmpl_val, ref_calc):
                            # Try negated — field may be force-positive (region_adjustments
                            # will flip the sign after Pass 4, e.g. I17 Net total Interest).
                            if _values_match(-tmpl_val, ref_calc) or _values_match(tmpl_val, -ref_calc):
                                needs_negate = True
                            else:
                                ref_match = False
                                break

                if not ref_match:
                    result["reason"] = (
                        "Calculated field — formula result does not match reference year(s): " +
                        formula +
                        (" (zeroed: " + ", ".join(zeroed) + ")" if zeroed else "")
                    )
                    continue

                # If negation was needed to match ref years, negate calc_value.
                # region_adjustments will then flip it back to positive convention.
                if needs_negate:
                    calc_value = -calc_value

                result["target_value"] = calc_value
                result["status"]       = "calculated_ok"
                result["reason"]       = (
                    "Calculated: " + formula +
                    " = " + str(round(calc_value, 2)) +
                    (" [zeroed: " + ", ".join(zeroed) + "]" if zeroed else "") +
                    " | ref-year verified"
                )
                # Make this value available for downstream calculations
                by_code[code] = result
                computed_this_iteration += 1
                print("  [ok] " + label + " = " + str(round(calc_value, 2)) +
                      (" (zeroed: " + ", ".join(zeroed) + ")" if zeroed else ""))

            except Exception as e:
                result["reason"] = "Calculated field — evaluation error: " + str(e)

        if computed_this_iteration == 0:
            break  # No progress — stop iterating

    # Report fields that could not be calculated
    still_missing = [r for r in calc_results if r.get("status") == "calculated"]
    for r in still_missing:
        r["status"] = "calculated_missing"
        if not r.get("reason") or r["reason"] == "Calculated field — formula: .":
            r["reason"] = "Calculated field — dependencies not resolved after " + str(MAX_ITERATIONS) + " iterations."

    ok      = len([r for r in calc_results if r["status"] == "calculated_ok"])
    missing = len([r for r in calc_results if r["status"] == "calculated_missing"])
    print(NL + "  Pass 4: " + str(ok) + " calculated | " + str(missing) + " missing dependencies" + NL)

    return results


# ═══════════════════════════════════════════════════════════════════════════════
# Pipeline orchestrator
# ═══════════════════════════════════════════════════════════════════════════════

def map_fields(
    fields: list,
    extraction_rows: list,
    target_year: int,
    ref_year: int | None = None,
    provider: Any = None,
    model: Any = None,
    statement_type: str = "",
    region: str = "US",
    enable_calculations: bool = True,
) -> list:
    """
    Map a list of BREF fields to extraction rows using the four-stage pipeline.

    Pre-processing
    --------------
    - Calculated fields (is_calculated=True in field_mappings.py) are skipped
      immediately — their values are formula-derived, not extracted.
    - Fields with no reference_value AND no all_ref_values are marked
      no_ref_values and passed through unchanged — there is nothing to compare
      against so mapping would be pure guesswork.

    Args:
        fields:          Output of excel.load_bref_fields().
        extraction_rows: Extraction rows from the annual report parser.
        target_year:     Fiscal year to populate (e.g. 2024).
        provider:        LLM provider key ("maia", "gemma", …).  None = config default.
        model:           Model ID string.  None = config default.

    Returns:
        List of result dicts (one per field, same order as input).
    """
    # Use the ref_year passed in from the pipeline (detected from the template's
    # most recently populated year) rather than blindly assuming target_year - 1.
    # This handles templates where the last populated year is 2022 but target is 2024.
    if ref_year is None:
        ref_year = target_year - 1
    used_labels: set = set()

    # ── Pre-processing ──────────────────────────────────────────────────────
    # All fields — including calculated ones — go through Passes 1-3.
    # Calculated fields are tagged with their formula so Pass 4 can use it
    # as a fallback if the LLM passes could not match them directly.
    # Fields with no reference values at all are skipped (nothing to compare).
    skipped_results = []
    mappable_fields = []
    calc_formulas   = {}  # label → formula string for calculated fields

    for f in fields:
        calc, formula = _is_calculated(f["label"])
        if calc:
            # Calculated fields ALWAYS go directly to Pass 4 — never through LLM.
            # They are handled separately in the Calculations section of the UI.
            f = {**f, "_is_calculated": True, "_formula": formula}
            calc_formulas[f["label"]] = formula
            r = _build_empty_result(f, "calculated",
                "Calculated field — handled by Pass 4 formula evaluation.", 0)
            r["formula"] = formula
            skipped_results.append(r)
        elif f.get("reference_value") is None and not f.get("all_ref_values"):
            skipped_results.append(_build_empty_result(
                f, "no_ref_values", "No reference year values — not mapped.", 0))
        else:
            mappable_fields.append(f)

    print(
        NL + "  Calculated (formula path): " + str(sum(1 for r in skipped_results if r.get("status") == "calculated")) +
        " | No ref values: " + str(sum(1 for r in skipped_results if r.get("status") == "no_ref_values")) +
        " | To map via LLM: " + str(len(mappable_fields)) +
        " (ref_year=" + str(ref_year) + ", target_year=" + str(target_year) + ")"
    )

    if not mappable_fields:
        return _finalise(skipped_results, [], [], [], fields)

    # ── Pass 1 ──────────────────────────────────────────────────────────────
    _section("PASS 1 — Value Match (LLM)", len(mappable_fields))
    pass1_results = []
    for i in range(0, len(mappable_fields), BATCH_SIZE):
        batch = mappable_fields[i: i + BATCH_SIZE]
        _batch_header(i, batch, BATCH_SIZE)
        pass1_results.extend(
            _pass1_batch(batch, extraction_rows, ref_year, target_year, provider, model, used_labels)
        )

    pass1_mapped = [r for r in pass1_results if r["status"] == "mapped"]
    after_pass1  = [r for r in pass1_results if r["status"] in ("no_match", "ambiguous")]
    print(NL + "  Pass 1: " + str(len(pass1_mapped)) + " mapped | " + str(len(after_pass1)) + " → Pass 1.1")

        # ── Pass 1.1 ─────────────────────────────────────────────────────────────
    sign_results, after_sign = _pass1_sign_check(
        after_pass1, extraction_rows, ref_year, target_year, used_labels, region, statement_type
    )
    pass1_results.extend(sign_results)
    print("  Pass 1.1: " + str(len(sign_results)) + " sign-inverted matched | " + str(len(after_sign)) + " → Pass 2")

    if not after_sign:
        return _finalise(skipped_results, pass1_results, [], [], fields)

    # ── Pass 2 ──────────────────────────────────────────────────────────────
    _section("PASS 2 — Alias Match (LLM)", len(after_sign))
    pass2_results = []
    for i in range(0, len(after_sign), BATCH_SIZE):
        batch = after_sign[i: i + BATCH_SIZE]
        _batch_header(i, batch, BATCH_SIZE)
        pass2_results.extend(
            _pass2_batch(batch, extraction_rows, ref_year, target_year, provider, model, used_labels)
        )

    pass2_mapped = [r for r in pass2_results if r["status"] == "mapped_alias"]
    after_pass2  = [r for r in pass2_results if r["status"] == "no_match"]
    print(NL + "  Pass 2: " + str(len(pass2_mapped)) + " alias-matched | " + str(len(after_pass2)) + " → Pass 3")

    if not after_pass2:
        return _finalise(skipped_results, pass1_results, pass2_results, [], fields)

    # ── Pass 3 ──────────────────────────────────────────────────────────────
    _section("PASS 3 — Self-Reasoning (LLM)", len(after_pass2))
    pass3_results = []
    for i in range(0, len(after_pass2), BATCH_SIZE_PASS3):
        batch = after_pass2[i: i + BATCH_SIZE_PASS3]
        _batch_header(i, batch, BATCH_SIZE_PASS3)
        pass3_results.extend(
            _pass3_batch(batch, extraction_rows, ref_year, target_year, provider, model, used_labels)
        )

    pass3_mapped  = [r for r in pass3_results if r["status"] == "mapped_derived"]
    final_nomatch = [r for r in pass3_results if r["status"] == "no_match"]
    print(NL + "  Pass 3: " + str(len(pass3_mapped)) + " derived | " + str(len(final_nomatch)) + " no_match (final)")

    results = _finalise(skipped_results, pass1_results, pass2_results, pass3_results, fields)

    # Re-tag calculated fields that went through LLM passes but were not matched.
    # These still have their formula stored and should be attempted by Pass 4.
    for r in results:
        if r["label"] in calc_formulas and r.get("status") == "no_match":
            r["status"] = "calculated"
            r["formula"] = calc_formulas[r["label"]]
            r["reason"]  = "Calculated field — not matched by LLM, formula fallback: " + (calc_formulas[r["label"]] or "?")

    # ── Pass 4 ──────────────────────────────────────────────────────────────
    calc_fields = [r for r in results if r.get("status") == "calculated"]
    if calc_fields:
        if enable_calculations and statement_type:
            # Pass 4.0 — direct copy (no formula needed)
            _section("PASS 4.0 — Direct Copy for Calculated Fields", len(calc_fields))
            results = _pass4_direct_copy(results, ref_year)

            # Pass 4 — formula evaluation for remaining calculated fields
            still_calc = [r for r in results if r.get("status") == "calculated"]
            if still_calc:
                results = _pass4_calculate(results, statement_type, region, ref_year, target_year)

            _print_summary(results)
        else:
            for r in calc_fields:
                r["status"] = "calculated_missing"
                r["reason"] = "Calculated field — Pass 4 disabled by user."

    return results


# ═══════════════════════════════════════════════════════════════════════════════
# Output helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _finalise(skipped, pass1, pass2, pass3, fields) -> list:
    """
    Merge all result lists, preserve the original field order, and print summary.
    All fields — including calculated ones — now flow through Passes 1-3, so
    there is no separate calculated list.  Fields with no ref values appear with
    status no_ref_values.  Pass 4 runs after this on the merged list.
    """
    by_label: dict = {}
    for r in skipped + pass1 + pass2 + pass3:
        by_label[r["label"]] = r
    ordered = [by_label[f["label"]] for f in fields if f["label"] in by_label]
    _print_summary(ordered)
    return ordered


def _section(title: str, n: int) -> None:
    print(NL + "=" * 60)
    print(title + "  (" + str(n) + " fields, batch=" + str(BATCH_SIZE) + ")")
    print("=" * 60)


def _batch_header(i: int, batch: list, size: int) -> None:
    print(NL + "  Batch " + str(i // size + 1) + ": fields " + str(i + 1) + "–" + str(i + len(batch)))


def _print_summary(results: list) -> None:
    """Print a compact mapping summary table to stdout."""
    counts: dict = {}
    for r in results:
        counts[r["status"]] = counts.get(r["status"], 0) + 1

    icons = {
        "mapped":              "[ok]    ",
        "mapped_alias":        "[alias] ",
        "mapped_derived":      "[deriv] ",
        "calculated_ok":       "[calc]  ",
        "calculated_missing":  "[calc?] ",
        "calculated":          "[calc-] ",
        "no_match":            "[x]     ",
        "no_ref_values":       "[ ]     ",
        "ambiguous":           "[?]     ",
    }
    print(NL + "=" * 60)
    print("MAPPING SUMMARY")
    print("=" * 60)
    for status, n in sorted(counts.items()):
        print("  " + icons.get(status, "-       ") + status.ljust(22) + str(n).rjust(4))
    print("  " + "-" * 32)
    print("  " + "TOTAL".ljust(23) + str(len(results)).rjust(4))
    print("=" * 60 + NL)
