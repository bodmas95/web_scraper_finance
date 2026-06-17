"""
Parallel statement extraction.

Runs find_correct_page + extract_table for multiple statements concurrently
using the global thread pool. Each statement's LLM calls execute in parallel
instead of waiting for each other sequentially.
"""

import io
import contextlib
import traceback
import logging
from concurrent.futures import as_completed

from src.worker_pool import get_thread_pool

_log = logging.getLogger(__name__)


def _extract_one_statement(args):
    """
    Extract a single financial statement from a PDF.
    Uses Gemma by default, falls back to GPT-4o on failure.

    Args:
        args: tuple of (pdf_path, statement_type, stitch_fn, provider, model_id,
                         company_name, target_year)

    Returns:
        dict with keys: stype, result, logs, error, manual_needed
    """
    pdf_path, stype, stitch_fn, provider, model_id, company_name, target_year = args

    log_buf = io.StringIO()
    try:
        from src.extraction.page_validator import find_correct_page
        from src.extraction.extractor import extract_table_with_vision_fallback
        from src.extraction.model_config import get_fallback_model

        with contextlib.redirect_stdout(log_buf):
            page = find_correct_page(pdf_path, stype)

        if not page:
            return {
                "stype": stype,
                "result": None,
                "logs": log_buf.getvalue(),
                "error": None,
                "manual_needed": True,
            }

        # Try with primary model (Gemma)
        try:
            with contextlib.redirect_stdout(log_buf):
                table = extract_table_with_vision_fallback(
                    page, pdf_path, stitch_fn,
                    provider=provider, model=model_id,
                )
        except Exception as e:
            # Fallback to GPT-4o
            log_buf.write(f"\n⚠️ Primary model ({provider}/{model_id}) failed: {str(e)}\n")
            log_buf.write("🔄 Falling back to GPT-4o...\n")
            
            fallback_provider, fallback_model = get_fallback_model()
            with contextlib.redirect_stdout(log_buf):
                table = extract_table_with_vision_fallback(
                    page, pdf_path, stitch_fn,
                    provider=fallback_provider, model=fallback_model,
                )

        if not table["rows"]:
            return {
                "stype": stype,
                "result": None,
                "logs": log_buf.getvalue(),
                "error": None,
                "manual_needed": True,
            }

        result = {
            "page": page["page_display"],
            "page_num": page["page_num"],
            "all_page_nums": page.get("all_page_nums", [page["page_num"]]),
            "landscape_crop_bbox": page.get("landscape_crop_bbox"),
            "rows": table["rows"],
            "year_headers": table.get("year_headers", []),
            "year_currencies": table.get("year_currencies", {}),
            "unit_scale": table.get("unit_scale"),
            "year_end_date": table.get("year_end_date"),
            "total_rows": table["total_rows"],
            "extraction_method": "vision" if page.get("text_garbled") else "text",
            "company": company_name,
            "target_year": target_year,
            "statement": stype,
        }

        return {
            "stype": stype,
            "result": result,
            "logs": log_buf.getvalue(),
            "error": None,
            "manual_needed": False,
        }

    except Exception:
        return {
            "stype": stype,
            "result": None,
            "logs": log_buf.getvalue(),
            "error": traceback.format_exc(),
            "manual_needed": False,
        }


def extract_statements_parallel(
    pdf_path: str,
    statement_types: list[str],
    stitch_fn,
    provider: str = None,
    model_id: str = None,
    company_name: str = "",
    target_year: int = 0,
) -> dict:
    """
    Extract multiple financial statements from a PDF in parallel.

    Submits all statement types to the thread pool simultaneously so
    LLM API calls overlap instead of running sequentially.

    Args:
        pdf_path: Path to the PDF file
        statement_types: List of statement types to extract
                        (e.g. ['income_statement', 'balance_sheet', 'cash_flow'])
        stitch_fn: Function to stitch page images vertically
        provider: LLM provider override
        model_id: LLM model override
        company_name: Company name for result metadata
        target_year: Target year for result metadata

    Returns:
        dict with keys:
            results: {stype: result_dict} for successful extractions
            manual_needed: [stype, ...] for statements needing manual page input
            logs: {stype: log_text} for extraction logs
            errors: {stype: error_text} for extraction errors
    """
    if not statement_types:
        return {"results": {}, "manual_needed": [], "logs": {}, "errors": {}}

    pool = get_thread_pool()
    args_list = [
        (pdf_path, stype, stitch_fn, provider, model_id, company_name, target_year)
        for stype in statement_types
    ]

    _log.info(
        "Submitting %d statement extractions in parallel (types: %s)",
        len(statement_types), statement_types,
    )

    futures = {
        pool.submit(_extract_one_statement, args): args[1]
        for args in args_list
    }

    results = {}
    manual_needed = []
    logs = {}
    errors = {}

    for future in as_completed(futures):
        stype = futures[future]
        try:
            out = future.result()
            logs[out["stype"]] = out["logs"]

            if out["error"]:
                errors[out["stype"]] = out["error"]
            elif out["manual_needed"]:
                manual_needed.append(out["stype"])
            elif out["result"]:
                results[out["stype"]] = out["result"]
        except Exception:
            errors[stype] = traceback.format_exc()

    _log.info(
        "Parallel extraction done: %d success, %d manual, %d errors",
        len(results), len(manual_needed), len(errors),
    )

    return {
        "results": results,
        "manual_needed": manual_needed,
        "logs": logs,
        "errors": errors,
    }
