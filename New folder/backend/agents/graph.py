from langgraph.graph import StateGraph, END
from agents.state import BREFState
from agents.finance_agent import finance_agent_node
from agents.notes_agent import notes_agent_node
from agents.mapper_agent import mapper_agent_node
from services.pdf_service import extract_pdf_text, extract_pdf_tables
from services.excel_service import parse_bref_template
from services.gridfs_service import download_file
from database import get_db
from bson import ObjectId
from field_mappings import get_field_mappings
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


def build_extraction_graph():
    graph = StateGraph(BREFState)
    graph.add_node("finance_agent", finance_agent_node)
    graph.add_node("notes_agent", notes_agent_node)
    graph.set_entry_point("finance_agent")
    graph.add_edge("finance_agent", "notes_agent")
    graph.add_edge("notes_agent", END)
    return graph.compile()


def build_mapping_graph():
    graph = StateGraph(BREFState)
    graph.add_node("mapper_agent", mapper_agent_node)
    graph.set_entry_point("mapper_agent")
    graph.add_edge("mapper_agent", END)
    return graph.compile()


def _rows_to_fields(rows: list) -> dict:
    """Convert row-based data to field dict for backwards compatibility."""
    fields = {}
    for row in rows:
        if isinstance(row, dict):
            label = row.get("label", "")
            if label:
                fields[label] = {k: v for k, v in row.items() if k not in ("label", "parent")}
    return fields


async def run_extraction_pipeline(source_id: str) -> dict:
    db = get_db()
    source = await db["source"].find_one({"_id": ObjectId(source_id)})

    pdf_bytes = await download_file(source["annual_report"]["gridfs_id"])
    pdf_text = await extract_pdf_text(pdf_bytes)
    pdf_tables = await extract_pdf_tables(pdf_bytes)

    initial_state: BREFState = {
        "source_id": source_id,
        "pdf_text": pdf_text,
        "pdf_tables": pdf_tables,
        "region": source["region_code"],
        "report_year": source["report_year"],
        "field_mappings": {},
        "financial_statements": {},
        "notes": {},
        "bref_previous": {},
        "bref_mappings": {},
        "status": "started",
        "messages": [],
    }

    extraction_graph = build_extraction_graph()
    result = await extraction_graph.ainvoke(initial_state)

    fs = result.get("financial_statements", {})

    cache_doc = {
        "source_id": source_id,
        "company_id": source["company_id"],
        "report_year": source["report_year"],
        "income_statement": fs.get("income_statement"),
        "balance_sheet": fs.get("balance_sheet"),
        "cash_flow": fs.get("cash_flow"),
        "notes": result.get("notes"),
        "extracted_at": datetime.utcnow(),
    }
    await db["extraction_cache"].replace_one(
        {"source_id": source_id}, cache_doc, upsert=True
    )

    return cache_doc


async def run_mapping_pipeline(source_id: str) -> dict:
    db = get_db()
    source = await db["source"].find_one({"_id": ObjectId(source_id)})

    extraction = await db["extraction_cache"].find_one({"source_id": source_id})
    if not extraction:
        raise ValueError("Run extraction first")

    bref_bytes = await download_file(source["bref_template"]["gridfs_id"])
    bref_data = await parse_bref_template(bref_bytes)

    region = source["region_code"]
    field_maps = get_field_mappings(region)

    financial_statements = {}
    for stmt_type in ["income_statement", "balance_sheet", "cash_flow"]:
        stmt = extraction.get(stmt_type) or {}
        if "rows" in stmt:
            financial_statements[stmt_type] = stmt
        elif "fields" in stmt:
            financial_statements[stmt_type] = {"fields": stmt.get("fields", {})}
        else:
            financial_statements[stmt_type] = stmt

    initial_state: BREFState = {
        "source_id": source_id,
        "pdf_text": {},
        "pdf_tables": {},
        "region": region,
        "report_year": source["report_year"],
        "field_mappings": field_maps,
        "financial_statements": financial_statements,
        "notes": extraction.get("notes", {}),
        "bref_previous": bref_data,
        "bref_mappings": {},
        "status": "mapping_started",
        "messages": [],
    }

    mapping_graph = build_mapping_graph()
    result = await mapping_graph.ainvoke(initial_state)

    cache_doc = {
        "source_id": source_id,
        "company_id": source["company_id"],
        "region_code": region,
        "report_year": source["report_year"],
        "income_statement": result.get("bref_mappings", {}).get("income_statement"),
        "balance_sheet": result.get("bref_mappings", {}).get("balance_sheet"),
        "cash_flow": result.get("bref_mappings", {}).get("cash_flow"),
        "mapped_at": datetime.utcnow(),
    }
    await db["bref_mapping_cache"].replace_one(
        {"source_id": source_id}, cache_doc, upsert=True
    )

    return cache_doc
