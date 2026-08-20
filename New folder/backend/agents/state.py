from typing import TypedDict, Any, Optional


class BREFState(TypedDict, total=False):
    source_id: str
    pdf_text: dict[int, str]
    pdf_tables: dict[int, list]
    bref_previous: dict[str, Any]
    region: str
    report_year: int
    field_mappings: dict[str, Any]
    financial_statements: dict[str, Any]
    notes: dict[str, Any]
    bref_mappings: dict[str, Any]
    status: str
    messages: list[str]
