from pydantic import BaseModel, Field
from typing import Optional, Any
from datetime import datetime


class ExtractionCache(BaseModel):
    source_id: str
    company_id: str
    report_year: int
    income_statement: Optional[dict[str, Any]] = None
    balance_sheet: Optional[dict[str, Any]] = None
    cash_flow: Optional[dict[str, Any]] = None
    notes: Optional[dict[str, Any]] = None
    extracted_at: datetime = Field(default_factory=datetime.utcnow)


class BREFMappingCache(BaseModel):
    source_id: str
    company_id: str
    region_code: str
    report_year: int
    income_statement: Optional[dict[str, Any]] = None
    balance_sheet: Optional[dict[str, Any]] = None
    cash_flow: Optional[dict[str, Any]] = None
    mapped_at: datetime = Field(default_factory=datetime.utcnow)
