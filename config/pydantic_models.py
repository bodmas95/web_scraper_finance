"""
============================================================
  PYDANTIC MODELS FOR STRUCTURED OUTPUT
============================================================
"""

from pydantic import BaseModel
from typing import List

class SectionItem(BaseModel):
    analysis: str
    citation: List[str]
    source_table: str

class Report(BaseModel):
    executive_summary: str
    revenue_analysis: SectionItem
    gross_profit_margin_analysis: SectionItem
    ebitda_and_ebitda_margin_analysis: SectionItem
    other_income_analysis: SectionItem
    interest_expenses_analysis: SectionItem
    net_income_and_earnings_quality_analysis: SectionItem
    key_credit_metrics_from_pnL: SectionItem
