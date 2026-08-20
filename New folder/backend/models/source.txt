from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class FileRef(BaseModel):
    filename: str
    gridfs_id: str


class Source(BaseModel):
    company_id: str
    region_code: str
    country_code: str
    report_year: int
    annual_report: Optional[FileRef] = None
    bref_template: Optional[FileRef] = None
    uploaded_at: datetime = Field(default_factory=datetime.utcnow)


class SourceResponse(BaseModel):
    id: str
    company_id: str
    region_code: str
    country_code: str
    report_year: int
    annual_report: Optional[FileRef] = None
    bref_template: Optional[FileRef] = None
    uploaded_at: datetime
