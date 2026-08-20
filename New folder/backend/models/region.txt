from pydantic import BaseModel, Field
from typing import Optional


class Company(BaseModel):
    company_id: str
    company_name: str
    currency: str = "USD"
    unit: int = 1000


class Country(BaseModel):
    country_code: str
    country_name: str
    companies: list[Company] = []


class Region(BaseModel):
    region_code: str
    region_name: str
    countries: list[Country] = []


class CompanyCreate(BaseModel):
    company_name: str
    currency: str = "USD"
    unit: int = 1000
