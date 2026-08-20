from fastapi import APIRouter, HTTPException
from database import get_db
from models.region import CompanyCreate

router = APIRouter(prefix="/api/regions", tags=["regions"])


@router.get("")
async def list_regions():
    db = get_db()
    regions = await db["region"].find({}, {"_id": 0}).to_list(None)
    return regions


@router.get("/{region_code}/countries")
async def list_countries(region_code: str):
    db = get_db()
    region = await db["region"].find_one(
        {"region_code": region_code}, {"_id": 0, "countries": 1}
    )
    if not region:
        raise HTTPException(404, "Region not found")
    return region["countries"]


@router.get("/{region_code}/countries/{country_code}/companies")
async def list_companies(region_code: str, country_code: str):
    db = get_db()
    region = await db["region"].find_one({"region_code": region_code})
    if not region:
        raise HTTPException(404, "Region not found")
    for country in region.get("countries", []):
        if country["country_code"] == country_code:
            return country.get("companies", [])
    raise HTTPException(404, "Country not found")


@router.post("/{region_code}/countries/{country_code}/companies")
async def add_company(region_code: str, country_code: str, body: CompanyCreate):
    db = get_db()
    company_id = body.company_name.lower().replace(" ", "_").replace(".", "")
    company = {
        "company_id": company_id,
        "company_name": body.company_name,
        "currency": body.currency,
        "unit": body.unit,
    }
    result = await db["region"].update_one(
        {"region_code": region_code, "countries.country_code": country_code},
        {"$push": {"countries.$.companies": company}},
    )
    if result.modified_count == 0:
        raise HTTPException(400, "Could not add company — check region/country codes")
    return company
