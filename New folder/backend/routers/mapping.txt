from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse, JSONResponse
from database import get_db
from bson import ObjectId
import io

router = APIRouter(prefix="/api/map", tags=["mapping"])


@router.post("/{source_id}")
async def trigger_mapping(source_id: str, force: bool = Query(False)):
    db = get_db()
    source = await db["source"].find_one({"_id": ObjectId(source_id)})
    if not source:
        raise HTTPException(404, "Source not found")
    if not source.get("bref_template"):
        raise HTTPException(400, "No BREF template uploaded")

    extraction = await db["extraction_cache"].find_one({"source_id": source_id})
    if not extraction:
        raise HTTPException(400, "Run extraction first")

    if not force:
        cached = await db["bref_mapping_cache"].find_one({"source_id": source_id})
        if cached:
            cached["id"] = str(cached.pop("_id"))
            return {"status": "cached", "mapping": cached}

    from agents.graph import run_mapping_pipeline

    result = await run_mapping_pipeline(source_id)
    return {"status": "completed", "mapping": result}


@router.get("/{source_id}")
async def get_mapping(source_id: str):
    db = get_db()
    cached = await db["bref_mapping_cache"].find_one({"source_id": source_id})
    if not cached:
        raise HTTPException(404, "No mapping found — trigger mapping first")
    cached["id"] = str(cached.pop("_id"))
    return cached


@router.delete("/{source_id}")
async def clear_mapping(source_id: str):
    db = get_db()
    result = await db["bref_mapping_cache"].delete_one({"source_id": source_id})
    return {"deleted": result.deleted_count > 0}


@router.get("/{source_id}/export")
async def export_mapping(source_id: str):
    db = get_db()
    cached = await db["bref_mapping_cache"].find_one({"source_id": source_id})
    if not cached:
        raise HTTPException(404, "No mapping found")

    from services.excel_service import export_bref_to_excel

    buffer = await export_bref_to_excel(cached)
    return StreamingResponse(
        io.BytesIO(buffer),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename=BREF_{source_id}.xlsx"},
    )


@router.get("/{source_id}/summary")
async def get_summary(source_id: str):
    db = get_db()
    cached = await db["bref_mapping_cache"].find_one({"source_id": source_id})
    if not cached:
        raise HTTPException(404, "No mapping found — trigger mapping first")

    from services.summary_service import generate_summary_from_mapping

    region = cached.get("region_code", "APAC")
    summaries = generate_summary_from_mapping(cached, region)
    return JSONResponse(content=summaries)


@router.get("/{source_id}/summary/export")
async def export_summary(source_id: str):
    db = get_db()
    cached = await db["bref_mapping_cache"].find_one({"source_id": source_id})
    if not cached:
        raise HTTPException(404, "No mapping found")

    from services.summary_service import generate_summary_excel

    region = cached.get("region_code", "APAC")
    buffer = generate_summary_excel(cached, region)
    company = cached.get("company_id", "company")
    year = cached.get("report_year", "")
    return StreamingResponse(
        io.BytesIO(buffer),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename=Summary_{company}_{year}.xlsx"},
    )
