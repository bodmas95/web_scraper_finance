from fastapi import APIRouter, HTTPException, Query
from database import get_db
from bson import ObjectId
from services.gridfs_service import download_file
from services.pdf_service import render_pdf_page

router = APIRouter(prefix="/api/extract", tags=["extraction"])


@router.get("/page-image/{source_id}")
async def get_page_image(source_id: str, page_num: int = Query(...)):
    db = get_db()
    source = await db["source"].find_one({"_id": ObjectId(source_id)})
    if not source or not source.get("annual_report"):
        raise HTTPException(404, "Source or annual report not found")

    pdf_bytes = await download_file(source["annual_report"]["gridfs_id"])
    try:
        result = render_pdf_page(pdf_bytes, page_num)
        return result
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        raise HTTPException(500, f"Failed to render page: {e}")


@router.post("/{source_id}")
async def trigger_extraction(source_id: str):
    db = get_db()
    source = await db["source"].find_one({"_id": ObjectId(source_id)})
    if not source:
        raise HTTPException(404, "Source not found")
    if not source.get("annual_report"):
        raise HTTPException(400, "No annual report uploaded")

    cached = await db["extraction_cache"].find_one({"source_id": source_id})
    if cached:
        cached["id"] = str(cached.pop("_id"))
        return {"status": "cached", "extraction": cached}

    from agents.graph import run_extraction_pipeline

    result = await run_extraction_pipeline(source_id)
    return {"status": "completed", "extraction": result}


@router.get("/{source_id}")
async def get_extraction(source_id: str):
    db = get_db()
    cached = await db["extraction_cache"].find_one({"source_id": source_id})
    if not cached:
        raise HTTPException(404, "No extraction found — trigger extraction first")
    cached["id"] = str(cached.pop("_id"))
    return cached


@router.delete("/{source_id}")
async def clear_extraction(source_id: str):
    db = get_db()
    result = await db["extraction_cache"].delete_one({"source_id": source_id})
    await db["bref_mapping_cache"].delete_one({"source_id": source_id})
    return {"deleted": result.deleted_count > 0}
