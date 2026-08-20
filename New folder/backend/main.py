from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from database import connect_db, close_db
from routers import regions, documents, extraction, mapping
import logging
from config import settings

logging.basicConfig(level=getattr(logging, settings.LOG_LEVEL, logging.INFO))


@asynccontextmanager
async def lifespan(app: FastAPI):
    await connect_db()
    logging.info("MongoDB connected — database: %s", settings.MONGO_DATABASE)
    yield
    await close_db()
    logging.info("MongoDB disconnected")


app = FastAPI(
    title="Nx Intelligence",
    description="BREF Financial Automation Platform",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(regions.router)
app.include_router(documents.router)
app.include_router(extraction.router)
app.include_router(mapping.router)


@app.get("/api/health")
async def health():
    return {"status": "ok", "database": settings.MONGO_DATABASE}
