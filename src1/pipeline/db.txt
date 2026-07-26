import gridfs
from datetime import datetime
from bson import ObjectId
from pymongo import MongoClient
from config.config import get_mongo_uri


class MongoDBClient:
    """
    Unified MongoDB client.

    OVH pipeline  — use as a context manager:
        with MongoDBClient() as mongo:
            mongo.db.collection.find(...)

    HKEX pipeline — call connect() / close() directly:
        client = MongoDBClient()
        client.connect()
        client.get_company_by_symbol("01929")
        client.close()
    """

    def __init__(self):
        self._client = None
        self._db     = None
        self.fs      = None

    def connect(self) -> "MongoDBClient":
        uri, db_name   = get_mongo_uri()
        self._client   = MongoClient(uri)
        self._client.admin.command("ismaster")   # verify connection
        self._db       = self._client[db_name]
        self.fs        = gridfs.GridFS(self._db)
        return self

    @property
    def db(self):
        if self._db is None:
            raise RuntimeError("Not connected. Call connect() first.")
        return self._db

    def close(self):
        if self._client:
            self._client.close()
            self._client = None
            self._db     = None
            self.fs      = None

    def __enter__(self) -> "MongoDBClient":
        return self.connect()

    def __exit__(self, *_):
        self.close()

    # -------------------------------------------------------------------------
    # HKEX-specific methods
    # -------------------------------------------------------------------------

    def get_company_by_symbol(self, symbol: str, exchange: str = "HKEX") -> dict | None:
        """Fetch company document by ticker symbol and exchange."""
        return self.db.companies.find_one({
            "tickers.symbol":   symbol,
            "tickers.exchange": exchange,
        })

    def get_source_for_company(self, company_id, source_name: str = "HKEX_NEWS") -> dict | None:
        """Fetch source configuration for a company."""
        return self.db.sources.find_one({
            "companyId": str(company_id),
            "source":    source_name,
        })

    def get_hkex_ticker(self, company_doc: dict) -> dict:
        """Extract HKEX ticker object from a company document."""
        for ticker in company_doc.get("tickers", []):
            if ticker.get("exchange") == "HKEX":
                return ticker
        raise ValueError("HKEX ticker not found in company document")

    def file_exists_in_gridfs(self, filename: str) -> dict | None:
        """Return the GridFS file document if it already exists, else None."""
        return self.db.fs.files.find_one({"filename": filename})

    def save_file_to_gridfs(self, file_bytes: bytes, filename: str, metadata: dict) -> str:
        """Save file to GridFS; return existing id if already stored."""
        existing = self.file_exists_in_gridfs(filename)
        if existing:
            return str(existing["_id"])
        file_id = self.fs.put(
            file_bytes,
            filename=filename,
            content_type="application/pdf",
            metadata=metadata,
        )
        return str(file_id)

    def upsert_report(
        self,
        company_id,
        source_id,
        exchange: str,
        source_name: str,
        source_file_id: str,
        reporting_id: str,
        report_type: str,
        fiscal_year: int | None,
        file_entry: dict,
    ) -> None:
        """Insert or update a report document."""
        query  = {
            "companyId":   company_id,
            "sourceId":    source_id,
            "reportingId": reporting_id,
        }
        update = {
            "$set": {
                "exchange":     exchange,
                "source":       source_name,
                "sourceFileId": source_file_id,
                "reportType":   report_type,
                "fiscalyear":   fiscal_year,
                "status":       "active",
                "updatedAt":    datetime.utcnow(),
            },
            "$setOnInsert": {
                "companyId": company_id,
                "sourceId":  source_id,
                "createdAt": datetime.utcnow(),
            },
            "$addToSet": {"files": file_entry},
        }
        self.db.reports.update_one(query, update, upsert=True)

    def insert_ingestion_log(
        self,
        company_id,
        source_id,
        source_name: str,
        exchange: str,
        report_type: str,
        fiscal_year: int | None,
        urls: list[str],
        files: list[dict],
        result: dict,
        parser_status: str,
        parser_error_message: str | None = None,
    ) -> None:
        """Insert an ingestion log entry."""
        self.db.ingestionLogs.insert_one({
            "companyId":  company_id,
            "sourceId":   source_id,
            "source":     source_id,
            "exchange":   exchange,
            "reportType": report_type,
            "fiscalyear": fiscal_year,
            "urls":       urls,
            "files":      files,
            "result":     result,
            "parser": {
                "status":       parser_status,
                "errormessage": parser_error_message,
            },
            "createdAt": datetime.utcnow(),
            "updatedAt": datetime.utcnow(),
        })

    # -------------------------------------------------------------------------
    # OVH-specific methods
    # -------------------------------------------------------------------------

    def get_ovh_lei(self, company_doc: dict) -> str | None:
        """Extract the LEI from the company document's tickers array."""
        for ticker in company_doc.get("tickers", []):
            if ticker.get("lei"):
                return ticker["lei"]
        return None

    def insert_report(self, doc: dict) -> str:
        """Insert a report document. Returns the inserted _id as a string."""
        now = datetime.utcnow()
        doc.setdefault("createdAt", now)
        doc.setdefault("updatedAt", now)
        result = self.db.reports.insert_one(doc)
        return str(result.inserted_id)

    def update_report(self, report_id: str, fields: dict) -> None:
        """Update specific fields of a report document by _id. Always bumps updatedAt."""
        fields["updatedAt"] = datetime.utcnow()
        self.db.reports.update_one(
            {"_id": ObjectId(report_id)},
            {"$set": fields},
        )

    def insert_log(self, doc: dict) -> str:
        """Insert a pre-built ingestion log document. Returns the inserted _id as a string."""
        now = datetime.utcnow()
        doc.setdefault("createdAt", now)
        doc.setdefault("updatedAt", now)
        result = self.db.ingestionLogs.insert_one(doc)
        return str(result.inserted_id)

    def update_log(self, log_id: str, fields: dict) -> None:
        """Update specific fields of an ingestion log by _id. Always bumps updatedAt."""
        fields["updatedAt"] = datetime.utcnow()
        self.db.ingestionLogs.update_one(
            {"_id": ObjectId(log_id)},
            {"$set": fields},
        )

    def save_bytes_to_gridfs(self, data: bytes, filename: str, metadata: dict = None) -> str:
        """Store binary content (PDF, Excel, etc.) in GridFS. Returns file_id as string."""
        file_id = self.fs.put(data, filename=filename, **(metadata or {}))
        return str(file_id)

    def save_text_to_gridfs(self, text: str, filename: str, metadata: dict = None) -> str:
        """Store plain-text content in GridFS (UTF-8 encoded). Returns file_id as string."""
        file_id = self.fs.put(
            text.encode("utf-8"),
            filename=filename,
            content_type="text/plain",
            **(metadata or {}),
        )
        return str(file_id)
