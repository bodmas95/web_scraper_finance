from pymongo import MongoClient
from config.config import get_mongo_uri


class MongoDBClient:
    """Thin wrapper around MongoClient. Supports use as a context manager."""

    def __init__(self):
        self._client = None
        self._db     = None

    def connect(self) -> "MongoDBClient":
        uri, db_name  = get_mongo_uri()
        self._client  = MongoClient(uri)
        self._db      = self._client[db_name]
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

    def __enter__(self) -> "MongoDBClient":
        return self.connect()

    def __exit__(self, *_):
        self.close()
