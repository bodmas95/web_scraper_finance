from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorGridFSBucket
from config import settings

client: AsyncIOMotorClient = None
db = None
gridfs: AsyncIOMotorGridFSBucket = None


async def connect_db():
    global client, db, gridfs
    client = AsyncIOMotorClient(settings.mongo_uri)
    db = client[settings.MONGO_DATABASE]
    gridfs = AsyncIOMotorGridFSBucket(db)
    await _seed_regions()


async def close_db():
    global client
    if client:
        client.close()


def get_db():
    return db


def get_gridfs():
    return gridfs


SEED_REGIONS = [
    {
        "region_code": "AMER",
        "region_name": "Americas",
        "countries": [
            {
                "country_code": "US",
                "country_name": "United States",
                "companies": [],
            }
        ],
    },
    {
        "region_code": "APAC",
        "region_name": "Asia Pacific",
        "countries": [
            {
                "country_code": "HK",
                "country_name": "Hong Kong",
                "companies": [],
            }
        ],
    },
    {
        "region_code": "EMEA",
        "region_name": "Europe, Middle East & Africa",
        "countries": [
            {
                "country_code": "FR",
                "country_name": "France",
                "companies": [],
            },
            {
                "country_code": "IT",
                "country_name": "Italy",
                "companies": [],
            },
        ],
    },
]


async def _seed_regions():
    collection = db["region"]
    existing = await collection.count_documents({})
    if existing == 0:
        await collection.insert_many(SEED_REGIONS)
