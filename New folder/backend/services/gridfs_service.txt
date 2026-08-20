from database import get_gridfs
from bson import ObjectId
import io


async def download_file(gridfs_id: str) -> bytes:
    gridfs = get_gridfs()
    stream = await gridfs.open_download_stream(ObjectId(gridfs_id))
    content = await stream.read()
    return content


async def delete_file(gridfs_id: str):
    gridfs = get_gridfs()
    await gridfs.delete(ObjectId(gridfs_id))
