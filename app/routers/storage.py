from io import BytesIO
import mimetypes

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from app.services.responses import ok
from app.services.storage import object_storage

router = APIRouter(tags=["storage"])


@router.get("/storage/objects/{storage_key:path}")
async def get_storage_object(storage_key: str):
    media_type = mimetypes.guess_type(storage_key)[0] or "application/octet-stream"
    return StreamingResponse(BytesIO(object_storage.read_bytes(storage_key)), media_type=media_type)


@router.put("/storage/objects/{storage_key:path}")
async def put_storage_object(storage_key: str, request: Request):
    content_type = request.headers.get("content-type", "application/octet-stream")
    object_storage.write_bytes(storage_key, await request.body(), content_type)
    return ok(request, {"storage_key": storage_key})
