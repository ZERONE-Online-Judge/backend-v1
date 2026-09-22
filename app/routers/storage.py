from io import BytesIO
import mimetypes

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from app.services.authz import require_contest_staff
from app.services.errors import AppError, not_found, permission_denied
from app.services.responses import ok
from app.services.storage import object_storage

router = APIRouter(tags=["storage"])


def _authorize_object(request: Request, storage_key: str, expires: int, signature: str) -> None:
    try:
        object_storage.validate_key(storage_key)
    except ValueError:
        raise AppError(422, "invalid_storage_key", "Invalid storage key.")
    if signature and object_storage.valid_signature(request.method, storage_key, expires, signature):
        return
    # Editors can inspect testcase/source text with their current token. Images use
    # short-lived signed URLs because browser image requests carry no bearer token.
    parts = storage_key.split("/")
    if request.method == "GET" and len(parts) >= 3 and parts[0] == "contests":
        require_contest_staff(request, parts[1], "contest.problem.resource.view")
        return
    raise permission_denied("A valid storage URL is required.")


@router.get("/storage/objects/{storage_key:path}")
def get_storage_object(storage_key: str, request: Request, expires: int = 0, signature: str = ""):
    _authorize_object(request, storage_key, expires, signature)
    media_type = mimetypes.guess_type(storage_key)[0] or "application/octet-stream"
    try:
        content = object_storage.read_bytes(storage_key)
    except FileNotFoundError:
        raise not_found()
    return StreamingResponse(BytesIO(content), media_type=media_type)


@router.put("/storage/objects/{storage_key:path}")
async def put_storage_object(storage_key: str, request: Request, expires: int = 0, signature: str = ""):
    _authorize_object(request, storage_key, expires, signature)
    content_type = request.headers.get("content-type", "application/octet-stream")
    object_storage.write_bytes(storage_key, await request.body(), content_type)
    return ok(request, {"storage_key": storage_key})
