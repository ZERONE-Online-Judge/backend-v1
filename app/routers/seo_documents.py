from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from urllib.parse import urlsplit

from app.services.seo import build_page_metadata
from app.services.seo_document import frontend_template, render_document


router = APIRouter(tags=["public"])


@router.api_route("/public/seo/document", methods=["GET", "HEAD"])
def public_document(request: Request):
    # Nginx overwrites this header. Metadata never reads authentication or
    # participant resources, so every visitor receives the same public HTML.
    path = request.headers.get("x-zoj-page-path", "/")
    try:
        metadata = build_page_metadata(path)
    except ValueError:
        metadata = build_page_metadata("/not-found")
        metadata["status_code"] = 400
    if metadata["status_code"] == 200 and "noindex" not in metadata["robots"]:
        if urlsplit(path).path != metadata["path"]:
            return RedirectResponse(metadata["canonical"], status_code=308)
    try:
        document = render_document(frontend_template(), metadata)
    except (OSError, ValueError):
        return HTMLResponse(
            '<!doctype html><html lang="ko"><head><meta name="robots" content="noindex">'
            '<title>ZOJ · 잠시 후 다시 접속해 주세요</title></head><body>'
            '<h1>화면을 준비하고 있습니다.</h1><p>잠시 후 다시 접속해 주세요.</p></body></html>',
            status_code=503,
            headers={"Retry-After": "60", "Cache-Control": "no-store", "X-Robots-Tag": "noindex"},
        )
    return HTMLResponse(
        document,
        status_code=metadata["status_code"],
        headers={"Cache-Control": "no-store", "X-Robots-Tag": metadata["robots"]},
    )
