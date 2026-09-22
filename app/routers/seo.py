from fastapi import APIRouter, HTTPException, Query, Request, Response

from app.services.responses import ok
from app.services.seo import build_page_metadata, build_robots_txt, build_sitemap_xml


router = APIRouter(tags=["public"])


@router.get("/public/seo")
async def page_metadata(request: Request, response: Response, path: str = Query(default="/", max_length=8192)):
    try:
        metadata = build_page_metadata(path)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid page path.") from exc
    response.headers["Cache-Control"] = "no-store"
    response.headers["X-Robots-Tag"] = "noindex"
    return ok(request, metadata)


@router.api_route("/sitemap.xml", methods=["GET", "HEAD"], include_in_schema=False)
async def sitemap():
    return Response(build_sitemap_xml(), media_type="application/xml", headers={"Cache-Control": "no-cache"})


@router.api_route("/robots.txt", methods=["GET", "HEAD"], include_in_schema=False)
async def robots():
    return Response(build_robots_txt(), media_type="text/plain", headers={"Cache-Control": "no-cache"})
