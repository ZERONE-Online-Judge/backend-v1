"""Reuse serialized public scoreboards after current request authorization."""
import hashlib
import base64
import gzip

from fastapi import Response


def scoreboard_response(request, board):
    content = base64.b64decode(board.removeprefix("gzip:"))
    etag = '"' + hashlib.sha256(content).hexdigest() + '"'
    headers = {"ETag": "W/"+etag, "Cache-Control": "private, no-cache", "Vary": "Authorization, Cookie, Accept-Encoding"}
    matches = [value.strip().removeprefix("W/").strip()
        for value in request.headers.get("if-none-match", "").split(",")]
    # Nginx weakens ETags when gzip transforms the representation. GET cache
    # validation uses weak comparison, including through that production proxy.
    if etag in matches or "*" in matches:
        return Response(status_code=304, headers=headers)
    encodings = {parts[0].strip(): parts[1:] for value in request.headers.get("accept-encoding", "").split(",")
        if (parts := value.strip().lower().split(";"))}
    if "gzip" in encodings and not any(part.strip() in {"q=0", "q=0.0", "q=0.00", "q=0.000"} for part in encodings["gzip"]):
        headers["Content-Encoding"] = "gzip"
    else:
        content = gzip.decompress(content)
    # A cached representation cannot contain a per-request correlation ID.
    # X-Request-ID remains unique and authoritative, including on 304 replies.
    headers["X-Request-ID"] = request.state.request_id
    return Response(content=content, media_type="application/json", headers=headers)
