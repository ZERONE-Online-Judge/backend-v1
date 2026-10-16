"""Bound waiting work separately from short API calls, before touching the DB."""
import json
import hashlib
import time
from uuid import uuid4

from app.settings import settings


def busy_payload(request_id):
    return {"error": {"code": "api_busy", "message": "요청이 많습니다. 잠시 후 다시 시도해 주세요.",
        "request_id": request_id, "details": {"retry_after_seconds": 1}}}


class RequestCapacityMiddleware:
    def __init__(self, app):
        self.app = app
        self.active = {"request": 0, "wait": 0, "stream": 0}
        self.accounts = {}

    async def __call__(self, scope, receive, send):
        path = scope.get("path", "")
        if scope["type"] != "http" or not path.startswith("/api/") or path == "/api/health" or path.startswith("/api/internal/"):
            return await self.app(scope, receive, send)
        lane = "stream" if path.endswith(("/events", "/session-events")) else "wait" if path.endswith(":wait") else "request"
        limit = {"request": settings.api_max_inflight_requests, "wait": settings.api_max_wait_requests,
            "stream": settings.api_max_session_streams}[lane]
        # No await between checking and incrementing: each process has one loop.
        authorization = dict(scope.get("headers", [])).get(b"authorization")
        if authorization:
            scheme, separator, token = authorization.partition(b" ")
            if separator and scheme.lower() == b"bearer":
                authorization = token
        identity = (lane, hashlib.sha256(authorization).digest()) if authorization else None
        account_full = identity is not None and self.accounts.get(identity, 0) >= settings.api_max_account_requests
        if self.active[lane] >= limit or account_full:
            rid = f"req_{uuid4().hex}"
            body = json.dumps(busy_payload(rid), ensure_ascii=False).encode()
            await send({"type": "http.response.start", "status": 429 if account_full else 503, "headers": [
                (b"content-type", b"application/json"), (b"retry-after", b"1"),
                (b"cache-control", b"no-store"), (b"x-request-id", rid.encode()),
                (b"content-length", str(len(body)).encode())]})
            await send({"type": "http.response.body", "body": body})
            return
        self.active[lane] += 1
        if identity is not None:
            self.accounts[identity] = self.accounts.get(identity, 0) + 1
        started = time.perf_counter()
        async def timed_send(message):
            if message["type"] == "http.response.start" and settings.api_server_timing:
                message = {**message, "headers": [*message.get("headers", []),
                    (b"server-timing", f"app;dur={(time.perf_counter()-started)*1000:.2f}".encode())]}
            await send(message)
        try:
            await self.app(scope, receive, timed_send)
        finally:
            self.active[lane] -= 1
            if identity is not None:
                remaining = self.accounts[identity]-1
                if remaining:
                    self.accounts[identity] = remaining
                else:
                    self.accounts.pop(identity)
