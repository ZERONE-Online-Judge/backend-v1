"""Fixed-origin server transport; never forward staff tokens to Editor."""
import base64
import hashlib
import hmac
import json
import ssl
import tempfile
import time
from contextlib import contextmanager
from urllib.parse import urlsplit
from uuid import uuid4
import httpx
from app.settings import settings
from app.services.errors import AppError

MAX_ARCHIVE = 128 * 1024 * 1024


def configured():
    return bool(settings.editor_api_url and settings.editor_bridge_secret)


def client_settings(method, path, email, content=b""):
    key = settings.editor_bridge_secret.get_secret_value() if settings.editor_bridge_secret else ""
    if not configured() or len(key) < 32:
        raise AppError(503, "editor_library_disabled", "Editor 연결이 아직 설정되지 않았습니다.")
    url = urlsplit(settings.editor_api_url)
    if url.scheme not in {"http", "https"} or not url.hostname or url.username or url.password or url.query or url.fragment or url.path.rstrip("/") != "/api" or (settings.app_env == "production" and url.scheme != "https"):
        raise AppError(503, "editor_library_configuration", "Editor API 연결 주소를 확인하세요.")
    encoded = base64.urlsafe_b64encode(email.lower().encode()).decode().rstrip("=")
    stamp, nonce = str(int(time.time())), uuid4().hex
    digest = hashlib.sha256(content).hexdigest()
    message = "\n".join(["zoj-library-v1", method, path, stamp, nonce, encoded, digest])
    headers = {"X-Editor-Request": "1", "X-Editor-Identity": encoded, "X-Editor-Timestamp": stamp,
        "X-Editor-Nonce": nonce, "X-Editor-Content-Sha256": digest,
        "X-Editor-Signature": hmac.new(key.encode(), message.encode(), hashlib.sha256).hexdigest(), "Content-Type": "application/json"}
    try:
        verify = ssl.create_default_context(cafile=settings.editor_ca_file) if settings.editor_ca_file else True
    except (OSError, ssl.SSLError):
        raise AppError(503, "editor_library_configuration", "Editor CA 인증서 설정을 확인하세요.")
    return settings.editor_api_url.rstrip("/") + path.removeprefix("/api"), headers, verify


def check_response(response, body=None):
    if response.is_redirect:
        raise AppError(502, "editor_transport_redirect", "Editor API가 다른 주소로 이동했습니다. 서버 설정을 확인하세요.")
    if response.is_error:
        try:
            raw = json.loads(body) if body is not None else response.json()
            error = raw.get("error", {})
        except (ValueError, AttributeError):
            error = {}
        raise AppError(response.status_code if response.status_code in {400, 401, 403, 404, 409, 413, 422, 429, 503} else 502,
            error.get("code", "editor_request_failed"), error.get("message", "Editor 요청을 처리하지 못했습니다."))


def request_json(method, path, email, values=None):
    content = json.dumps(values, ensure_ascii=False, separators=(",", ":")).encode() if values is not None else b""
    target, headers, verify = client_settings(method, path, email, content)
    try:
        with httpx.Client(timeout=httpx.Timeout(30, connect=5), follow_redirects=False, verify=verify, trust_env=False) as client:
            with client.stream(method, target, headers=headers, content=content) as response:
                body = bytearray()
                for chunk in response.iter_bytes():
                    body.extend(chunk)
                    if len(body) > 2 * 1024 * 1024:
                        raise AppError(502, "editor_response_too_large", "Editor 응답 크기가 너무 큽니다.")
                check_response(response, body)
                data = json.loads(body)
                if not isinstance(data, dict):
                    raise AppError(502, "editor_response_invalid", "Editor 응답 형식이 올바르지 않습니다.")
                return data
    except (httpx.HTTPError, ValueError):
        raise AppError(502, "editor_unavailable", "Editor 서버에 연결하지 못했습니다. 잠시 후 다시 시도하세요.")


@contextmanager
def download_package(path, email, expected_sha):
    target, headers, verify = client_settings("GET", path, email)
    try:
        with tempfile.TemporaryFile("w+b") as output, httpx.Client(timeout=httpx.Timeout(120, connect=5), follow_redirects=False, verify=verify, trust_env=False) as client:
            with client.stream("GET", target, headers=headers) as response:
                if response.is_error or response.is_redirect:
                    error_body = bytearray()
                    for chunk in response.iter_bytes():
                        error_body.extend(chunk[:65536 - len(error_body)])
                        if len(error_body) >= 65536:
                            break
                    check_response(response, error_body)
                if response.headers.get("content-type", "").split(";")[0] != "application/zip":
                    raise AppError(502, "editor_archive_invalid", "Editor가 ZIP 패키지를 반환하지 않았습니다.")
                size, digest = 0, hashlib.sha256()
                for chunk in response.iter_bytes():
                    size += len(chunk)
                    if size > MAX_ARCHIVE:
                        raise AppError(413, "editor_archive_too_large", "Editor ZIP은 128MiB 이하만 지원합니다.")
                    digest.update(chunk)
                    output.write(chunk)
                if digest.hexdigest() != expected_sha:
                    raise AppError(502, "editor_archive_changed", "Editor 패키지의 무결성 검사가 실패했습니다.")
            output.seek(0)
            yield output
    except httpx.HTTPError:
        raise AppError(502, "editor_unavailable", "Editor 패키지를 가져오지 못했습니다. 같은 요청을 다시 시도해 주세요.")
