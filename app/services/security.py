from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import time
from typing import Any


def hash_password(password: str, salt: str | None = None) -> str:
    actual_salt = salt or secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), actual_salt.encode("utf-8"), 210_000)
    return f"pbkdf2_sha256${actual_salt}${digest.hex()}"


def verify_password(password: str, password_hash: str) -> bool:
    try:
        algorithm, salt, expected = password_hash.split("$", 2)
    except ValueError:
        return False
    if algorithm != "pbkdf2_sha256":
        return False
    actual = hash_password(password, salt).split("$", 2)[2]
    return hmac.compare_digest(actual, expected)


def new_token() -> str:
    return secrets.token_urlsafe(32)


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(f"{value}{padding}".encode("ascii"))


def new_session_token(token_type: str, subject: str, ttl_seconds: int, extra_claims: dict[str, Any] | None = None) -> str:
    from app.settings import settings

    now = int(time.time())
    header = {"alg": "HS256", "typ": "JWT"}
    payload: dict[str, Any] = {
        "iss": settings.auth_token_issuer,
        "sub": subject,
        "typ": token_type,
        "iat": now,
        "exp": now + ttl_seconds,
        "jti": secrets.token_urlsafe(18),
    }
    if extra_claims:
        payload.update(extra_claims)
    signing_input = ".".join(
        [
            _b64url_encode(json.dumps(header, separators=(",", ":"), sort_keys=True).encode("utf-8")),
            _b64url_encode(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")),
        ]
    )
    signature = hmac.new(settings.auth_token_secret.encode("utf-8"), signing_input.encode("ascii"), hashlib.sha256).digest()
    return f"{signing_input}.{_b64url_encode(signature)}"


def decode_session_token(token: str, expected_type: str | None = None) -> dict[str, Any] | None:
    from app.settings import settings

    try:
        header_raw, payload_raw, signature_raw = token.split(".", 2)
        signing_input = f"{header_raw}.{payload_raw}"
        expected_signature = hmac.new(settings.auth_token_secret.encode("utf-8"), signing_input.encode("ascii"), hashlib.sha256).digest()
        actual_signature = _b64url_decode(signature_raw)
        if not hmac.compare_digest(actual_signature, expected_signature):
            return None
        header = json.loads(_b64url_decode(header_raw))
        payload = json.loads(_b64url_decode(payload_raw))
    except (ValueError, json.JSONDecodeError, UnicodeDecodeError):
        return None
    if header.get("alg") != "HS256" or header.get("typ") != "JWT":
        return None
    if payload.get("iss") != settings.auth_token_issuer:
        return None
    if expected_type is not None and payload.get("typ") != expected_type:
        return None
    if int(payload.get("exp", 0)) <= int(time.time()):
        return None
    return payload


def token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()
