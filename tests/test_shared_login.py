import base64
import hashlib
import hmac
import json
import time
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import select, func
from app.main import app
from app.database import SessionLocal
from app.orm_models import OtpCodeRow, EditorNonceRow, IdentityAccountRow, AccessLogRow
from app.services import shared_login
from app.services.store import store
from app.services.session_events import SessionWatch, session_watch_active
from app.settings import settings

KEY = "shared-auth-test-key-with-at-least-32-characters"


@pytest.fixture
def browser(monkeypatch):
    monkeypatch.setattr(settings, "editor_auth_bridge_secret", SecretStr(KEY))
    monkeypatch.setattr(settings, "allow_empty_otp", False)
    with TestClient(app) as client:
        yield client


def rpc(client, action, values=None, purpose="zoj-shared-auth-v1", headers=None):
    path = "/api/editor/auth/" + action
    body = json.dumps(values or {}, separators=(",", ":")).encode()
    stamp, nonce = str(int(time.time())), uuid4().hex
    encoded = base64.urlsafe_b64encode(b"editor@zoj.kr").decode().rstrip("=")
    sha = hashlib.sha256(body).hexdigest()
    canonical = "\n".join([purpose, "POST", path, stamp, nonce, encoded, sha])
    signed = {
        "Content-Type": "application/json",
        "X-Editor-Timestamp": stamp,
        "X-Editor-Nonce": nonce,
        "X-Editor-Identity": encoded,
        "X-Editor-Content-Sha256": sha,
        "X-Editor-Signature": hmac.new(
            KEY.encode(), canonical.encode(), hashlib.sha256
        ).hexdigest(),
    }
    return client.post(path, content=body, headers=headers or signed), signed, body


def login(client):
    email = "shared-" + uuid4().hex + "@example.com"
    assert rpc(client, "code-request", {"email": email})[0].status_code == 200
    with SessionLocal() as db:
        code = db.get(OtpCodeRow, email).code
    result = rpc(client, "code-verify", {"email": email, "code": code})[0]
    assert result.status_code == 200, result.text
    return email, result.json()["data"]


def test_editor_login_restores_the_same_central_session_and_logout_revokes_both(
    browser,
):
    email, result = login(browser)
    token, user = result["session_token"], result["user"]
    assert user["email"] == email and "access_token" not in user
    browser.cookies.set(shared_login.COOKIE, token, domain="testserver.local", path="/")
    restored = browser.post("/api/auth/general/sso").json()["data"]
    assert restored["account"]["account_id"] == user["account_id"]
    assert restored["refresh_token"] == token
    assert (
        restored["participant_contests"] == [] and restored["operator_session"] is None
    )
    watch = SessionWatch("general_access", shared_login.resolve(token)["session_id"])
    assert session_watch_active(watch)
    assert rpc(browser, "me", {"session_token": token})[0].status_code == 200
    assert browser.post("/api/auth/logout").status_code == 200
    assert browser.cookies.get(shared_login.COOKIE) is None
    assert not session_watch_active(watch)
    assert rpc(browser, "me", {"session_token": token})[0].status_code == 401
    assert (
        browser.get(
            "/api/auth/general/me",
            headers={"Authorization": "Bearer " + restored["access_token"]},
        ).status_code
        == 401
    )
    with SessionLocal() as db:
        assert (
            db.scalar(
                select(AccessLogRow).where(
                    AccessLogRow.email == email,
                    AccessLogRow.event_type == "general_login",
                )
            )
            is not None
        )


def test_signatures_are_purpose_bound_mutations_single_use_and_reads_do_not_write(
    browser,
):
    _, result = login(browser)
    values = {"session_token": result["session_token"]}
    response, headers, body = rpc(browser, "me", values)
    assert response.status_code == 200
    with SessionLocal() as db:
        before = db.scalar(select(func.count()).select_from(EditorNonceRow))
    assert rpc(browser, "me", values, headers=headers)[0].status_code == 200
    with SessionLocal() as db:
        assert db.scalar(select(func.count()).select_from(EditorNonceRow)) == before
    assert rpc(browser, "me", values, purpose="zoj-editor-v1")[0].status_code == 401
    assert browser.post("/api/editor/auth/me", content=body).status_code == 401
    response, headers, _ = rpc(
        browser, "profile", {**values, "display_name": "공통 이름"}
    )
    assert response.status_code == 200
    assert (
        rpc(
            browser, "profile", {**values, "display_name": "공통 이름"}, headers=headers
        )[0].status_code
        == 401
    )
    assert (
        rpc(
            browser, "profile", {**values, "display_name": "다른 이름"}, headers=headers
        )[0].status_code
        == 401
    )
    assert rpc(browser, "me", {"session_token": uuid4().hex})[0].status_code == 401


def test_login_proof_is_single_use_and_failed_attempts_are_bounded(browser):
    email = "attempts-" + uuid4().hex + "@example.com"
    rpc(browser, "code-request", {"email": email})
    with SessionLocal() as db:
        code = db.get(OtpCodeRow, email).code
    wrong = "000000" if code != "000000" else "111111"
    for _ in range(5):
        assert (
            rpc(browser, "code-verify", {"email": email, "code": wrong})[0].status_code
            == 401
        )
    assert (
        rpc(browser, "code-verify", {"email": email, "code": code})[0].status_code
        == 429
    )
    with SessionLocal() as db:
        assert (
            db.scalar(
                select(IdentityAccountRow).where(IdentityAccountRow.email == email)
            )
            is None
        )
    email, _ = login(browser)
    with SessionLocal() as db:
        code = db.get(OtpCodeRow, email).code
    assert (
        rpc(
            browser,
            "code-verify",
            {"email": email, "code": code, "force_new_session": True},
        )[0].status_code
        == 401
    )


def test_production_common_cookie_is_secure_and_has_one_domain(monkeypatch):
    from fastapi import Response

    email = "cookie-" + uuid4().hex + "@example.com"
    with SessionLocal() as db:
        db.add(IdentityAccountRow(email=email, display_name="Cookie"))
        db.commit()
    token = store.verify_general_otp(email, store.create_general_otp(email))[
        "refresh_token"
    ]
    monkeypatch.setattr(settings, "app_env", "production")
    response = Response()
    shared_login.set_cookie(response, token)
    cookie = response.headers["set-cookie"]
    assert (
        "Domain=.zoj.kr" in cookie
        and "HttpOnly" in cookie
        and "Secure" in cookie
        and "SameSite=lax" in cookie
    )
    cleared = Response()
    shared_login.clear_cookie(cleared)
    assert (
        "Domain=.zoj.kr" in cleared.headers["set-cookie"]
        and "Max-Age=0" in cleared.headers["set-cookie"]
    )
