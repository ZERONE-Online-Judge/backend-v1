import json
import os

import pytest

os.environ.setdefault("ENABLE_DEMO_SEED", "true")
os.environ.setdefault("ALLOW_EMPTY_OTP", "true")

from app.settings import settings
from app.workers import mail_worker


class _FakeResponse:
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False


def test_send_mail_uses_resend_provider(monkeypatch):
    captured = {}

    def fake_urlopen(request, timeout):
        captured["request"] = request
        captured["timeout"] = timeout
        return _FakeResponse()

    monkeypatch.setattr(settings, "mail_delivery_provider", "resend")
    monkeypatch.setattr(settings, "resend_api_key", "re_test")
    monkeypatch.setattr(settings, "resend_from_email", "ZOJ <noreply@mail.example.com>")
    monkeypatch.setattr(settings, "resend_api_url", "https://api.resend.com/emails")
    monkeypatch.setattr(settings, "smtp_timeout_seconds", 7)
    monkeypatch.setattr(mail_worker, "urlopen", fake_urlopen)

    mail_worker.send_mail("user@example.com", "Subject", "Plain body", "<p>HTML body</p>")

    request = captured["request"]
    payload = json.loads(request.data.decode("utf-8"))
    assert captured["timeout"] == 7
    assert request.full_url == "https://api.resend.com/emails"
    assert request.get_header("Authorization") == "Bearer re_test"
    assert payload == {
        "from": "ZOJ <noreply@mail.example.com>",
        "to": ["user@example.com"],
        "subject": "Subject",
        "text": "Plain body",
        "html": "<p>HTML body</p>",
    }


def test_resend_requires_api_key(monkeypatch):
    monkeypatch.setattr(settings, "mail_delivery_provider", "resend")
    monkeypatch.setattr(settings, "resend_api_key", None)
    monkeypatch.setattr(settings, "resend_from_email", "noreply@mail.example.com")

    with pytest.raises(RuntimeError, match="RESEND_API_KEY"):
        mail_worker.send_mail("user@example.com", "Subject", "Plain body")
