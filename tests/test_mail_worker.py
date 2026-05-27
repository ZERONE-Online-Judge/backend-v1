import os
from types import SimpleNamespace

import pytest

os.environ.setdefault("ENABLE_DEMO_SEED", "true")
os.environ.setdefault("ALLOW_EMPTY_OTP", "true")

from app.settings import settings
from app.workers import mail_worker


def test_send_mail_uses_resend_provider(monkeypatch):
    captured = {}

    class FakeEmails:
        @staticmethod
        def send(params):
            captured["params"] = params
            return {"id": "email_test"}

    fake_resend = SimpleNamespace(api_key=None, Emails=FakeEmails)

    monkeypatch.setattr(settings, "mail_delivery_provider", "resend")
    monkeypatch.setattr(settings, "resend_api_key", "re_test")
    monkeypatch.setattr(settings, "resend_from_email", "ZOJ <noreply@mail.example.com>")
    monkeypatch.setattr(settings, "resend_api_url", "https://api.resend.com/emails")
    monkeypatch.setattr(mail_worker, "resend", fake_resend)

    mail_worker.send_mail("user@example.com", "Subject", "Plain body", "<p>HTML body</p>")

    assert fake_resend.api_key == "re_test"
    assert fake_resend.api_url == "https://api.resend.com"
    assert captured["params"] == {
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
