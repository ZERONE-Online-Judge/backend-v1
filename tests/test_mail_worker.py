import base64
import os
from types import SimpleNamespace

import pytest

os.environ.setdefault("ENABLE_DEMO_SEED", "true")
os.environ.setdefault("ALLOW_EMPTY_OTP", "true")

from app.settings import settings
from app.services.mail_templates import render_branded_email
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


def test_resend_keeps_inline_brand_logo_and_plain_text_alternative(monkeypatch, tmp_path):
    captured = {}
    logo = tmp_path / "wordmark.png"
    logo_bytes = b"\x89PNG\r\n\x1a\ninline-brand-logo"
    logo.write_bytes(logo_bytes)

    class FakeEmails:
        @staticmethod
        def send(params):
            captured.update(params)
            return {"id": "email_test"}

    monkeypatch.setattr(settings, "mail_delivery_provider", "resend")
    monkeypatch.setattr(settings, "resend_api_key", "re_test")
    monkeypatch.setattr(settings, "resend_from_email", "ZOJ <noreply@mail.example.com>")
    monkeypatch.setattr(mail_worker, "LOGO_PATH", logo)
    monkeypatch.setattr(mail_worker, "resend", SimpleNamespace(Emails=FakeEmails))
    html = render_branded_email(title="대회 안내", preheader="대회 소식입니다", body=["안녕하세요."])

    mail_worker.send_mail("user@example.com", "대회 안내", "안녕하세요.", html)

    assert captured["text"] == "안녕하세요."
    assert captured["html"] == html
    assert "cid:zoj-wordmark" in html
    attachment, = captured["attachments"]
    assert attachment["content_id"] == "zoj-wordmark"
    assert attachment["content_type"] == "image/png"
    assert base64.b64decode(attachment["content"]) == logo_bytes


def test_smtp_keeps_inline_brand_logo_related_to_html(monkeypatch, tmp_path):
    captured = {}
    logo = tmp_path / "wordmark.png"
    logo_bytes = b"\x89PNG\r\n\x1a\ninline-brand-logo"
    logo.write_bytes(logo_bytes)

    class FakeSMTP:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def send_message(self, message):
            captured["message"] = message

    monkeypatch.setattr(settings, "mail_delivery_provider", "smtp")
    monkeypatch.setattr(settings, "smtp_host", "smtp.example.com")
    monkeypatch.setattr(settings, "smtp_from_email", "ZOJ <noreply@mail.example.com>")
    monkeypatch.setattr(settings, "smtp_use_tls", False)
    monkeypatch.setattr(settings, "smtp_username", None)
    monkeypatch.setattr(mail_worker, "LOGO_PATH", logo)
    monkeypatch.setattr(mail_worker.smtplib, "SMTP", FakeSMTP)
    html = render_branded_email(title="대회 안내", preheader="대회 소식입니다", body=["안녕하세요."])

    mail_worker.send_mail("user@example.com", "대회 안내", "안녕하세요.", html)

    message = captured["message"]
    assert message.get_content_type() == "multipart/alternative"
    text_part, related_part = message.get_payload()
    assert text_part.get_content_type() == "text/plain"
    assert text_part.get_content().strip() == "안녕하세요."
    assert related_part.get_content_type() == "multipart/related"
    html_part, logo_part = related_part.get_payload()
    assert html_part.get_content_type() == "text/html"
    assert html_part.get_content().strip() == html.strip()
    assert "cid:zoj-wordmark" in html_part.get_content()
    assert logo_part["Content-ID"] == "<zoj-wordmark>"
    assert logo_part.get_content_type() == "image/png"
    assert logo_part.get_payload(decode=True) == logo_bytes
