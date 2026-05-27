import asyncio
import base64
from email.message import EmailMessage
from pathlib import Path
import smtplib

try:
    import resend
except ImportError:  # pragma: no cover - production image installs requirements.
    resend = None

from app.settings import settings
from app.services.store import is_internal_mail_recipient, store

LOGO_PATH = Path(__file__).resolve().parents[1] / "assets" / "logos" / "wordmark-logo.png"


def send_mail(recipient_email: str, subject: str, body_text: str, body_html: str | None = None) -> None:
    provider = settings.mail_delivery_provider.strip().lower()
    if provider == "smtp":
        _send_smtp_mail(recipient_email, subject, body_text, body_html)
        return
    if provider == "resend":
        _send_resend_mail(recipient_email, subject, body_text, body_html)
        return
    raise RuntimeError(f"Unsupported MAIL_DELIVERY_PROVIDER: {settings.mail_delivery_provider}")


def _send_smtp_mail(recipient_email: str, subject: str, body_text: str, body_html: str | None = None) -> None:
    if not settings.smtp_host or not settings.smtp_from_email:
        raise RuntimeError("SMTP_HOST and SMTP_FROM_EMAIL must be configured.")

    message = EmailMessage()
    message["From"] = settings.smtp_from_email
    message["To"] = recipient_email
    message["Subject"] = subject
    message.set_content(body_text)
    if body_html:
        message.add_alternative(body_html, subtype="html")
        if "cid:zoj-wordmark" in body_html and LOGO_PATH.exists():
            html_part = message.get_payload()[-1]
            html_part.add_related(
                LOGO_PATH.read_bytes(),
                maintype="image",
                subtype="png",
                cid="<zoj-wordmark>",
                filename="zerone-online-judge.png",
            )

    with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=settings.smtp_timeout_seconds) as smtp:
        if settings.smtp_use_tls:
            smtp.starttls()
        if settings.smtp_username:
            smtp.login(settings.smtp_username, settings.smtp_password or "")
        smtp.send_message(message)


def _send_resend_mail(recipient_email: str, subject: str, body_text: str, body_html: str | None = None) -> None:
    from_email = settings.resend_from_email or settings.smtp_from_email
    if not settings.resend_api_key or not from_email:
        raise RuntimeError("RESEND_API_KEY and RESEND_FROM_EMAIL or SMTP_FROM_EMAIL must be configured.")
    if resend is None:
        raise RuntimeError("resend package is not installed.")

    api_url = settings.resend_api_url.rstrip("/")
    if api_url.endswith("/emails"):
        api_url = api_url.removesuffix("/emails")
    resend.api_key = settings.resend_api_key
    resend.api_url = api_url
    params: dict[str, object] = {
        "from": from_email,
        "to": [recipient_email],
        "subject": subject,
        "text": body_text,
    }
    if body_html:
        params["html"] = body_html
        if "cid:zoj-wordmark" in body_html and LOGO_PATH.exists():
            params["attachments"] = [
                {
                    "filename": "zerone-online-judge.png",
                    "content": base64.b64encode(LOGO_PATH.read_bytes()).decode("ascii"),
                    "content_id": "zoj-wordmark",
                    "content_type": "image/png",
                }
            ]
    resend.Emails.send(params)


async def main() -> None:
    while True:
        store.enqueue_due_contest_reminders()
        for item in store.pending_mail(settings.mail_worker_batch_size):
            if is_internal_mail_recipient(str(item.recipient_email)):
                store.mark_mail_status(item.mail_queue_id, "canceled")
                print(f"[mail-worker] canceled internal recipient {item.mail_type} to {item.recipient_email}")
                continue
            store.mark_mail_status(item.mail_queue_id, "sending")
            try:
                send_mail(item.recipient_email, item.subject, item.body_text, item.body_html)
            except Exception as exc:
                store.mark_mail_status(item.mail_queue_id, "failed")
                print(f"[mail-worker] failed {item.mail_type} to {item.recipient_email}: {exc}")
            else:
                store.mark_mail_status(item.mail_queue_id, "sent")
                print(f"[mail-worker] sent {item.mail_type} to {item.recipient_email}")
        await asyncio.sleep(settings.mail_worker_poll_interval_seconds)


if __name__ == "__main__":
    asyncio.run(main())
