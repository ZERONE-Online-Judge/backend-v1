import asyncio
from email.message import EmailMessage
from pathlib import Path
import smtplib

from app.settings import settings
from app.services.store import is_internal_mail_recipient, store

LOGO_PATH = Path(__file__).resolve().parents[1] / "assets" / "logos" / "wordmark-logo.png"


def send_mail(recipient_email: str, subject: str, body_text: str, body_html: str | None = None) -> None:
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
