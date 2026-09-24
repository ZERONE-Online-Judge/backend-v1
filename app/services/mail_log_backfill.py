"""Recover only explicit system-generated contest links from older mail."""
import re
from urllib.parse import urlsplit

from sqlalchemy import text


def backfill_mail_contests(connection) -> None:
    known_ids = set(connection.execute(text("SELECT contest_id FROM contests")).scalars())
    rows = connection.execute(text(
        "SELECT mail_queue_id, mail_type, body_text FROM mail_queue WHERE contest_id IS NULL"
    )).mappings()
    allowed = {"participant_invited", "contest_operator_assigned", "contest_question_created",
               "contest_question_answered", "contest_reminder_24h", "contest_reminder_1h", "contest_reminder_10m"}
    for row in rows:
        if row["mail_type"] not in allowed:
            continue
        # Never infer a contest from the recipient's current memberships, a
        # title, or arbitrary links in a user-authored question/answer body.
        last_line = (row["body_text"] or "").rstrip().split("\n")[-1].strip()
        if not last_line.startswith("바로가기: "):
            continue
        try:
            link = urlsplit(last_line.removeprefix("바로가기: "))
        except ValueError:
            continue
        if link.scheme not in {"http", "https"} or link.netloc not in {"zoj.kr", "www.zoj.kr", "judge.zerone01.kr"}:
            continue
        match = re.fullmatch(r"/(?:operator/)?contests/([^/]+)(?:/board)?/?", link.path)
        if match and match[1] in known_ids:
            connection.execute(text("UPDATE mail_queue SET contest_id = :contest_id WHERE mail_queue_id = :mail_id AND contest_id IS NULL"),
                               {"contest_id": match[1], "mail_id": row["mail_queue_id"]})
