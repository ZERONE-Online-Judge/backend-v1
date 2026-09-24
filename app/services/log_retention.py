"""Delete expired logs in small, independently committed batches."""
from datetime import timedelta

from sqlalchemy import delete, select, text

from app.database import SessionLocal
from app.models import now_utc
from app.orm_models import AccessLogRow, JudgeAgentLogRow, OperationalAuditLogRow
from app.settings import settings


def cleanup_expired_logs() -> dict[str, int]:
    policies = [
        (AccessLogRow, "access_log_id", settings.access_log_retention_days),
        (OperationalAuditLogRow, "operational_audit_log_id", settings.audit_log_retention_days),
        (JudgeAgentLogRow, "judge_agent_log_id", settings.judge_log_retention_days),
    ]
    counts = {}
    limit = max(1, min(settings.log_cleanup_batch_size, 5000))
    for model, id_field, days in policies:
        # Zero explicitly disables deletion for that log category.
        if days <= 0:
            counts[model.__tablename__] = 0
            continue
        cutoff = now_utc() - timedelta(days=days)
        with SessionLocal() as db:
            if db.bind.dialect.name == "postgresql":
                db.execute(text("SET LOCAL statement_timeout = '3s'"))
                db.execute(text("SET LOCAL lock_timeout = '500ms'"))
            identity = getattr(model, id_field)
            expired = select(identity).where(model.created_at < cutoff).order_by(
                model.created_at, identity,
            ).limit(limit).with_for_update(skip_locked=True)
            ids = list(db.scalars(expired))
            if ids:
                db.execute(delete(model).where(identity.in_(ids), model.created_at < cutoff))
            db.commit()
            counts[model.__tablename__] = len(ids)
    return counts
