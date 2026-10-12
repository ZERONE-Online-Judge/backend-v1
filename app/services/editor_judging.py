"""Authenticated Editor snapshots dispatched to the existing judge protocol."""
import gzip
import copy
import hashlib
import hmac
import json
import re
import stat
import threading
import time
import zipfile
from datetime import timedelta, timezone
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from app.database import SessionLocal
from app.models import JUDGE_FINAL_STATUSES, now_utc
from app.orm_models import EditorJudgeRunRow as Run, IdentityAccountRow, JudgeNodeRow
from app.services.errors import AppError
from app.services.node_credentials import valid_node_credential
from app.services.storage import object_storage
from app.settings import settings

ACTIVE = {"waiting", "preparing", "judging"}
RUNNING = {"preparing", "judging"}
MAX_ARCHIVE = 128 * 1024**2
MAX_CONTENT = 256 * 1024**2
HASH = r"^[a-f0-9]{64}$"
_maintenance_lock = threading.Lock()
_last_maintenance = 0.0


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Source(Strict):
    name: str = Field(min_length=1, max_length=150)
    language: Literal["cpp17", "python313", "c99", "java8"]
    sha256: str = Field(pattern=HASH)


class Case(Strict):
    index: int = Field(ge=1, le=1000000)
    input_sha: str = Field(pattern=HASH)
    answer_sha: str = Field(pattern=HASH)


class Resource(Strict):
    name: str = Field(min_length=1, max_length=150)
    role: Literal["checker", "package-resource"]
    sha256: str = Field(pattern=HASH)

    @field_validator("name")
    @classmethod
    def safe_name(cls, value):
        if value in {".", ".."} or any(c in value for c in ("/", "\\")) or any(ord(c) < 32 for c in value):
            raise ValueError("Unsafe resource filename")
        return value


class Snapshot(Strict):
    format: Literal["zoj-editor-judge-v1"]
    operation_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    project_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    context_hash: str = Field(pattern=HASH)
    source: Source
    time_limit_ms: int = Field(ge=100, le=10000)
    memory_limit_mb: int = Field(ge=16, le=1024)
    testcases: list[Case] = Field(min_length=1, max_length=1000)
    resources: list[Resource] = Field(min_length=1, max_length=100)


def aware(value):
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value


def owner(db, email, lock=False):
    query = select(IdentityAccountRow).where(IdentityAccountRow.email == email.lower())
    if lock:
        query = query.with_for_update()
    row = db.scalar(query)
    if not row:
        raise AppError(403, "editor_identity_missing", "ZOJ 공통 계정으로 로그인한 뒤 검증하세요.")
    return row


def public_run(row):
    return {
        "id": row.run_id, "status": row.status,
        "progress_current": row.progress_current, "progress_total": row.testcase_count,
        "node_name": row.node_name, "agent_version": row.agent_version,
        "result": row.result, "runtime_revision": row.payload["runtime_revision"],
    }


def capabilities():
    with SessionLocal() as db:
        nodes = db.scalars(select(JudgeNodeRow).where(JudgeNodeRow.schedulable.is_(True))).all()
        versions = sorted({n.agent_version for n in nodes})
        revision = hashlib.sha256(json.dumps(["editor-dispatch-v1", settings.release_version, versions], separators=(",", ":")).encode()).hexdigest()[:16]
        return {
            "backend": "zoj", "runtime_revision": "zoj-judge-" + revision,
            "languages": ["cpp17", "python313", "c99", "java8"],
            "standard_io_only": True, "contest_submissions_first": True,
            "online_nodes": sum(aware(n.last_heartbeat_at) >= now_utc() - timedelta(seconds=settings.judge_node_active_window_seconds) for n in nodes),
        }


def create(email, source, fingerprint):
    """Validate every byte before storage; the signed caller supplies no URLs."""
    source.seek(0, 2)
    if source.tell() > MAX_ARCHIVE:
        raise AppError(413, "editor_judge_archive_large", "채점 자료 ZIP은 128MiB 이하여야 합니다.")
    source.seek(0)
    try:
        archive = zipfile.ZipFile(source)
        entries = archive.infolist()
        if len(entries) > 2102 or len({i.filename for i in entries}) != len(entries):
            raise ValueError("Duplicate or too many archive entries")
        if sum(i.file_size for i in entries) > MAX_CONTENT:
            raise ValueError("Expanded snapshot exceeds 256MiB")
        for entry in entries:
            mode = entry.external_attr >> 16
            if entry.flag_bits & 1 or (stat.S_IFMT(mode) and not stat.S_ISREG(mode)):
                raise ValueError("Only unencrypted regular files are allowed")
            if entry.filename != "manifest.json" and not re.fullmatch(r"blobs/[a-f0-9]{64}", entry.filename):
                raise ValueError("Unsafe snapshot archive path")
        if archive.getinfo("manifest.json").file_size > 2 * 1024**2:
            raise ValueError("Snapshot manifest exceeds 2MiB")
        snapshot = Snapshot.model_validate_json(archive.read("manifest.json"))
        if len({c.index for c in snapshot.testcases}) != len(snapshot.testcases):
            raise ValueError("Duplicate testcase order")
        if sum(r.role == "checker" for r in snapshot.resources) != 1 or len({r.name for r in snapshot.resources}) != len(snapshot.resources):
            raise ValueError("Exactly one checker and unique resource filenames are required")
        hashes = {snapshot.source.sha256}
        hashes.update(s for c in snapshot.testcases for s in (c.input_sha, c.answer_sha))
        hashes.update(r.sha256 for r in snapshot.resources)
        if {i.filename for i in entries} != {"manifest.json", *("blobs/" + s for s in hashes)}:
            raise ValueError("Snapshot files do not match the manifest")
        content = {}
        for sha in hashes:
            if archive.getinfo("blobs/" + sha).file_size > 32 * 1024**2:
                raise ValueError("A snapshot file exceeds 32MiB")
            blob = archive.read("blobs/" + sha)
            if hashlib.sha256(blob).hexdigest() != sha:
                raise ValueError("Snapshot content hash mismatch")
            content[sha] = blob
        if len(content[snapshot.source.sha256]) > 1024**2:
            raise ValueError("Solution source exceeds 1MiB")
        code = content[snapshot.source.sha256].decode("utf-8-sig")
        cases = [
            {"display_order": c.index, "input_text": content[c.input_sha].decode("utf-8-sig"),
             "output_text": content[c.answer_sha].decode("utf-8-sig")}
            for c in sorted(snapshot.testcases, key=lambda c: c.index)
        ]
    except (ValueError, KeyError, UnicodeError, zipfile.BadZipFile, RuntimeError, ValidationError) as error:
        raise AppError(422, "editor_judge_snapshot_invalid", "채점 자료 형식·파일 무결성을 확인하세요.") from error
    revision = capabilities()["runtime_revision"]
    with SessionLocal() as db:
        account = owner(db, email, lock=True)
        operation = hashlib.sha256((account.account_id + "\n" + snapshot.operation_id).encode()).hexdigest()
        existing = db.scalar(select(Run).where(Run.operation_key == operation))
        if existing:
            if existing.fingerprint != fingerprint:
                raise AppError(409, "editor_judge_request_changed", "같은 채점 요청의 자료가 변경되었습니다.")
            return public_run(existing)
        if db.scalar(select(func.count()).select_from(Run).where(Run.owner_id == account.account_id, Run.status.in_(ACTIVE))) >= 32:
            raise AppError(429, "editor_judge_busy", "내 계정의 채점 작업이 많습니다. 완료 후 다시 요청하세요.")
        if db.scalar(select(func.count()).select_from(Run).where(Run.status.in_(ACTIVE))) >= 256:
            raise AppError(429, "editor_judge_busy", "채점 대기열이 가득 찼습니다. 잠시 후 다시 요청하세요.")
        dataset_key = hashlib.sha256(json.dumps([
            snapshot.project_id, snapshot.context_hash, snapshot.time_limit_ms, snapshot.memory_limit_mb,
            [c.model_dump() for c in sorted(snapshot.testcases, key=lambda c: c.index)],
            [r.model_dump() for r in sorted(snapshot.resources, key=lambda r: r.name)],
        ], sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        prefix = "editor-judge/" + account.account_id + "/" + dataset_key + "/"
        bundle_key = prefix + "testcases.json.gz"
        written = []
        try:
            if object_storage.size_bytes(bundle_key) is None:
                object_storage.write_bytes(bundle_key, gzip.compress(json.dumps({"testcases": cases}, ensure_ascii=False).encode(), mtime=0), "application/gzip")
            written.append(bundle_key)
            case_refs = []
            for c in sorted(snapshot.testcases, key=lambda c: c.index):
                for sha in (c.input_sha, c.answer_sha):
                    key = prefix + sha
                    if key not in written:
                        if object_storage.size_bytes(key) is None:
                            object_storage.write_bytes(key, content[sha])
                        written.append(key)
                case_refs.append({"display_order": c.index, "input_storage_key": prefix + c.input_sha,
                                  "output_storage_key": prefix + c.answer_sha})
            resources = []
            for r in snapshot.resources:
                key = prefix + r.sha256
                if key not in written:
                    if object_storage.size_bytes(key) is None:
                        object_storage.write_bytes(key, content[r.sha256])
                    written.append(key)
                resources.append({"original_filename": r.name, "role": r.role, "storage_key": key, "sha256": r.sha256})
            row = Run(operation_key=operation, owner_id=account.account_id, dataset_key=dataset_key, project_id=snapshot.project_id,
                      fingerprint=fingerprint, testcase_count=len(cases), storage_keys=written,
                      payload={"language": snapshot.source.language, "source_code": code,
                               "source_name": snapshot.source.name, "context_hash": snapshot.context_hash,
                               "time_limit_ms": snapshot.time_limit_ms, "memory_limit_mb": snapshot.memory_limit_mb,
                               "bundle_key": bundle_key, "base_bundle_key": bundle_key, "excluded_orders": [],
                               "testcases": case_refs,
                               "dataset_storage_keys": list(written),
                               "resources": resources, "runtime_revision": revision})
            db.add(row)
            db.commit()
            return public_run(row)
        except IntegrityError:
            db.rollback()
            existing = db.scalar(select(Run).where(Run.operation_key == operation))
            if existing and existing.fingerprint == fingerprint:
                return public_run(existing)
            raise AppError(409, "editor_judge_request_changed", "같은 채점 요청의 자료가 변경되었습니다.")
        except Exception:
            db.rollback()
            if not db.scalar(select(Run.run_id).where(Run.owner_id == account.account_id, Run.dataset_key == dataset_key).limit(1)):
                for key in written:
                    object_storage.delete(key)
            raise


def resume(email, parent_id, operation_id):
    """Remove the observed failure using stored immutable input, without upload."""
    with SessionLocal() as db:
        account = owner(db, email, lock=True)
        parent = db.scalar(select(Run).where(Run.run_id == parent_id, Run.owner_id == account.account_id).with_for_update())
        failed = (parent.result or {}).get("failed_testcase_order") if parent else None
        if not parent or parent.status in ACTIVE or not failed or parent.testcase_count < 2:
            raise AppError(409, "editor_judge_resume_invalid", "실패가 확인된 테스트의 나머지 자료만 재검증할 수 있습니다.")
        operation = hashlib.sha256((account.account_id + "\n" + operation_id).encode()).hexdigest()
        fingerprint = hashlib.sha256((parent_id + "\n" + str(failed)).encode()).hexdigest()
        existing = db.scalar(select(Run).where(Run.operation_key == operation))
        if existing:
            if existing.fingerprint != fingerprint:
                raise AppError(409, "editor_judge_request_changed", "같은 채점 요청의 자료가 변경되었습니다.")
            return public_run(existing)
        if db.scalar(select(func.count()).select_from(Run).where(Run.owner_id == account.account_id, Run.status.in_(ACTIVE))) >= 32:
            raise AppError(429, "editor_judge_busy", "내 계정의 채점 작업이 많습니다.")
        if db.scalar(select(func.count()).select_from(Run).where(Run.status.in_(ACTIVE))) >= 256:
            raise AppError(429, "editor_judge_busy", "채점 대기열이 가득 찼습니다.")
        payload = copy.deepcopy(parent.payload)
        excluded = set(payload["excluded_orders"]) | {failed}
        if object_storage.size_bytes(payload["base_bundle_key"]) is None:
            raise AppError(410, "editor_judge_snapshot_expired", "임시 채점 자료가 만료되었습니다. 새 검증을 요청하세요.")
        all_cases = json.loads(gzip.decompress(object_storage.read_bytes(payload["base_bundle_key"])))['testcases']
        remaining = [c for c in all_cases if c['display_order'] not in excluded]
        if len(remaining) != parent.testcase_count - 1:
            raise AppError(409, "editor_judge_snapshot_changed", "보관된 테스트 자료의 개수가 다릅니다.")
        key = "editor-judge/" + account.account_id + "/variants/" + operation + "-" + fingerprint + ".json.gz"
        object_storage.write_bytes(key, gzip.compress(json.dumps({"testcases": remaining}, ensure_ascii=False).encode(), mtime=0), "application/gzip")
        payload["bundle_key"], payload["excluded_orders"] = key, sorted(excluded)
        payload["testcases"] = [c for c in payload["testcases"] if c["display_order"] != failed]
        row = Run(operation_key=operation, owner_id=account.account_id, dataset_key=parent.dataset_key,
                  project_id=parent.project_id, fingerprint=fingerprint, testcase_count=len(remaining),
                  payload=payload, storage_keys=[key])
        db.add(row)
        try:
            db.commit()
        except IntegrityError:
            db.rollback()
            existing = db.scalar(select(Run).where(Run.operation_key == operation))
            if existing and existing.fingerprint == fingerprint:
                return public_run(existing)
            object_storage.delete(key)
            raise AppError(409, "editor_judge_request_changed", "같은 채점 요청의 자료가 변경되었습니다.")
        return public_run(row)


def get_run(email, run_id, cancel=False):
    with SessionLocal() as db:
        account = owner(db, email)
        query = select(Run).where(Run.run_id == run_id, Run.owner_id == account.account_id)
        row = db.scalar(query.with_for_update() if cancel else query)
        if not row:
            raise AppError(404, "editor_judge_run_missing", "내 채점 요청을 찾을 수 없습니다.")
        if cancel and row.status in ACTIVE:
            row.status = "cancelled"
            row.lease_token = None
            row.completed_at = now_utc()
            db.commit()
        return public_run(row)


def maintenance(session_factory=None):
    global _last_maintenance
    if time.monotonic() - _last_maintenance < 60 or not _maintenance_lock.acquire(blocking=False):
        return
    try:
        with (session_factory or SessionLocal)() as db:
            for row in db.scalars(select(Run).where(Run.status.in_(ACTIVE), Run.created_at < now_utc() - timedelta(hours=1)).with_for_update(skip_locked=True)):
                row.status = "system_error"
                row.result = {"final_status": "system_error", "judge_message": "채점 대기·실행 한도를 초과했습니다."}
                row.lease_token = None
                row.completed_at = now_utc()
            db.commit()
            old_ids = db.scalars(select(Run.run_id).where(Run.status.not_in(ACTIVE), Run.created_at < now_utc() - timedelta(hours=48)).limit(1000)).all()
            for run_id in old_ids:
                candidate = db.get(Run, run_id)
                if not candidate:
                    continue
                # Creation and subset requests acquire owner then run locks.
                # Use the same order so GC cannot race new dataset references.
                db.scalar(select(IdentityAccountRow).where(IdentityAccountRow.account_id == candidate.owner_id).with_for_update())
                row = db.scalar(select(Run).where(Run.run_id == run_id).with_for_update())
                if not row or row.status in ACTIVE or aware(row.created_at) >= now_utc() - timedelta(hours=48):
                    db.rollback()
                    continue
                # Keep the row on storage failure so a later sweep can retry.
                shared = db.scalar(select(Run.run_id).where(Run.owner_id == row.owner_id, Run.dataset_key == row.dataset_key, Run.run_id != row.run_id).limit(1))
                keys = set(row.storage_keys)
                if not shared:
                    keys.update(row.payload.get("dataset_storage_keys", []))
                    if row.payload.get("base_bundle_key"):
                        keys.add(row.payload["base_bundle_key"])
                    keys.update(r["storage_key"] for r in row.payload.get("resources", []))
                    keys.update(c[k] for c in row.payload.get("testcases", []) for k in ("input_storage_key", "output_storage_key"))
                for key in keys:
                    if "/variants/" in key or not shared:
                        object_storage.delete(key)
                db.delete(row)
                db.commit()
            db.commit()
        _last_maintenance = time.monotonic()
    finally:
        _maintenance_lock.release()


def claim_jobs(node_id, node_secret, max_count, session_factory=None):
    if max_count <= 0:
        return []
    with (session_factory or SessionLocal)() as db:
        node = db.scalar(select(JudgeNodeRow).where(JudgeNodeRow.judge_node_id == node_id).with_for_update())
        if not valid_node_credential(node, node_secret):
            raise ValueError("node secret invalid")
        cutoff = now_utc() - timedelta(seconds=settings.judge_lease_timeout_seconds)
        expired = db.scalars(select(Run).where(Run.status.in_(RUNNING), Run.leased_at < cutoff).with_for_update(skip_locked=True)).all()
        for row in expired:
            row.status = "waiting" if row.attempts < 3 else "system_error"
            row.assigned_node_id = row.lease_token = row.leased_at = None
            row.progress_current = 0
            if row.status == "system_error":
                row.result = {"final_status": "system_error", "judge_message": "채점기 연결이 반복해서 중단되었습니다."}
                row.completed_at = now_utc()
        db.flush()
        rows = db.scalars(select(Run).where(Run.status == "waiting").order_by(Run.created_at, Run.run_id).limit(min(max_count, settings.judge_claim_max_batch_size)).with_for_update(skip_locked=True)).all()
        jobs = []
        for row in rows:
            row.status, row.assigned_node_id, row.lease_token, row.leased_at = "preparing", node_id, str(uuid4()), now_utc()
            row.node_name, row.agent_version = node.node_name, node.agent_version
            row.attempts += 1
            payload = row.payload
            jobs.append({
                "judge_job_id": row.run_id, "lease_token": row.lease_token,
                "submission": {"submission_id": row.run_id, "submission_kind": "editor_verification", "language": payload["language"], "source_code": payload["source_code"]},
                "problem": {"problem_id": row.project_id, "time_limit_ms": payload["time_limit_ms"], "memory_limit_mb": payload["memory_limit_mb"], "language_resource_limits": {}},
                # The bundle is an optimization. Keep real per-object fallback
                # references so a download failure cannot become a no-test AC.
                "testcases": [{**c, "input_url": object_storage.internal_presigned_get_url(c["input_storage_key"]),
                               "output_url": object_storage.internal_presigned_get_url(c["output_storage_key"])} for c in payload["testcases"]],
                "bundle_url": object_storage.internal_presigned_get_url(payload["bundle_key"]),
                "package_files": [{**r, "asset_id": r["sha256"], "url": object_storage.internal_presigned_get_url(r["storage_key"])} for r in payload["resources"]],
            })
        db.commit()
        return jobs


def update(run_id, node_secret, lease_token, *, result=None, status=None, current=None, total=None, session_factory=None):
    with (session_factory or SessionLocal)() as db:
        row = db.scalar(select(Run).where(Run.run_id == run_id).with_for_update())
        if not row:
            return None
        node = db.get(JudgeNodeRow, row.assigned_node_id) if row.assigned_node_id else None
        if not valid_node_credential(node, node_secret):
            raise ValueError("node secret invalid")
        if not row.lease_token or not hmac.compare_digest(row.lease_token, lease_token):
            raise ValueError("lease mismatch")
        if result is not None and row.status not in ACTIVE:
            if row.result == result:
                return {"accepted": True, "editor_run": public_run(row)}
            raise ValueError("lease mismatch")
        if row.status not in RUNNING or not row.leased_at or aware(row.leased_at) <= now_utc() - timedelta(seconds=settings.judge_lease_timeout_seconds):
            raise ValueError("lease mismatch")
        row.leased_at = now_utc()
        if result is not None:
            if result["final_status"] not in {s.value for s in JUDGE_FINAL_STATUSES}:
                raise ValueError("invalid final status")
            failed = result.get("failed_testcase_order")
            if failed is not None and failed not in {c["display_order"] for c in row.payload["testcases"]}:
                raise ValueError("lease mismatch")
            row.status = result["final_status"]
            row.result = result
            row.completed_at = now_utc()
            if row.status == "accepted":
                row.progress_current = row.testcase_count
        elif status:
            if status not in RUNNING or total not in {None, row.testcase_count}:
                raise ValueError("lease mismatch")
            row.status = status
            row.progress_current = max(row.progress_current, min(max(current or 0, 0), row.testcase_count))
        db.commit()
        if result is not None and row.payload["bundle_key"] != row.payload["base_bundle_key"]:
            # Only a derived subset is disposable here. The immutable dataset
            # remains available for later subsets and other solution runs.
            try:
                for key in row.storage_keys:
                    object_storage.delete(key)
                row.storage_keys = []
                db.commit()
            except Exception:
                db.rollback()  # The final result above is already committed.
        return {"accepted": True, "editor_run": public_run(row)}
