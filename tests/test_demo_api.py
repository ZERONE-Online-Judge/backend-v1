import os
import base64
import hashlib
import json
import zipfile
import pytest
from datetime import datetime, timedelta, timezone
from fastapi.testclient import TestClient
from io import BytesIO
from uuid import uuid4

os.environ.setdefault("ENABLE_DEMO_SEED", "true")
os.environ.setdefault("ALLOW_EMPTY_OTP", "true")

from app.main import app
from app.models import ContestStatus, JudgeJobStatus, SubmissionStatus
from app.orm_models import JudgeJobRow, SubmissionRow
from app.services.store import store
from app.services.storage import object_storage


client = TestClient(app)


def first_contest_id() -> str:
    public_ids = [contest["contest_id"] for contest in client.get("/api/public/contests").json()["data"]]
    return next(
        contest_id
        for contest_id in public_ids
        if any(team.contest_id == contest_id for team in store.teams.values())
        and any(problem.contest_id == contest_id for problem in store.problems.values())
    )


def set_contest_running(contest_id: str) -> None:
    now = datetime.now(timezone.utc)
    store.update_contest_settings(
        contest_id,
        status=ContestStatus.RUNNING,
        start_at=now - timedelta(hours=1),
        freeze_at=now + timedelta(hours=2),
        end_at=now + timedelta(hours=3),
    )


def set_contest_mutable(contest_id: str) -> None:
    now = datetime.now(timezone.utc)
    store.update_contest_settings(
        contest_id,
        status=ContestStatus.OPEN,
        start_at=now + timedelta(days=1),
        freeze_at=now + timedelta(days=1, hours=3),
        end_at=now + timedelta(days=1, hours=4),
    )


@pytest.fixture(autouse=True)
def reset_demo_contest_window():
    set_contest_running(first_contest_id())
    yield


def staff_tokens(email: str = "test3@zoj.com") -> dict:
    response = client.post("/api/auth/general/otp/verify", json={"email": email, "otp_code": ""})
    assert response.status_code == 200
    operator_session = response.json()["data"].get("operator_session")
    assert operator_session
    return operator_session


def auth_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def claim_jobs_until(node_id: str, node_secret: str, submission_ids: list[str]) -> dict[str, dict]:
    remaining = set(submission_ids)
    found: dict[str, dict] = {}
    for _ in range(20):
        claim = client.post(
            f"/api/internal/judge/nodes/{node_id}/assignments:claim",
            json={"node_secret": node_secret, "max_count": 100},
        )
        assert claim.status_code == 200
        jobs = claim.json()["data"]["jobs"]
        for job in jobs:
            submission_id = job["submission"]["submission_id"]
            if submission_id in remaining:
                found[submission_id] = job
                remaining.remove(submission_id)
            else:
                client.post(
                    f"/api/internal/judge/jobs/{job['judge_job_id']}/result",
                    json={
                        "node_secret": node_secret,
                        "lease_token": job["lease_token"],
                        "final_status": "system_error",
                        "awarded_score": 0,
                    },
                )
        if not remaining:
            return found
        if not jobs:
            break
    missing = ", ".join(sorted(remaining))
    raise AssertionError(f"target judge jobs were not claimed: {missing}")


def jwt_payload(token: str) -> dict:
    parts = token.split(".")
    assert len(parts) == 3
    padded = parts[1] + "=" * (-len(parts[1]) % 4)
    return json.loads(base64.urlsafe_b64decode(padded.encode("ascii")))


def participant_login(email: str = "test2@zoj.com") -> tuple[str, dict]:
    contest_id = first_contest_id()
    response = client.post(
        f"/api/contests/{contest_id}/participant-login/otp/verify",
        json={"email": email, "otp_code": ""},
    )
    assert response.status_code == 200
    return contest_id, response.json()["data"]


def operator_problem(contest_id: str, division_id: str) -> dict:
    operator = staff_tokens("test4@zoj.com")
    response = client.get(f"/api/operator/contests/{contest_id}/problems", headers=auth_headers(operator["access_token"]))
    assert response.status_code == 200
    return next(item for item in response.json()["data"] if item["division_id"] == division_id)


def test_public_home():
    response = client.get("/api/public/home")
    assert response.status_code == 200
    assert response.json()["data"]["active_contest_count"] >= 1


def test_service_master_general_otp_login_success():
    response = client.post("/api/auth/general/otp/verify", json={"email": "test3@zoj.com", "otp_code": ""})
    assert response.status_code == 200
    session = response.json()["data"]
    assert session["access_token"]
    assert session["refresh_token"]
    assert jwt_payload(session["access_token"])["typ"] == "general_access"
    assert jwt_payload(session["refresh_token"])["typ"] == "general_refresh"
    assert session["operator_session"]["default_redirect"] == "/admin"
    assert session["operator_session"]["access_token"]
    assert session["operator_session"]["refresh_token"]
    assert session["operator_session"]["access_token"] == session["access_token"]
    assert session["operator_session"]["refresh_token"] == session["refresh_token"]
    assert jwt_payload(session["operator_session"]["access_token"])["typ"] == "general_access"
    assert jwt_payload(session["operator_session"]["refresh_token"])["typ"] == "general_refresh"


def test_legacy_staff_login_is_removed():
    response = client.post("/api/auth/staff/login", json={"email": "test3@zoj.com", "password": "bad"})
    assert response.status_code == 410
    assert response.json()["error"]["code"] == "staff_login_removed"


def test_operator_general_otp_login_success():
    request = client.post("/api/auth/general/otp/request", json={"email": "test4@zoj.com"})
    assert request.status_code == 200

    code = store.otp_codes["test4@zoj.com"]
    verify = client.post("/api/auth/general/otp/verify", json={"email": "test4@zoj.com", "otp_code": code})
    assert verify.status_code == 200
    assert verify.json()["data"]["operator_session"]["default_redirect"] == "/operator"
    assert verify.json()["data"]["operator_session"]["access_token"] == verify.json()["data"]["access_token"]
    contests = client.get("/api/operator/contests", headers=auth_headers(verify.json()["data"]["access_token"]))
    assert contests.status_code == 200


def test_general_otp_request_is_rate_limited_for_staff_account():
    first = client.post("/api/auth/general/otp/request", json={"email": "test3@zoj.com"})
    assert first.status_code == 200
    assert first.json()["data"]["cooldown_seconds"] == 10

    second = client.post("/api/auth/general/otp/request", json={"email": "test3@zoj.com"})
    assert second.status_code == 429
    assert second.json()["error"]["code"] == "otp_request_rate_limited"
    assert second.json()["error"]["details"]["retry_after_seconds"] >= 1


def test_contest_notice_and_board_flow():
    contest_id, participant = participant_login()
    operator = staff_tokens("test4@zoj.com")

    notice = client.post(
        f"/api/operator/contests/{contest_id}/notices",
        headers=auth_headers(operator["access_token"]),
        json={
            "title": "공지",
            "body": "본문 $O(N)$",
            "pinned": True,
            "emergency": True,
            "visibility": "participants",
        },
    )
    assert notice.status_code == 200
    assert notice.json()["data"]["pinned"] is True

    public_notices = client.get(f"/api/contests/{contest_id}/notices")
    assert public_notices.status_code == 200
    assert all(item["visibility"] == "public" for item in public_notices.json()["data"])

    participant_notices = client.get(
        f"/api/contests/{contest_id}/notices",
        headers=auth_headers(participant["access_token"]),
    )
    assert participant_notices.status_code == 200
    assert any(item["title"] == "공지" for item in participant_notices.json()["data"])

    question = client.post(
        f"/api/contests/{contest_id}/boards",
        headers=auth_headers(participant["access_token"]),
        json={"title": "비공개 질문", "body": "질문 내용", "visibility": "private"},
    )
    assert question.status_code == 200
    question_id = question.json()["data"]["contest_question_id"]

    public_board = client.get(f"/api/contests/{contest_id}/boards")
    assert public_board.status_code == 200
    assert all(item["visibility"] == "public" for item in public_board.json()["data"])

    operator_board = client.get(
        f"/api/operator/contests/{contest_id}/boards",
        headers=auth_headers(operator["access_token"]),
    )
    assert operator_board.status_code == 200
    assert any(item["contest_question_id"] == question_id for item in operator_board.json()["data"])

    answer = client.post(
        f"/api/operator/contests/{contest_id}/boards/{question_id}/answers",
        headers=auth_headers(operator["access_token"]),
        json={"body": "질문자 전용 답변", "visibility": "questioner"},
    )
    assert answer.status_code == 200

    participant_board = client.get(
        f"/api/contests/{contest_id}/boards",
        headers=auth_headers(participant["access_token"]),
    )
    assert participant_board.status_code == 200
    own_question = next(item for item in participant_board.json()["data"] if item["contest_question_id"] == question_id)
    assert own_question["answers"][0]["visibility"] == "questioner"


def test_admin_service_notice_management():
    admin = staff_tokens("test3@zoj.com")
    created = client.post(
        "/api/admin/service-notices",
        headers=auth_headers(admin["access_token"]),
        json={
            "title": "서비스 공지",
            "summary": "요약",
            "body": "서비스 공지 본문",
            "emergency": True,
        },
    )
    assert created.status_code == 200
    notice_id = created.json()["data"]["service_notice_id"]

    updated = client.patch(
        f"/api/admin/service-notices/{notice_id}",
        headers=auth_headers(admin["access_token"]),
        json={"summary": "수정 요약", "emergency": False},
    )
    assert updated.status_code == 200
    assert updated.json()["data"]["summary"] == "수정 요약"
    assert updated.json()["data"]["emergency"] is False

    public = client.get("/api/public/service-notices")
    assert public.status_code == 200
    assert any(item["service_notice_id"] == notice_id for item in public.json()["data"])


def test_general_operator_session_me_refresh_and_logout():
    tokens = staff_tokens()

    me = client.get("/api/auth/staff/me", headers=auth_headers(tokens["access_token"]))
    assert me.status_code == 200
    assert me.json()["data"]["email"] == "test3@zoj.com"

    refreshed = client.post("/api/auth/general/refresh", json={"refresh_token": tokens["refresh_token"]})
    assert refreshed.status_code == 200
    assert refreshed.json()["data"]["access_token"]
    assert refreshed.json()["data"]["refresh_token"] == tokens["refresh_token"]
    assert refreshed.json()["data"]["account"]["email"] == "test3@zoj.com"
    assert refreshed.json()["data"]["operator_session"]["default_redirect"] == "/admin"

    logout = client.post(
        "/api/auth/general/logout",
        headers=auth_headers(refreshed.json()["data"]["access_token"]),
        json={"refresh_token": tokens["refresh_token"]},
    )
    assert logout.status_code == 200
    assert logout.json()["data"]["revoked"] is True


def test_staff_me_requires_access_token():
    response = client.get("/api/auth/staff/me")
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "authentication_required"


def test_admin_requires_service_master_token():
    response = client.get("/api/admin/dashboard")
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "authentication_required"

    operator = staff_tokens("test4@zoj.com")
    denied = client.get("/api/admin/dashboard", headers=auth_headers(operator["access_token"]))
    assert denied.status_code == 403
    assert denied.json()["error"]["code"] == "permission_denied"

    master = staff_tokens()
    allowed = client.get("/api/admin/dashboard", headers=auth_headers(master["access_token"]))
    assert allowed.status_code == 200


def test_admin_can_bootstrap_contest_divisions_and_operator():
    master = staff_tokens()
    created = client.post(
        "/api/admin/contests",
        headers=auth_headers(master["access_token"]),
        json={
            "title": "Production Kickoff",
            "organization_name": "Zerone",
            "overview": "Initial production contest",
            "status": "open",
        },
    )
    assert created.status_code == 200
    contest_id = created.json()["data"]["contest_id"]

    division = client.post(
        f"/api/admin/contests/{contest_id}/divisions",
        headers=auth_headers(master["access_token"]),
        json={"code": "general", "name": "General", "description": "single division", "display_order": 1},
    )
    assert division.status_code == 200
    assert division.json()["data"]["code"] == "general"

    operator = client.post(
        f"/api/admin/contests/{contest_id}/operators",
        headers=auth_headers(master["access_token"]),
        json={"email": "operator-bootstrap@zoj.com", "display_name": "Bootstrap Operator"},
    )
    assert operator.status_code == 200
    assert operator.json()["data"]["contest_scopes"][contest_id] == ["contest.*"]


def test_operator_can_create_and_update_division():
    contest_id = first_contest_id()
    set_contest_mutable(contest_id)
    operator = staff_tokens("test4@zoj.com")
    suffix = uuid4().hex[:4]
    division_name = f"Expert {suffix}"

    created = client.post(
        f"/api/operator/contests/{contest_id}/divisions",
        headers=auth_headers(operator["access_token"]),
        json={"code": f"expert{suffix}", "name": division_name, "description": "상위 유형", "display_order": 9},
    )
    assert created.status_code == 200
    division = created.json()["data"]
    assert division["name"] == division_name

    updated = client.patch(
        f"/api/operator/contests/{contest_id}/divisions/{division['division_id']}",
        headers=auth_headers(operator["access_token"]),
        json={"name": f"{division_name} Final", "display_order": 10},
    )
    assert updated.status_code == 200
    assert updated.json()["data"]["name"] == f"{division_name} Final"
    assert updated.json()["data"]["display_order"] == 10

    duplicate = client.post(
        f"/api/operator/contests/{contest_id}/divisions",
        headers=auth_headers(operator["access_token"]),
        json={"name": f"{division_name} Final", "description": "duplicate name"},
    )
    assert duplicate.status_code == 422
    assert duplicate.json()["error"]["code"] == "validation_error"


def test_operator_mutations_are_locked_during_running_contest():
    contest_id = first_contest_id()
    operator = staff_tokens("test4@zoj.com")
    divisions = client.get(f"/api/operator/contests/{contest_id}/divisions", headers=auth_headers(operator["access_token"]))
    division_id = next(item for item in divisions.json()["data"] if item["code"] == "advanced")["division_id"]
    participants = client.get(f"/api/operator/contests/{contest_id}/participants", headers=auth_headers(operator["access_token"]))
    team = participants.json()["data"][0]

    division_create = client.post(
        f"/api/operator/contests/{contest_id}/divisions",
        headers=auth_headers(operator["access_token"]),
        json={"name": f"Locked {uuid4().hex[:4]}"},
    )
    assert division_create.status_code == 409
    assert division_create.json()["error"]["code"] == "contest_locked"

    participant_update = client.patch(
        f"/api/operator/contests/{contest_id}/participants/{team['participant_team_id']}",
        headers=auth_headers(operator["access_token"]),
        json={"status": "disqualified"},
    )
    assert participant_update.status_code == 409
    assert participant_update.json()["error"]["code"] == "contest_locked"

    problem_create = client.post(
        f"/api/operator/contests/{contest_id}/problems",
        headers=auth_headers(operator["access_token"]),
        json={
            "division_id": division_id,
            "problem_code": f"L{uuid4().hex[:6]}",
            "title": "Locked Problem",
            "statement": "should fail",
            "time_limit_ms": 1000,
            "memory_limit_mb": 512,
            "display_order": 99,
            "max_score": 100,
        },
    )
    assert problem_create.status_code == 409
    assert problem_create.json()["error"]["code"] == "contest_locked"

    settings_update = client.patch(
        f"/api/operator/contests/{contest_id}/settings",
        headers=auth_headers(operator["access_token"]),
        json={"status": "ended"},
    )
    assert settings_update.status_code == 409
    assert settings_update.json()["error"]["code"] == "contest_locked"

    emergency_update = client.patch(
        f"/api/operator/contests/{contest_id}/settings",
        headers=auth_headers(operator["access_token"]),
        json={"emergency_notice": "Queue delay notice"},
    )
    assert emergency_update.status_code == 200

    now = datetime.now(timezone.utc)
    time_update = client.patch(
        f"/api/operator/contests/{contest_id}/settings",
        headers=auth_headers(operator["access_token"]),
        json={
            "start_at": (now - timedelta(minutes=30)).isoformat(),
            "freeze_at": (now + timedelta(hours=1)).isoformat(),
            "end_at": (now + timedelta(hours=2)).isoformat(),
        },
    )
    assert time_update.status_code == 200
    assert "대회 운영 시간이 변경되었습니다" in time_update.json()["data"]["emergency_notice"]
    notices = client.get(
        f"/api/operator/contests/{contest_id}/notices",
        headers=auth_headers(operator["access_token"]),
    )
    assert notices.status_code == 200
    assert any(item["title"] == "대회 운영 시간이 변경되었습니다" and item["emergency"] for item in notices.json()["data"])


def test_admin_can_create_contest_with_operator_email_only():
    master = staff_tokens()
    created = client.post(
        "/api/admin/contests",
        headers=auth_headers(master["access_token"]),
        json={
            "organization_name": "Email Only Org",
            "status": "open",
            "operator_email": "email-only-operator@zoj.com",
        },
    )
    assert created.status_code == 200
    contest = created.json()["data"]
    assert contest["title"] == "Email Only Org Contest"

    accounts = client.get("/api/admin/service-managers", headers=auth_headers(master["access_token"]))
    operator = next(item for item in accounts.json()["data"] if item["email"] == "email-only-operator@zoj.com")
    assert operator["contest_scopes"][contest["contest_id"]] == ["contest.*"]

    mail_queue = client.get("/api/admin/mail-queue", headers=auth_headers(master["access_token"]))
    queued = [item for item in mail_queue.json()["data"] if item["recipient_email"] == "email-only-operator@zoj.com"]
    assert any(item["mail_type"] == "contest_operator_assigned" for item in queued)


def test_admin_created_contest_defaults_to_schedule_tbd_and_is_publicly_visible():
    master = staff_tokens()
    created = client.post(
        "/api/admin/contests",
        headers=auth_headers(master["access_token"]),
        json={
            "title": "Schedule TBD Contest",
            "organization_name": "Zerone",
            "operator_email": "schedule-tbd-operator@zoj.com",
        },
    )
    assert created.status_code == 200
    contest = created.json()["data"]
    assert contest["status"] == ContestStatus.SCHEDULE_TBD.value

    public_detail = client.get(f"/api/public/contests/{contest['contest_id']}")
    assert public_detail.status_code == 200
    assert public_detail.json()["data"]["contest"]["status"] == ContestStatus.SCHEDULE_TBD.value


def test_operator_contest_list_includes_scheduled_assigned_contest():
    master = staff_tokens()
    created = client.post(
        "/api/admin/contests",
        headers=auth_headers(master["access_token"]),
        json={
            "organization_name": "Hidden Operator Contest",
            "status": "scheduled",
            "operator_email": "scheduled-operator@zoj.com",
        },
    )
    assert created.status_code == 200
    contest = created.json()["data"]

    master_assigned = client.post(
        "/api/admin/contests/{contest_id}/operators".format(contest_id=contest["contest_id"]),
        headers=auth_headers(master["access_token"]),
        json={"email": "test4@zoj.com", "display_name": "Demo Operator"},
    )
    assert master_assigned.status_code == 200

    operator = staff_tokens("test4@zoj.com")
    response = client.get("/api/operator/contests", headers=auth_headers(operator["access_token"]))
    assert response.status_code == 200
    assert any(item["contest_id"] == contest["contest_id"] for item in response.json()["data"])


def test_operator_setting_change_enqueues_notification():
    contest_id = client.get("/api/public/contests").json()["data"][0]["contest_id"]
    operator = staff_tokens("test4@zoj.com")

    updated = client.patch(
        f"/api/operator/contests/{contest_id}/settings",
        headers=auth_headers(operator["access_token"]),
        json={"emergency_notice": "Delayed queue update"},
    )
    assert updated.status_code == 200

    master = staff_tokens()
    mail_queue = client.get("/api/admin/mail-queue", headers=auth_headers(master["access_token"]))
    queued = [item for item in mail_queue.json()["data"] if item["mail_type"] == "contest_settings_updated"]
    assert queued
    assert any("Delayed queue update" in item["body_text"] for item in queued)


def test_operator_can_update_and_remove_contest_operator():
    contest_id = first_contest_id()
    operator = staff_tokens("test4@zoj.com")
    email = f"operator-{uuid4().hex[:8]}@zoj.com"
    created = client.post(
        f"/api/operator/contests/{contest_id}/operators",
        headers=auth_headers(operator["access_token"]),
        json={"email": email, "display_name": "Temp Operator"},
    )
    assert created.status_code == 200

    updated = client.patch(
        f"/api/operator/contests/{contest_id}/operators/{email}",
        headers=auth_headers(operator["access_token"]),
        json={"display_name": "Edited Operator"},
    )
    assert updated.status_code == 200
    assert updated.json()["data"]["display_name"] == "Edited Operator"

    removed = client.delete(
        f"/api/operator/contests/{contest_id}/operators/{email}",
        headers=auth_headers(operator["access_token"]),
    )
    assert removed.status_code == 200

    listed = client.get(f"/api/operator/contests/{contest_id}/operators", headers=auth_headers(operator["access_token"]))
    assert listed.status_code == 200
    assert all(item["email"] != email for item in listed.json()["data"])


def test_operator_requires_contest_scope():
    contest_id = client.get("/api/public/contests").json()["data"][0]["contest_id"]
    response = client.get(f"/api/operator/contests/{contest_id}/dashboard")
    assert response.status_code == 401

    operator = staff_tokens("test4@zoj.com")
    allowed = client.get(f"/api/operator/contests/{contest_id}/dashboard", headers=auth_headers(operator["access_token"]))
    assert allowed.status_code == 200

    master = staff_tokens()
    created = client.post(
        "/api/admin/contests",
        headers=auth_headers(master["access_token"]),
        json={"title": "Other Contest", "organization_name": "Zerone", "overview": "scope check"},
    )
    other_contest_id = created.json()["data"]["contest_id"]
    denied = client.get(f"/api/operator/contests/{other_contest_id}/dashboard", headers=auth_headers(operator["access_token"]))
    assert denied.status_code == 403
    assert denied.json()["error"]["code"] == "scope_denied"


def test_operator_problem_asset_and_testcase_metadata_flow():
    contest_id = first_contest_id()
    set_contest_mutable(contest_id)
    operator = staff_tokens("test4@zoj.com")
    divisions = client.get(f"/api/operator/contests/{contest_id}/divisions", headers=auth_headers(operator["access_token"]))
    division_id = next(item for item in divisions.json()["data"] if item["code"] == "advanced")["division_id"]
    target_division_id = next(item for item in divisions.json()["data"] if item["code"] == "beginner")["division_id"]

    problem_code = f"P{uuid4().hex[:6]}"
    created = client.post(
        f"/api/operator/contests/{contest_id}/problems",
        headers=auth_headers(operator["access_token"]),
        json={
            "division_id": division_id,
            "problem_code": problem_code,
            "title": "MinIO Metadata",
            "statement": "Store testcase object metadata.",
            "time_limit_ms": 2000,
            "memory_limit_mb": 512,
            "display_order": 4,
            "max_score": 100,
        },
    )
    assert created.status_code == 200
    problem = created.json()["data"]

    patched = client.patch(
        f"/api/operator/contests/{contest_id}/problems/{problem['problem_id']}",
        headers=auth_headers(operator["access_token"]),
        json={"title": "MinIO Metadata Updated"},
    )
    assert patched.status_code == 200
    assert patched.json()["data"]["title"] == "MinIO Metadata Updated"

    moved = client.patch(
        f"/api/operator/contests/{contest_id}/problems/{problem['problem_id']}",
        headers=auth_headers(operator["access_token"]),
        json={"division_id": target_division_id},
    )
    assert moved.status_code == 200
    assert moved.json()["data"]["division_id"] == target_division_id

    asset = client.post(
        f"/api/operator/contests/{contest_id}/problems/{problem['problem_id']}/assets",
        headers=auth_headers(operator["access_token"]),
        json={
            "original_filename": "statement.pdf",
            "storage_key": f"contests/{contest_id}/problems/{problem['problem_id']}/statement.pdf",
            "mime_type": "application/pdf",
            "file_size": 1024,
            "sha256": "a" * 64,
        },
    )
    assert asset.status_code == 200
    statement_asset_id = asset.json()["data"]["asset_id"]

    package_asset = client.post(
        f"/api/operator/contests/{contest_id}/problems/{problem['problem_id']}/assets",
        headers=auth_headers(operator["access_token"]),
        json={
            "original_filename": "checker.cpp",
            "storage_key": f"contests/{contest_id}/problems/{problem['problem_id']}/package-files/checker/checker.cpp",
            "mime_type": "text/plain",
            "file_size": 256,
            "sha256": "d" * 64,
        },
    )
    assert package_asset.status_code == 200
    package_asset_id = package_asset.json()["data"]["asset_id"]

    testcase_set = client.post(
        f"/api/operator/contests/{contest_id}/problems/{problem['problem_id']}/testcase-sets",
        headers=auth_headers(operator["access_token"]),
        json={"is_active": True},
    )
    assert testcase_set.status_code == 200
    testcase_set_id = testcase_set.json()["data"]["testcase_set_id"]

    testcase = client.post(
        f"/api/operator/contests/{contest_id}/problems/{problem['problem_id']}/testcase-sets/{testcase_set_id}/testcases",
        headers=auth_headers(operator["access_token"]),
        json={
            "display_order": 1,
            "input_storage_key": "testcases/input/1.txt",
            "output_storage_key": "testcases/output/1.txt",
            "input_sha256": "b" * 64,
            "output_sha256": "c" * 64,
        },
    )
    assert testcase.status_code == 200

    listed = client.get(
        f"/api/operator/contests/{contest_id}/problems/{problem['problem_id']}/testcase-sets",
        headers=auth_headers(operator["access_token"]),
    )
    assert listed.status_code == 200
    assert listed.json()["data"][0]["testcases"][0]["display_order"] == 1

    second_set = client.post(
        f"/api/operator/contests/{contest_id}/problems/{problem['problem_id']}/testcase-sets",
        headers=auth_headers(operator["access_token"]),
        json={"is_active": False},
    )
    assert second_set.status_code == 200

    activated = client.patch(
        f"/api/operator/contests/{contest_id}/problems/{problem['problem_id']}/testcase-sets/{second_set.json()['data']['testcase_set_id']}",
        headers=auth_headers(operator["access_token"]),
        json={"is_active": True},
    )
    assert activated.status_code == 200
    assert activated.json()["data"]["is_active"] is True

    set_contest_running(contest_id)
    participant_assets = client.get(
        f"/api/contests/{contest_id}/problems/{problem['problem_id']}/assets",
        headers=auth_headers(participant_login("test1@zoj.com")[1]["access_token"]),
    )
    assert participant_assets.status_code == 200
    visible_assets = participant_assets.json()["data"]
    assert [item["asset_id"] for item in visible_assets] == [statement_asset_id]
    assert visible_assets[0]["download_url"]
    assert all("/package-files/" not in item["storage_key"] for item in visible_assets)

    set_contest_mutable(contest_id)
    deleted = client.delete(
        f"/api/operator/contests/{contest_id}/problems/{problem['problem_id']}/assets/{package_asset_id}",
        headers=auth_headers(operator["access_token"]),
    )
    assert deleted.status_code == 200


def test_operator_presign_upload_returns_storage_key_and_url():
    contest_id = first_contest_id()
    set_contest_mutable(contest_id)
    operator = staff_tokens("test4@zoj.com")

    response = client.post(
        f"/api/operator/contests/{contest_id}/storage/presign-upload",
        headers=auth_headers(operator["access_token"]),
        json={"category": "testcases/input", "filename": "sample.txt", "content_type": "text/plain"},
    )

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["method"] == "PUT"
    assert data["storage_key"].endswith("/testcases/input/sample.txt")
    assert data["upload_url"].startswith("file://")


def test_minio_browser_urls_use_api_proxy():
    original_backend = object_storage.backend
    object_storage.backend = "minio"
    try:
        assert object_storage.presigned_get_url("contests/demo/problem-assets/a.png") == "/api/storage/objects/contests/demo/problem-assets/a.png"
        assert object_storage.presigned_put_url("contests/demo/testcases/input.txt") == "/api/storage/objects/contests/demo/testcases/input.txt"
    finally:
        object_storage.backend = original_backend


def test_storage_proxy_put_and_get_local_object():
    key = f"tests/proxy/{uuid4().hex}.txt"
    put = client.put(f"/api/storage/objects/{key}", content=b"hello", headers={"content-type": "text/plain"})
    assert put.status_code == 200
    get = client.get(f"/api/storage/objects/{key}")
    assert get.status_code == 200
    assert get.text == "hello"


def test_operator_builds_package_from_recipe():
    contest_id = first_contest_id()
    set_contest_mutable(contest_id)
    operator = staff_tokens("test4@zoj.com")
    divisions = client.get(f"/api/operator/contests/{contest_id}/divisions", headers=auth_headers(operator["access_token"]))
    division_id = next(item for item in divisions.json()["data"] if item["code"] == "advanced")["division_id"]
    suffix = uuid4().hex[:8]
    created = client.post(
        f"/api/operator/contests/{contest_id}/problems",
        headers=auth_headers(operator["access_token"]),
        json={
            "division_id": division_id,
            "problem_code": f"B{suffix[:4]}",
            "title": "Package Build",
            "statement": "Build generated tests.",
            "time_limit_ms": 1000,
            "memory_limit_mb": 512,
            "display_order": 50,
            "max_score": 100,
        },
    )
    assert created.status_code == 200
    problem = created.json()["data"]

    def add_package_asset(role: str, filename: str, source: str):
        storage_key = f"contests/{contest_id}/problems/{problem['problem_id']}/package-files/{role}/{suffix}-{filename}"
        object_storage.write_text(storage_key, source)
        response = client.post(
            f"/api/operator/contests/{contest_id}/problems/{problem['problem_id']}/assets",
            headers=auth_headers(operator["access_token"]),
            json={
                "original_filename": filename,
                "storage_key": storage_key,
                "mime_type": "text/plain",
                "file_size": len(source.encode("utf-8")),
                "sha256": hashlib.sha256(source.encode("utf-8")).hexdigest(),
            },
        )
        assert response.status_code == 200

    add_package_asset("package-resource", "helper.py", "def emit(value):\n    print(value)\n")
    add_package_asset("generator", "gen_echo.py", "import sys\nfrom helper import emit\nemit(sys.argv[1])\n")
    add_package_asset("validator", "validator.py", "import sys\nvalue=sys.stdin.read().strip()\nassert value.isdigit() and int(value) > 0\n")
    add_package_asset("main-solution", "main.py", "import sys\nprint(int(sys.stdin.read().strip()) * 2)\n")
    response = client.post(
        f"/api/operator/contests/{contest_id}/problems/{problem['problem_id']}/package-builds",
        headers=auth_headers(operator["access_token"]),
        json={"script_text": "gen_echo 7\ngen_echo 11\n"},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["generated_count"] == 2
    assert data["testcase_set"]["is_active"] is True

    listed = client.get(
        f"/api/operator/contests/{contest_id}/problems/{problem['problem_id']}/testcase-sets",
        headers=auth_headers(operator["access_token"]),
    )
    assert listed.status_code == 200
    cases = listed.json()["data"][-1]["testcases"]
    assert len(cases) == 2
    assert object_storage.read_text(cases[0]["input_storage_key"]).strip() == "7"
    assert object_storage.read_text(cases[0]["output_storage_key"]).strip() == "14"


def test_operator_creates_verified_testcase_set_from_in_out_files():
    contest_id = first_contest_id()
    set_contest_mutable(contest_id)
    operator = staff_tokens("test4@zoj.com")
    divisions = client.get(f"/api/operator/contests/{contest_id}/divisions", headers=auth_headers(operator["access_token"]))
    division_id = next(item for item in divisions.json()["data"] if item["code"] == "advanced")["division_id"]
    suffix = uuid4().hex[:8]
    created = client.post(
        f"/api/operator/contests/{contest_id}/problems",
        headers=auth_headers(operator["access_token"]),
        json={
            "division_id": division_id,
            "problem_code": f"V{suffix[:4]}",
            "title": "Verified Testcases",
            "statement": "Validate uploaded in/out files.",
            "time_limit_ms": 1000,
            "memory_limit_mb": 512,
            "display_order": 60,
            "max_score": 100,
        },
    )
    assert created.status_code == 200
    problem = created.json()["data"]

    def add_asset(role: str, filename: str, source: str):
        storage_key = f"contests/{contest_id}/problems/{problem['problem_id']}/package-files/{role}/{suffix}-{filename}"
        object_storage.write_text(storage_key, source)
        response = client.post(
            f"/api/operator/contests/{contest_id}/problems/{problem['problem_id']}/assets",
            headers=auth_headers(operator["access_token"]),
            json={
                "original_filename": filename,
                "storage_key": storage_key,
                "mime_type": "text/plain",
                "file_size": len(source.encode("utf-8")),
                "sha256": hashlib.sha256(source.encode("utf-8")).hexdigest(),
            },
        )
        assert response.status_code == 200

    add_asset("package-resource", "testlib.h", "// testlib placeholder\n")
    add_asset("validator", "validator.py", "import sys\nvalue=sys.stdin.read().strip()\nassert value.isdigit() and int(value) > 0\n")
    add_asset(
        "checker",
        "checker.py",
        "import sys\ninp=int(open(sys.argv[1]).read().strip())\nans=open(sys.argv[2]).read().strip()\nout=open(sys.argv[3]).read().strip()\nassert ans == out\nassert int(ans) == inp * 2\n",
    )
    input_key = f"contests/{contest_id}/problems/{problem['problem_id']}/testcases/{suffix}-001.in"
    output_key = f"contests/{contest_id}/problems/{problem['problem_id']}/testcases/{suffix}-001.out"
    object_storage.write_text(input_key, "7\n")
    object_storage.write_text(output_key, "14\n")

    response = client.post(
        f"/api/operator/contests/{contest_id}/problems/{problem['problem_id']}/verified-testcase-sets",
        headers=auth_headers(operator["access_token"]),
        json={
            "cases": [
                {
                    "display_order": 1,
                    "input_storage_key": input_key,
                    "output_storage_key": output_key,
                    "input_sha256": hashlib.sha256(b"7\n").hexdigest(),
                    "output_sha256": hashlib.sha256(b"14\n").hexdigest(),
                }
            ]
        },
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["verified_count"] == 1
    assert data["testcase_set"]["is_active"] is True
    assert data["testcases"][0]["input_storage_key"] == input_key

    bad_checker = "import sys\nassert False, 'bad checker must not be accepted'\n"
    bad_checker_key = f"contests/{contest_id}/problems/{problem['problem_id']}/package-files/checker/{suffix}-bad-checker.py"
    object_storage.write_text(bad_checker_key, bad_checker)
    rejected = client.post(
        f"/api/operator/contests/{contest_id}/problems/{problem['problem_id']}/assets",
        headers=auth_headers(operator["access_token"]),
        json={
            "original_filename": "checker.py",
            "storage_key": bad_checker_key,
            "mime_type": "text/plain",
            "file_size": len(bad_checker.encode("utf-8")),
            "sha256": hashlib.sha256(bad_checker.encode("utf-8")).hexdigest(),
        },
    )
    assert rejected.status_code == 422
    assert rejected.json()["error"]["code"] == "package_asset_verification_failed"


def test_operator_can_delete_testcase_and_testcase_set():
    contest_id = first_contest_id()
    set_contest_mutable(contest_id)
    operator = staff_tokens("test4@zoj.com")
    divisions = client.get(f"/api/operator/contests/{contest_id}/divisions", headers=auth_headers(operator["access_token"]))
    division_id = next(item for item in divisions.json()["data"] if item["code"] == "advanced")["division_id"]
    suffix = uuid4().hex[:8]
    created = client.post(
        f"/api/operator/contests/{contest_id}/problems",
        headers=auth_headers(operator["access_token"]),
        json={
            "division_id": division_id,
            "problem_code": f"D{suffix[:4]}",
            "title": "Delete Testcases",
            "statement": "Delete testcase row and set.",
            "time_limit_ms": 1000,
            "memory_limit_mb": 512,
            "display_order": 62,
            "max_score": 100,
        },
    )
    assert created.status_code == 200
    problem = created.json()["data"]

    set_created = client.post(
        f"/api/operator/contests/{contest_id}/problems/{problem['problem_id']}/testcase-sets",
        headers=auth_headers(operator["access_token"]),
        json={"is_active": True},
    )
    assert set_created.status_code == 200
    testcase_set_id = set_created.json()["data"]["testcase_set_id"]

    input_key = f"contests/{contest_id}/problems/{problem['problem_id']}/testcases/{suffix}-delete-001.in"
    output_key = f"contests/{contest_id}/problems/{problem['problem_id']}/testcases/{suffix}-delete-001.out"
    object_storage.write_text(input_key, "1\n")
    object_storage.write_text(output_key, "2\n")
    testcase_created = client.post(
        f"/api/operator/contests/{contest_id}/problems/{problem['problem_id']}/testcase-sets/{testcase_set_id}/testcases",
        headers=auth_headers(operator["access_token"]),
        json={
            "display_order": 1,
            "input_storage_key": input_key,
            "output_storage_key": output_key,
            "input_sha256": hashlib.sha256(b"1\n").hexdigest(),
            "output_sha256": hashlib.sha256(b"2\n").hexdigest(),
        },
    )
    assert testcase_created.status_code == 200
    testcase_id = testcase_created.json()["data"]["testcase_id"]

    testcase_deleted = client.delete(
        f"/api/operator/contests/{contest_id}/problems/{problem['problem_id']}/testcase-sets/{testcase_set_id}/testcases/{testcase_id}",
        headers=auth_headers(operator["access_token"]),
    )
    assert testcase_deleted.status_code == 200

    set_deleted = client.delete(
        f"/api/operator/contests/{contest_id}/problems/{problem['problem_id']}/testcase-sets/{testcase_set_id}",
        headers=auth_headers(operator["access_token"]),
    )
    assert set_deleted.status_code == 200

    listed = client.get(
        f"/api/operator/contests/{contest_id}/problems/{problem['problem_id']}/testcase-sets",
        headers=auth_headers(operator["access_token"]),
    )
    assert listed.status_code == 200
    assert all(item["testcase_set_id"] != testcase_set_id for item in listed.json()["data"])


def test_operator_package_status_and_zip_testcase_import():
    contest_id = first_contest_id()
    set_contest_mutable(contest_id)
    operator = staff_tokens("test4@zoj.com")
    divisions = client.get(f"/api/operator/contests/{contest_id}/divisions", headers=auth_headers(operator["access_token"]))
    division_id = next(item for item in divisions.json()["data"] if item["code"] == "advanced")["division_id"]
    suffix = uuid4().hex[:8]
    created = client.post(
        f"/api/operator/contests/{contest_id}/problems",
        headers=auth_headers(operator["access_token"]),
        json={
            "division_id": division_id,
            "problem_code": f"Z{suffix[:4]}",
            "title": "Zip Testcase Import",
            "statement": "Import paired zip testcases.",
            "time_limit_ms": 1000,
            "memory_limit_mb": 512,
            "display_order": 61,
            "max_score": 100,
        },
    )
    assert created.status_code == 200
    problem = created.json()["data"]

    missing = client.get(
        f"/api/operator/contests/{contest_id}/problems/{problem['problem_id']}/package-status",
        headers=auth_headers(operator["access_token"]),
    )
    assert missing.status_code == 200
    assert missing.json()["data"]["ready"] is False
    assert any("validator.cpp" in item for item in missing.json()["data"]["warnings"])

    def add_asset(role: str, filename: str, source: str):
        storage_key = f"contests/{contest_id}/problems/{problem['problem_id']}/package-files/{role}/{suffix}-{filename}"
        object_storage.write_text(storage_key, source)
        response = client.post(
            f"/api/operator/contests/{contest_id}/problems/{problem['problem_id']}/assets",
            headers=auth_headers(operator["access_token"]),
            json={
                "original_filename": filename,
                "storage_key": storage_key,
                "mime_type": "text/plain",
                "file_size": len(source.encode("utf-8")),
                "sha256": hashlib.sha256(source.encode("utf-8")).hexdigest(),
            },
        )
        assert response.status_code == 200

    add_asset("package-resource", "testlib.h", "// testlib placeholder\n")
    add_asset("validator", "validator.py", "import sys\nvalue=sys.stdin.read().strip()\nassert value.isdigit() and int(value) > 0\n")
    add_asset(
        "checker",
        "checker.py",
        "import sys\ninp=int(open(sys.argv[1]).read().strip())\nans=open(sys.argv[2]).read().strip()\nout=open(sys.argv[3]).read().strip()\nassert ans == out\nassert int(ans) == inp * 2\n",
    )

    archive = BytesIO()
    with zipfile.ZipFile(archive, "w") as zipped:
        zipped.writestr("tests/001.in", "7\n")
        zipped.writestr("tests/001.out", "14\n")
        zipped.writestr("tests/sample.in", "8\n")
        zipped.writestr("tests/sample.out", "16\n")

    imported = client.post(
        f"/api/operator/contests/{contest_id}/problems/{problem['problem_id']}/verified-testcase-sets:zip",
        headers=auth_headers(operator["access_token"]),
        files={"file": ("cases.zip", archive.getvalue(), "application/zip")},
    )
    assert imported.status_code == 200
    data = imported.json()["data"]
    assert data["verified_count"] == 2
    assert data["imported_archive"]["case_count"] == 2

    ready = client.get(
        f"/api/operator/contests/{contest_id}/problems/{problem['problem_id']}/package-status",
        headers=auth_headers(operator["access_token"]),
    )
    assert ready.status_code == 200
    assert ready.json()["data"]["ready"] is True
    assert ready.json()["data"]["active_testcase_count"] == 2


def test_participant_login_returns_registered_division():
    contest_id = client.get("/api/public/contests").json()["data"][0]["contest_id"]
    otp = client.post(
        f"/api/contests/{contest_id}/participant-login/otp/request",
        json={"email": "test2@zoj.com"},
    )
    assert otp.status_code == 200

    response = client.post(
        f"/api/contests/{contest_id}/participant-login/otp/verify",
        json={"email": "test2@zoj.com", "otp_code": ""},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["division"]["code"] == "advanced"
    assert data["team"]["division_id"] == data["division"]["division_id"]


def test_participant_otp_request_is_rate_limited():
    contest_id = client.get("/api/public/contests").json()["data"][0]["contest_id"]
    first = client.post(f"/api/contests/{contest_id}/participant-login/otp/request", json={"email": "test1@zoj.com"})
    assert first.status_code == 200
    assert first.json()["data"]["cooldown_seconds"] == 10

    second = client.post(f"/api/contests/{contest_id}/participant-login/otp/request", json={"email": "test1@zoj.com"})
    assert second.status_code == 429
    assert second.json()["error"]["code"] == "otp_request_rate_limited"
    assert second.json()["error"]["details"]["retry_after_seconds"] >= 1


def test_participant_session_me_uses_access_token():
    contest_id, login = participant_login()
    token = login["access_token"]

    me = client.get(f"/api/contests/{contest_id}/participant-session/me", headers=auth_headers(token))
    assert me.status_code == 200
    assert me.json()["data"]["division"]["code"] == "advanced"

    denied = client.get(f"/api/contests/{contest_id}/participant-session/me")
    assert denied.status_code == 401


def test_contest_resources_hidden_without_participant_during_contest():
    contest_id = client.get("/api/public/contests").json()["data"][0]["contest_id"]

    problems = client.get(f"/api/contests/{contest_id}/problems")
    assert problems.status_code == 404
    assert problems.json()["error"]["code"] == "not_found"

    scoreboard = client.get(f"/api/contests/{contest_id}/scoreboard")
    assert scoreboard.status_code == 404
    assert scoreboard.json()["error"]["code"] == "not_found"

    submissions = client.get(f"/api/contests/{contest_id}/submissions")
    assert submissions.status_code == 404
    assert submissions.json()["error"]["code"] == "not_found"


def test_participant_cannot_view_problems_before_contest_start():
    contest_id, login = participant_login()
    set_contest_mutable(contest_id)

    problems = client.get(f"/api/contests/{contest_id}/problems", headers=auth_headers(login["access_token"]))
    assert problems.status_code == 404
    assert problems.json()["error"]["code"] == "not_found"

def test_operator_updates_contest_settings_and_public_after_end_policy():
    contest_id = first_contest_id()
    set_contest_mutable(contest_id)
    operator = staff_tokens("test4@zoj.com")
    now = datetime.now(timezone.utc)
    start_at = now - timedelta(hours=3)
    freeze_at = now - timedelta(hours=2)
    end_at = now - timedelta(hours=1)

    updated = client.patch(
        f"/api/operator/contests/{contest_id}/settings",
        headers=auth_headers(operator["access_token"]),
        json={
            "status": "ended",
            "start_at": start_at.isoformat(),
            "freeze_at": freeze_at.isoformat(),
            "end_at": end_at.isoformat(),
            "problem_public_after_end": True,
            "scoreboard_public_after_end": True,
            "submission_public_after_end": True,
            "emergency_notice": "Final review in progress",
        },
    )
    assert updated.status_code == 200
    assert updated.json()["data"]["status"] == "ended"
    assert updated.json()["data"]["problem_public_after_end"] is True

    public_problems = client.get(f"/api/contests/{contest_id}/problems")
    assert public_problems.status_code == 200

    invalid = client.patch(
        f"/api/operator/contests/{contest_id}/settings",
        headers=auth_headers(operator["access_token"]),
        json={"start_at": end_at.isoformat(), "end_at": start_at.isoformat()},
    )
    assert invalid.status_code == 422

    restore_start = now - timedelta(hours=1)
    restore_end = now + timedelta(hours=3)
    restore_freeze = now + timedelta(hours=2)
    restored = client.patch(
        f"/api/operator/contests/{contest_id}/settings",
        headers=auth_headers(operator["access_token"]),
        json={
            "status": "running",
            "start_at": restore_start.isoformat(),
            "freeze_at": restore_freeze.isoformat(),
            "end_at": restore_end.isoformat(),
            "problem_public_after_end": False,
            "scoreboard_public_after_end": False,
            "submission_public_after_end": False,
            "emergency_notice": "제출 지연은 long polling 상태창에서 확인하세요.",
        },
    )
    assert restored.status_code == 200


def test_contest_status_is_normalized_from_schedule_updates():
    contest_id = first_contest_id()
    set_contest_mutable(contest_id)
    operator = staff_tokens("test4@zoj.com")
    now = datetime.now(timezone.utc)
    future_start = now + timedelta(days=2)
    future_freeze = future_start + timedelta(hours=3)
    future_end = future_start + timedelta(hours=4)

    future = client.patch(
        f"/api/operator/contests/{contest_id}/settings",
        headers=auth_headers(operator["access_token"]),
        json={
            "status": "running",
            "start_at": future_start.isoformat(),
            "freeze_at": future_freeze.isoformat(),
            "end_at": future_end.isoformat(),
        },
    )
    assert future.status_code == 200
    assert future.json()["data"]["status"] == "open"

    running_start = now - timedelta(minutes=10)
    running_freeze = now + timedelta(hours=1)
    running_end = now + timedelta(hours=2)
    running = client.patch(
        f"/api/operator/contests/{contest_id}/settings",
        headers=auth_headers(operator["access_token"]),
        json={
            "status": "open",
            "start_at": running_start.isoformat(),
            "freeze_at": running_freeze.isoformat(),
            "end_at": running_end.isoformat(),
        },
    )
    assert running.status_code == 200
    assert running.json()["data"]["status"] == "running"


def test_operator_schedule_update_auto_adjusts_end_and_freeze_when_only_start_is_changed():
    contest_id = first_contest_id()
    set_contest_mutable(contest_id)
    operator = staff_tokens("test4@zoj.com")
    baseline = client.get(f"/api/operator/contests/{contest_id}/dashboard", headers=auth_headers(operator["access_token"]))
    assert baseline.status_code == 200
    before = baseline.json()["data"]["contest"]
    old_start = datetime.fromisoformat(before["start_at"].replace("Z", "+00:00"))
    old_end = datetime.fromisoformat(before["end_at"].replace("Z", "+00:00"))
    duration = old_end - old_start

    moved_start = old_end + timedelta(hours=2)
    updated = client.patch(
        f"/api/operator/contests/{contest_id}/settings",
        headers=auth_headers(operator["access_token"]),
        json={
            "start_at": moved_start.isoformat(),
        },
    )
    assert updated.status_code == 200
    data = updated.json()["data"]
    assert datetime.fromisoformat(data["start_at"].replace("Z", "+00:00")) == moved_start
    expected_end = moved_start + duration
    assert datetime.fromisoformat(data["end_at"].replace("Z", "+00:00")) == expected_end
    freeze = datetime.fromisoformat(data["freeze_at"].replace("Z", "+00:00"))
    assert moved_start <= freeze <= expected_end

def test_operator_updates_participant_team_status_and_division():
    contest_id = first_contest_id()
    set_contest_mutable(contest_id)
    operator = staff_tokens("test4@zoj.com")
    participants = client.get(f"/api/operator/contests/{contest_id}/participants", headers=auth_headers(operator["access_token"]))
    team = next(item for item in participants.json()["data"] if item["team_name"] == "Team Rookie")
    divisions = client.get(f"/api/operator/contests/{contest_id}/divisions", headers=auth_headers(operator["access_token"]))
    advanced = next(item for item in divisions.json()["data"] if item["code"] == "advanced")
    beginner = next(item for item in divisions.json()["data"] if item["code"] == "beginner")

    updated = client.patch(
        f"/api/operator/contests/{contest_id}/participants/{team['participant_team_id']}",
        headers=auth_headers(operator["access_token"]),
        json={"team_name": "Team Rookie Updated", "division_id": advanced["division_id"], "status": "disqualified"},
    )
    assert updated.status_code == 200
    assert updated.json()["data"]["team_name"] == "Team Rookie Updated"
    assert updated.json()["data"]["division"]["code"] == "advanced"
    assert updated.json()["data"]["status"] == "disqualified"

    invalid = client.patch(
        f"/api/operator/contests/{contest_id}/participants/{team['participant_team_id']}",
        headers=auth_headers(operator["access_token"]),
        json={"status": "deleted"},
    )
    assert invalid.status_code == 422

    restored = client.patch(
        f"/api/operator/contests/{contest_id}/participants/{team['participant_team_id']}",
        headers=auth_headers(operator["access_token"]),
        json={"team_name": "Team Rookie", "division_id": beginner["division_id"], "status": "active"},
    )
    assert restored.status_code == 200


def test_operator_bulk_creates_participant_teams():
    contest_id = first_contest_id()
    set_contest_mutable(contest_id)
    operator = staff_tokens("test4@zoj.com")
    divisions = client.get(f"/api/operator/contests/{contest_id}/divisions", headers=auth_headers(operator["access_token"]))
    division_id = next(item for item in divisions.json()["data"] if item["code"] == "advanced")["division_id"]
    suffix = uuid4().hex[:8]

    bulk = client.post(
        f"/api/operator/contests/{contest_id}/participants:bulk-create",
        headers=auth_headers(operator["access_token"]),
        json={
            "teams": [
                {
                    "team_name": f"Bulk Team {suffix}",
                    "division_id": division_id,
                    "leader": {"name": "Bulk Leader", "email": f"bulk-leader-{suffix}@zoj.com"},
                    "members": [{"name": "Bulk Member", "email": f"bulk-member-{suffix}@zoj.com"}],
                }
            ]
        },
    )
    assert bulk.status_code == 200
    assert len(bulk.json()["data"]["created"]) == 1
    assert bulk.json()["data"]["errors"] == []

    duplicate = client.post(
        f"/api/operator/contests/{contest_id}/participants:bulk-create",
        headers=auth_headers(operator["access_token"]),
        json={
            "teams": [
                {
                    "team_name": f"Bulk Team Duplicate {suffix}",
                    "division_id": division_id,
                    "leader": {"name": "Bulk Leader", "email": f"bulk-leader-{suffix}@zoj.com"},
                    "members": [],
                }
            ]
        },
    )
    assert duplicate.status_code == 200
    assert duplicate.json()["data"]["created"] == []
    assert duplicate.json()["data"]["errors"][0]["message"] == "participant email already registered"


def test_participant_email_conflict_with_staff_is_scoped_to_same_contest():
    contest_id = first_contest_id()
    set_contest_mutable(contest_id)
    operator = staff_tokens("test4@zoj.com")
    divisions = client.get(f"/api/operator/contests/{contest_id}/divisions", headers=auth_headers(operator["access_token"]))
    division_id = next(item for item in divisions.json()["data"] if item["code"] == "advanced")["division_id"]
    suffix = uuid4().hex[:8]

    master = staff_tokens()
    other_operator_email = f"other-operator-{suffix}@zoj.com"
    other_contest = client.post(
        "/api/admin/contests",
        headers=auth_headers(master["access_token"]),
        json={
            "organization_name": f"Other Org {suffix}",
            "operator_email": other_operator_email,
        },
    )
    assert other_contest.status_code == 200

    allowed = client.post(
        f"/api/operator/contests/{contest_id}/participants",
        headers=auth_headers(operator["access_token"]),
        json={
            "team_name": f"Scoped Staff Team {suffix}",
            "division_id": division_id,
            "leader": {"name": "Scoped Leader", "email": other_operator_email},
            "members": [],
        },
    )
    assert allowed.status_code == 200

    denied = client.post(
        f"/api/operator/contests/{contest_id}/participants",
        headers=auth_headers(operator["access_token"]),
        json={
            "team_name": f"Same Contest Staff Team {suffix}",
            "division_id": division_id,
            "leader": {"name": "Same Contest Staff", "email": "test4@zoj.com"},
            "members": [],
        },
    )
    assert denied.status_code == 422
    assert "participant email cannot be operator/staff account" in denied.json()["error"]["message"]


def test_operator_deletes_participant_team_without_history():
    contest_id = first_contest_id()
    set_contest_mutable(contest_id)
    operator = staff_tokens("test4@zoj.com")
    divisions = client.get(f"/api/operator/contests/{contest_id}/divisions", headers=auth_headers(operator["access_token"]))
    division_id = next(item for item in divisions.json()["data"] if item["code"] == "advanced")["division_id"]
    suffix = uuid4().hex[:8]

    created = client.post(
        f"/api/operator/contests/{contest_id}/participants",
        headers=auth_headers(operator["access_token"]),
        json={
            "team_name": f"Delete Team {suffix}",
            "division_id": division_id,
            "leader": {"name": "Delete Leader", "email": f"delete-leader-{suffix}@zoj.com"},
            "members": [],
        },
    )
    assert created.status_code == 200
    team_id = created.json()["data"]["participant_team_id"]

    deleted = client.delete(
        f"/api/operator/contests/{contest_id}/participants/{team_id}",
        headers=auth_headers(operator["access_token"]),
    )
    assert deleted.status_code == 200
    assert deleted.json()["data"]["deleted"] is True

    participants = client.get(f"/api/operator/contests/{contest_id}/participants", headers=auth_headers(operator["access_token"]))
    assert all(item["participant_team_id"] != team_id for item in participants.json()["data"])


def test_operator_manages_team_members_and_revokes_member_sessions():
    contest_id, login = participant_login()
    set_contest_mutable(contest_id)
    operator = staff_tokens("test4@zoj.com")
    participants = client.get(f"/api/operator/contests/{contest_id}/participants", headers=auth_headers(operator["access_token"]))
    team = next(item for item in participants.json()["data"] if item["team_name"] == login["team"]["team_name"])
    suffix = uuid4().hex[:8]
    member_email = f"new-member-{suffix}@zoj.com"
    renamed_email = f"renamed-member-{suffix}@zoj.com"

    created = client.post(
        f"/api/operator/contests/{contest_id}/participants/{team['participant_team_id']}/members",
        headers=auth_headers(operator["access_token"]),
        json={"name": "New Member", "email": member_email},
    )
    assert created.status_code == 200
    member = created.json()["data"]
    assert member["role"] == "member"

    duplicate = client.post(
        f"/api/operator/contests/{contest_id}/participants/{team['participant_team_id']}/members",
        headers=auth_headers(operator["access_token"]),
        json={"name": "Duplicate", "email": "test2@zoj.com"},
    )
    assert duplicate.status_code == 422

    updated = client.patch(
        f"/api/operator/contests/{contest_id}/participants/{team['participant_team_id']}/members/{member['team_member_id']}",
        headers=auth_headers(operator["access_token"]),
        json={"name": "Renamed Member", "email": renamed_email},
    )
    assert updated.status_code == 200
    assert updated.json()["data"]["name"] == "Renamed Member"
    assert updated.json()["data"]["email"] == renamed_email

    leader = next(item for item in team["members"] if item["role"] == "leader")
    revoked = client.post(
        f"/api/operator/contests/{contest_id}/participants/{team['participant_team_id']}/members/{leader['team_member_id']}/sessions:revoke",
        headers=auth_headers(operator["access_token"]),
    )
    assert revoked.status_code == 200
    assert revoked.json()["data"]["active_sessions"] == 0

    denied = client.get(f"/api/contests/{contest_id}/participant-session/me", headers=auth_headers(login["access_token"]))
    assert denied.status_code == 401


def test_submission_rejects_problem_from_other_division():
    contest_id, login = participant_login()
    contest_detail = client.get(f"/api/public/contests/{contest_id}").json()["data"]
    beginner_division = next(item for item in contest_detail["divisions"] if item["code"] == "beginner")
    beginner_problem = operator_problem(contest_id, beginner_division["division_id"])

    response = client.post(
        f"/api/contests/{contest_id}/problems/{beginner_problem['problem_id']}/submissions",
        headers=auth_headers(login["access_token"]),
        json={"language": "cpp17", "source_code": "int main(){return 0;}"},
    )

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "not_found"


def test_judge_claim_uses_database_queue():
    contest_id, login = participant_login()
    advanced_division = login["division"]
    advanced_problem = client.get(
        f"/api/contests/{contest_id}/divisions/{advanced_division['division_id']}/problems",
        headers=auth_headers(login["access_token"]),
    ).json()["data"][0]

    submission = client.post(
        f"/api/contests/{contest_id}/problems/{advanced_problem['problem_id']}/submissions",
        headers=auth_headers(login["access_token"]),
        json={"language": "cpp17", "source_code": "int main(){return 0;}"},
    )
    assert submission.status_code == 200

    node = client.post(
        "/api/internal/judge/nodes/register",
        json={"node_name": "pytest-node", "node_secret": "demo", "total_slots": 10},
    )
    assert node.status_code == 200
    node_id = node.json()["data"]["judge_node_id"]

    claim = client.post(
        f"/api/internal/judge/nodes/{node_id}/assignments:claim",
        json={"node_secret": "demo", "max_count": 1},
    )
    assert claim.status_code == 200
    assert claim.json()["data"]["jobs"][0]["submission"]["division_id"] == advanced_division["division_id"]


def test_judge_node_secret_is_required_for_claim_and_result():
    contest_id, login = participant_login()
    advanced_division = login["division"]
    advanced_problem = client.get(
        f"/api/contests/{contest_id}/divisions/{advanced_division['division_id']}/problems",
        headers=auth_headers(login["access_token"]),
    ).json()["data"][0]
    client.post(
        f"/api/contests/{contest_id}/problems/{advanced_problem['problem_id']}/submissions",
        headers=auth_headers(login["access_token"]),
        json={"language": "python313", "source_code": "print(42)"},
    )

    node = client.post(
        "/api/internal/judge/nodes/register",
        json={"node_name": f"secret-node-{uuid4().hex[:6]}", "node_secret": "correct", "total_slots": 1},
    )
    node_id = node.json()["data"]["judge_node_id"]

    denied = client.post(
        f"/api/internal/judge/nodes/{node_id}/assignments:claim",
        json={"node_secret": "wrong", "max_count": 1},
    )
    assert denied.status_code == 403
    assert denied.json()["error"]["code"] == "node_secret_invalid"

    claim = client.post(
        f"/api/internal/judge/nodes/{node_id}/assignments:claim",
        json={"node_secret": "correct", "max_count": 1},
    )
    assert claim.status_code == 200
    job = claim.json()["data"]["jobs"][0]

    result_denied = client.post(
        f"/api/internal/judge/jobs/{job['judge_job_id']}/result",
        json={
            "node_secret": "wrong",
            "lease_token": job["lease_token"],
            "final_status": "accepted",
            "awarded_score": 100,
        },
    )
    assert result_denied.status_code == 403
    assert result_denied.json()["error"]["code"] == "node_secret_invalid"


def test_submission_progress_is_updated_during_judging():
    contest_id, login = participant_login()
    advanced_division = login["division"]
    advanced_problem = client.get(
        f"/api/contests/{contest_id}/divisions/{advanced_division['division_id']}/problems",
        headers=auth_headers(login["access_token"]),
    ).json()["data"][0]
    submission = client.post(
        f"/api/contests/{contest_id}/problems/{advanced_problem['problem_id']}/submissions",
        headers=auth_headers(login["access_token"]),
        json={"language": "python313", "source_code": "print(42)"},
    )
    assert submission.status_code == 200
    submission_id = submission.json()["data"]["submission_id"]

    node_secret = "progress-secret"
    node = client.post(
        "/api/internal/judge/nodes/register",
        json={"node_name": f"progress-node-{uuid4().hex[:6]}", "node_secret": node_secret, "total_slots": 1},
    )
    node_id = node.json()["data"]["judge_node_id"]
    job = claim_jobs_until(node_id, node_secret, [submission_id])[submission_id]

    detail = client.get(
        f"/api/contests/{contest_id}/submissions/{submission_id}",
        headers=auth_headers(login["access_token"]),
    )
    assert detail.status_code == 200
    assert detail.json()["data"]["status"] == SubmissionStatus.PREPARING.value

    progress = client.post(
        f"/api/internal/judge/jobs/{job['judge_job_id']}/progress",
        json={
            "node_secret": node_secret,
            "lease_token": job["lease_token"],
            "status": SubmissionStatus.JUDGING.value,
            "progress_current": 2,
            "progress_total": 5,
        },
    )
    assert progress.status_code == 200
    assert progress.json()["data"]["submission"]["progress_current"] == 2
    assert progress.json()["data"]["submission"]["progress_total"] == 5

    waited = client.get(
        f"/api/contests/{contest_id}/submissions/{submission_id}/status:wait",
        headers=auth_headers(login["access_token"]),
    )
    assert waited.status_code == 200
    assert waited.json()["data"]["status"] == SubmissionStatus.JUDGING.value
    assert waited.json()["data"]["progress_current"] is None
    assert waited.json()["data"]["progress_total"] is None
    assert waited.json()["data"]["progress_percent"] == 40


def test_operator_and_admin_submission_detail_include_source_without_list_payload_bloat():
    contest_id, login = participant_login()
    set_contest_running(contest_id)
    operator = staff_tokens("test4@zoj.com")
    master = staff_tokens()
    division_id = login["division"]["division_id"]
    problem = operator_problem(contest_id, division_id)
    source_code = f"print({uuid4().hex[:6]!r})"

    created = client.post(
        f"/api/contests/{contest_id}/problems/{problem['problem_id']}/submissions",
        headers=auth_headers(login["access_token"]),
        json={"language": "python313", "source_code": source_code},
    )
    assert created.status_code == 200
    submission_id = created.json()["data"]["submission_id"]

    operator_list = client.get(
        f"/api/operator/contests/{contest_id}/submissions",
        headers=auth_headers(operator["access_token"]),
    )
    assert operator_list.status_code == 200
    listed = next(item for item in operator_list.json()["data"] if item["submission_id"] == submission_id)
    assert listed["source_code"] is None

    operator_detail = client.get(
        f"/api/operator/contests/{contest_id}/submissions/{submission_id}",
        headers=auth_headers(operator["access_token"]),
    )
    assert operator_detail.status_code == 200
    assert operator_detail.json()["data"]["source_code"] == source_code

    operator_wait = client.get(
        f"/api/operator/contests/{contest_id}/submissions/{submission_id}/status:wait?wait_seconds=0",
        headers=auth_headers(operator["access_token"]),
    )
    assert operator_wait.status_code == 200
    assert operator_wait.json()["data"]["source_code"] is None

    admin_list = client.get("/api/admin/judge/submissions", headers=auth_headers(master["access_token"]))
    assert admin_list.status_code == 200
    admin_listed = next(item for item in admin_list.json()["data"] if item["submission"]["submission_id"] == submission_id)
    assert admin_listed["submission"]["source_code"] is None

    admin_detail = client.get(f"/api/admin/judge/submissions/{submission_id}", headers=auth_headers(master["access_token"]))
    assert admin_detail.status_code == 200
    assert admin_detail.json()["data"]["submission"]["source_code"] == source_code

    admin_wait = client.get(
        f"/api/admin/judge/submissions/{submission_id}/status:wait?wait_seconds=0",
        headers=auth_headers(master["access_token"]),
    )
    assert admin_wait.status_code == 200
    assert admin_wait.json()["data"]["source_code"] is None

    node_secret = f"detail-secret-{uuid4().hex[:6]}"
    node = client.post(
        "/api/internal/judge/nodes/register",
        json={"node_name": f"detail-node-{uuid4().hex[:6]}", "node_secret": node_secret, "total_slots": 10},
    )
    node_id = node.json()["data"]["judge_node_id"]
    claimed = claim_jobs_until(node_id, node_secret, [submission_id])[submission_id]
    completed = client.post(
        f"/api/internal/judge/jobs/{claimed['judge_job_id']}/result",
        json={
            "node_secret": node_secret,
            "lease_token": claimed["lease_token"],
            "final_status": "accepted",
            "awarded_score": 100,
        },
    )
    assert completed.status_code == 200


def test_scoreboard_uses_icpc_attempt_policy_per_problem():
    contest_id, login = participant_login()
    set_contest_mutable(contest_id)
    operator = staff_tokens("test4@zoj.com")
    division_id = login["division"]["division_id"]

    problem = client.post(
        f"/api/operator/contests/{contest_id}/problems",
        headers=auth_headers(operator["access_token"]),
        json={
            "division_id": division_id,
            "problem_code": f"S{uuid4().hex[:6]}",
            "title": "ICPC Attempt Policy",
            "statement": "Scoreboard should use solved count and penalty, not partial scores.",
            "time_limit_ms": 1000,
            "memory_limit_mb": 512,
            "display_order": 120,
            "max_score": 100,
        },
    ).json()["data"]
    set_contest_running(contest_id)

    submissions = []
    for source_code in ["print(30)", "print(20)", "broken"]:
        response = client.post(
            f"/api/contests/{contest_id}/problems/{problem['problem_id']}/submissions",
            headers=auth_headers(login["access_token"]),
            json={"language": "python313", "source_code": source_code},
        )
        assert response.status_code == 200
        submissions.append(response.json()["data"])

    node_secret = "score-secret"
    node = client.post(
        "/api/internal/judge/nodes/register",
        json={"node_name": f"score-node-{uuid4().hex[:6]}", "node_secret": node_secret, "total_slots": 10},
    )
    node_id = node.json()["data"]["judge_node_id"]
    jobs_by_submission = claim_jobs_until(node_id, node_secret, [submission["submission_id"] for submission in submissions])

    results = [
        (submissions[0]["submission_id"], "wrong_answer", 30),
        (submissions[1]["submission_id"], "wrong_answer", 20),
        (submissions[2]["submission_id"], "compile_error", 100),
    ]
    for submission_id, status, score in results:
        job = jobs_by_submission[submission_id]
        response = client.post(
            f"/api/internal/judge/jobs/{job['judge_job_id']}/result",
            json={
                "node_secret": node_secret,
                "lease_token": job["lease_token"],
                "final_status": status,
                "awarded_score": score,
            },
        )
        assert response.status_code == 200

    scoreboard = client.get(
        f"/api/operator/contests/{contest_id}/divisions/{division_id}/scoreboard/internal",
        headers=auth_headers(operator["access_token"]),
    )
    assert scoreboard.status_code == 200
    team_row = next(row for row in scoreboard.json()["data"]["rows"] if row["team_id"] == login["team"]["participant_team_id"])
    problem_score = next(item for item in team_row["problem_scores"] if item["problem_id"] == problem["problem_id"])
    assert "score" not in problem_score
    assert "max_score" not in problem_score
    assert problem_score["best_submission_id"] is None
    assert problem_score["attempts"] == 2
    assert problem_score["wrong_attempts"] == 2
    assert problem_score["solved"] is False


def test_manual_rejudge_api_is_not_available_to_service_master_or_operator():
    contest_id, login = participant_login()
    set_contest_mutable(contest_id)
    operator = staff_tokens("test4@zoj.com")
    master = staff_tokens("test3@zoj.com")
    division_id = login["division"]["division_id"]

    problem = client.post(
        f"/api/operator/contests/{contest_id}/problems",
        headers=auth_headers(operator["access_token"]),
        json={
            "division_id": division_id,
            "problem_code": f"R{uuid4().hex[:6]}",
            "title": "Rejudge Policy",
            "statement": "Manual rejudge API must not be exposed.",
            "time_limit_ms": 1000,
            "memory_limit_mb": 512,
            "display_order": 130,
            "max_score": 100,
        },
    ).json()["data"]
    set_contest_running(contest_id)
    submission = client.post(
        f"/api/contests/{contest_id}/problems/{problem['problem_id']}/submissions",
        headers=auth_headers(login["access_token"]),
        json={"language": "python313", "source_code": "print(90)"},
    ).json()["data"]

    node_secret = "rejudge-secret"
    node = client.post(
        "/api/internal/judge/nodes/register",
        json={"node_name": f"rejudge-node-{uuid4().hex[:6]}", "node_secret": node_secret, "total_slots": 10},
    )
    node_id = node.json()["data"]["judge_node_id"]
    claim = client.post(
        f"/api/internal/judge/nodes/{node_id}/assignments:claim",
        json={"node_secret": node_secret, "max_count": 100},
    )
    job = next(job for job in claim.json()["data"]["jobs"] if job["submission"]["submission_id"] == submission["submission_id"])
    result = client.post(
        f"/api/internal/judge/jobs/{job['judge_job_id']}/result",
        json={
            "node_secret": node_secret,
            "lease_token": job["lease_token"],
            "final_status": "wrong_answer",
            "awarded_score": 90,
        },
    )
    assert result.status_code == 200

    denied = client.post(
        f"/api/admin/contests/{contest_id}/submissions/{submission['submission_id']}/rejudge",
        headers=auth_headers(operator["access_token"]),
    )
    assert denied.status_code == 404

    master_denied = client.post(
        f"/api/admin/contests/{contest_id}/submissions/{submission['submission_id']}/rejudge",
        headers=auth_headers(master["access_token"]),
    )
    assert master_denied.status_code == 404

    scoreboard = client.get(
        f"/api/operator/contests/{contest_id}/divisions/{division_id}/scoreboard/internal",
        headers=auth_headers(operator["access_token"]),
    )
    problem_score = next(
        item
        for row in scoreboard.json()["data"]["rows"]
        if row["team_id"] == login["team"]["participant_team_id"]
        for item in row["problem_scores"]
        if item["problem_id"] == problem["problem_id"]
    )
    assert "score" not in problem_score
    assert problem_score["best_submission_id"] is None
    assert problem_score["attempts"] == 1
    assert problem_score["wrong_attempts"] == 1
    assert problem_score["solved"] is False


def test_judge_claim_includes_active_testcases():
    contest_id, login = participant_login()
    set_contest_mutable(contest_id)
    operator = staff_tokens("test4@zoj.com")
    division_id = login["division"]["division_id"]

    problem = client.post(
        f"/api/operator/contests/{contest_id}/problems",
        headers=auth_headers(operator["access_token"]),
        json={
            "division_id": division_id,
            "problem_code": f"T{uuid4().hex[:6]}",
            "title": "Claim Testcases",
            "statement": "Use active testcase set.",
            "time_limit_ms": 1000,
            "memory_limit_mb": 512,
            "display_order": 99,
            "max_score": 100,
        },
    ).json()["data"]
    testcase_set = client.post(
        f"/api/operator/contests/{contest_id}/problems/{problem['problem_id']}/testcase-sets",
        headers=auth_headers(operator["access_token"]),
        json={"is_active": True},
    ).json()["data"]
    client.post(
        f"/api/operator/contests/{contest_id}/problems/{problem['problem_id']}/testcase-sets/{testcase_set['testcase_set_id']}/testcases",
        headers=auth_headers(operator["access_token"]),
        json={
            "display_order": 1,
            "input_storage_key": "local/input.txt",
            "output_storage_key": "local/output.txt",
            "input_sha256": "d" * 64,
            "output_sha256": "e" * 64,
        },
    )
    set_contest_running(contest_id)
    submission = client.post(
        f"/api/contests/{contest_id}/problems/{problem['problem_id']}/submissions",
        headers=auth_headers(login["access_token"]),
        json={"language": "python313", "source_code": "print(42)"},
    )
    assert submission.status_code == 200
    submission_id = submission.json()["data"]["submission_id"]

    node = client.post(
        "/api/internal/judge/nodes/register",
        json={"node_name": f"pytest-node-{uuid4().hex[:6]}", "node_secret": "demo", "total_slots": 1},
    )
    node_id = node.json()["data"]["judge_node_id"]
    claim = client.post(
        f"/api/internal/judge/nodes/{node_id}/assignments:claim",
        json={"node_secret": "demo", "max_count": 20},
    )
    assert claim.status_code == 200
    job = next(item for item in claim.json()["data"]["jobs"] if item["submission"]["submission_id"] == submission_id)
    assert job["testcase_set"]["testcase_set_id"] == testcase_set["testcase_set_id"]
    assert job["testcases"][0]["input_storage_key"] == "local/input.txt"
    assert job["testcases"][0]["input_url"].startswith("file://")
    assert job["problem"]["problem_id"] == problem["problem_id"]
    assert job["problem"]["time_limit_ms"] == 1000
    assert job["leased_at"]

    empty_claim = client.post(
        f"/api/internal/judge/nodes/{node_id}/assignments:claim",
        json={"node_secret": "demo", "max_count": 20, "wait_seconds": 0.01},
    )
    assert empty_claim.status_code == 200
    assert empty_claim.json()["data"]["jobs"] == []


def test_judge_dispatcher_recovers_expired_leases():
    contest_id, login = participant_login()
    advanced_division = login["division"]
    advanced_problem = client.get(
        f"/api/contests/{contest_id}/divisions/{advanced_division['division_id']}/problems",
        headers=auth_headers(login["access_token"]),
    ).json()["data"][0]

    submission = client.post(
        f"/api/contests/{contest_id}/problems/{advanced_problem['problem_id']}/submissions",
        headers=auth_headers(login["access_token"]),
        json={"language": "python313", "source_code": "print(42)"},
    ).json()["data"]
    node = client.post(
        "/api/internal/judge/nodes/register",
        json={"node_name": f"expired-node-{uuid4().hex[:6]}", "node_secret": "demo", "total_slots": 1},
    )
    node_id = node.json()["data"]["judge_node_id"]
    claim = client.post(
        f"/api/internal/judge/nodes/{node_id}/assignments:claim",
        json={"node_secret": "demo", "max_count": 1},
    )
    job = claim.json()["data"]["jobs"][0]

    with store._session() as db:
        row = db.get(JudgeJobRow, job["judge_job_id"])
        row.leased_at = datetime.now(timezone.utc) - timedelta(seconds=180)
        db.commit()

    recovered = client.post(
        f"/api/internal/judge/nodes/{node_id}/assignments:claim",
        json={"node_secret": "demo", "max_count": 1},
    )
    assert recovered.status_code == 200
    recovered_job = recovered.json()["data"]["jobs"][0]
    assert recovered_job["judge_job_id"] == job["judge_job_id"]
    assert recovered_job["lease_token"] != job["lease_token"]

    with store._session() as db:
        submission_row = db.get(SubmissionRow, submission["submission_id"])
        job_row = db.get(JudgeJobRow, job["judge_job_id"])
        assert submission_row.status == SubmissionStatus.PREPARING.value
        assert job_row.status == JudgeJobStatus.RUNNING.value


def test_participant_submission_requires_token_and_filters_team():
    contest_id, login = participant_login()
    advanced_division = login["division"]
    advanced_problem = client.get(
        f"/api/contests/{contest_id}/divisions/{advanced_division['division_id']}/problems",
        headers=auth_headers(login["access_token"]),
    ).json()["data"][0]

    denied = client.post(
        f"/api/contests/{contest_id}/problems/{advanced_problem['problem_id']}/submissions",
        json={"language": "cpp17", "source_code": "int main(){return 0;}"},
    )
    assert denied.status_code == 401

    submission = client.post(
        f"/api/contests/{contest_id}/problems/{advanced_problem['problem_id']}/submissions",
        headers=auth_headers(login["access_token"]),
        json={"language": "cpp17", "source_code": "int main(){return 0;}"},
    )
    assert submission.status_code == 200
    submission_id = submission.json()["data"]["submission_id"]

    own_list = client.get(f"/api/contests/{contest_id}/submissions", headers=auth_headers(login["access_token"]))
    assert own_list.status_code == 200
    assert all(item["participant_team_id"] == login["team"]["participant_team_id"] for item in own_list.json()["data"])

    detail = client.get(f"/api/contests/{contest_id}/submissions/{submission_id}", headers=auth_headers(login["access_token"]))
    assert detail.status_code == 200


def test_submission_status_wait_does_not_auto_accept_without_judge_result():
    contest_id, login = participant_login()
    advanced_division = login["division"]
    advanced_problem = client.get(
        f"/api/contests/{contest_id}/divisions/{advanced_division['division_id']}/problems",
        headers=auth_headers(login["access_token"]),
    ).json()["data"][0]

    submission = client.post(
        f"/api/contests/{contest_id}/problems/{advanced_problem['problem_id']}/submissions",
        headers=auth_headers(login["access_token"]),
        json={"language": "python313", "source_code": "print(42)"},
    )
    assert submission.status_code == 200
    submission_id = submission.json()["data"]["submission_id"]

    waited = client.get(
        f"/api/contests/{contest_id}/submissions/{submission_id}/status:wait",
        headers=auth_headers(login["access_token"]),
    )
    assert waited.status_code == 200
    assert waited.json()["data"]["status"] == SubmissionStatus.WAITING.value

    detail = client.get(
        f"/api/contests/{contest_id}/submissions/{submission_id}",
        headers=auth_headers(login["access_token"]),
    )
    assert detail.status_code == 200
    assert detail.json()["data"]["status"] == SubmissionStatus.WAITING.value
