"""Contest ownership is unique, transferable, and never an assignable role."""
import json
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text

from app.main import app
from app.orm_models import ContestRow, StaffAccountRow
from app.services.contest_ownership import backfill_contest_owners
from app.services.contest_roles import title_for_roles, title_for_scopes
from app.services.errors import AppError
from app.services.store import store

client = TestClient(app)


def email():
    return f"owner-test-{uuid4().hex}@zoj.com"


def login(address):
    response = client.post('/api/auth/general/otp/verify', json={"email": address, "otp_code": "", "force_new_session": True})
    assert response.status_code == 200, response.text
    return {"Authorization": "Bearer " + response.json()["data"]["operator_session"]["access_token"]}


@pytest.fixture
def context():
    contest = store.create_contest("Ownership", "Test", "Test")
    cid = contest.contest_id
    owner = store.upsert_contest_operator(cid, email(), "Original owner")
    master = store.upsert_contest_operator(cid, email(), "Other master", ["master"])
    target = store.upsert_contest_operator(cid, email(), "Author", ["problem_author"])
    return cid, owner, master, target, f"/api/operator/contests/{cid}"


def assert_owner(cid, expected):
    with store._session() as db:
        assert db.get(ContestRow, cid).owner_staff_account_id == expected.staff_account_id
    accounts = store.contest_operator_accounts(cid)
    assert [a.staff_account_id for a in accounts if a.contest_roles[cid] == ["owner"]] == [expected.staff_account_id]
    assert [a.staff_account_id for a in accounts if cid in a.protected_master_contests] == [expected.staff_account_id]
    assert [a.staff_account_id for a in accounts if "contest.owner" in a.contest_scopes[cid]] == [expected.staff_account_id]


def test_initial_name_owner_assignment_and_second_admin_master():
    admin = login("test3@zoj.com")
    first = email()
    response = client.post('/api/admin/contests', headers=admin, json={"organization_name": "Test", "operator_email": first, "operator_display_name": "  손동열  "})
    assert response.status_code == 200, response.text
    cid = response.json()["data"]["contest_id"]
    owner, = store.contest_operator_accounts(cid)
    assert owner.display_name == "손동열"
    assert owner.contest_scopes[cid] == ["contest.*", "contest.owner"]
    assert_owner(cid, owner)
    assigned = client.post(f'/api/admin/contests/{cid}/operators', headers=admin, json={"email": email(), "display_name": "Second master"})
    assert assigned.status_code == 200
    assert assigned.json()["data"]["contest_roles"][cid] == ["master"]
    assert assigned.json()["data"]["protected_master_contests"] == []
    assert_owner(cid, owner)
    for name in [" ", "x" * 121]:
        bad = client.post('/api/admin/contests', headers=admin, json={"organization_name": "Test", "operator_email": first, "operator_display_name": name})
        assert bad.status_code == 422


def test_no_staff_contest_requires_initial_master_without_silent_escalation():
    contest = store.create_contest("Empty", "Test", "Test")
    with store._session() as db:
        assert db.get(ContestRow, contest.contest_id).owner_staff_account_id is None
    with pytest.raises(AppError) as failure:
        store.upsert_contest_operator(contest.contest_id, email(), "Reviewer", ["problem_reviewer"])
    assert failure.value.code == "contest_owner_required"
    assert store.contest_operator_accounts(contest.contest_id) == []
    owner = store.upsert_contest_operator(contest.contest_id, email(), "Owner")
    assert_owner(contest.contest_id, owner)


def test_owner_cannot_be_injected_demoted_or_removed_even_by_admin(context):
    cid, owner, master, target, base = context
    for actor in [owner, master]:
        auth = login(str(actor.email))
        for method, path in [("post", "/operators"), ("patch", "/operators/" + str(target.email))]:
            result = client.request(method, base + path, headers=auth, json={"email": str(target.email), "display_name": "Attempt", "roles": ["owner"]})
            assert result.status_code == 422
    for auth in [login(str(owner.email)), login(str(master.email)), login("test3@zoj.com")]:
        changed = client.patch(base + "/operators/" + str(owner.email), headers=auth, json={"display_name": "Attempt", "roles": ["problem_reviewer"]})
        removed = client.delete(base + "/operators/" + str(owner.email), headers=auth)
        assert changed.status_code == removed.status_code == 409
    # Direct store mutations have the same protection as the HTTP routes.
    for operation in [
        lambda: store.remove_contest_operator(cid, str(owner.email)),
        lambda: store.update_contest_operator(cid, str(owner.email), "Attempt", ["problem_reviewer"]),
        lambda: store.upsert_contest_operator(cid, str(owner.email), "Attempt", ["problem_reviewer"]),
    ]:
        with pytest.raises(AppError):
            operation()
    assert_owner(cid, owner)


@pytest.mark.parametrize("actor_kind", ["owner", "master", "service_master"])
def test_owner_display_name_can_change_without_changing_ownership_or_session(context, actor_kind):
    cid, owner, master, _, base = context
    owner_auth = login(str(owner.email))
    auth = owner_auth if actor_kind == "owner" else login(str(master.email) if actor_kind == "master" else "test3@zoj.com")
    response = client.patch(base + "/operators/" + str(owner.email), headers=auth, json={
        "email": str(owner.email), "display_name": "  새 총괄 이름  ", "roles": ["master"],
    })
    assert response.status_code == 200, response.text
    data = response.json()["data"]
    assert data["display_name"] == "새 총괄 이름"
    assert data["email"] == str(owner.email)
    assert data["contest_roles"][cid] == ["owner"]
    assert data["contest_scopes"][cid] == ["contest.*", "contest.owner"]
    assert_owner(cid, owner)
    # Editing a display name must not revoke the owner's existing session.
    listed = client.get(base + "/operators", headers=owner_auth)
    assert listed.status_code == 200, listed.text
    assert next(a for a in store.contest_operator_accounts(cid) if a.email == owner.email).display_name == "새 총괄 이름"


def test_staff_manager_cannot_edit_owner_and_blank_name_is_rejected(context):
    cid, owner, _, _, base = context
    manager = store.upsert_contest_operator(cid, email(), "Staff manager", ["staff_manager"])
    denied = client.patch(base + "/operators/" + str(owner.email), headers=login(str(manager.email)), json={
        "display_name": "Not allowed", "roles": ["master"],
    })
    assert denied.status_code == 403, denied.text
    blank = client.patch(base + "/operators/" + str(owner.email), headers=login(str(owner.email)), json={
        "display_name": "   ", "roles": ["master"],
    })
    assert blank.status_code == 422, blank.text
    assert next(a for a in store.contest_operator_accounts(cid) if a.email == owner.email).display_name == owner.display_name
    assert_owner(cid, owner)


def test_transfer_is_owner_only_and_updates_existing_sessions_and_titles(context):
    cid, owner, master, target, base = context
    owner_auth, master_auth, target_auth = [login(str(a.email)) for a in [owner, master, target]]
    for auth in [master_auth, target_auth, login("test3@zoj.com")]:
        denied = client.post(base + "/owner:transfer", headers=auth, json={"email": str(target.email)})
        assert denied.status_code == 403
        assert_owner(cid, owner)
    transferred = client.post(base + "/owner:transfer", headers=owner_auth, json={"email": str(target.email)})
    assert transferred.status_code == 200, transferred.text
    assert_owner(cid, target)
    previous = next(a for a in store.contest_operator_accounts(cid) if a.staff_account_id == owner.staff_account_id)
    assert previous.contest_roles[cid] == ["master"]
    assert previous.contest_scopes[cid] == ["contest.*"]
    assert client.get(base + "/participants", headers=target_auth).status_code == 200
    listed = client.get(base + "/operators", headers=owner_auth).json()["data"]
    assert next(a for a in listed if a["email"] == str(target.email))["contest_roles"][cid] == ["owner"]
    # A stale former-owner token cannot move ownership again.
    again = client.post(base + "/owner:transfer", headers=owner_auth, json={"email": str(master.email)})
    assert again.status_code == 403
    assert_owner(cid, target)
    # The new owner can demote the former owner, but cannot be demoted itself.
    demoted = client.patch(base + "/operators/" + str(owner.email), headers=target_auth, json={"display_name": "Former owner", "roles": ["problem_reviewer"]})
    assert demoted.status_code == 200
    assert client.get(base + "/participants", headers=owner_auth).status_code == 403
    assert_owner(cid, target)
    assert title_for_roles(["owner"]) == title_for_scopes(["contest.*", "contest.owner"]) == "총괄"


def test_invalid_target_or_other_contest_does_not_change_ownership(context):
    cid, owner, master, target, base = context
    auth = login(str(owner.email))
    other = store.create_contest("Other", "Other", "Other")
    other_owner = store.upsert_contest_operator(other.contest_id, email(), "Other owner")
    for address in [str(owner.email), str(other_owner.email), "test3@zoj.com", email()]:
        assert client.post(base + "/owner:transfer", headers=auth, json={"email": address}).status_code == 422
        assert_owner(cid, owner)
    other_route = f"/api/operator/contests/{other.contest_id}/owner:transfer"
    assert client.post(other_route, headers=auth, json={"email": str(target.email)}).status_code == 403
    assert_owner(other.contest_id, other_owner)


def test_preview_to_owner_preserves_other_contests_and_removes_preview_sessions(context):
    cid, owner, master, target, base = context
    preview = store.upsert_contest_operator(cid, email(), "Preview", ["participant_preview"])
    other = store.create_contest("Other", "Other", "Other")
    store.upsert_contest_operator(other.contest_id, str(preview.email), "Preview")
    division = store.create_contest_division(cid, "A", "A")
    # Create a real preview selection before transferring this role.
    auth = login(str(preview.email))
    selection = client.post(f"/api/auth/general/contests/{cid}/participant-preview-session", headers=auth, json={"division_id": division.division_id})
    assert selection.status_code == 200, selection.text
    changed = client.post(base + "/owner:transfer", headers=login(str(owner.email)), json={"email": str(preview.email)})
    assert changed.status_code == 200, changed.text
    assert_owner(cid, preview)
    assert_owner(other.contest_id, preview)
    from app.orm_models import ParticipantPreviewSessionRow
    from sqlalchemy import select
    with store._session() as db:
        assert db.scalar(select(ParticipantPreviewSessionRow).where(ParticipantPreviewSessionRow.contest_id == cid, ParticipantPreviewSessionRow.staff_account_id == preview.staff_account_id)) is None


def test_backfill_is_explicit_for_ambiguous_masters_and_idempotent():
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE contests (contest_id TEXT PRIMARY KEY, owner_staff_account_id TEXT)"))
        connection.execute(text("CREATE TABLE staff_accounts (staff_account_id TEXT, email TEXT, is_service_master BOOLEAN, contest_scopes TEXT, contest_roles TEXT, protected_master_contests TEXT)"))
        for cid in ["empty", "single", "protected", "ambiguous"]:
            connection.execute(text("INSERT INTO contests VALUES (:cid, NULL)"), {"cid": cid})
        for id, scopes, protected in [("one", ["single", "protected", "ambiguous"], ["protected"]), ("two", ["protected", "ambiguous"], [])]:
            connection.execute(text("INSERT INTO staff_accounts VALUES (:id, :email, false, :scopes, '{}', :protected)"), {"id": id, "email": f"{id}@example.com", "scopes": json.dumps({cid: ["contest.*"] for cid in scopes}), "protected": json.dumps(protected)})
        with pytest.raises(RuntimeError, match="explicit initial owner"):
            backfill_contest_owners(connection, {})
        backfill_contest_owners(connection, {"ambiguous": "two@example.com"})
        owners = dict(connection.execute(text("SELECT contest_id, owner_staff_account_id FROM contests")).all())
        assert owners == {"empty": None, "single": "one", "protected": "one", "ambiguous": "two"}
        before = connection.execute(text("SELECT * FROM staff_accounts")).all()
        backfill_contest_owners(connection, {"ambiguous": "one@example.com"})
        assert connection.execute(text("SELECT * FROM staff_accounts")).all() == before
        assert dict(connection.execute(text("SELECT contest_id, owner_staff_account_id FROM contests")).all()) == owners


def test_simultaneous_transfers_leave_exactly_one_owner(context):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    cid, owner, master, target, base = context
    start = Barrier(2)

    def transfer(recipient):
        start.wait(timeout=5)
        try:
            store.transfer_contest_owner(cid, str(recipient.email), owner)
            return recipient
        except AppError as failure:
            assert failure.code in {"contest_owner_transfer_denied", "contest_owner_changed"}
            return None

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(transfer, [master, target]))
    winners = [result for result in results if result]
    assert len(winners) == 1
    assert_owner(cid, winners[0])
