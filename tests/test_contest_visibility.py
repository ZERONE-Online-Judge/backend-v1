"""Private contest discovery, independent lifecycle, and post-contest access."""
from datetime import timedelta
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.models import ContestStatus, ContestVisibility, now_utc
from app.services.contest_visibility import RESOURCE_ACCESS_FIELDS
from app.services.seo import build_page_metadata, build_sitemap_xml
from app.services.store import store

client = TestClient(app)


def login(email):
    result = client.post('/api/auth/general/otp/verify', json={
        'email': email, 'otp_code': '', 'force_new_session': True,
    })
    assert result.status_code == 200, result.text
    return {'Authorization': 'Bearer ' + result.json()['data']['access_token']}


def directory(headers=None):
    result = client.get('/api/public/contests', headers=headers)
    assert result.status_code == 200
    assert 'no-store' in result.headers['cache-control']
    assert result.headers['vary'] == 'Authorization'
    return {item['contest_id'] for item in result.json()['data']}


@pytest.fixture
def context():
    now = now_utc()
    contest = store.create_contest(
        'Secret contest ' + uuid4().hex, 'Hidden host', 'Hidden description',
        start_at=now - timedelta(hours=1), end_at=now + timedelta(hours=2),
        freeze_at=now + timedelta(hours=1), status=ContestStatus.RUNNING,
        visibility=ContestVisibility.PRIVATE,
        visibility_after_end=ContestVisibility.PRIVATE,
    )
    cid = contest.contest_id
    owner = store.upsert_contest_operator(cid, f'owner-{uuid4().hex}@zoj.com', 'Owner')
    division = store.create_contest_division(cid, 'a', 'General')
    email = f'member-{uuid4().hex}@zoj.com'
    team = store.create_participant_team(cid, division.division_id, 'Team', 'Member', email, [])
    problem = store.create_problem(cid, division.division_id, 'A', 'Private problem', 'Private statement', 1000, 128, {}, 1)
    store.update_problem(cid, problem.problem_id, editorial='Private editorial')
    other = store.create_contest('Other', 'Host', '')
    outsider = store.upsert_contest_operator(other.contest_id, f'outsider-{uuid4().hex}@zoj.com', 'Outsider')
    return {'cid': cid, 'contest': contest, 'team': team, 'email': email, 'problem': problem,
            'owner': login(str(owner.email)), 'member': login(email), 'outsider': login(str(outsider.email))}


@pytest.mark.parametrize('phase', ['before', 'running', 'ended'])
@pytest.mark.parametrize('visibility,after_end', [('public', 'public'), ('private', 'public'), ('public', 'private'), ('private', 'private')])
def test_visibility_matrix_and_seo(context, phase, visibility, after_end):
    c = context
    now = now_utc()
    start, end = {
        'before': (now + timedelta(hours=1), now + timedelta(hours=4)),
        'running': (now - timedelta(hours=1), now + timedelta(hours=2)),
        'ended': (now - timedelta(hours=4), now - timedelta(hours=1)),
    }[phase]
    store.update_contest_settings(c['cid'], status=ContestStatus.OPEN if phase == 'before' else ContestStatus.RUNNING if phase == 'running' else ContestStatus.ENDED,
        start_at=start, end_at=end, freeze_at=end-timedelta(hours=1), visibility=visibility, visibility_after_end=after_end)
    public = (after_end if phase == 'ended' else visibility) == 'public'
    for auth in [None, c['outsider'], {'Authorization': 'Bearer invalid'}]:
        assert (c['cid'] in directory(auth)) == public
        response = client.get(f"/api/public/contests/{c['cid']}", headers=auth)
        assert response.status_code == (200 if public else 404)
        if not public:
            assert c['contest'].title not in response.text
    for auth in [c['member'], c['owner']]:
        assert c['cid'] in directory(auth)
        assert client.get(f"/api/public/contests/{c['cid']}", headers=auth).status_code == 200
    assert (c['cid'] in build_sitemap_xml()) == public
    metadata = build_page_metadata(f"/contests/{c['cid']}")
    assert metadata['status_code'] == (200 if public else 404)
    if not public:
        assert 'noindex' in metadata['robots']
        assert c['contest'].title not in str(metadata)
        assert c['cid'] not in str(build_page_metadata('/contests')['links'])


@pytest.mark.parametrize('status', [ContestStatus.DRAFT, ContestStatus.SCHEDULE_TBD, ContestStatus.SCHEDULED])
def test_lifecycle_hidden_states_are_still_operator_only(context, status):
    c = context
    now = now_utc()
    store.update_contest_settings(c['cid'], status=status, start_at=now+timedelta(days=1), end_at=now+timedelta(days=1,hours=3), visibility='public', visibility_after_end='public')
    for auth in [None, c['member']]:
        assert c['cid'] not in directory(auth)
        assert client.get(f"/api/public/contests/{c['cid']}", headers=auth).status_code == 404
        assert client.get(f"/api/contests/{c['cid']}/notices", headers=auth).status_code == 404
    profile = client.get('/api/auth/general/me', headers=c['member'])
    assert profile.status_code == 200
    assert c['cid'] not in {item['contest']['contest_id'] for item in profile.json()['data']['participant_contests']}
    # The account can still sign in; hiding a contest does not delete its membership.
    assert login(c['email'])
    assert client.get(f"/api/operator/contests/{c['cid']}/dashboard", headers=c['owner']).status_code == 200


def test_private_resources_cannot_be_reached_by_direct_urls_or_another_account(context):
    c = context
    # Even public resource flags must not bypass a private running contest.
    store.update_contest_settings(c['cid'], visibility_after_end='public', **{field: 'public' for field in RESOURCE_ACCESS_FIELDS})
    paths = ['/workspace', '/problems', '/scoreboard', '/submissions', '/notices', '/boards',
             f"/problems/{c['problem'].problem_id}", f"/problems/{c['problem'].problem_id}/assets"]
    for suffix in paths:
        path = f"/api/contests/{c['cid']}" + suffix
        assert client.get(path).status_code == 404, suffix
        assert client.get(path, headers=c['outsider']).status_code == 404, suffix
        assert client.get(path, headers=c['member']).status_code == 200, suffix
    # The legacy participant login path still allows members to obtain an OTP.
    otp = client.post(f"/api/contests/{c['cid']}/participant-login/otp/request", json={'email': c['email']})
    assert otp.status_code == 200, otp.text
    verified = client.post(f"/api/contests/{c['cid']}/participant-login/otp/verify", json={'email': c['email'], 'otp_code': '', 'force_new_session': True})
    auth = {'Authorization': 'Bearer ' + verified.json()['data']['access_token']}
    assert c['cid'] in directory(auth)
    assert client.get(f"/api/public/contests/{c['cid']}", headers=auth).status_code == 200
    assert client.get(f"/api/contests/{c['cid']}/problems", headers=auth).status_code == 200


def test_private_after_end_normalizes_every_resource_and_allows_settings_during_contest(context):
    c = context
    path = f"/api/operator/contests/{c['cid']}/settings"
    patch = {'visibility': 'public', 'visibility_after_end': 'private', **{field: 'public' for field in RESOURCE_ACCESS_FIELDS}}
    result = client.patch(path, headers=c['owner'], json=patch)
    assert result.status_code == 200, result.text
    assert result.json()['data']['status'] == 'running'
    assert all(result.json()['data'][field] == 'participants' for field in RESOURCE_ACCESS_FIELDS)
    assert client.patch(path, headers=c['outsider'], json={'visibility':'public'}).status_code == 403
    assert client.patch(path, headers=c['member'], json={'visibility':'public'}).status_code in {401,403}
    store.update_contest_settings(c['cid'], status=ContestStatus.ENDED)
    assert c['cid'] not in directory()
    for suffix in ['/problems', '/scoreboard', '/submissions', '/notices', '/boards', f"/problems/{c['problem'].problem_id}"]:
        path = f"/api/contests/{c['cid']}" + suffix
        assert client.get(path).status_code == 404, suffix
        assert client.get(path, headers=c['member']).status_code == 200, suffix
    detail = client.get(f"/api/contests/{c['cid']}/problems/{c['problem'].problem_id}", headers=c['member']).json()['data']
    assert detail['editorial'] == 'Private editorial'
    # Switching back to public does not silently re-publish all its materials.
    changed = store.update_contest_settings(c['cid'], visibility_after_end='public')
    assert all(getattr(changed, field) == 'participants' for field in RESOURCE_ACCESS_FIELDS)


def test_expired_or_revoked_membership_does_not_keep_private_contests_visible(context):
    c = context
    assert c['cid'] in directory(c['member'])
    fresh = login(c['email'])
    assert c['cid'] not in directory(c['member'])
    assert c['cid'] in directory(fresh)
    deleted, _ = store.delete_participant_team(c['cid'], c['team'].participant_team_id)
    assert deleted
    assert c['cid'] not in directory(fresh)
    assert client.get(f"/api/public/contests/{c['cid']}", headers=fresh).status_code == 404


def test_end_time_switches_visibility_without_an_operator_action(context, monkeypatch):
    import app.services.store as store_module
    import app.services.contest_visibility as visibility_module
    c = context
    now = now_utc()
    store.update_contest_settings(c['cid'], end_at=now+timedelta(minutes=1), visibility='public', visibility_after_end='private')
    assert c['cid'] in directory()
    after = now + timedelta(minutes=2)
    monkeypatch.setattr(store_module, 'now_utc', lambda: after)
    monkeypatch.setattr(visibility_module, 'now_utc', lambda: after)
    assert c['cid'] not in directory()
    assert c['cid'] in directory(c['member'])


def test_admin_creation_accepts_visibility_and_preserves_public_defaults():
    admin = login('test3@zoj.com')
    result = client.post('/api/admin/contests', headers=admin, json={'organization_name':'Host', 'visibility':'private', 'visibility_after_end':'private'})
    assert result.status_code == 200, result.text
    contest = result.json()['data']
    assert (contest['visibility'], contest['visibility_after_end'], contest['status']) == ('private', 'private', 'draft')
    assert contest['notice_access_after_end'] == 'participants'
    default = client.post('/api/admin/contests', headers=admin, json={'organization_name':'Host'}).json()['data']
    assert default['visibility'] == default['visibility_after_end'] == 'public'
    assert client.post('/api/admin/contests', headers=admin, json={'organization_name':'Host', 'visibility':'participants'}).status_code == 422


def test_first_request_after_start_includes_registered_private_contest(context):
    from app.orm_models import ContestRow
    c = context
    # Simulate a scheduled row that has not yet been refreshed by another request.
    with store._session() as db:
        row = db.get(ContestRow, c['cid'])
        row.status = 'scheduled'
        db.commit()
    assert c['cid'] in directory(c['member'])
    assert client.get(f"/api/public/contests/{c['cid']}", headers=c['member']).status_code == 200
