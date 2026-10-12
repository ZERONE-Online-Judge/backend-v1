import hashlib
import io
from contextlib import contextmanager
from uuid import uuid4
import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import func, select
from app.database import SessionLocal
from app.orm_models import EditorImportRow, OperationalAuditLogRow, StaffAccountRow
from app.services import editor_library as transport
from app.services.errors import AppError
from app.services.store import store
from app.settings import settings
from test_problem_archives import client, context, export
from test_editor_bridge import email_for

PID, JOB = 'a' * 32, 'b' * 32


@pytest.fixture(autouse=True)
def configuration(monkeypatch):
    monkeypatch.setattr(settings, 'editor_api_url', 'https://editor.zoj.kr/api')
    monkeypatch.setattr(settings, 'editor_bridge_secret', SecretStr('editor-library-test-key-at-least-32-characters'))


def test_library_permissions_identity_and_fixed_signed_transport(context, monkeypatch):
    c = context
    path = c['prefix'] + '/editor-problems'
    calls = []
    def remote(method, route, email, values=None):
        calls.append((method, route, email, values))
        return {'email': email, 'problems': [], 'folders': [], 'total': 0}
    monkeypatch.setattr(transport, 'request_json', remote)
    assert client.get(path).status_code == 401
    assert client.get(path, headers=c['reviewer']).status_code == 403
    response = client.get(path + '?q=hello&folder=root', headers=c['owner'])
    assert response.status_code == 200
    assert calls == [('GET', '/api/zoj-library/problems?q=hello&offset=0&folder=root', email_for(c), None)]
    signed_url, headers, _ = transport.client_settings('GET', calls[0][1], email_for(c))
    assert signed_url.startswith('https://editor.zoj.kr/api/')
    assert 'Authorization' not in headers and 'Cookie' not in headers
    monkeypatch.setattr(transport, 'request_json', lambda *args: {'email':'foreign@example.com'})
    assert client.get(path, headers=c['owner']).status_code == 502
    monkeypatch.setattr(settings, 'editor_api_url', '')
    assert client.get(path, headers=c['owner']).json()['data']['configured'] is False


def test_import_is_atomic_retryable_and_audited_once_per_request(context, monkeypatch):
    c = context
    archive = export(c)
    sha = hashlib.sha256(archive).hexdigest()
    calls = []
    def status(method, path, email, values=None):
        calls.append(email)
        return {'status':'completed', 'revision':4, 'package': {'format':'zoj', 'verified':True, 'sha256':sha}}
    @contextmanager
    def download(path, email, digest):
        assert email == email_for(c) and digest == sha
        yield io.BytesIO(archive)
    monkeypatch.setattr(transport, 'request_json', status)
    monkeypatch.setattr(transport, 'download_package', download)
    path = c['prefix'] + f'/editor-problems/{PID}/import'
    values = {'job_id':JOB, 'division_id':c['division'], 'problem_code':'B'}
    assert client.post(path, headers=c['reviewer'], json=values).status_code == 403
    assert client.post(path, headers=c['owner'], json={**values, 'email':'foreign@example.com'}).status_code == 422
    response = client.post(path, headers=c['owner'], json=values)
    assert response.status_code == 200, response.text
    problem_id = response.json()['data']['problem_id']
    with SessionLocal() as db:
        assert db.scalar(select(func.count()).select_from(EditorImportRow).where(EditorImportRow.problem_id==problem_id)) == 1
        audit = db.scalars(select(OperationalAuditLogRow).where(OperationalAuditLogRow.contest_id==c['cid'], OperationalAuditLogRow.action=='Editor 문제 가져오기', OperationalAuditLogRow.status_code==200)).all()
        assert len(audit) == 1
    retry = client.post(path, headers=c['owner'], json=values)
    assert retry.status_code == 200 and retry.json()['data']['problem_id'] == problem_id
    assert len([p for p in store.problems.values() if p.contest_id==c['cid']]) == 2
    assert client.post(path, headers=c['owner'], json={**values, 'division_id':str(uuid4()), 'problem_code':'C'}).status_code == 404
    with SessionLocal() as db:
        row = db.scalar(select(StaffAccountRow).where(StaffAccountRow.email==email_for(c)))
        row.contest_scopes = '{}'
        row.contest_roles = '{}'
        db.commit()
    assert client.post(path, headers=c['owner'], json=values).status_code == 403


def test_revoke_permission_during_download_prevents_import(context, monkeypatch):
    c = context
    archive = export(c)
    monkeypatch.setattr(transport, 'request_json', lambda *args: {'status':'completed', 'package':{'format':'zoj','verified':True,'sha256':hashlib.sha256(archive).hexdigest()}})
    @contextmanager
    def download(*args):
        with SessionLocal() as db:
            row = db.scalar(select(StaffAccountRow).where(StaffAccountRow.email==email_for(c)))
            row.contest_scopes = '{}'
            row.contest_roles = '{}'
            db.commit()
        yield io.BytesIO(archive)
    monkeypatch.setattr(transport, 'download_package', download)
    result = client.post(c['prefix'] + f'/editor-problems/{PID}/import', headers=c['owner'], json={'job_id':JOB,'division_id':c['division'],'problem_code':'B'})
    assert result.status_code == 403
    assert len([p for p in store.problems.values() if p.contest_id==c['cid']]) == 1


def test_unverified_metadata_cannot_reach_download(context, monkeypatch):
    c = context
    monkeypatch.setattr(transport, 'request_json', lambda *args: {'status':'completed','package':{'format':'zoj','verified':False}})
    def forbidden(*args):
        raise AssertionError('Must not fetch an unverified package')
    monkeypatch.setattr(transport, 'download_package', forbidden)
    result = client.post(c['prefix'] + f'/editor-problems/{PID}/import', headers=c['owner'], json={'job_id':JOB,'division_id':c['division'],'problem_code':'B'})
    assert result.status_code == 409


def test_streaming_transport_rejects_redirects_wrong_hash_and_bad_json(monkeypatch):
    real_client = httpx.Client
    replies = []
    def reply(request):
        assert 'authorization' not in request.headers and 'cookie' not in request.headers
        return replies.pop(0)
    mock = httpx.MockTransport(reply)
    monkeypatch.setattr(transport.httpx, 'Client', lambda **kwargs: real_client(transport=mock, **kwargs))
    replies.append(httpx.Response(200, json=[]))
    with pytest.raises(AppError) as error:
        transport.request_json('GET', '/api/zoj-library/problems', 'staff@example.com')
    assert error.value.code == 'editor_response_invalid'
    replies.append(httpx.Response(302, headers={'Location':'https://other.example/'}))
    with pytest.raises(AppError):
        transport.request_json('GET', '/api/zoj-library/problems', 'staff@example.com')
    replies.append(httpx.Response(200, content=b'changed', headers={'Content-Type':'application/zip'}))
    with pytest.raises(AppError) as error:
        with transport.download_package('/api/zoj-library/problems/file', 'staff@example.com', '0'*64):
            raise AssertionError('Bad hash must never be handed to importer')
    assert error.value.code == 'editor_archive_changed'
