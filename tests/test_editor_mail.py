import base64
import hashlib
import hmac
import json
import time
from uuid import uuid4
import pytest
from pydantic import SecretStr
from sqlalchemy import select
from app.database import SessionLocal
from app.orm_models import MailQueueItemRow
from app.services.mail_logs import mail_log_preview
from app.settings import settings
from test_problem_archives import client

KEY = 'separate-editor-mail-relay-test-key-at-least-32-characters'
PATH = '/api/editor/mail/verification'


def signed(email, body, purpose='zoj-editor-mail-v1', path=PATH):
    encoded = base64.urlsafe_b64encode(email.encode()).decode().rstrip('=')
    stamp, nonce, sha = str(int(time.time())), uuid4().hex, hashlib.sha256(body).hexdigest()
    message = '\n'.join([purpose,'POST',path,stamp,nonce,encoded,sha])
    return {'X-Editor-Identity':encoded,'X-Editor-Timestamp':stamp,'X-Editor-Nonce':nonce,'X-Editor-Content-Sha256':sha,
        'X-Editor-Signature':hmac.new(KEY.encode(),message.encode(),hashlib.sha256).hexdigest()}


@pytest.fixture(autouse=True)
def config(monkeypatch):
    monkeypatch.setattr(settings, 'editor_mail_relay_secret', SecretStr(KEY))
    monkeypatch.setattr(settings, 'editor_auth_bridge_secret', SecretStr(KEY + '-auth'))


def test_fixed_mail_template_signature_replay_tampering_and_preview():
    email = uuid4().hex + '@example.com'
    body = json.dumps({'code':'123456','purpose':'password_reset'}).encode()
    headers = signed(email,body)
    result = client.post(PATH,content=body,headers=headers)
    assert result.status_code == 200, result.text
    mail_id = result.json()['data']['delivery_id']
    with SessionLocal() as db:
        row = db.get(MailQueueItemRow,mail_id)
        assert row.recipient_email == email and row.mail_type == 'editor_auth_otp'
        assert 'ZOJ Editor' in row.subject and '123456' in row.body_text
        assert '비밀번호 재설정' in row.body_html
    assert mail_log_preview(mail_id)['restricted'] is True
    assert client.post(PATH,content=body,headers=headers).status_code == 401
    assert client.post(PATH,content=body,headers=signed(email,body,purpose='zoj-editor-v1')).status_code == 401
    assert client.post(PATH,content=b'{}',headers=signed(email,body)).status_code == 401
    invalid = json.dumps({'code':'123456','subject':'Send anything','html':'<script>x</script>'}).encode()
    assert client.post(PATH,content=invalid,headers=signed(email,invalid)).status_code == 422


def test_mail_quotas_are_persisted_and_no_unbounded_mail_is_queued():
    email = uuid4().hex + '@example.com'
    body = b'{"code":"123456","purpose":"login"}'
    for _ in range(10):
        assert client.post(PATH,content=body,headers=signed(email,body)).status_code == 200
    assert client.post(PATH,content=body,headers=signed(email,body)).status_code == 429
    with SessionLocal() as db:
        assert len(db.scalars(select(MailQueueItemRow).where(MailQueueItemRow.recipient_email==email)).all()) == 10


ACCESS_PATH = '/api/editor/mail/access'


def access_headers(email, body, purpose='zoj-editor-access-mail-v1'):
    headers = signed(email, body, purpose, ACCESS_PATH)
    canonical = '\n'.join([purpose, 'POST', ACCESS_PATH, headers['X-Editor-Timestamp'],
        headers['X-Editor-Nonce'], headers['X-Editor-Identity'], headers['X-Editor-Content-Sha256']])
    headers['X-Editor-Signature'] = hmac.new((KEY + '-auth').encode(), canonical.encode(), hashlib.sha256).hexdigest()
    return headers


def access_request(email, values):
    body = json.dumps(values, ensure_ascii=False).encode()
    return client.post(ACCESS_PATH, content=body, headers=access_headers(email, body))


def access_values(kind='problem_access'):
    return dict(event_id=uuid4().hex, kind=kind, resource_id=uuid4().hex,
        resource_name='자료 <script>unsafe</script> "문제"', actor_name='출제자 <img src=x>',
        permissions=['execute', 'issues'])


@pytest.mark.parametrize('kind', ['problem_access', 'folder_invitation', 'folder_access'])
def test_access_templates_and_delivery_retries_are_idempotent(kind):
    email = uuid4().hex + '@example.com'
    values = access_values(kind)
    if kind == 'folder_invitation':
        values['expires_at'] = int(time.time()) + 604800
    first = access_request(email, values)
    assert first.status_code == 200, first.text
    mail_id = first.json()['data']['delivery_id']
    second = access_request(email, {**values, 'permissions':list(reversed(values['permissions']))})
    assert second.status_code == 200, second.text
    assert second.json()['data']['delivery_id'] == mail_id
    with SessionLocal() as db:
        row = db.get(MailQueueItemRow, mail_id)
        assert row.recipient_email == email and row.mail_type == 'editor_access'
        assert '검증 실행' in row.body_text and '이슈 작성' in row.body_text
        assert '<script>unsafe</script>' not in row.body_html
        assert '&lt;script&gt;unsafe&lt;/script&gt;' in row.body_html
        assert '<img src=x>' not in row.body_html
        assert 'https://editor.zoj.kr/problems' in row.body_html
        if kind == 'folder_invitation':
            assert '수락' in row.body_text and '초대 만료' in row.body_text
        elif kind == 'folder_access':
            assert '?folder=' + values['resource_id'] in row.body_text
        else:
            assert '/problems/' + values['resource_id'] in row.body_text
        assert len(db.scalars(select(MailQueueItemRow).where(MailQueueItemRow.recipient_email == email)).all()) == 1
    assert mail_log_preview(mail_id)['restricted'] is False
    assert access_request(email, {**values, 'actor_name':'Someone else'}).status_code == 409
    assert access_request(uuid4().hex + '@example.com', values).status_code == 409


def test_access_signature_is_separate_from_login_and_authentication_mail():
    email = uuid4().hex + '@example.com'; values = access_values()
    body = json.dumps(values).encode(); headers = access_headers(email, body)
    assert client.post(ACCESS_PATH, content=body, headers=headers).status_code == 200
    assert client.post(ACCESS_PATH, content=body, headers=headers).status_code == 401
    for purpose in ('zoj-shared-auth-v1', 'zoj-editor-mail-v1', 'zoj-editor-v1'):
        assert client.post(ACCESS_PATH, content=body, headers=access_headers(email, body, purpose)).status_code == 401
    assert client.post(ACCESS_PATH, content=b'{}', headers=access_headers(email, body)).status_code == 401
    assert client.post(ACCESS_PATH, content=body, headers=signed(email, body, 'zoj-editor-access-mail-v1', ACCESS_PATH)).status_code == 401


@pytest.mark.parametrize('patch', [
    {'resource_id':'https://evil.example'}, {'event_id':'not-an-id'},
    {'actor_name':'Name\r\nBcc: other@example.com'}, {'permissions':['admin']},
    {'permissions':['execute','execute']}, {'permissions':['manage_folders']},
    {'html':'<p>Anything</p>'}, {'url':'https://evil.example'},
    {'kind':'folder_invitation'}, {'expires_at':int(time.time()) + 100},
])
def test_access_relay_rejects_arbitrary_mail_content(patch):
    email = uuid4().hex + '@example.com'
    result = access_request(email, {**access_values(), **patch})
    assert result.status_code == 422, result.text
    with SessionLocal() as db:
        assert not db.scalar(select(MailQueueItemRow).where(MailQueueItemRow.recipient_email == email))


def test_access_notification_quotas_are_separate_from_login_mail(monkeypatch):
    from app.routers import editor_mail
    calls = []
    monkeypatch.setattr(editor_mail, 'consume', lambda *args: calls.append(args))
    email = uuid4().hex + '@example.com'
    assert access_request(email, access_values()).status_code == 200
    assert calls == [('editor-access-mail-email', email, 100, 3600), ('editor-access-mail-global', 'editor', 5000, 3600)]
