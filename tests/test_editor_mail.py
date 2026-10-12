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


def signed(email, body, purpose='zoj-editor-mail-v1'):
    encoded = base64.urlsafe_b64encode(email.encode()).decode().rstrip('=')
    stamp, nonce, sha = str(int(time.time())), uuid4().hex, hashlib.sha256(body).hexdigest()
    message = '\n'.join([purpose,'POST',PATH,stamp,nonce,encoded,sha])
    return {'X-Editor-Identity':encoded,'X-Editor-Timestamp':stamp,'X-Editor-Nonce':nonce,'X-Editor-Content-Sha256':sha,
        'X-Editor-Signature':hmac.new(KEY.encode(),message.encode(),hashlib.sha256).hexdigest()}


@pytest.fixture(autouse=True)
def config(monkeypatch):
    monkeypatch.setattr(settings, 'editor_mail_relay_secret', SecretStr(KEY))


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
