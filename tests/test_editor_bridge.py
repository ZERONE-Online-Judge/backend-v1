import base64
import hashlib
import hmac
import json
import time
from uuid import uuid4
from urllib.parse import urlencode
import pytest
from pydantic import SecretStr
from sqlalchemy import select, func
from fastapi.testclient import TestClient
from app.main import app
from app.database import SessionLocal
from app.orm_models import EditorImportRow, OperationalAuditLogRow, StaffAccountRow, ProblemRow
from app.settings import settings
from test_problem_archives import context, export

client=TestClient(app)
KEY='editor-bridge-test-secret-at-least-32-characters'

@pytest.fixture(autouse=True)
def config(monkeypatch):
    monkeypatch.setattr(settings,'editor_bridge_secret',SecretStr(KEY))

def headers(method,path,email,content=b'',stamp=None,nonce=None):
    ident=base64.urlsafe_b64encode(email.lower().encode()).decode().rstrip('=')
    stamp=str(stamp or int(time.time()));nonce=nonce or uuid4().hex
    sha=hashlib.sha256(content).hexdigest()
    text='\n'.join(['zoj-editor-v1',method,path,stamp,nonce,ident,sha])
    return {'X-Editor-Identity':ident,'X-Editor-Timestamp':stamp,'X-Editor-Nonce':nonce,'X-Editor-Content-Sha256':sha,
            'X-Editor-Signature':hmac.new(KEY.encode(),text.encode(),hashlib.sha256).hexdigest(),'Content-Type':'application/zip'}

def email_for(context,reviewer=False):
    with SessionLocal() as db:
        rows=db.scalars(select(StaffAccountRow).where(StaffAccountRow.contest_scopes.contains(context['cid']))).all()
        return next(r.email for r in rows if ('review-' in r.email)==reviewer)

def target(c,operation=None,code='B'):
    query=urlencode({'division_id':c['division'],'problem_code':code,'operation_id':operation or uuid4().hex})
    return f'/api/editor/contests/{c["cid"]}/imports?{query}'

def test_signed_identity_replay_expiry_tampering_and_scope(context):
    c=context;email=email_for(c);path='/api/editor/contests'
    h=headers('GET',path,email)
    result=client.get(path,headers=h);assert result.status_code==200,result.text
    assert any(x['id']==c['cid'] for x in result.json()['data']['contests'])
    assert client.get(path,headers=h).status_code==401
    assert client.get(path,headers=headers('GET',path,email,stamp=int(time.time())-200)).status_code==401
    assert client.get(path,headers={**headers('GET',path,email),'X-Editor-Identity':base64.urlsafe_b64encode(b'attacker@example.com').decode().rstrip('=')}).status_code==401
    reviewer=email_for(c,True)
    r=client.get(path,headers=headers('GET',path,reviewer));assert r.status_code==200
    assert all(x['id']!=c['cid'] for x in r.json()['data']['contests'])
    content=export(c);path=target(c)
    assert client.post(path,content=content,headers=headers('POST',path,reviewer,content)).status_code==403
    assert client.post(path,content=content,headers=headers('POST',path,email,b'other')).status_code==401


def test_atomic_receipt_retry_revoked_permission_and_audit(context):
    c=context;email=email_for(c);content=export(c);path=target(c)
    first=client.post(path,content=content,headers=headers('POST',path,email,content));assert first.status_code==200,first.text
    pid=first.json()['data']['problem_id']
    retry=client.post(path,content=content,headers=headers('POST',path,email,content));assert retry.status_code==200,retry.text
    assert retry.json()['data']['problem_id']==pid
    changed=path.replace('problem_code=B','problem_code=C')
    assert client.post(changed,content=content,headers=headers('POST',changed,email,content)).status_code==409
    with SessionLocal() as db:
        assert db.scalar(select(func.count()).select_from(EditorImportRow).where(EditorImportRow.problem_id==pid))==1
        assert db.scalar(select(func.count()).select_from(OperationalAuditLogRow).where(OperationalAuditLogRow.action=='Editor 문제 가져오기',OperationalAuditLogRow.contest_id==c['cid']))==1
        account=db.scalar(select(StaffAccountRow).where(StaffAccountRow.email==email));account.contest_scopes='{}';account.contest_roles='{}';db.commit()
    assert client.post(path,content=content,headers=headers('POST',path,email,content)).status_code==403
