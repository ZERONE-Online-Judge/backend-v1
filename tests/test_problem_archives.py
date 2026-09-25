import hashlib
import io
import json
import os
import stat
import zipfile
from datetime import timedelta
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

os.environ.setdefault('ENABLE_DEMO_SEED', 'true')
os.environ.setdefault('ALLOW_EMPTY_OTP', 'true')
from app.main import app
from app.database import SessionLocal
from app.models import ContestStatus, now_utc
from app.orm_models import ProblemAssetRow, ProblemRow, TestcaseRow as CaseRow, TestcaseSetRow as CaseSetRow
from app.services.store import store
from app.services.storage import object_storage
from app.services import problem_archive as archive_service

client = TestClient(app)


def login(email):
    response = client.post('/api/auth/general/otp/verify', json={'email': email, 'otp_code': '', 'force_new_session': True})
    assert response.status_code == 200, response.text
    return {'Authorization': 'Bearer ' + response.json()['data']['operator_session']['access_token']}


@pytest.fixture
def context():
    marker = uuid4().hex
    contest = store.create_contest('문제 백업 '+marker, 'ZOJ', '', now_utc()+timedelta(days=3), status=ContestStatus.OPEN)
    cid = contest.contest_id
    owner = store.upsert_contest_operator(cid, f'archive-{marker}@example.com', '출제자', ['master'])
    reviewer = store.upsert_contest_operator(cid, f'review-{marker}@example.com', '검수자', ['problem_reviewer'])
    division = store.create_contest_division(cid, 'A', '초등부', '', 1)
    target = store.create_contest_division(cid, 'B', '중등부', '', 2)
    problem = store.create_problem(cid, division.division_id, 'A', '한글 파일 복원', '', 1500, 256, {'java8': {'time_limit_ms': 3000}}, 1)
    pid = problem.problem_id
    contents = {'image': b'\x89PNG\r\n\x1a\nfixture', 'checker': b'int main(){return 0;}\n', 'resource': b'// testlib fixture\n',
                'solution': b'print(sum(map(int,input().split())))\n', 'old-input': b'1 2\r\n', 'old-output': b'3\r\n', 'new-input': b'5 6\n\x00', 'new-output': b''}
    keys = {name: f'contests/{cid}/problems/{pid}/'+({'image':'assets/image.png','checker':'package-files/checker/checker.cpp','resource':'support/package-resource/testlib.h','solution':'verification-solutions/wrong_answer/wrong.py'}.get(name, 'testcases/'+name)) for name in contents}
    for name, content in contents.items(): object_storage.write_bytes(keys[name], content)
    with SessionLocal() as db:
        ids = {}
        for name, filename, mime in [('image','그림.png','image/png'),('checker','checker.cpp','text/plain'),('resource','testlib.h','text/plain'),('solution','wrong.py','text/plain')]:
            aid = str(uuid4());ids[name]=aid
            db.add(ProblemAssetRow(asset_id=aid, contest_id=cid, problem_id=pid, original_filename=filename, storage_key=keys[name], mime_type=mime, file_size=len(contents[name]), sha256=hashlib.sha256(contents[name]).hexdigest(), asset_status='active'))
        row = db.get(ProblemRow,pid)
        row.statement = '<!--ZOJ_META:'+json.dumps({'inputDescription':'두 수', 'outputDescription':'합', 'note':'보충', 'examples':[{'input':'1 2\n','output':'3\n','note':'예제'}]},ensure_ascii=False)+'-->\n본문 ![그림](asset://'+ids['image']+')\n![이전 링크](/api/storage/objects/'+keys['image']+'?expires=1&signature=old)'
        row.editorial='해설 ![풀이 그림](asset://'+ids['image']+')'
        for version in [1,2]:
            sid=str(uuid4());db.add(CaseSetRow(testcase_set_id=sid,problem_id=pid,version=version,is_active=version==2));db.flush()
            prefix='old' if version==1 else 'new'
            db.add(CaseRow(testcase_set_id=sid,display_order=1,input_storage_key=keys[prefix+'-input'],output_storage_key=keys[prefix+'-output'],input_sha256=hashlib.sha256(contents[prefix+'-input']).hexdigest(),output_sha256=hashlib.sha256(contents[prefix+'-output']).hexdigest(),time_limit_ms_override=2000,memory_limit_mb_override=128))
        db.commit()
    return {'cid':cid,'pid':pid,'division':target.division_id,'owner':login(str(owner.email)),'reviewer':login(str(reviewer.email)), 'keys':keys,'contents':contents,'prefix':f'/api/operator/contests/{cid}'}


def export(c):
    response=client.get(c['prefix']+'/problems/'+c['pid']+'/archive',headers=c['owner'])
    assert response.status_code==200,response.text
    assert response.headers['content-type']=='application/zip'
    return response.content


def upload(c,content,action='inspect',code='B',division=None,headers=None):
    params={'division_id':division or c['division'],'problem_code':code} if action=='import' else {}
    return client.post(c['prefix']+'/problem-archives:'+action,params=params,content=content,headers={**(headers or c['owner']),'Content-Type':'application/zip'})


def mutate_zip(content, change):
    with zipfile.ZipFile(io.BytesIO(content)) as source: files={name:source.read(name) for name in source.namelist()}
    manifest=json.loads(files['manifest.json']);change(manifest,files);files['manifest.json']=json.dumps(manifest,ensure_ascii=False).encode()
    result=io.BytesIO()
    with zipfile.ZipFile(result,'w',zipfile.ZIP_DEFLATED) as out:
        for name,data in files.items():out.writestr(name,data)
    return result.getvalue()


def test_round_trip_restores_all_bytes_metadata_versions_and_references(context):
    c=context;content=export(c)
    preview=upload(c,content);assert preview.status_code==200,preview.text
    details=preview.json()['data'];assert details['example_count']==1;assert details['asset_count']==4;assert details['testcase_count']==2
    assert [v['is_active'] for v in details['testcase_sets']]==[False,True]
    imported=upload(c,content,'import');assert imported.status_code==200,imported.text
    problem=imported.json()['data'];assert problem['division_id']==c['division'];assert problem['problem_code']=='B';assert problem['language_resource_limits']['java8']['time_limit_ms']==3000
    assert c['pid'] not in problem['statement'];assert 'expires=' not in problem['statement']
    assets=store.problem_assets_for_problem(c['cid'],problem['problem_id']);image=next(a for a in assets if a.mime_type=='image/png')
    assert f'asset://{image.asset_id}' in problem['statement'] and f'asset://{image.asset_id}' in problem['editorial']
    assert len(assets)==4
    assert any('/verification-solutions/wrong_answer/' in a.storage_key for a in assets)
    assert any('/package-files/package-resource/' in a.storage_key for a in assets)
    for asset in assets:assert hashlib.sha256(object_storage.read_bytes(asset.storage_key)).hexdigest()==asset.sha256
    versions=store.testcase_sets_for_problem(c['cid'],problem['problem_id'])
    for item,prefix in zip(versions,['old','new']):
        case=item['testcases'][0];assert object_storage.read_bytes(case['input_storage_key'])==c['contents'][prefix+'-input'];assert object_storage.read_bytes(case['output_storage_key'])==c['contents'][prefix+'-output'];assert case['time_limit_ms_override']==2000;assert case['memory_limit_mb_override']==128
    # The imported copy can be exported and imported again, even in another contest.
    second=export({**c,'pid':problem['problem_id']})
    again=upload(c,second,'import',code='C');assert again.status_code==200,again.text


def test_permissions_contest_scope_lock_and_duplicate_codes(context):
    c=context;content=export(c)
    assert client.get(c['prefix']+'/problems/'+c['pid']+'/archive',headers=c['reviewer']).status_code==403
    assert upload(c,content,headers=c['reviewer']).status_code==403
    assert client.get(c['prefix']+'/problems/'+c['pid']+'/archive').status_code==401
    other=store.create_contest('다른 대회', 'ZOJ', '', now_utc()+timedelta(days=3),status=ContestStatus.OPEN)
    assert client.get(f'/api/operator/contests/{other.contest_id}/problems/{c["pid"]}/archive',headers=c['owner']).status_code==403
    assert upload(c,content,'import',division=str(uuid4())).status_code==404
    first=upload(c,content,'import');assert first.status_code==200,first.text
    assert upload(c,content,'import').status_code==409
    store.update_contest_settings(c['cid'],status='running',start_at=now_utc()-timedelta(minutes=1),end_at=now_utc()+timedelta(hours=1))
    assert upload(c,content,'import',code='C').status_code==409


@pytest.mark.parametrize('kind',['hash','missing','traversal','version','active','reference','unknown','duplicate_json','symlink'])
def test_invalid_archives_do_not_create_any_problem(context,kind):
    c=context;content=export(c)
    def change(m,files):
        if kind=='hash':files[m['files'][0]['path']]=b'wrong'
        if kind=='missing':del files[m['files'][0]['path']]
        if kind=='traversal':files['../evil']=b'bad'
        if kind=='version':m['version']=999
        if kind=='active':m['testcase_sets'][0]['is_active']=True
        if kind=='reference':m['problem']['statement']='![x](asset://asset-999)'
        if kind=='unknown':m['files'][0]['url']='http://127.0.0.1/secret'
    content=mutate_zip(content,change)
    if kind in {'duplicate_json','symlink'}:
        buffer=io.BytesIO()
        with zipfile.ZipFile(io.BytesIO(content)) as source,zipfile.ZipFile(buffer,'w') as output:
            for info in source.infolist():
                data=source.read(info)
                if kind=='duplicate_json' and info.filename=='manifest.json':data=data.replace(b'"version": 1',b'"version": 1, "version": 1',1)
                if kind=='symlink' and info.filename!='manifest.json':info.external_attr=(stat.S_IFLNK|0o777)<<16
                output.writestr(info,data)
        content=buffer.getvalue()
    response=upload(c,content,'import');assert response.status_code==422,response.text
    assert len([p for p in store.problems.values() if p.contest_id==c['cid']])==1


def test_storage_failure_rolls_back_database_and_uploaded_objects(context,monkeypatch):
    c=context;content=export(c);written=[];real_write=object_storage.write_stream
    def fail(key,*args):
        written.append(key)
        real_write(key,*args)
        if len(written)==2:raise OSError('simulated storage outage')
    monkeypatch.setattr(object_storage,'write_stream',fail)
    assert upload(c,content,'import').status_code == 503
    assert len([p for p in store.problems.values() if p.contest_id==c['cid']])==1
    assert written and all(object_storage.size_bytes(key) is None for key in written)


def test_missing_source_file_blocks_export_and_limits_block_upload(context,monkeypatch):
    c=context;object_storage.delete(c['keys']['image'])
    response=client.get(c['prefix']+'/problems/'+c['pid']+'/archive',headers=c['owner'])
    assert response.status_code==409,response.text
    from app.routers import problem_archives
    monkeypatch.setattr(problem_archives,'MAX_UPLOAD',10)
    assert upload(c,b'X'*11).status_code==413


def test_import_can_target_another_contest_without_reusing_source_files(context):
    c=context;content=export(c)
    other=store.create_contest('다른 대회로 복원', 'ZOJ', '', now_utc()+timedelta(days=4),status=ContestStatus.OPEN)
    cid=other.contest_id
    owner=store.upsert_contest_operator(cid,f'new-archive-{uuid4().hex}@example.com','새 출제자',['master'])
    division=store.create_contest_division(cid,'NEW','새 유형')
    target={**c,'prefix':f'/api/operator/contests/{cid}','division':division.division_id,'owner':login(str(owner.email))}
    response=upload(target,content,'import',code='NEW');assert response.status_code==200,response.text
    pid=response.json()['data']['problem_id']
    assert pid!=c['pid']
    assets=store.problem_assets_for_problem(cid,pid)
    assert all(a.storage_key.startswith(f'contests/{cid}/problems/{pid}/') for a in assets)
    assert all(c['cid'] not in a.storage_key for a in assets)
    assert len(store.testcase_sets_for_problem(cid,pid))==2


def test_unpacked_size_limit_and_missing_asset_reference_are_rejected(context,monkeypatch):
    c=context;content=export(c)
    monkeypatch.setattr(archive_service,'MAX_UNPACKED',100)
    assert upload(c,content,'import').status_code==422
    assert len([p for p in store.problems.values() if p.contest_id==c['cid']])==1


def test_failed_import_is_audited_once_without_archived_content(context):
    c=context;response=upload(c,b'not a zip','import');assert response.status_code==422
    logs=client.get(c['prefix']+'/audit-logs',headers=c['owner']).json()['data']
    events=[event for event in logs if event['path'].endswith('problem-archives:import')]
    assert len(events)==1
    assert events[0]['details']['change_kind']=='failed'
    assert 'body' not in events[0]['details']


def test_contest_start_during_file_copy_rolls_back_import(context,monkeypatch):
    c=context;content=export(c);written=[];real_write=object_storage.write_stream
    contest=store.contests[c['cid']]
    def write(key,*args):
        real_write(key,*args);written.append(key)
        monkeypatch.setattr(archive_service,'now_utc',lambda:contest.start_at+timedelta(seconds=1))
    monkeypatch.setattr(object_storage,'write_stream',write)
    response=upload(c,content,'import');assert response.status_code==409,response.text
    assert len([p for p in store.problems.values() if p.contest_id==c['cid']])==1
    assert written and all(object_storage.size_bytes(key) is None for key in written)
