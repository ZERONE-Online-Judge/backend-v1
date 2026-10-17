"""Fixed-purpose Editor mail templates over signed private server transport."""
import hashlib
import json
from typing import Literal
from fastapi import APIRouter, Request
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator
from uuid import NAMESPACE_URL, uuid5
from app.services.editor_bridge import identity
from app.services.auth_rate_limit import consume
from app.services.errors import AppError
from app.services.mail_templates import render_branded_email
from app.services.mail_templates import format_korean_datetime
from datetime import datetime, timezone
from app.services.responses import ok
from app.services.store import store
from app.settings import settings

router = APIRouter(prefix='/editor/mail', tags=['Editor mail relay'])


class VerificationMail(BaseModel):
    model_config = ConfigDict(extra='forbid')
    code: str = Field(pattern=r'^[0-9]{6}$')
    purpose: Literal['login', 'password_set', 'password_reset'] = 'login'


@router.post('/verification')
async def verification_mail(request: Request):
    key = settings.editor_mail_relay_secret.get_secret_value() if settings.editor_mail_relay_secret else ''
    email, expected = identity(request, secret_override=key, purpose='zoj-editor-mail-v1')
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > 4096:
            raise AppError(413, 'editor_mail_too_large', '요청 크기가 너무 큽니다.')
    if hashlib.sha256(body).hexdigest() != expected:
        raise AppError(401, 'editor_mail_changed', '메일 요청 내용이 변경되었습니다.')
    try:
        values = VerificationMail.model_validate(json.loads(body))
    except (ValueError, ValidationError):
        raise AppError(422, 'editor_mail_invalid', '메일 인증 요청 형식을 확인하세요.')
    consume('editor-mail-email', email, 10, 3600)
    consume('editor-mail-global', 'editor', 1000, 3600)
    action = {'login':'로그인', 'password_set':'비밀번호 등록·변경', 'password_reset':'비밀번호 재설정'}[values.purpose]
    subject = '[ZOJ Editor] ' + action + ' 인증번호'
    body_text = f'ZOJ Editor {action} 인증번호: {values.code}\n\n5분 동안 한 번 사용할 수 있습니다.\n요청하지 않았다면 이 메일을 무시하세요.'
    content = render_branded_email(eyebrow='ZOJ EDITOR', title=action + ' 인증번호',
        preheader='ZOJ Editor ' + action + ' 인증번호가 도착했습니다.',
        body=['아래 인증번호를 ZOJ Editor 화면에 입력하세요.', '요청하지 않았다면 이 메일을 무시하세요.'],
        verification_code=values.code, verification_hint='5분 동안 한 번 사용할 수 있습니다.')
    queued = store.enqueue_mail('editor_auth_otp', email, subject, body_text, content)
    return ok(request, {'queued':True, 'delivery_id':queued.mail_queue_id})


PERMISSIONS = {
    'edit_info': '기본 정보 수정', 'edit_statements': '지문 수정',
    'edit_files': '파일·리소스 수정', 'edit_solutions': '풀이 수정',
    'edit_tests': '테스트 수정', 'edit_materials': '검수 자료 수정',
    'translate': '번역', 'execute': '검증 실행', 'render': '미리보기·인쇄',
    'prepare': '입력·정답 생성', 'package': '패키지 내보내기',
    'publish': 'ZOJ 문제 등록·업데이트', 'issues': '이슈 작성',
    'manage_access': '협업 권한 관리', 'manage_folders': '폴더 관리',
    'create_problems': '문제 생성',
}


class AccessMail(BaseModel):
    model_config = ConfigDict(extra='forbid')
    event_id: str = Field(pattern=r'^[a-f0-9]{32}$')
    kind: Literal['problem_access', 'folder_invitation', 'folder_access']
    resource_id: str = Field(pattern=r'^[a-f0-9]{32}$')
    resource_name: str = Field(min_length=1, max_length=200)
    actor_name: str = Field(min_length=1, max_length=80)
    permissions: list[str] = Field(max_length=16)
    expires_at: int | None = Field(default=None, ge=1, le=4102444800)

    @field_validator('resource_name', 'actor_name')
    @classmethod
    def clean_name(cls, value):
        if any(ord(c) < 32 or ord(c) == 127 for c in value) or not value.strip():
            raise ValueError('Control characters are not allowed')
        return value

    @model_validator(mode='after')
    def fixed_permissions(self):
        if set(self.permissions) - PERMISSIONS.keys() or len(set(self.permissions)) != len(self.permissions):
            raise ValueError('Unknown or repeated permissions')
        if self.kind == 'problem_access' and set(self.permissions) & {'manage_folders', 'create_problems'}:
            raise ValueError('Folder-only permission')
        if (self.kind == 'folder_invitation') != (self.expires_at is not None):
            raise ValueError('Invitations require an expiry')
        return self


def access_content(values):
    invitation = values.kind == 'folder_invitation'
    problem = values.kind == 'problem_access'
    title = '팀 폴더에 초대되었습니다' if invitation else ('문제 협업 권한이 부여되었습니다' if problem else '팀 폴더 권한이 추가되었습니다')
    url = ('https://editor.zoj.kr/problems/' + values.resource_id if problem else
           'https://editor.zoj.kr/problems' + ('' if invitation else '?folder=' + values.resource_id))
    permissions = '읽기' + ''.join(' · ' + label for key, label in PERMISSIONS.items() if key in values.permissions)
    body = [f'{values.actor_name} 님이 「{values.resource_name}」에 ' + ('초대했습니다.' if invitation else '협업 권한을 부여했습니다.'),
        '초대받은 이메일로 ZOJ에 로그인한 뒤 문제 목록 위의 팀 폴더 초대를 수락하세요.' if invitation else
        '권한을 받은 이메일로 ZOJ에 로그인하면 해당 자료를 확인할 수 있습니다.']
    meta = [('팀 폴더' if not problem else '문제', values.resource_name), ('초대한 사람', values.actor_name), ('부여된 권한', permissions)]
    if invitation:
        meta.append(('초대 만료', format_korean_datetime(datetime.fromtimestamp(values.expires_at, timezone.utc)) + ' KST'))
    text = '\n'.join([*body, '', *(f'{key}: {value}' for key, value in meta), '바로가기: ' + url])
    html = render_branded_email(title=title, preheader=values.resource_name + ' · ' + title,
        eyebrow='ZOJ EDITOR', body=body, meta=meta, button_label='초대 확인하기' if invitation else 'Editor에서 확인하기', button_url=url)
    return '[ZOJ Editor] ' + title + ': ' + values.resource_name, text, html


@router.post('/access')
async def access_mail(request: Request):
    key = settings.editor_auth_bridge_secret.get_secret_value() if settings.editor_auth_bridge_secret else ''
    email, expected = identity(request, secret_override=key, purpose='zoj-editor-access-mail-v1')
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > 8192:
            raise AppError(413, 'editor_mail_too_large', '요청 크기가 너무 큽니다.')
    if hashlib.sha256(body).hexdigest() != expected:
        raise AppError(401, 'editor_mail_changed', '메일 요청 내용이 변경되었습니다.')
    try:
        values = AccessMail.model_validate(json.loads(body))
    except (ValueError, ValidationError):
        raise AppError(422, 'editor_mail_invalid', '권한 안내 요청 형식을 확인하세요.')
    consume('editor-access-mail-email', email, 100, 3600)
    consume('editor-access-mail-global', 'editor', 5000, 3600)
    subject, text, html = access_content(values)
    queued = store.enqueue_mail('editor_access', email, subject, text, html,
        delivery_id=str(uuid5(NAMESPACE_URL, 'zoj-editor-access:' + values.event_id)))
    return ok(request, {'queued': True, 'delivery_id': queued.mail_queue_id})
