"""Only fixed authentication templates, authenticated with a separate key."""
import hashlib
import json
from typing import Literal
from fastapi import APIRouter, Request
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from app.services.editor_bridge import identity
from app.services.auth_rate_limit import consume
from app.services.errors import AppError
from app.services.mail_templates import render_branded_email
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
