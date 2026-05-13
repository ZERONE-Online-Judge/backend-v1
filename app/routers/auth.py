from fastapi import APIRouter, Request
from pydantic import BaseModel, EmailStr

from app.settings import settings
from app.services.errors import AppError
from app.services.authz import bearer_token, require_staff
from app.services.responses import ok
from app.services.store import store

router = APIRouter(tags=["auth"])


class StaffLoginRequest(BaseModel):
    email: EmailStr
    password: str


class StaffRefreshRequest(BaseModel):
    refresh_token: str


class StaffLogoutRequest(BaseModel):
    refresh_token: str | None = None


class StaffOtpRequest(BaseModel):
    email: EmailStr


class StaffOtpVerifyRequest(BaseModel):
    email: EmailStr
    otp_code: str


class GeneralOtpRequest(BaseModel):
    email: EmailStr


class GeneralOtpVerifyRequest(BaseModel):
    email: EmailStr
    otp_code: str = ""


class GeneralLoginMethodRequest(BaseModel):
    email: EmailStr


class GeneralPasswordLoginRequest(BaseModel):
    email: EmailStr
    password: str


class GeneralPasswordOtpRequest(BaseModel):
    email: EmailStr
    password: str


class GeneralPasswordOtpVerifyRequest(BaseModel):
    email: EmailStr
    password: str
    otp_code: str = ""


class GeneralRefreshRequest(BaseModel):
    refresh_token: str


class GeneralLogoutRequest(BaseModel):
    refresh_token: str | None = None


@router.post("/auth/staff/login")
async def staff_login(payload: StaffLoginRequest, request: Request):
    raise AppError(410, "staff_login_removed", "Staff login has been unified into general email OTP login.")


@router.post("/auth/staff/otp/request")
async def staff_otp_request(payload: StaffOtpRequest, request: Request):
    raise AppError(410, "staff_login_removed", "Staff login has been unified into general email OTP login.")


@router.post("/auth/staff/otp/verify")
async def staff_otp_verify(payload: StaffOtpVerifyRequest, request: Request):
    raise AppError(410, "staff_login_removed", "Staff login has been unified into general email OTP login.")


@router.get("/auth/staff/me")
async def staff_me(request: Request):
    account = require_staff(request)
    return ok(request, account.model_dump(mode="json"))


@router.post("/auth/staff/logout")
async def staff_logout(payload: StaffLogoutRequest, request: Request):
    revoked = store.revoke_staff_session(bearer_token(request), payload.refresh_token)
    return ok(request, {"revoked": revoked})


@router.post("/auth/staff/refresh")
async def staff_refresh(payload: StaffRefreshRequest, request: Request):
    refreshed = store.refresh_staff_session(payload.refresh_token)
    if not refreshed:
        raise AppError(401, "invalid_credentials", "Invalid refresh token.")
    return ok(request, refreshed)


@router.post("/auth/general/otp/request")
async def general_otp_request(payload: GeneralOtpRequest, request: Request):
    retry_after = store.general_otp_retry_after_seconds(str(payload.email))
    if retry_after > 0:
        raise AppError(
            429,
            "otp_request_rate_limited",
            f"Please wait {retry_after} seconds before requesting another verification code.",
            {"retry_after_seconds": retry_after},
        )
    sent = store.create_general_otp(str(payload.email))
    if not sent:
        raise AppError(404, "not_found", "This email is not registered.")
    data = {"sent": True, "delivery": "email", "cooldown_seconds": settings.otp_request_cooldown_seconds}
    if settings.allow_empty_otp:
        data["demo_otp"] = sent
    return ok(request, data)


@router.post("/auth/general/otp/verify")
async def general_otp_verify(payload: GeneralOtpVerifyRequest, request: Request):
    session = store.verify_general_otp(str(payload.email), payload.otp_code)
    if not session:
        raise AppError(401, "invalid_credentials", "Invalid email or verification code.")
    return ok(request, session)


@router.post("/auth/general/login-method")
async def general_login_method(payload: GeneralLoginMethodRequest, request: Request):
    return ok(request, {"method": "otp"})


@router.post("/auth/general/password/login")
async def general_password_login(payload: GeneralPasswordLoginRequest, request: Request):
    raise AppError(410, "password_login_removed", "Password login has been removed. Use email OTP login.")


@router.post("/auth/general/password/otp/request")
async def general_password_otp_request(payload: GeneralPasswordOtpRequest, request: Request):
    raise AppError(410, "password_login_removed", "Password login has been removed. Use email OTP login.")


@router.post("/auth/general/password/otp/verify")
async def general_password_otp_verify(payload: GeneralPasswordOtpVerifyRequest, request: Request):
    raise AppError(410, "password_login_removed", "Password login has been removed. Use email OTP login.")


@router.get("/auth/general/me")
async def general_me(request: Request):
    token = bearer_token(request)
    session = store.get_general_by_access_token(token) if token else None
    if not session:
        raise AppError(401, "authentication_required", "General access token is required.")
    return ok(request, session)


@router.post("/auth/general/refresh")
async def general_refresh(payload: GeneralRefreshRequest, request: Request):
    refreshed = store.refresh_general_session(payload.refresh_token)
    if not refreshed:
        raise AppError(401, "invalid_credentials", "Invalid refresh token.")
    return ok(request, refreshed)


@router.post("/auth/general/logout")
async def general_logout(payload: GeneralLogoutRequest, request: Request):
    revoked = store.revoke_general_session(bearer_token(request), payload.refresh_token)
    return ok(request, {"revoked": revoked})


@router.post("/auth/general/contests/{contest_id}/participant-session")
async def general_participant_session(contest_id: str, request: Request):
    token = bearer_token(request)
    session = store.get_general_by_access_token(token) if token else None
    if not session:
        raise AppError(401, "authentication_required", "General access token is required.")
    verified = store.issue_participant_session_for_general(session["account"]["email"], contest_id)
    if not verified:
        raise AppError(403, "scope_denied", "This account is not registered as a participant for the contest.")
    team, member, division, access_token = verified
    return ok(
        request,
        {
            "access_token": access_token,
            "team": team.model_dump(mode="json"),
            "member": member.model_dump(mode="json"),
            "division": division.model_dump(mode="json"),
        },
    )
