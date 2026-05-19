from dataclasses import dataclass
from datetime import datetime
from html import escape
from zoneinfo import ZoneInfo

from app.settings import settings


KST = ZoneInfo("Asia/Seoul")


@dataclass(frozen=True)
class MailContent:
    subject: str
    body_text: str
    body_html: str


def absolute_url(path: str) -> str:
    base = settings.public_base_url.rstrip("/")
    normalized = path if path.startswith("/") else f"/{path}"
    return f"{base}{normalized}"


def format_korean_datetime(value: datetime) -> str:
    local = value.astimezone(KST)
    return f"{local.year}년 {local.month}월 {local.day}일 {local:%H:%M}"


def render_branded_email(
    *,
    title: str,
    preheader: str,
    body: list[str],
    button_label: str | None = None,
    button_url: str | None = None,
    meta: list[tuple[str, str]] | None = None,
) -> str:
    body_html = "".join(
        f'<p style="margin:0 0 12px;color:#334155;font-size:15px;line-height:1.7">{escape(line)}</p>'
        for line in body
        if line
    )
    meta_html = ""
    if meta:
        meta_html = """
        <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="margin:22px 0;border-collapse:collapse;border:1px solid #e2e8f0;border-radius:8px;overflow:hidden">
        """
        for label, value in meta:
            meta_html += f"""
            <tr>
              <th align="left" style="width:118px;background:#f8fafc;border-bottom:1px solid #e2e8f0;padding:11px 13px;color:#64748b;font-size:12px;line-height:1.4">{escape(label)}</th>
              <td style="border-bottom:1px solid #e2e8f0;padding:11px 13px;color:#0f172a;font-size:13px;line-height:1.5;font-weight:700">{escape(value)}</td>
            </tr>
            """
        meta_html += "</table>"
    button_html = ""
    if button_label and button_url:
        button_html = f"""
        <div style="margin-top:24px">
          <a href="{escape(button_url)}" style="display:inline-block;background:#111827;color:#ffffff;text-decoration:none;border-radius:7px;padding:12px 18px;font-size:14px;font-weight:800">
            {escape(button_label)}
          </a>
        </div>
        """
    return f"""<!doctype html>
<html lang="ko">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <title>{escape(title)}</title>
  </head>
  <body style="margin:0;background:#f1f5f9;padding:28px 12px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif">
    <span style="display:none!important;visibility:hidden;opacity:0;color:transparent;height:0;width:0;overflow:hidden">{escape(preheader)}</span>
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse">
      <tr>
        <td align="center">
          <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="max-width:620px;border-collapse:collapse">
            <tr>
              <td style="padding:0 0 14px">
                <img src="cid:zoj-wordmark" alt="Zerone Online Judge" width="184" style="display:block;border:0;max-width:184px;height:auto">
              </td>
            </tr>
            <tr>
              <td style="background:#ffffff;border:1px solid #e2e8f0;border-radius:10px;padding:30px 28px">
                <h1 style="margin:0 0 16px;color:#0f172a;font-size:24px;line-height:1.35;letter-spacing:0;font-weight:900">{escape(title)}</h1>
                {body_html}
                {meta_html}
                {button_html}
              </td>
            </tr>
            <tr>
              <td style="padding:16px 4px 0;color:#94a3b8;font-size:12px;line-height:1.6">
                본 메일은 Zerone Online Judge에서 자동 발송되었습니다.
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>"""


def render_basic_html(subject: str, body_text: str) -> str:
    lines = [line.strip() for line in body_text.splitlines() if line.strip()]
    return render_branded_email(
        title=subject,
        preheader=lines[0] if lines else subject,
        body=lines or [subject],
    )


def participant_invite_mail(*, contest_title: str, organization_name: str, team_name: str, division_name: str, contest_url: str) -> MailContent:
    subject = f"[Zerone OJ] {contest_title} 대회에 초대되었습니다"
    body = [
        f"{team_name} 팀이 {contest_title} 대회에 참가팀으로 등록되었습니다.",
        "아래 버튼에서 대회 페이지로 이동한 뒤, 등록된 이메일로 로그인하면 문제와 공지, 제출 현황을 확인할 수 있습니다.",
    ]
    meta = [
        ("대회", contest_title),
        ("주최", organization_name),
        ("팀", team_name),
        ("유형", division_name),
    ]
    text = "\n".join([*body, "", f"대회: {contest_title}", f"팀: {team_name}", f"유형: {division_name}", f"바로가기: {contest_url}"])
    return MailContent(
        subject=subject,
        body_text=text,
        body_html=render_branded_email(
            title="대회에 초대되었습니다",
            preheader=f"{contest_title} 참가팀 등록 안내",
            body=body,
            meta=meta,
            button_label="대회 페이지 열기",
            button_url=contest_url,
        ),
    )


def contest_reminder_mail(*, contest_title: str, organization_name: str, team_name: str, division_name: str, starts_at: datetime, remaining_label: str, contest_url: str) -> MailContent:
    subject = f"[Zerone OJ] {contest_title} 시작 {remaining_label} 전 안내"
    starts_at_text = format_korean_datetime(starts_at)
    body = [
        f"{contest_title} 시작이 {remaining_label} 남았습니다.",
        "대회 시작 전에 로그인 상태와 참가 유형을 확인해 주세요.",
    ]
    meta = [
        ("대회", contest_title),
        ("주최", organization_name),
        ("시작", f"{starts_at_text} KST"),
        ("팀", team_name),
        ("유형", division_name),
    ]
    text = "\n".join([*body, "", f"시작: {starts_at_text} KST", f"팀: {team_name}", f"유형: {division_name}", f"바로가기: {contest_url}"])
    return MailContent(
        subject=subject,
        body_text=text,
        body_html=render_branded_email(
            title=f"대회 시작 {remaining_label} 전입니다",
            preheader=f"{contest_title} 시작 전 안내",
            body=body,
            meta=meta,
            button_label="대회 페이지 열기",
            button_url=contest_url,
        ),
    )


def contest_notice_mail(
    *,
    contest_title: str,
    organization_name: str,
    notice_title: str,
    notice_body: str,
    notice_url: str,
    pinned: bool,
    emergency: bool,
) -> MailContent:
    subject = f"[Zerone OJ] {contest_title} 공지: {notice_title}"
    labels = []
    if pinned:
        labels.append("고정")
    if emergency:
        labels.append("긴급")
    notice_type = " · ".join(labels) if labels else "공지"
    body = [
        f"{contest_title}에 새 공지가 등록되었습니다.",
        notice_body.strip(),
    ]
    meta = [
        ("대회", contest_title),
        ("주최", organization_name),
        ("구분", notice_type),
        ("공지 제목", notice_title),
    ]
    text = "\n".join(
        [
            *body,
            "",
            f"구분: {notice_type}",
            f"바로가기: {notice_url}",
        ]
    )
    return MailContent(
        subject=subject,
        body_text=text,
        body_html=render_branded_email(
            title=notice_title,
            preheader=f"{contest_title} 새 공지",
            body=body,
            meta=meta,
            button_label="공지 확인하기",
            button_url=notice_url,
        ),
    )
