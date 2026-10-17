# Editor access notifications

The existing private Editor ingress accepts `POST /api/editor/mail/access`.
Editor uses its configured shared-login bridge key and CA over internal TLS;
no additional SMTP credentials or network route are required. This endpoint
uses the separate HMAC purpose `zoj-editor-access-mail-v1`. Login, package,
and legacy OTP-mail signatures cannot be reused here.

The signed identity is the recipient email. The fixed payload contains an
`event_id` and `resource_id` (32 lowercase hexadecimal characters), kind
(`problem_access`, `folder_invitation`, or `folder_access`), resource/actor
names, and permission keys. Invitations also require an `expires_at` epoch.
Arbitrary subjects, HTML, links, control characters, and unknown permissions
are rejected. All links point to `https://editor.zoj.kr` and all user-supplied
names are escaped in the branded email template.

The response acknowledges queue acceptance, not SMTP completion. A stable
UUID derived from `event_id` deduplicates retries even when the first response
is lost. Reusing an event for another recipient or content returns 409. A new
permission grant needs a new event ID. The existing mail worker sends these
`editor_access` messages; their status and plain-text previews appear in the
service email logs. Notification quotas are separate from login email quotas.

Deploy this API before the Editor outbox worker. No ZOJ schema migration is
needed. The Editor migration creates an empty outbox and never sends historical
permissions. Rolling back Editor stops new notifications; queued ZOJ mail is
still delivered by the ordinary mail worker.
