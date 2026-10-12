# Editor 인증 메일 API

Editor는 `/api/editor/mail/verification`에 서명된 서버 요청을 보내고 ZOJ의
기존 발송 큐·메일 제공자를 사용합니다. SMTP 비밀번호를 Editor에 복사하지
않습니다. 브라우저 로그인 토큰이나 비밀번호도 이 API에 전달하지 않습니다.

ZOJ: `EDITOR_MAIL_RELAY_SECRET`에 전용 무작위 키를 설정합니다.
Editor: `EDITOR_MAIL_MODE=zoj`, `EDITOR_MAIL_RELAY_URL=https://zoj.kr/api`,
동일한 `EDITOR_MAIL_RELAY_SECRET`을 설정합니다. TLS 내부 API를 사용하면
Editor의 `EDITOR_MAIL_RELAY_CA_FILE`에 신뢰하는 CA 파일 경로를 지정합니다.

문제 가져오기 키와 메일 키는 별개입니다. 요청 목적은 `zoj-editor-mail-v1`
이며 경로·수신자·본문·시간을 서명하고 nonce 재사용을 거부합니다. 로그인,
비밀번호 등록·변경, 재설정의 고정 양식만 허용합니다. 임의 제목·본문·링크는
지정할 수 없습니다. 인증번호는 운영자 메일 프리뷰에서 제외합니다.

수신자별 시간당 10건, 전체 시간당 1,000건을 DB 카운터로 제한합니다.
Editor 자체의 인증 요청·재시도 제한도 별도로 적용됩니다. API의 성공은
발송 큐 접수를 뜻하며 실제 발송 성공 여부는 ZOJ 메일 작업자가 기록합니다.
