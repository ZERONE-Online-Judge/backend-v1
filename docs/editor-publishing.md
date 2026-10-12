# Editor 문제 전송 수신

Editor 계정과 ZOJ 계정은 각각 이메일 OTP로 로그인합니다. 이 기능은 별도
Editor API가 인증한 이메일을 HMAC 서명으로 전달할 때, 같은 이메일의 현재
ZOJ 운영자 권한으로 대회에 새 문제 ZIP을 등록합니다. 기존 로그인이나
문제 가져오기 API의 인증 방법은 변경하지 않습니다.

## 설정

서로 다른 운영 배포와 검토를 거쳐 수신 기능을 반영합니다. 신규 테이블은
`0040_editor_problem_imports` 마이그레이션으로 만듭니다.

ZOJ API 환경변수 `EDITOR_BRIDGE_SECRET`에 32자 이상의 별도 무작위 키를
설정하고 API를 재생성합니다. 동일 키를 Editor API의
`EDITOR_ZOJ_BRIDGE_SECRET`에 설정합니다. 키를 브라우저·로그·Git에 넣거나
세션/채점 노드 키와 재사용하지 않습니다. 미설정이면 전용 수신 API는 503을
반환합니다. production의 Editor 전송 주소는 HTTPS여야 하며 자체 CA는
인증서 파일을 등록합니다. 인증서 검증을 끄지 않습니다.

## 허용하는 작업

- `GET /api/editor/contests`: 동일 이메일의 등록 가능 대회·참가 유형 조회
- `POST /api/editor/contests/{id}/imports`: ZOJ ZIP을 새 문제로 가져오기

`contest.problem.manage` 권한을 DB에서 확인합니다. 다른 이메일이나 권한을
클라이언트 입력으로 받지 않고 일반 운영자/로그인 토큰도 발급하지 않습니다.
대회 진행 잠금과 참가 유형 범위, 같은 문제 번호의 충돌은 기존 가져오기와
동일하게 처리합니다. 등록 내부에서도 현재 운영자 권한을 행 잠금과 함께
재확인합니다. 권한을 회수하면 전송 재시도도 거부됩니다.

서명 형식은 줄바꿈으로 연결한 버전 `zoj-editor-v1`, HTTP 메서드, `/api`를
포함한 경로와 원래 쿼리 문자열, Unix 초, 32자 hex nonce, base64url 이메일,
ZIP SHA256입니다. HMAC-SHA256을 별도 키로 계산합니다. 90초 범위를 벗어난
시각, 반복 nonce, 변조한 경로/이메일/파일은 거부합니다. GET의 파일 해시는
빈 바이트의 SHA256입니다. TLS와 함께 서버 간 요청에만 사용합니다.

문제·파일·전송 receipt·운영 작업 로그를 한 트랜잭션으로 기록합니다. 같은
이메일의 같은 operation_id와 같은 대상·파일 해시로 재시도하면 기존 문제를
반환합니다. 내용이 달라지면 409입니다. 새 문제의 활성 테스트 세트는 기존
문제 가져오기와 같이 번들 warm queue에 등록됩니다.

Editor 서버는 이메일 신원 발급자로 신뢰됩니다. 연동 키를 가진 서버는 임의
이메일 신원을 서명할 수 있으므로 별도 키와 접근 가능한 네트워크를 제한하고
유출 시 양쪽 키를 교체합니다. Editor 사용자 요청에서 이메일을 지정하지
못하게 하는 검증과 운영 SMTP 설정이 전제입니다.

## 로컬 검증

운영 DB를 사용하지 않고 별도 SQLite·파일 저장소에서 실행합니다.

```sh
DATABASE_URL=sqlite:////tmp/editor-bridge-tests.db \
LOCAL_OBJECT_STORAGE_ROOT=/tmp/editor-bridge-test-files \
ENABLE_DEMO_SEED=true ALLOW_EMPTY_OTP=true \
python -m pytest -q tests/test_editor_bridge.py tests/test_problem_archives.py
```

서명·기간·nonce·본문 변조, 검수자 범위, 권한 회수, 재시도 시 한 번의 등록과
한 번의 운영 로그를 검사합니다. Editor에서 실제 격리 검증한 ZIP은 기존
`inspect_archive`로 검증하고 로컬 수신 API에서 실제 등록까지 확인합니다.
