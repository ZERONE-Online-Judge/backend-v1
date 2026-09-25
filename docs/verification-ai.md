# 검증 코드 AI 판정 분석

문제 관리 → 테스트케이스의 검증 코드에서 **기대 판정과 실제 판정이 다를 때** OpenAI Responses API로 상세 분석합니다. 일반 참가자 제출에는 실행하지 않습니다. 공식 판정이나 코드는 변경하지 않습니다.

## 서버 환경변수

운영 서버 `zoj_server`의 `/home/zoj/zerone-online-judge/backend-v1/deploy/env/backend.env`를 편집합니다. 키 값은 저장소, 프런트엔드 `VITE_*` 변수, 채팅에 넣지 않습니다.

```dotenv
OPENAI_API_KEY=발급받은_API_키
OPENAI_MODEL=gpt-5.4
VERIFICATION_AI_ENABLED=true
VERIFICATION_AI_DAILY_LIMIT=50
VERIFICATION_AI_MAX_INPUT_CHARS=400000
VERIFICATION_AI_FILE_MAX_BYTES=65536
VERIFICATION_AI_MAX_OUTPUT_TOKENS=16000
VERIFICATION_AI_TIMEOUT_SECONDS=180
```

키가 비어 있으면 새 API 호출은 하지 않습니다. 키와 기능 스위치가 모두 켜져야 자동 분석합니다. 키를 제거해도 저장된 보고서는 계속 열람할 수 있습니다. 모델을 바꾸려면 Responses API의 이미지 입력·구조화 출력·`reasoning.effort=medium`을 지원하는 모델을 사용합니다.

설정 저장 후 활성 API와 분석 워커에 새 환경변수를 적용합니다. 단순 `restart`는 컨테이너 환경변수를 갱신하지 않으므로 재생성합니다. 아래 명령은 잠깐의 API 재시작을 수반합니다. 무중단 적용이 필요하면 기존 `deploy/deploy-main-bluegreen.sh` 배포 경로를 사용합니다.

```sh
cd /home/zoj/zerone-online-judge/backend-v1
active_color=$(sh deploy/bluegreen.sh active)
docker compose -f deploy/compose.backend.yaml up -d --no-deps --force-recreate "api-$active_color" verification-ai-worker
```

API와 워커 모두 키가 설정되어야 UI의 이용 가능 상태와 실제 호출이 일치합니다. 키를 처음 넣으면 이미 저장된 불일치 검증 기록도 순서대로 분석합니다. 과거 배포 전의 검증 실행에는 연결 기록이 없으므로 코드를 다시 채점합니다.

## 흐름과 캐시

1. 등록된 검증 파일과 제출 코드가 일치하는지 확인한 뒤 서버에 실행 기록을 저장합니다.
2. 채점기가 작업을 가져가는 순간 문제 지문·해설·제한·활성 테스트케이스 버전·파일 체크섬을 기록합니다.
3. 불일치로 끝난 실행을 별도 워커가 분석합니다. 채점 결과 보고나 웹 요청은 OpenAI 응답을 기다리지 않습니다.
4. 원인·근거·관련 코드, 수정 예시·재검증 방법, 추가 반례, 검토 한계를 JSON 보고서로 DB에 저장합니다.
5. 다른 운영자의 조회와 새로고침에는 저장된 결과를 돌려줍니다. 같은 대회·문제 자료·검증 소스·판정 및 진단 정보·모델·프롬프트 버전이면 재채점도 기존 분석을 재사용합니다. 실행 시간 등 진단 정보가 바뀌면 새로운 분석이 생성될 수 있습니다.
6. 현재 자료와 채점 당시 자료가 다르면 ‘이전 채점 기준’을 표시합니다. 최신 자료로 다시 채점하면 새 기준을 사용합니다.

문제 자료 열람 권한(`contest.problem.resource.view`)이 있는 운영자만 접근할 수 있습니다. 수동 요청/재시도는 검증 제출 권한(`contest.problem.test`)도 필요합니다. 보고서에 숨겨진 테스트나 정답 코드가 포함될 수 있으므로 참가자나 일반 검수자에게 공개하지 않습니다. 문제를 삭제하면 관련 기록과 분석도 삭제됩니다.

## 분석 자료와 한도

문제 지문·예제·해설, 검증 소스, 기대/실제 판정, 컴파일/채점 로그, 실패 테스트 번호, 시간/메모리, 테스트 입력/정답 및 케이스별 제한, checker·validator 등 보조 소스를 제공합니다. 실패 케이스를 먼저 포함하고 나머지 케이스도 입력 한도 안에서 포함합니다. 보조 파일이 테스트 입력 예산을 전부 소진하지 않도록 최소 절반을 테스트에 예약합니다.

첨부된 PNG/JPEG/WebP 이미지 중 지문·해설에서 참조하는 파일은 최대 4개, 각 1 MiB까지 제공합니다. 외부 URL은 가져오지 않습니다. 파일 길이·전체 입력 한도, 삭제·체크섬 불일치, 지원하지 않는 첨부 형식, 외부 이미지로 제외/잘린 자료를 ‘분석 범위’에 표시합니다. 큰 파일 일부만 제공한 경우 전체 파일의 체크섬을 검증한 것으로 간주하지 않습니다. 전체 자료가 들어가도 모델의 설명 정확성을 보장하지 않으며 수정·반례는 실제 채점으로 확인합니다.

API에는 계정 이름·이메일·토큰을 넣지 않습니다. 출제 자료·소스·테스트·진단 로그는 외부 OpenAI API로 전송됩니다. 파일 안에 직접 적힌 개인정보나 비밀은 자동 제거되지 않습니다. 문서/소스 내 지시는 검토 데이터로 취급하고 모델에 실행 도구나 외부 조회 도구를 제공하지 않습니다. UI는 보고서를 텍스트로 렌더링합니다. API 요청은 `store:false`로 보내며, 서비스 자체의 공유 보고서는 ZOJ DB에 보관합니다. 참고: [Responses API](https://developers.openai.com/api/docs/guides/migrate-to-responses), [구조화 출력](https://developers.openai.com/api/docs/guides/structured-outputs).

## 장애 대응

- **AI 연결 미설정**: 키를 서버 환경파일에 넣고 활성 API와 `verification-ai-worker`를 재생성합니다.
- **대기 지속**: 워커 실행 상태, API 키, 하루 한도를 확인합니다. 한도는 UTC 날짜 기준 기본 50회 요청 시도이며 실패/재시도도 집계합니다. 이전 날짜의 시도를 포함해 보수적으로 계산할 수 있습니다. 초과분은 다음 날짜까지 대기합니다.
- **OpenAI 사용량/호출 한도**: OpenAI 프로젝트의 결제·사용량과 모델 권한을 확인합니다. 오류 원문/키는 로그나 보고서에 저장하지 않습니다.
- **중단/시간초과**: 10분 이상 실행 상태가 남으면 실패로 전환합니다. 자동으로 유료 재시도하지 않으며 운영자가 다시 요청할 수 있습니다. 동일 분석은 최대 3회 시도합니다.
- **검증 코드 삭제**: 아직 시작하지 않은 분석을 취소합니다. 이미 전송된 요청은 취소하지 못할 수 있습니다.
- **checker compile failed 등**: AI는 인프라 오류와 논리 오답을 구분하도록 안내받습니다. 채점 메시지와 에이전트 로그로 실제 오류를 확인하세요.

```sh
docker compose -f deploy/compose.backend.yaml ps verification-ai-worker
docker compose -f deploy/compose.backend.yaml logs --tail=50 verification-ai-worker
```

상태 전환은 DB 잠금과 고유 캐시 키로 중복을 막고, 외부 API 호출 중에는 DB 트랜잭션을 열어 두지 않습니다. 보고서 조회는 추가 과금 없이 DB에서 처리합니다.
