# 환경변수 레퍼런스

모든 환경변수는 `.env` 파일에 설정합니다. `.env.example`을 복사하여 시작하세요.

```bash
cp .env.example .env
```

설정값은 시스템 환경변수로도 주입 가능합니다 (컨테이너·PaaS 환경). 환경변수가 `.env`보다 우선합니다.

---

## 필수 설정

### 데이터베이스

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `DB_HOST` | `localhost` | PostgreSQL 호스트 |
| `DB_PORT` | `5432` | PostgreSQL 포트 |
| `DB_NAME` | `news_db` | 데이터베이스 이름 |
| `DB_USER` | `news_user` | 접속 계정 |
| `DB_PASSWORD` | *(없음)* | 비밀번호 — **필수, 기본값 없음** |
| `DATABASE_URL` | *(없음)* | DSN 문자열. 설정 시 `DB_*` 개별 변수를 대체. 형식: `postgresql://user:password@host:port/dbname` |

`DATABASE_URL`을 설정하면 `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`는 모두 무시됩니다.

### Telegram

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `TELEGRAM_TOKEN` | *(없음)* | BotFather에서 발급한 봇 토큰 — **필수** |
| `TELEGRAM_CHAT_ID` | *(없음)* | DM을 받을 사용자의 Chat ID. `ALLOWED_CHAT_IDS`와 둘 중 하나는 반드시 설정 |
| `ALLOWED_CHAT_IDS` | *(없음)* | 쉼표로 구분한 허용 Chat ID 목록. 여러 명이 봇을 사용할 때 사용 |
| `TELEGRAM_CHANNEL_ID` | *(없음)* | 스크리너 결과를 브로드캐스트할 채널 ID (예: `@mychannel`). 미설정 시 채널 발송 없음 |

`TELEGRAM_CHAT_ID`와 `ALLOWED_CHAT_IDS` 둘 다 미설정 시 봇이 모든 사용자의 명령어를 거부하고 시작 실패합니다.

---

## 로컬 LLM

### Ollama (기본 백엔드)

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `OLLAMA_BASE` | `http://localhost:11434` | Ollama 서버 URL |
| `OLLAMA_MODEL` | `qwen3.5:9b` | 사용할 모델. `ollama pull <model>`로 사전 다운로드 필요 |

권장 모델: `qwen3.5:9b` (한국어 강세, 4~8GB VRAM), `qwen2.5:7b` (경량).

### LM Studio (폴백 백엔드)

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `LM_STUDIO_BASE` | `http://localhost:1234` | LM Studio 로컬 서버 URL |
| `LM_STUDIO_MODEL` | `eeve-korean-instruct-10.8b-v1.0` | LM Studio에 로드된 모델 식별자 |

Ollama가 응답하지 않으면 LM Studio로 자동 폴백합니다. 둘 다 실패하면 해당 기사는 스킵됩니다.

---

## 거시경제

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `KOREA_BASE_RATE` | `2.5` | 한국은행 기준금리 (%). BOK 발표 시 수동 업데이트 필요. LLM 프롬프트 컨텍스트로 사용. 90일 이상 미업데이트 시 시작 시 경고 출력 |

---

## KRX 데이터

### KRX OpenAPI (OHLCV·종목 마스터)

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `KRX_OPENAPI_KEY` | *(없음)* | [openapi.krx.co.kr](https://openapi.krx.co.kr) 가입 후 발급. 주봉·일봉 OHLCV, 종목 마스터 수집에 사용. 미설정 시 yfinance로 폴백 |

### KRX 포털 수급 데이터

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `KRX_ID` | *(없음)* | [data.krx.co.kr](https://data.krx.co.kr) 로그인 계정. 설정 시 `krx_flow_sync.py`가 자동 로그인·재로그인(2026-07-11부터 정상 동작) — **권장 방식** |
| `KRX_PW` | *(없음)* | 위 계정 비밀번호 |
| `KRX_SESSION` | *(없음)* | JSESSIONID 쿠키. 브라우저 로그인 후 DevTools > Application > Cookies에서 복사. `KRX_ID`/`KRX_PW` 없이도 수급 수집(`krx_flow_sync.py`)을 돌릴 수 있는 대안이지만, 만료 시 브라우저에서 수동 갱신해야 함 |
| `KRX_VISITOR` | *(없음)* | `__smVisitorID` 쿠키 (선택). 세션 갱신 성공률 향상 |

KRX 세션 쿠키는 24시간 후 만료됩니다. `KRX_ID`/`KRX_PW`가 설정돼 있으면 만료 시 `krx_flow_sync.py`가 자동으로 재로그인해 수동 갱신 없이 계속 동작합니다. `KRX_SESSION`만 설정한 경우에는 만료마다 브라우저에서 쿠키를 수동 갱신해야 합니다.

---

## 키움 실 계좌

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `KIWOOM_APPKEY` | *(없음)* | [openapi.kiwoom.com](https://openapi.kiwoom.com) 발급 App Key — `daily_aftermarket_sync_job`(ka10032/ka10098)과 `krx_flow_sync.py --backend kiwoom`(ka10045, 수동 폴백)이 공유 |
| `KIWOOM_SECRETKEY` | *(없음)* | App Secret Key |
| `KIWOOM_TOKEN` | *(없음)* | 미리 발급된 토큰 직접 주입 (선택, 토큰 발급 생략) |

미설정 시 `daily_aftermarket_sync_job`은 경고 후 스킵된다. `krx_flow_sync.py`의 기본 백엔드(`krx-direct`)는 이 변수들을 쓰지 않는다 — `KRX_ID`/`KRX_PW` 자동 재로그인까지 실패하는 예외적 상황에서만 수동 폴백(`--backend kiwoom`)으로 필요.

---

## 키움 모의투자

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `KIWOOM_MOCK_APPKEY` | *(없음)* | [mockapi.kiwoom.com](https://mockapi.kiwoom.com) 발급 App Key |
| `KIWOOM_MOCK_APPSECRET` | *(없음)* | App Secret Key |
| `KIWOOM_MOCK_ACCOUNT` | *(없음)* | 모의투자 계좌번호 (예: `50081234-01`) |

세 변수 모두 설정되어야 모의주문이 활성화됩니다. 미설정 시 `/paper` 관련 기능이 DB 조회만 수행하고 실제 주문은 발행하지 않습니다.

---

## 성능 튜닝

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `SCREENER_WORKERS` | `1` | 주봉 스크리너 및 일봉 분류기(전종목, 캡 없음) 병렬 워커 수. 프로덕션: `8` 권장. 1 워커로 300종목 ≈ 90초. 16:30~17:00 KST 데드라인을 맞추려면 최소 `4` 이상 |

---

## 스크리너 고급 설정

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `SCREENER_G_NAN_STRICT` | *(미설정)* | 주봉 스크리너 조건 G (120주선) NaN 처리 방식. **미설정(기본)**: 데이터 부족 종목은 조건 G를 통과(pass). **`1` 또는 `true`**: 데이터 부족 시 조건 G 실패. DB에서 `null_pct > 20%` 확인 후 활성화 권장. [스크리너 Calibration 가이드](howto-screener.md#condition-g-calibration) 참고 |

---

## 대시보드 인증

웹 대시보드(`dashboard/backend/main.py`)는 역할 기반 인증을 지원합니다.

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `ADMIN_USER` | *(없음)* | 관리자 계정 이름. 설정 시 스케줄러 트리거 등 쓰기 API 접근 가능 |
| `ADMIN_PASSWORD` | *(없음)* | 관리자 비밀번호 |
| `DASHBOARD_USER` | *(없음)* | 일반 사용자 계정 이름. 읽기 전용 (스케줄러 트리거 불가) |
| `DASHBOARD_PASSWORD` | *(없음)* | 일반 사용자 비밀번호 |

**역할 우선순위:**
- `ADMIN_USER`/`ADMIN_PASSWORD` 설정 → `role=admin` (쓰기 권한)
- `DASHBOARD_USER`/`DASHBOARD_PASSWORD` 설정 → `role=user` (읽기 전용)
- `ADMIN_USER` 미설정 시 `DASHBOARD_USER`도 admin 취급 (하위 호환)
- 둘 다 미설정 → 인증 비활성화 (로컬 개발 환경)

Caddy basicauth와 병행 사용하지 마세요. 두 인증 레이어가 충돌하여 Caddy에 없는 계정(`ADMIN_USER` 등)이 401 루프에 빠집니다. HTTPS 설정은 [HTTPS-Setup.md](HTTPS-Setup.md) 참고.

---

## YouTube 내러티브 스크리닝

삼프로TV 자막 수집·LLM 추출 파이프라인(`data/youtube_narrative_sync.py`).

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `YOUTUBE_API_KEY` | *(없음)* | [Google Cloud Console](https://console.cloud.google.com) YouTube Data API v3 키. 삼프로TV 영상 목록 수집에 사용. 미설정 시 `youtube_narrative_sync_job`이 조기 종료 |

자막 → 종목 언급 JSON 추출은 Ollama 로컬 LLM(`OLLAMA_MODEL`)을 사용합니다. `GEMINI_API_KEY`는 구 버전 호환용으로만 남아 있으며 현재 파이프라인에서 사용하지 않습니다.

---

## 네트워크 프록시 (선택)

ISP 차단 환경(KT 회선 등)에서 YouTube 자막 수집과 Telegram API 접근에, 그리고 IP 차단된 `data.krx.co.kr` 접근(`krx_flow_sync.py` krx-direct 백엔드)에 Tor SOCKS5 프록시를 우회 경로로 사용합니다. 전용 헤드리스 Tor 데몬(`tor-daemon/torrc`, SocksPort 9250 / ControlPort 9251)이 로그온 시 자동 기동됩니다 — 2026-07-14부터 Tor Browser GUI 의존 제거(포트도 Tor Browser 기본값 9150/9151과 충돌해 9250/9251로 이전).

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `TOR_PROXY` | *(없음)* | YouTube 자막 수집·`data.krx.co.kr` 접근 Tor 우회 프록시. 형식: `socks5h://127.0.0.1:9250` (헤드리스 tor-daemon, 기본 배포). 미설정 시 직접 연결 시도 |
| `TELEGRAM_PROXY` | *(없음, 기본 비활성)* | Telegram API(`api.telegram.org`) Tor 우회 프록시. KT 회선에서 TCP 443이 차단된 경우에만 설정. 형식: `socks5h://127.0.0.1:9250`. 미설정 시 직접 연결 |
| `TOR_CONTROL_PORT` | `9251` | Tor control port. `krx_flow_sync.py`가 403 응답을 받거나 세션 만료가 의심될 때(연속 빈 응답)만 `SIGNAL NEWNYM`으로 출구 노드를 로테이션할 때 사용 — 이미 실패가 확인된 상황에서만 시도한다 (KRX_SESSION 쿠키가 발급 IP에 묶여 있을 가능성이 있어, 멀쩡히 동작 중인 세션을 무작정 로테이션하면 오히려 깨질 수 있기 때문에 정상 동작 중에는 로테이션하지 않음). 인증은 [stem](https://stem.torproject.org/)이 PROTOCOLINFO로 쿠키 경로를 자동 탐색 — 별도 쿠키 경로 설정 불필요 |

**주의:** `.env` 파일에서 인라인 주석(`KEY=value  # comment`)을 사용하면 `start_crawler.bat` 파서가 주석 텍스트까지 값에 포함시킵니다. 주석은 반드시 별도 줄에 작성하세요.

**주의 2:** `TELEGRAM_PROXY`와 `TOR_PROXY`를 동시에 켜둔 채 텔레그램 봇이 백그라운드로 계속 폴링하면, tor-daemon 포트(9250) 점유 경쟁이 발생할 수 있습니다. `api.telegram.org`가 직접 연결로 막힘 없이 열리는지 먼저 확인(`Test-NetConnection api.telegram.org -Port 443`)한 뒤 필요한 경우에만 켜세요. (2026-06-21 직접 연결 테스트 결과 KT 차단 해제 확인 → 기본값 비활성으로 전환)

```env
# 헤드리스 tor-daemon 9250 — YouTube 자막 수집용, KT 차단 시 필수
TOR_PROXY=socks5h://127.0.0.1:9250
# api.telegram.org KT 차단 우회 — 직접 연결이 막힐 때만 주석 해제
# TELEGRAM_PROXY=socks5h://127.0.0.1:9250
```

SOCKS5 프록시 지원은 `socksio` 패키지가 필요합니다 (`pip install socksio`).

---

## 빠른 진단

환경변수가 제대로 로드되었는지 확인:

```bash
python -c "from db import get_dsn; print(get_dsn())"
python -c "import os; print(os.getenv('TELEGRAM_TOKEN', 'NOT SET')[:10])"
python -c "from summarizer import OLLAMA_BASE, OLLAMA_MODEL; print(OLLAMA_BASE, OLLAMA_MODEL)"
```

## 관련 문서

- [USER_MANUAL.md](USER_MANUAL.md) — 전체 설치 가이드
- [howto-screener.md](howto-screener.md) — 스크리너 Calibration
- [howto-stage-classifier.md](howto-stage-classifier.md) — 3단계 분류기 설정
