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
| `KRX_ID` | *(없음)* | [data.krx.co.kr](https://data.krx.co.kr) 로그인 계정 |
| `KRX_PW` | *(없음)* | 위 계정 비밀번호 |
| `KRX_SESSION` | *(없음)* | JSESSIONID 쿠키. 브라우저 로그인 후 DevTools > Application > Cookies에서 복사. 수급 수집(`krx_flow_sync.py`)에 필수 |
| `KRX_VISITOR` | *(없음)* | `__smVisitorID` 쿠키 (선택). 세션 갱신 성공률 향상 |

KRX 세션 쿠키는 24시간 후 만료됩니다. 만료 시 `krx_flow_sync.py`가 자동 재로그인을 시도합니다. `KRX_ID`와 `KRX_PW`가 있어야 재로그인이 가능합니다.

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
| `SCREENER_WORKERS` | `1` | 주봉 스크리너 및 일봉 분류기 병렬 워커 수. 프로덕션: `8` 권장. 1 워커로 300종목 ≈ 90초. 16:30~17:00 KST 데드라인을 맞추려면 최소 `4` 이상 |
| `DAILY_CLASSIFIER_TICKERS` | `150` | 일봉 3단계 분류기가 하루에 처리할 최대 종목 수. Ichimoku 통과 종목은 이 한도에 관계없이 항상 포함. p99 yfinance fetch latency < 0.5s 확인 후 `300`으로 확장 가능 |

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

삼프로TV 자막 수집·LLM 추출 파이프라인(`data/youtube_narrative_sync.py`). 세 변수 모두 설정 시 활성화.

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `YOUTUBE_API_KEY` | *(없음)* | [Google Cloud Console](https://console.cloud.google.com) YouTube Data API v3 키. 삼프로TV 영상 목록 수집에 사용. 미설정 시 `youtube_narrative_sync_job`이 조기 종료 |
| `GEMINI_API_KEY` | *(없음)* | [Google AI Studio](https://aistudio.google.com) Gemini API 키. 자막 → 종목 언급 JSON 추출에 사용. Gemini 2.5 Flash 모델. 미설정 시 `youtube_narrative_sync_job`이 조기 종료 |

두 키 모두 무료 등급으로 운영 가능합니다. YouTube Data API는 일일 할당량 10,000 units (영상 목록 조회 기준 약 100회), Gemini Free Tier는 분당 15 요청(15 RPM) 제한이 있어 `youtube_narrative_sync.py`가 영상당 4초 대기를 삽입합니다.

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
