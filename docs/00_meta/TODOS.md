# TODOS

Items deferred from code review and planning sessions.

---

## P3: 리팩토링 Phase A~C 후속 — 심(shim) 삭제 + 범위 제외분 정리 (2026-07-15)

**배경:** 2026-07-15 구조 리팩토링(우선순위 3단계 컷)으로 core/dates.py,
core/tor.py, core/db_sync.py, core/env.py 신설 및 analysis/backtest_engine.py
(3,360줄) → analysis/backtest/ 8모듈 분해 완료. 모든 이동은 옛 위치에
re-export 심을 남김.

**심 삭제 (스케줄러가 새 코드로 ~1주 정상 가동 후):**
- `data/krx_flow_sync.py`, `analysis/chart_screener.py` 등의
  `_last_trading_day`/`_jittered_delay`/`_tor_new_identity`/`_prev_business_day`/
  `_connect` 별칭 — grep으로 잔여 importer 0 확인 후 테스트를 canonical
  경로로 갱신하고 삭제.
- `analysis/backtest_engine.py`(79줄 순수 심) — 소비자(telegram_bot,
  paper_jobs, strategy_compose, scripts/, tests/)를 analysis.backtest.*
  직접 import로 점진 전환 후 삭제 검토.
- **재평가(2026-07-16, Phase D 실행 중 재탐색):** 아직 둘 다 삭제 불가 —
  5개 별칭 전부와 backtest_engine 심 전부 실제 소비자가 남아있음(별칭은
  주로 alias-regression 테스트, backtest_engine은 프로덕션 6곳도). 최초
  Effort 추정(S)을 M으로 정정, 마이그레이션(테스트/소비자 canonical 전환)
  → zero-importer 재확인 → 삭제의 2단계로 분리. 상세는
  [refactoring-roadmap.md](refactoring-roadmap.md) Phase D 참조.

**범위 제외로 남긴 것:**
- ~~`dashboard/backend/main.py:2374` psycopg2 직접 연결 → core.db_sync 미적용~~
  — 완료(2026-07-16, 대시보드 라우터 분리 작업에서 `core.db_sync.connect()`로 전환).
- `scripts/*`의 psycopg2/DSN 중복 — 일회성 스크립트라 기회적 정리.
- `jobs/ohlcv_warm.py`의 주말 스킵 로직 — last_trading_day와 의미가 다름
  (월요일 실행 시 금요일 daily_ohlcv를 채우는 회차가 없는 잠재 커버리지 갭).
  보정 전환은 동작 변경이라 별도 fix로 판단 필요 (코드에 TODO 주석 있음).
- 전체 9단계 계획의 나머지: 텔레그램 계층 정리, 지표/OHLCV 통합,
  ~~대시보드 라우터 분리~~ — 완료(2026-07-16, `b591f19`~`54d5001` 7커밋),
  stage_classifier 레거시 분리, run_scheduler 분해.

**후속(2026-07-16):** core/db.py(1,689줄) 도메인 분리 완료 — facade + 6모듈,
`96bfb3c`~`7b31706` 5커밋. 이후의 리팩토링 후속 계획(Phase D~G: 저위험 정리 →
대시보드 마무리 → 백필 플러밍 통합 → run_scheduler 분해)과 방법론 원칙은
[refactoring-roadmap.md](refactoring-roadmap.md)로 이관.

**후속(2026-07-16, Phase D 실행):** test_scan_cmd.py 교체, jobs/_common.py
추출, core/db_schema.py 분리 3건 완료. 심 삭제만 위 재평가대로 디스코프.

---

## 완료: Tor Browser(GUI) → 헤드리스 Tor 데몬 전환 (2026-07-11, 완료 2026-07-14)

**What:** `data/krx_flow_sync.py`의 krx-direct 백엔드가 `TOR_PROXY`(SOCKS5, 포트 9150)와 `TOR_CONTROL_PORT`(회로 로테이션용)로 로컬 Tor Browser에 의존한다. Tor Browser는 데스크톱 GUI 애플리케이션(`Desktop\Tor Browser\...`) — `tor.exe`(Tor Expert Bundle) 또는 서비스로 등록 가능한 헤드리스 Tor 데몬으로 교체.

**Why:** `data.krx.co.kr` 직접 접속이 IP 차단(403)돼 2026-07-10에 Tor 프록시를 도입했다(`/plan-eng-review` D2). `daily_flow_sync_job`(평일 18:00 KST 스케줄)이 이제 통째로 "Tor Browser가 데스크톱에 켜져있는지"에 암묵적으로 의존하게 됐다 — 사람이 데스크톱에 로그인해서 Tor Browser를 띄워둬야 스케줄러가 정상 동작한다. 실패 시 텔레그램 알림(`send_admin_alert`)을 붙였지만, 이는 사후 감지일 뿐 근본 해결이 아니다. 헤드리스 데몬으로 바꾸면 머신 재부팅 후에도 서비스로 자동 기동되고, 사람이 데스크톱을 켜둘 필요가 없어진다.

**Pros:** 무인 운영 가능(재부팅 생존). 사람 개입 없이 Windows 서비스로 자동 시작. GUI 오버헤드 없음(리소스 절약).

**Cons:** `tor.exe` 설치·서비스 등록 작업 필요(운영 환경 변경, 코드 변경 아님). `TOR_CONTROL_COOKIE` 등 control port 인증 경로가 Tor Browser와 다를 수 있어 재검증 필요 — 다만 `stem`이 PROTOCOLINFO로 자동 탐색하므로 대부분 호환될 것으로 예상.

**Context:** 이번 세션에서 `_tor_new_identity()`를 raw socket 구현에서 `stem` 라이브러리로 이전했다(`data/krx_flow_sync.py`) — `stem`은 Controller 종류(Tor Browser든 데몬이든)를 가리지 않으므로 이 전환의 코드 쪽 준비는 이미 돼 있다. 남은 건 순수 운영/설치 작업.

**Depends on / blocked by:** 없음 — 독립적으로 아무 때나 진행 가능.

**완료 내역(2026-07-14):** Tor Browser 번들의 `tor.exe`를 재사용해 전용 `tor-daemon/torrc`(SocksPort 9150, ControlPort 9151, CookieAuthentication)로 헤드리스 실행 — 별도 다운로드 없이 기존 바이너리만 재활용. Tor Browser GUI(및 그 자체 tor.exe)는 완전히 종료. **같은 날 후속 수정(`2a3653c`):** Tor Browser 기본 포트(9150/9151)와 충돌해 헤드리스 데몬 포트를 **9250(SocksPort)/9251(ControlPort)**로 이전, `.env`(`TOR_PROXY`/`TOR_CONTROL_PORT`) 반영 완료.

자동기동은 새 예약 작업 대신(이 세션 권한으로는 `schtasks /Create`의 LogonTrigger 등록이 거부됨 — Access denied, 이 머신의 제한된 토큰 이슈) **기존 `NewsCrawler` 태스크가 실행하는 `scripts/start_crawler.bat`에 통합**: 로그온 시 `tor.exe`가 안 떠있으면 먼저 `tor-daemon\torrc`로 기동한 뒤 기존처럼 `run_scheduler.py`를 실행하도록 수정. 새 예약 작업 없이 기존 로그온 트리거 인프라를 재사용.

검증: 헤드리스 데몬 부트스트랩 100% 확인 → `krx_flow_sync.py`의 자동 ID/PW 로그인·데이터 조회 정상 동작 확인(`CD001`, 실 데이터 수신).

**잔존 리스크:** KRX가 Tor 출구 노드를 광범위하게 차단하는 근본 문제는 이 전환과 무관하게 여전함 — 로그인 403/circuit rotation 재시도는 이번에도 동일하게 관측됨(Tor Browser든 헤드리스든 동일 바이너리라 회로 품질은 동일). 이 전환이 해결하는 건 "사람이 데스크톱에 Tor Browser를 띄워둬야 하는 의존성"만이며, KRX 차단 자체의 완화는 아님.

---

## 완료: personal_net 결손 — daily_flow_sync_job을 krx-direct로 되돌림 (2026-06-22)

**배경:** `daily_flow_sync_job`이 평일 `--backend kiwoom`(ka10045)으로 운영되며 `personal_net=NULL` 행이 쌓이는 문제 발생 — `classify_stage_v15`(Stage2 "개인 출회" 게이트)가 조용히 무력화됨(크래시 없음, 정확도만 저하). 같은 날 주간 krx-direct 캐치업 잡으로 임시 대응했었으나, "Kiwoom으로 개인 순매수를 구할 방법이 있는지" 점검 결과 **구조적으로 불가능**함을 확인:
- 키움 REST API의 모든 TR(`ka10045`, `ka10032`, `ka10087`, `ka10098`, `ka10001`, `kt00018` 등)에 투자자 유형별(개인/기관/외국인) 분류 데이터는 `ka10045` 하나뿐이고, 이마저도 기관/외국인만 제공(`tests/test_krx_flow_sync.py:447`에 이미 테스트로 고정됨).
- 이유: 개인/기관/외국인 분류는 거래소(KRX)가 전 증권사 체결을 모아 투자자 유형코드로 집계하는 데이터라, 단일 증권사 API(키움)는 원천적으로 시장 전체 개인 순매수를 알 수 없음.

**최종 결정:** `daily_flow_sync_job`을 다시 `--backend krx-direct`로 되돌림 — `personal_net`을 매일 정확히 채움. 이에 따라 임시로 추가했던 주간 캐치업 잡(`weekly_flow_personal_backfill_job` + 관련 cron/트리거)은 중복이 되어 제거함.

**되돌린 리스크(원래부터 있던 것):** `KRX_SESSION` 쿠키가 만료되면 `daily_flow_sync_job` 자체가 실패함(`[flow-sync] 비정상 종료 — KRX_SESSION 만료 의심` 로그) — `.env`의 `KRX_SESSION`을 브라우저에서 주기적으로 갱신해야 함. kiwoom 백엔드(`--backend kiwoom`)는 `krx_flow_sync.py`에 코드 자체는 남아있어, 쿠키 만료 시 기관/외국인만이라도 임시로 채우는 수동 폴백으로 쓸 수 있음.

---

## P1: YouTube 내러티브 — 블라인드 백테스트 실행 (분산 백필 완료 후) — ✅ 완료, [조건부]

**What:** `python scripts/youtube_backtest.py --ret ret_5d` 실행.

**결과:** 분산 백필 완전 소진(`youtube_backfill_queue` 964 `ok` / 8 `no_transcript`, pending 0). 백테스트 실행 결과(n=2,587) — **Spearman IC +0.0136, t-stat +0.69, p=0.4889 → [조건부]** (`_verdict()` 기준 IC>0.01이면 t-stat 무관하게 조건부 — 합격 기준 IC>0.05 AND t-stat>1.65에는 미달). rolling window·가중치 조정 후 v2 재검증 필요. `attention_score`는 아직 `effective_confidence`에 편입하지 않음.

자세한 내용은 [백필 계획](plan-youtube-backfill.md) 참고.

**Effort:** XS (human: ~5min / CC: ~2min)
**Priority:** P1 → 완료 (후속 v2 재검증은 별도 항목으로 분리 필요)
**Depends on:** 스케줄러 운영 중 (forward return 자동 누적)

---

## P1: YouTube 내러티브 — 스케줄러 재시작으로 운영 시작

**What:** `run_scheduler.py` 재시작. 이미 09:05/09:10/15:40 KST 잡 등록 완료.
`.env`에 `YOUTUBE_API_KEY` 있으면 자동 가동 (LLM 추출은 Gemini가 아닌 Ollama 로컬 모델로 전환됨 — `OLLAMA_BASE`/`OLLAMA_MODEL`).

**체크리스트:**
- [x] 서버 재시작 (또는 스케줄러 프로세스 재시작) — 2026-06-03 12:53 KST, `schtasks /Run "NewsCrawler"`
- [ ] 다음날 09:10 이후 `youtube_attention_scores` 테이블에 데이터 확인
- [ ] 6/4(목) 15:40 이후 `youtube_mention_forward_returns` 채워졌는지 확인

**Effort:** XS (human: ~2min)
**Priority:** P1
**Depends on:** 없음 (즉시 가능)
**Completed:** 2026-06-03 — 스케줄러 재시작 완료. 내일(6/4) 09:10 이후 테이블 확인 필요.

---

## P2: YouTube 내러티브 — feat 브랜치 머지

**What:** `feat/youtube-narrative-screening` → `master` PR 생성 및 머지.

**머지 조건:**
- 백테스트 결과 확인 후 (합격/불합격 무관하게 파이프라인은 머지)
- attention_score 가중치는 백테스트 결과 반영 후 별도 PR

**Effort:** XS (human: ~5min / CC: ~5min)
**Priority:** P2
**Depends on:** 블라인드 백테스트 실행 후
**Completed:** v0.10.0.0 (2026-06-01)

---

## P2: YouTube 내러티브 — Whisper STT 폴백 (v2)

**What:** `youtube_narrative_sync.py`에 `--backfill` 모드에서
자막 없는 영상 → `openai-whisper` 또는 `faster-whisper`로 STT 폴백.

**Why:** 삼프로TV 라이브 방송(핵심 콘텐츠)은 자막 자동 생성이 안 되는 경우 많음.
현재 파이프라인은 자막 있는 영상만 처리 → 라이브 방송 누락.

**구현 포인트:**
- `is_live_fallback=True` 플래그 영상에만 STT 적용
- 로컬 faster-whisper(small/medium 모델) 우선, 실패 시 OpenAI Whisper API
- 비용 추정: faster-whisper 로컬 → 무료, API → $0.006/min

**Effort:** M (human: ~1h / CC: ~30min)
**Priority:** P2
**Depends on:** 백테스트 합격 (신호 가치 확인 후 투자 결정)

---

## P3: YouTube 내러티브 — 테마/섹터 오버레이 (v3)

**What:** 개별 종목 점수 대신 섹터(반도체/배터리/AI/방산 등) 레벨 attention_score.
히트맵에 섹터 오버레이로 표시.

**Why:** 삼프로TV는 개별 종목 진입 타이밍보다 섹터·테마 내러티브에 강함.
개별 종목 disambiguation 노이즈 없이 더 높은 정확도 가능.

**Effort:** M (human: ~2h / CC: ~45min)
**Priority:** P3
**Depends on:** 백테스트 결과 (개별 종목 IC ≈ 0이면 P2로 승격)

---

## P3: 모의투자 — 모델별 요약 카드에 통계 통합

**What:** `PaperPortfolio` 상단의 "모델별 요약" 카드(오픈·대기·청산 건수, 평균수익)에 승률·실현누적 통계를 추가. 모델 카드 클릭 시 통계 행이 확장되는 형태로 재설계.

> **컨텍스트:** `PaperAnalytics.tsx` 컴포넌트(모델 통계 테이블·누적 P&L 커브·미실현 포지션 리더보드)는 v0.9.9.0에서 제거됨. 현재 `/api/paper/curve` 엔드포인트는 여전히 모델 통계를 반환하지만 프론트엔드에서 사용하지 않음.

**Why:** `/api/report/paper`와 `/api/paper/curve` 두 API가 유사한 모델별 집계 데이터를 별도 로드. 카드에 통계가 없어 모델 성과를 확인하려면 별도 조회 필요.

**How to apply:**
1. `/api/paper/curve` 응답에 `model_summary`(오픈/대기/청산 건수) 필드 병합 — 또는 `/api/report/paper`에 통계 필드 추가
2. `PaperPortfolio` 상단 카드에 승률·실현누적 컬럼 추가, 클릭 시 전체 통계 행 펼침
3. 불필요해진 `/api/paper/curve` API 호출 제거 (프론트엔드 미사용 상태)

**Pros:** API 호출 1회 감소. 모델별 정보가 한 곳으로. 스크롤 단축.
**Cons:** 카드 UI가 복잡해짐. 백엔드 API 응답 구조 변경 필요.
**Effort:** S (human: ~2h / CC: ~30min)
**Priority:** P3
**Depends on:** 없음

---

## P3: MarketSummaryBanner — 앱 레벨 헤더로 승격 (Phase 2)

**What:** `MarketSummaryBanner`를 `Heatmap.tsx` 내부에서 `App.tsx` 최상단 헤더로 이동. 모든 탭(히트맵·종목 분석·Top·모의투자·매크로)에서 항상 표시.

**Why:** Phase 1은 히트맵 탭에만 배너를 단다. 다른 탭으로 이동하면 시장 맥락이 사라진다. 초보자가 모의투자 탭에서 포지션을 볼 때도 "오늘 급락장"임을 알아야 한다.

**How to apply:**
- `Heatmap.tsx`에서 `<MarketSummaryBanner />` 마운트 제거
- `App.tsx` 탭 컨텐츠 위 공통 영역에 `<MarketSummaryBanner />` 추가
- `MarketSummaryBanner.tsx` 자체 수정 없음 — 마운트 위치만 변경
- 모바일 레이아웃 확인: 헤더 + 탭 바 + 배너가 화면 상단을 과도하게 차지하지 않는지 검토

**Pros:** Phase 1 실사용 데이터로 수요 확인 후 결정 가능. 마운트 위치 변경이라 코드 변경 최소.
**Cons:** App.tsx 수정 → 전체 렌더링 영향 범위 주의. 모바일에서 화면 상단 공간 소모.
**Effort:** XS (human: ~30min / CC: ~5min)
**Priority:** P3
**Depends on:** MarketSummaryBanner Phase 1 배포 (2026-05-24) 후 실사용 피드백

---

## P3: MarketSummaryBanner — 접기/펼치기 (localStorage)

**What:** 배너 우상단에 chevron 버튼 추가. 클릭 시 배너 접힘. `localStorage.setItem('market-banner-collapsed', 'true')` 저장. 재방문 시 접힌 상태 복원.

**Why:** 파워 유저가 매번 배너를 보면 시끄럽다. 초보자 팁 텍스트는 한 번 읽으면 더 볼 필요 없다.

**How to apply:**
- `MarketSummaryBanner.tsx`에 `useState(localStorage.getItem('market-banner-collapsed') === 'true')` 추가
- 접힌 상태: 코스피/코스닥 수치만 한 줄 표시 (팁 텍스트·한마디 숨김)
- 완전히 닫을 경우 vs 최소화할 경우 UX 결정 필요 (Phase 2에서 논의)

**Pros:** 파워 유저 노이즈 제거. localStorage 1줄 수준 구현.
**Cons:** 상태 관리 코드 추가로 컴포넌트 복잡도 소폭 증가.
**Effort:** XS (human: ~1h / CC: ~5min)
**Priority:** P3
**Depends on:** MarketSummaryBanner Phase 1 배포 후 실사용 피드백 (접기 수요 실제로 있는지 확인)

---

## P4: Move compare_tx_amt.py → scripts/compare_tx_amt.py (v0.7.0.0)

**What:** `compare_tx_amt.py` is a dev validation script (Naver 거래대금 vs yfinance Vol×Close 오차 검증). It lives in the project root alongside production modules.

**Why:** Avoids confusion between production code and dev tooling. The root should contain only production-runnable modules.

**How to apply:** `git mv compare_tx_amt.py scripts/compare_tx_amt.py`. Create `scripts/` if it doesn't exist. No imports reference `compare_tx_amit.py` directly.

**Effort:** XS (human: ~2 min / CC: ~1 min)
**Priority:** P4
**Completed:** v0.7.3.0 (2026-05-08) — 파일 삭제됨 (`krx_flow_sync.py` 도입 후 역할 없음)

---

## P4: Rename test_feeds.py → scripts/check_feeds.py (QA-2026-04-18)

**What:** `test_feeds.py` is a standalone RSS feed connectivity script, not a pytest test file. Rename to `scripts/check_feeds.py` (or just `check_feeds.py`) and update any references.

**Why:** The `test_` prefix causes pytest to attempt collection (0 tests found — no harm, but misleading). Companion issue to ISSUE-QA-001 (fixed). Low urgency since there are no side effects.

**Effort:** XS (human: ~2 min / CC: ~1 min)
**Priority:** P4
**Completed:** v0.4.2.0 (2026-04-19)

---

## P3: HTML Screener Report — Sparklines

**What:** Add close-price sparklines (mini bar or line charts) per stock row in the HTML screener report. Each row would show a 12-week price trend alongside the existing columns.

**Why:** The screener design UI kit (`ui_kits/screener/index.html`) includes sparklines. They let you visually distinguish a recent breakout from a stale one at a glance, without opening the ticker in a charting app.

**How to apply:**
- Add `close_history: list[float] = field(default_factory=list)` to `ScreenResult` (after `ma_120w`)
- In `screen_ticker()`, pass the last 12 Close values from `df["Close"].dropna().tail(12).tolist()` into the constructor
- In `generate_html_report.py`, render as inline SVG `<polyline>` (no JS dependency) scaled to the cell height
- Tests: add a test that `close_history` is populated when rows ≥ 12, and that empty history renders as empty cell not crash

**Pros:** Matches design intent. Zero new API calls (data already fetched in `fetch_weekly_ohlcv`). SVG is inline — no JS or chart lib dependency.
**Cons:** Changes `ScreenResult` dataclass interface — requires updating all constructors in tests (but CC makes this fast). Adds ~12 floats per stock to the in-memory result set (negligible).
**Effort:** XS-S (human: ~3h / CC: ~20 min)
**Priority:** P3
**Blocked by:** ~~HTML report v1 (generate_html_report.py) must be implemented first~~ — unblocked (shipped v0.4.1.0)
**Completed:** v0.4.2.0 (2026-04-19)

---

## P3: HTML Screener Report — Sector-Grouped View

**What:** Add an alternative sector-grouped view to the HTML screener report, either as a second `<section>` at the bottom or as a toggle. v1 uses 정배열/일반 grouping; this adds grouping by KIND 업종명.

**Why:** Sector rotation is one of the core use cases for the screener. Seeing "반도체 4종목, 바이오 3종목, 에너지 2종목" in one glance shows whether the breakout is broad-based or concentrated. The existing `ScreenResult.sector` field is already populated — no new fetches.

**How to apply:**
- In `generate_html_report.py`, add `_group_by_sector(results)` that returns `dict[str, list[ScreenResult]]` sorted by sector name
- Render as a second section `<h2>업종별</h2>` below the 정배열/일반 table
- Stocks with `sector=""` go under "기타"
- Tests: add test that sector grouping collapses stocks correctly and "기타" bucket catches empty sectors

**Pros:** No data cost. Uses `ScreenResult.sector` already in hand. Adds real analytical value.
**Cons:** Longer HTML output (two tables instead of one). Sector names from KIND can be verbose and inconsistent.
**Effort:** XS (human: ~1h / CC: ~10 min)
**Priority:** P3
**Blocked by:** ~~HTML report v1 (generate_html_report.py) must be implemented first~~ — unblocked (shipped v0.4.1.0)
**Completed:** v0.4.2.0 (2026-04-19)

---

## P3: Screener v2 — Condition G NaN Calibration (after first W17 run)

**What:** After the first Sunday run with v2 (2026-W17 or later), check what fraction of passed stocks have `ma_120w IS NULL` (i.e., passed via the NaN-safe fallback, not a real 120wMA comparison):

```sql
SELECT
    COUNT(*) FILTER (WHERE ma_120w IS NULL) AS null_count,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) FILTER (WHERE ma_120w IS NULL) / COUNT(*), 1) AS null_pct
FROM chart_signals
WHERE week_of = '2026-W17';
```

**Decision rule:**
- If `null_pct > 20%`: tighten condition G — NaN → G **fails** (require full data). Change `G = (not ma_120w_valid) or (close > ma_120w)` to `G = ma_120w_valid and (close > ma_120w)` in `screen_ticker()`.
- If `null_pct ≤ 20%`: current NaN-pass behavior is acceptable. No change.

**Why:** Stocks with < 100 weeks of data (recently listed, data gaps) pass condition G automatically. If these dominate the results, the 120wMA filter has no real effect on them. The 20% threshold ensures the filter is doing real work on at least 80% of passed stocks.

**How to apply:** Run the SQL after the first v2 screener run. Update `screen_ticker()` in `chart_screener.py` if threshold is breached. Re-run screener manually to confirm count change.

**Pros:** Closes the product hypothesis loop for condition G calibration.
**Cons:** Requires one full run of data before the decision can be made.
**Effort:** XS (human: ~10 min / CC: ~5 min)
**Priority:** P3
**Blocked by:** First successful v2 screener run.
**Completed:** v0.9.3.0 (2026-05-20) — `SCREENER_G_NAN_STRICT=1` env var 토글 추가; strict 모드에서 NaN→fail 동작.

---

## P4: Index on price_outcomes(checkpoint) for Backtest Query Performance

**What:** Add `CREATE INDEX IF NOT EXISTS idx_price_outcomes_checkpoint ON price_outcomes(checkpoint, return_pct)` to `init_db()` in `db.py`.

**Why:** `calculate_metrics()` and the new market_baseline query both filter `WHERE po.checkpoint = '1d'`. On a small table this is fine. As `price_outcomes` accumulates months of data (weeks × signals × checkpoints), a full table scan on every `/backtest` command or weekly report will become noticeable.

**How to apply:**
```python
# In db.py init_db() — after the existing CREATE TABLE statements
await conn.execute("""
    CREATE INDEX IF NOT EXISTS idx_price_outcomes_checkpoint
    ON price_outcomes(checkpoint, return_pct)
    WHERE return_pct IS NOT NULL
""")
```

**Pros:** Future-proof. One SQL line. `CREATE INDEX IF NOT EXISTS` is idempotent.
**Cons:** Premature optimization — no benefit until the table has 10k+ rows.
**Effort:** XS (human: ~10 min / CC: ~2 min)
**Priority:** P4
**Blocked by:** Nothing. Add after several months of accumulated backtest data confirm the query is slow.

---

## P2: Ticker Resolution Diagnostics (Phase 1 of approved design)

**What:** Add `_resolution_misses: Counter` to `market_data.py` and `get_resolution_miss_report(top_n=10)`. Log WARNING at step 5 when a ticker fully fails resolution (with fuzzy-miss guard). Call `get_resolution_miss_report(10)` at end of `collect_job()` in `run_scheduler.py`.

**Why:** We can't tune fuzzy matching without knowing which ticker names actually miss. Run for 1–2 weeks, collect the top misses, then calibrate threshold. Approved in design doc: `~/.gstack/projects/VontineDev-test_feed/Jin-feat-krx-listings-db-design-20260414-183901.md`.

**Priority:** P2
**Deferred from:** `feat/krx-listings-db` (v0.3.0.0)
**Completed:** v0.4.2.0 (2026-04-19)

---

## P2: resolve_fuzzy() in TickerCache (Phase 2 of approved design)

**What:** Add `resolve_fuzzy(name, threshold=0.82)` to `ticker_cache.py` using `difflib.SequenceMatcher`. Insert as step 3.5 in `market_data.get_price_context()` between the exact-cache lookup and the static YFINANCE_MAP. Change step 4 `elif` → `if`.

**Why:** Exact-name misses on lesser-known KRX stocks (e.g., LLM writes "셀트리온헬스케어" vs KRX "셀트리온헬스케어(주)"). Threshold 0.82 validated: 현대차 vs 현대차증권 = 0.75 < 0.82 (no false positive). Implement AFTER Phase 1 diagnostics produce real miss data.

**Priority:** P2
**Deferred from:** `feat/krx-listings-db` (v0.3.0.0)
**Blocked by:** Phase 1 diagnostics (need 1–2 weeks of real miss data to calibrate)
**Completed:** v0.9.3.0 (2026-05-20) — `resolve_fuzzy()` 추가(`ticker_cache.py`); `_parse_signal_json()`에서 exact→fuzzy→miss 순 해석; `_resolution_misses` 연동; `test_resolve_fuzzy.py` (13 tests).

---

## Completed

- `/backtest` command, `backtest_report_telegram()`, weekly Sunday report, DRY fix for `cross_analyze_historical()`, `await asyncio.sleep()`, WATCH hit_rate=None, data quality log, `fetch_pending_outcomes` limit 500, `test_backtest.py` (12 tests) **Completed:** v0.1.0.0 (2026-04-04)
- ISSUE-005 Telegram routing: all articles (Korean + foreign) gated behind `signal.is_actionable`; dead `tg_send` import removed (ISSUE-006); `test_telegram_routing.py` (4 regression tests) **Completed:** v0.2.1.0 (2026-04-06)
- ISSUE-001 LM Studio health check inference probe; ISSUE-002 Qwen3 `/no_think` prefix in `_call_ollama_native`; ISSUE-003 `requirements.txt`; ISSUE-004 stale comment in `signal_detector.py:104`; `test_summarizer_regression_1.py` regression tests **Completed:** v0.2.1.0 (2026-04-06)
- P3 backlog clean sweep: asyncio fix, KOREA_BASE_RATE staleness warning, market baseline in calculate_metrics(), APScheduler SQLAlchemyJobStore persistence, dict cache with isocalendar() in backfill_historical(); `test_backtest.py` expanded to 20 tests **Completed:** 2026-04-10
- ISSUE-001 (QA) Screener Telegram formatter over-escaped tickers in code spans (`005930.KS` → `005930\\.KS`); ISSUE-002 local `esc()` missing backtick; ISSUE-003 `test_db_dsn` isolation failure (load_dotenv restoring DB_PASSWORD during reload). All 3 fixed. `test_screener_telegram_regression_1.py` (8 regression tests). **Completed:** /qa 2026-04-16
- Article type classification: `article_type` field on `TradeSignal` + `SIGNAL_PROMPT` + `_parse_signal_json()` + DB migration + `save_signal()` + `fetch_latest_signals()` + `run_scheduler.py` call site + Telegram type badges + backtest type breakdown; `test_article_type.py` (17 tests) + 2 backtest tests. **Completed:** v0.2.5.0 (2026-04-16); **QA:** ISSUE-001 (WATCH inflating type breakdown denominator) fixed + 1 regression test (2026-04-16)
- ISSUE-QA-001 `test_screener_cmd.py` missing `__main__` guard — `asyncio.run(main())` at module level connected to production DB and sent 499-ticker screener results to Telegram on every `pytest` run. Fixed by adding guard. `test_screener_cmd_regression_1.py` (1 regression test). **Completed:** /qa 2026-04-18

## P2: HIGH CONFIDENCE Integration (v2 — after screener validation)

**What:** Cross-signal confirmation: when a stock appears in `chart_signals` this week AND triggers a news signal, flag the Telegram alert as HIGH CONFIDENCE. Requires: `confidence: str = "NORMAL"` field added to `TradeSignal` (last field in dataclass), `get_chart_signals_this_week(_db_pool)` called once per `collect_job()` cycle, intersection check `set(signal.ticker_symbols.values()) & chart_candidates` in `summary_worker()`, distinct Telegram format in `telegram_notify.py`.

**Why:** Validate screener output manually first (2-3 weeks). If weekly breakout stocks don't show visible momentum, the integration adds noise without signal. This is the riskiest assumption in the screener design.

**How to apply:** After 2-3 weeks of manual review of the Sunday Telegram screener output, if the stocks are showing real momentum: implement the integration. Files: `signal_detector.py`, `run_scheduler.py`, `telegram_notify.py`.

**Pros:** Reduces false positives in news signals. Startup-differentiating feature. The actual product hypothesis.
**Cons:** Adds DB read per collect_job() cycle (fast, indexed). Requires manual validation period.
**Effort:** S (human: ~1 day / CC: ~20 min)
**Priority:** P2
**Blocked by:** Screener running for 2-3 weeks. Manual review of output.
**Completed:** v0.9.3.0 (2026-05-20) — `confidence` 필드 추가(`TradeSignal`); 게이팅 버그 수정(`.values()` 기준 교차); `signal.confidence="HIGH"` 상향; Telegram `🔥 HIGH CONFIDENCE` 배지; `test_high_confidence.py` (7 tests).

---

## P3: Enhanced Ichimoku Conditions (v2 — after first Sunday run)

**What:** Add G/H/I conditions to `screen_ticker()` — conversion line > base line (전환선 > 기준선), both rising. Add J (ma_20w > ma_60w, 정배열) for ranking. Show "Enhanced" badge in Telegram weekly summary.

**Why:** The basic 6-condition screen may produce too many or too few candidates on the first real run. Calibrate count first, then add conditions to tighten quality.

**How to apply:** After first Sunday run, check `SELECT COUNT(*) FROM chart_signals WHERE week_of = current_week`. If > 50 candidates, add G+H+I to reduce noise. If < 5, consider loosening conditions instead.

**Pros:** Higher quality breakout signals. The `is_enhanced` column already exists in DB (zero migration cost).
**Cons:** Reduces candidate count — may over-filter in thin markets.
**Effort:** S (human: ~4h / CC: ~15 min)
**Priority:** P3
**Blocked by:** First real Sunday screener run.
**Completed:** v0.9.3.0 (2026-05-20) — `calc_ichimoku()`에 tenkan_sen/kijun_sen 추가; `screen_ticker()`에서 H(전환>기준)/I(둘다 상승) 판정 → `is_enhanced` 설정.

---

## P3: Backtest Chart Signal Accuracy (30+ days after launch)

**What:** Add `backtest_chart_signals()` to `backtest.py`. Pull `chart_signals` from last 4 weeks. Join with `price_outcomes` on ticker + date range. Compute hit rate (positive return at 1wk, 4wk checkpoints) vs KOSPI baseline. Report: does the Ichimoku breakout filter actually identify outperformers?

**Why:** The product hypothesis (chart breakout = higher upside probability) is unvalidated. This is the quantitative validation. Without it, you're shipping a filter based on faith in Ichimoku theory.

**How to apply:** After 30+ Sundays of screener runs (need enough samples). Extend the existing `weekly_backtest_report` job or add a separate command. The `price_outcomes` table already tracks future prices for trade_signals — extend it to also track `chart_signals` tickers.

**Pros:** Evidence-based decision to keep, tune, or drop the screener. Closes the product hypothesis loop.
**Cons:** Needs 30+ data points (6+ months). Requires extending price_outcomes tracking to chart_signals tickers.
**Effort:** M (human: ~2 days / CC: ~30 min)
**Priority:** P3
**Blocked by:** price_outcomes tracking extended to chart_signals tickers; 30+ weeks of screener data.

---

## P2: USER_MANUAL.md — Ollama/LLM Install Section Depth

**What:** When writing USER_MANUAL.md section 3 (설치 가이드), give Ollama installation
especially detailed treatment: model download step (`ollama pull qwen3.5:9b` — 4-8GB,
can take 10-30 min on slow connections), port configuration, and a dedicated
troubleshooting subsection for LLM failures (model not found, port in use,
download interrupted, out of disk space).

**Why:** Outside-voice review of the user manual design identified Ollama model setup
as the highest-abandonment step in the install flow. A Korean VPS user downloading
a 7B model on limited bandwidth with no progress feedback will give up or misdiagnose
failure. Getting this section right determines whether a stranger successfully completes
the install.

**How to apply:** When Claude Code writes USER_MANUAL.md, instruct it to treat the
Ollama section as a first-class install guide (not a one-liner) and add these
troubleshooting entries to section 10: `ollama list` shows no models, `ollama serve`
port 11434 already in use, model download interrupted (resume with same pull command).

**Pros:** Reduces the most common first-time install failure. Directly improves
Success Criterion 1 (stranger installs without asking a question).
**Cons:** Adds ~1-2 pages to the manual. Minor length increase.
**Effort:** S (human: ~30 min / CC: ~5 min)
**Priority:** P2
**Blocked by:** USER_MANUAL.md writing session started.
**Completed:** v0.4.2.0 (2026-04-19)

---

## P3: 3-Stage Classifier — Daily Ticker Cap: Start at 150, Expand to 300 After Measurement

**What:** Sprint 2 initial deployment caps `daily_flow` fetch to 150 tickers (not 300). After 2 weeks of production runs, check p50/p99 yfinance fetch latency in logs. If p99 < 0.5s, expand to 300.

**Why:** yfinance throttles at large batch sizes. The 17:00 KST deadline (30-minute job window) is at risk if fetch latency exceeds ~0.3s/ticker with 8 workers. Starting at 150 ensures the deadline holds on launch day. Real data informs expansion — not estimates.

**How to apply:**
- In the Sprint 2 daily job, use: `cap = int(os.environ.get("DAILY_CLASSIFIER_TICKERS", "150"))`
- Log p99 fetch time per daily run: sort `[INFO] [일봉] ... → API 수집` timestamps
- When p99 < 0.5s for 5 consecutive runs: set `DAILY_CLASSIFIER_TICKERS=300` in .env

**Pros:** Safe launch. Real measurement informs scale-up. Env-var configurable at runtime.
**Cons:** Initially misses the 150-300 ticker range. Top Ichimoku candidates by score are still included.
**Effort:** XS (human: ~15 min / CC: ~5 min)
**Priority:** P3
**Blocked by:** Sprint 2 daily job implementation.
**Completed:** v0.9.3.0 (2026-05-20) — `DAILY_CLASSIFIER_TICKERS=150` env var; Ichimoku 통과 종목 우선 포함 후 나머지 채움.

---

## P3: 3-Stage Classifier — Tighten News Gating After Sprint 2 Ships

**What:** Post-Sprint 2, change news eligibility rule from:
`ticker in Ichimoku output OR in daily_flow within 7 days`
to:
`ticker in Ichimoku output OR has active stage_classification (Stage 1/2/3) within 7 days`

**Why:** The current Sprint 1 gating is an improvement but still lets through tickers that have daily_flow data but failed all stage conditions (classified as None). True screener-first requires an active stage classification.

**How to apply:**
- In `summary_worker` eligibility check, add DB query: `SELECT 1 FROM stage_classifications WHERE ticker=$1 AND classified_date >= now()-interval '7 days' AND stage IS NOT NULL LIMIT 1`
- Replaces the `daily_flow` 7d check entirely
- Sprint 1 gating remains unchanged until Sprint 2 is in production with 7+ days of stage_classifications data

**Pros:** True screener-first. News only for stocks actively staged.
**Cons:** Requires Sprint 2 populated before tightening is meaningful.
**Effort:** XS (human: ~20 min / CC: ~5 min)
**Priority:** P3
**Blocked by:** Sprint 2 (stage_classifications table populated for ≥ 7 days).
**Completed:** v0.9.3.0 (2026-05-20) — `_active_stage_tickers` 전역 캐시; `get_active_stage_tickers()` DB 함수; 게이팅: 스크리너 OR Stage 7일 이내 분류 종목 통과, 스크리너 교차 시 HIGH CONFIDENCE.

---

## P3: backtest_engine — Non-Standard Measurement Period (on-the-fly N-week)

**What:** When `/backtest stage 8` is called and 8w is not a stored period, compute the actual return on-the-fly from the stored price snapshot instead of falling back to the nearest standard period (4w). Currently the MVP falls back to nearest standard period with a "(closest: 4w)" note.

**Why:** Users may want specific horizons (6w, 8w, 10w) that don't map cleanly to the stored 1/4/13 week checkpoints. The stored OHLCV data is already present in signals2.json — on-the-fly computation is straightforward.

**How to apply:**
- In `backtest_engine.py`, `build_comparison_report(measure_weeks=N)` checks if N is in [1, 4, 13]. If not, for each signal fetch `_nearest_price(stock_lookup, signal_date + timedelta(weeks=N))` and compute the return directly.
- Requires that signals2.json stores the 60-day OHLCV snapshot per ticker, OR re-fetches from yfinance at report time (slower but simpler).
- If re-fetching: cache results in `/tmp/backtest_ohlcv_cache/` keyed by ticker+date to avoid duplicate fetches within one report run.

**Pros:** Flexible measurement horizon for power users. No schema change to signals2.json.
**Cons:** Re-fetching OHLCV adds 5-30 seconds to report generation for non-standard periods. Acceptable if the note explains the delay.
**Effort:** XS (human: ~30 min / CC: ~5 min)
**Priority:** P3
**Blocked by:** ~~Sprint 3 (backtest_engine.py) must ship first.~~ Unblocked (shipped v0.7.0.0). Note: actual implementation uses direct yfinance OHLCV fetch per-run, not signals2.json; re-fetch approach is already in place. Just needs `--hold-weeks N` param added to `BacktestConfig` and `_fill_returns`.
**Completed:** v0.7.3.1 (2026-05-09) — `hold_weeks` added to `BacktestConfig`, `_fill_returns`, `_compute_group_metrics`; custom period shown in both reports.

---

## P3: backtest_engine — Stage 2 Replay (_replay_stage2)

**What:** Add `_replay_stage2()` to `backtest_engine.py`. Currently only Stage 1 is replayed in `stage` mode. Stage 2 walk-forward requires: (1) replaying Stage 1 signals first, (2) looking forward 14 days from each Stage 1 signal for Stage 2 conditions.

Stage 2 conditions to replay:
- Condition 1: `close` in Stage 1 high −5% ~ −20% range (uses `s1_high = close_at_signal_day`)
- Condition 2: `close >= MA20 * 0.95`
- Condition 3: `vol_today / vol_s1_day` in [0.30, 0.60] (거래량 비율)
- Condition 4: `inst_streak >= 0` — not replayable (no historical 수급), skip as with Stage 1 수급

**거래대금 vs 거래량 note:** `compare_tx_amt.py` validated (2026-04-27, 10 tickers) that `Volume × Close` approximates actual 거래대금 with 1.38% mean absolute error, 3.55% max. For the 30~60% ratio check, errors partially cancel. Use volume-based ratio for Condition 3 backtest.

**How to apply:**
- In `backtest_engine.py`, add `_replay_stage2(ticker, name, daily_df, market, config)` that first calls `_replay_stage()` (or inlines Stage 1 check) to find S1 dates, then for each S1 signal date scans the next 14 days for Stage 2 conditions.
- Add `"stage2"` to `BacktestConfig.mode` valid values.
- Add `test_replay_stage2.py` with at least: S1 prerequisite check, each condition independently, 14-day lookback boundary, condition 4 skipped gracefully.
- (Note: `run_backtest.py` was removed — `/backtest stage2` Telegram command is the intended interface)

**Pros:** Validates the full 3-stage classifier pipeline end-to-end. Can measure whether Stage 2 entries outperform raw Stage 1.
**Cons:** Replaying Stage 2 requires Stage 1 history in the same data window — increases memory footprint for long backtests. The 수급 skip means Stage 2 replay is 3/4 conditions, same limitation as Stage 1.
**Effort:** S (human: ~1 day / CC: ~20 min)
**Priority:** P3
**Blocked by:** Nothing. `compare_tx_amt.py` validation complete (2026-04-27).
**Completed:** v0.7.3.2 (2026-05-09) — `_replay_stage2`, `mode="stage2"`, `tests/test_replay_stage2.py` (25 tests).

---

## P3: /watchlist bot command — on-demand watchlist view

**What:** Add `/watchlist` to `telegram_bot.py` as an on-demand command. Calls `_watchlist_brief_job()` logic and sends the result to the requesting chat.

**Why:** The 17:00 KST brief is scheduled, but if you want to check the watchlist at 10:00 or after a news event, you have to wait. On-demand access makes the system interactive.

**Context:** `_watchlist_brief_job()` is module-level and uses `_db_pool` global. The bot command would need to call the core query/format logic without the scheduler context. Best approach: extract a `get_watchlist_entries(pool)` helper from `_watchlist_brief_job` and call it from both the scheduler job and the bot command handler.

**How to apply:**
1. Extract the data assembly logic (Steps 2-7 in `_watchlist_brief_job`) into `async def _build_watchlist_entries(pool)` in `run_scheduler.py` or a new `watchlist.py`.
2. In `telegram_bot.py`, add `/watchlist` handler that calls `_build_watchlist_entries` and sends the brief to the requesting `chat_id`.
3. Register the command in `init_bot()`.

**Pros:** Makes the system interactive — user can check status anytime. Enables manual refresh after a Stage 1 signal fires during the day.
**Cons:** Requires refactoring `_watchlist_brief_job` to extract the data logic into a reusable function. Adds one bot command to maintain.
**Effort:** XS→S (human: ~1h / CC: ~15 min after the refactor)
**Priority:** P3
**Depends on:** Watchlist brief being stable with real `stage_classifications` data (2-3 weeks post-launch).
**Completed:** v0.9.3.0 (2026-05-20) — `_build_watchlist_entries(pool)` 추출; `/watchlist` 핸들러 + 라우팅; `_register_commands()` 등록; `send_watchlist_brief(target_chat_id=)` 파라미터 추가.

---

## P3: Vol ratio delta (∆ vs yesterday) in watchlist brief

**What:** Show "+5%" or "-12%" change vs yesterday's vol ratio in each ticker line of the daily brief.

**Why:** Trend matters more than absolute ratio. A ratio of 0.8 falling from 1.2 is very different from a ratio of 0.8 rising from 0.5. The delta tells you which direction the rally is heading.

**Context:** `watchlist_vol_log` table is now created and populated by `_watchlist_brief_job`. Yesterday's ratio is available via `get_watchlist_vol_log(pool, tickers, lookback=2)`. The delta is `today_ratio - yesterday_ratio`.

**How to apply:**
1. In `_watchlist_brief_job`, load `lookback=2` from `watchlist_vol_log` to get yesterday's ratio.
2. Compute `delta = today_ratio - yesterday_ratio` (None if no prior entry).
3. Pass `vol_ratio_delta` to each entry dict.
4. In `send_watchlist_brief`, format as `+5%▲` or `-12%▼` after the ratio line.

**Pros:** Trend visibility with near-zero extra cost (one query already done). Makes the brief more actionable.
**Cons:** First day of tracking has no delta. Minor display change.
**Effort:** XS (human: ~30 min / CC: ~10 min)
**Priority:** P3
**Depends on:** `watchlist_vol_log` populated for at least 1 trading day (now being built).
**Completed:** v0.9.3.0 (2026-05-20) — `vol_ratio_delta` 계산·전달; `send_watchlist_brief`에서 `+5%▲`/`-12%▼` 포맷 표시.

---

## P3: YouTube 내러티브 — fill_forward_returns 배치 처리 개선

**What:** `fill_forward_returns()`를 현재의 per-row commit 루프에서 단일 배치 upsert로 변경.
130줄 함수를 `_calc_returns()` + `_upsert_forward_return()` 헬퍼로 분리.

**Why:** pre-landing review에서 발견. 현재 루프는 종목당 개별 `commit()`을 호출해
N회 트랜잭션이 발생. `save_mentions()` / `compute_attention_scores()`의 배치 패턴과 불일치.

**Effort:** S (human: ~30min / CC: ~15min)
**Priority:** P3
**Depends on:** 없음
**Completed:** v0.10.1.0 (2026-06-03) — per-row commit 루프 → 단일 execute_values + commit으로 교체.

---

## P3: YouTube 내러티브 — 매직 넘버 상수화

**What:** `youtube_narrative_sync.py`의 인라인 리터럴을 모듈 레벨 상수로 추출.
- `8000` → `_MAX_TRANSCRIPT_CHARS = 8000` (Gemini 토큰 상한)
- `4` → `_GEMINI_RPM_SLEEP = 4.0` (free-tier 15 RPM 대응)
- `500` → `_FILL_RETURNS_BATCH = 500` (per-call 처리 행 수)

**Why:** pre-landing review에서 발견. 숫자만으로는 조정 의도를 알기 어렵고
변경 시 같은 값을 두 곳에서 수정해야 하는 위험 있음.

**Effort:** XS (human: ~5min / CC: ~5min)
**Priority:** P3
**Depends on:** 없음
**Completed:** v0.10.1.0 (2026-06-03) — 모듈 상수 3개 추출; 참조 3곳 교체.

---

## P3: D+10 retirement notice in watchlist brief

**What:** When a ticker's `days_since == 10` (its last tracking day), add a "[마지막 추적일]" marker in the brief and a closing line summarizing the final status.

**Why:** Currently tickers just disappear from the next brief. A retirement notice closes the loop — user knows the 10-day period ended and can make a final decision on the position.

**Context:** `days_since = (today - s1_date).days` is already computed in `_watchlist_brief_job`. When `days_since >= 10`, add a retirement marker to the entry dict and format it in `send_watchlist_brief`.

**How to apply:**
1. In `entries` assembly, add `"retiring": days_since >= 10`.
2. In `send_watchlist_brief`, when `e.get("retiring")`, append `" [마지막 추적일]"` to the ticker line.
3. Optionally send a separate "퇴장" message for retiring tickers.

**Pros:** User clarity. Prevents confusion about why a ticker disappeared.
**Cons:** `get_stage1_watchlist(days=14)` would still include it for 4 more days after D+10. Need to cap at `days <= 10` or display as "retired."
**Effort:** XS (human: ~20 min / CC: ~5 min)
**Priority:** P3
**Depends on:** Watchlist brief stable with real data.
**Completed:** v0.9.3.0 (2026-05-20) — `"retiring": days_since >= 10`; `send_watchlist_brief`에서 D+10부터 `[마지막 추적일]` 표시.
