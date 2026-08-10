# TODOS

Items deferred from code review and planning sessions.

---

## P3: TechnicalQuant SCENARIO2 필터+진입/청산 파라미터 최적화 — ✅ 완료

**What:** `scripts/run_quant_backtest.py --condition SCENARIO2 --use-fundamentals`
(시가총액 상위200 ∩ PER≤15 — 문서 2안 정확 재현)만 네 가지 검증 방식(기술적단독/
범용필터/문서정확재현/필터스윕) 전부에서 일관되게 양호 — 나머지 개별 조건(A~E)과
1안(문서 정확 재현 시 승률 22.2%/평균수익 -1.3%, 마이너스)은 거래비용 감안 시
무의미하거나 음의 엣지로 판정됨(`project_technicalquant_backtest` 메모리 참고).

**필터(종목선택) 최적화 — ✅ 완료(2026-08-07):** `scripts/run_quant_filter_sweep.py`로
PER 상한 `[10,12,15,18,20,25]` × 시가총액 상위 `[100,150,200,300,500]` 30조합
그리드서치. 매매타이밍(RSI 30/70, -7% 손절)은 고정. 결과: PER≤18, 시총상위200이
문서 원안(PER≤15)보다 우수(신호100→129건, 승률43.0%→46.5%, 평균+2.9%→+4.06%). 시총
상위500까지 넓히면 PER 상한과 무관하게 전부 성능 하락(유니버스 품질 희석). 전체 결과:
`results/quant_scenario2_filter_sweep.csv`.

**진입/청산 파라미터 최적화 — ✅ 완료(2026-08-07):** 필터를 위 최적값(PER≤18/
시총상위200)으로 고정하고 RSI 진입 `[20,25,30,35,40]` × RSI 청산 `[60,65,70,75,80]`
× 손절폭 `[5,7,9,12]%` 100조합 그리드서치(`scripts/run_quant_entry_exit_sweep.py`).
패턴: RSI 청산/손절폭을 넓히는 방향(70→80, -7%→-12%)이 진입 임계값과 무관하게
일관되게 평균수익 개선. 진입 임계값은 원안(30) 유지 시(=신호셋 129건 불변, 과최적화
위험 낮음) 청산만 80/-12%로 넓히면 승률46.5%→50.4%, 평균+4.06%→+9.47%. 진입까지
완화(예: 40)하면 표면상 더 높지만(+14%대) "과매도" 정의를 벗어나고 상승장 베타 포획
위험이 있어 채택 보류. 상세 비교표는 `project_technicalquant_backtest` 메모리
5단계 참고.

`analysis/backtest/quant_signals.py`의 `replay_quant()`에 `rsi_oversold`/
`rsi_overbought` 파라미터를 추가(기본값은 문서 원안과 동일 — 기존 호출부 무변경).

**필터 배리에이션(SCENARIO3~8) — ✅ 완료(2026-08-08):** `scripts/run_quant_scenario_variants.py`로
1안/2안의 기존 매매타이밍은 그대로 두고 필터·유니버스만 바꿔 6개 시나리오 추가
비교. 2안 기술 유지 + **PBR 0.2~1.0 단독 필터**(SCENARIO5)가 PER≤18보다도 우수
(신호121, 승률51.2%, 평균+5.4%) — 지금까지 찾은 최고 필터. 1안(MA20돌파)은
유니버스·필터를 어느 쪽으로 완화해도(신호9→최대323건) 전부 breakeven 이하로
확인돼 **폐기 확정**.

**SCENARIO5 + 최적청산 결합 — ✅ 완료(2026-08-08):** `run_quant_entry_exit_sweep.py`에
`--filter-mode {per,pbr}` 추가, PBR 유니버스에도 동일한 RSI/손절 100조합 그리드서치
재적용. 최고 조합(진입30/청산80/손절-12%)이 PER 유니버스와 정확히 같은 방향으로
재현 — **신호116건, 승률55.2%, 평균+16.82%(전체 연구 최고, 과최적화 위험 있어
절대수치는 액면가 신뢰 금지, 방향성만 신뢰)**.

**최근 2개월 구간 재현 + 시장 귀인 분석 — ✅ 완료(2026-08-10):** 2026-06-01~08-06만
떼어 재현하면 신호 20~23건(신뢰기준 30건 미달)에 평균수익 마이너스(PER18 -1.3%,
PBR -2.5%) — 언뜻 전략이 무너진 것처럼 보였으나, `_fetch_index("^KS11"/"^KQ11")`로
같은 기간 KOSPI/KOSDAQ과 비교한 결과 **이 구간 자체가 KOSPI -28.5%/KOSDAQ -21.9%
급락장**(직전 구간은 +260%/+49% 상승장, 변동성도 2배 급등)이었음이 확인됨.
지수 대비 알파(종목수익률-동일기간지수수익률)는 **PER18 +11.1%p, PBR +13.9%p로
크게 플러스**(신호의 80%↑가 지수를 상회) — 절대수익 마이너스는 전략 결함이
아니라 시장 전체 하락(베타) 때문이었고, 종목선정·타이밍(알파)은 오히려 이
급락장에서 시장보다 훨씬 방어적이었음. 표본이 작아 일반화는 보류, 이번 한 번의
급락 사례에서의 상대적 방어력으로만 해석.

**최종 권장 SCENARIO2 설정**: 실전 고려 시 보수적 대안은 PER≤18∩시총상위200 +
진입RSI30 + 청산RSI80/손절-12%(신호129, 승률50.4%, 평균+9.47%, 여러 단계에서
반복 검증됨). 이론적 상한은 PBR 0.2~1.0∩시총상위200 + 동일 청산(신호116,
승률55.2%, 평균+16.82%, 과최적화 위험). 어느 쪽이든 [[project_compose_strategies]]의
FUNNEL-1/SCORE-1 대비 여전히 열위 — 주력 전략 채택은 보류, 참고용 보조 신호로만 남김.

**Effort:** S (human: ~1h / CC: ~20min)
**Priority:** P3 (완료)
**Found:** 2026-08-06, TechnicalQuant.md 백테스트 세션. 필터 최적화·진입/청산
최적화 2026-08-07 완료, 필터 배리에이션(SCENARIO3~8)·PBR+최적청산 결합·최근구간
시장귀인 분석 2026-08-08~08-10 완료 — 항목 종결. 상세 근거·비교표는
`project_technicalquant_backtest` 메모리(1~8단계) 참고.

---

## P2: 섹터통계 잡 — daily_ohlcv 컬럼명 불일치로 매일 조용히 실패 — ✅ 완료

**What:** `jobs/sector_stats_job.py`(평일 20:30 KST)가 매일 `[섹터통계] 집계 쿼리
실패: column o_today.ticker does not exist`로 실패 — 최소 07-31부터(도입 시점부터로
추정) 하루도 빠짐없이 실패해 `sector_daily_stats` 테이블이 **0건**이었음.

**원인:** `daily_ohlcv` 테이블의 실제 컬럼명은 `symbol`/`date`인데, 쿼리는
`daily_flow`/`stage_classifications`와 같은 네이밍(`ticker`/`trade_date`)으로
JOIN하고 있었음 — `daily_ohlcv`만 다른 네이밍 컨벤션.

**영향:** 이 테이블을 읽는 대시보드/텔레그램 소비처가 아직 없어 사용자 영향은
없었음 — 매일 에러 로그만 남기던 죽은 기능.

**수정:** JOIN 조건 4곳을 실제 스키마(`symbol`/`date`)에 맞게 교체. 실 DB 대상
라이브 스모크 테스트로 쿼리가 더 이상 크래시하지 않고(반환값 0, "집계 결과
없음") 정상 동작함을 확인 — `daily_flow`에 실행 시점 기준 당일 데이터가 아직
없어 0건인 것도 정상 동작(내일 20:30 정규 실행 시 데이터 있으면 실제 집계됨).
이 잡을 커버하는 기존 테스트가 없고, 버그 자체가 "스키마와 안 맞는 컬럼명"이라
목업 기반 단위테스트로는애초에 못 잡았을 유형이라 별도 유닛테스트는 추가하지
않음 — 실 DB 스모크 테스트로 검증.

**부수 수정:** 같은 세션에서 `tests/test_scheduler_collect.py`의 사이트맵 신선도
테스트가 고정 날짜 문자열(`2026-08-05T20:10:21+09:00`)을 써서 다음 날 실행 시
"24시간 초과"로 깨지는 걸 발견 — 테스트 실행 시각 기준 상대 타임스탬프로 교체.

**Effort:** XS (human: ~15min / CC: ~10min)
**Priority:** P2
**Found:** 2026-08-06, "다음 작업 찾기" 세션 중 로그 이상치 스캔으로 발견

---

## P1: Kiwoom 토큰 자동 만료 — 장시간 가동 시 손절 감시 완전 무력화 — ✅ 완료 (자동 재발급)

**What:** `KiwoomClient.issue_token()`은 프로세스 시작 시 단 한 번만 호출되고
갱신 로직이 없음. 2026-08-04 라이브 로그 확인: 08-03 19:46 KST 스케줄러 재시작
이후 재시작 없이 계속 가동되다가, 08-04 15:20 KST `paper_exit_checker_job`
실행 시 **오픈 포지션 40건 전부**가 `ka10001` 현재가 조회에서
`API 오류 [ka10001]: 인증에 실패했습니다[8005:Token이 유효하지 않습니다]`로
실패 → `현재가 없음 — 스킵` → 청산=0으로 종료. 관측된 토큰 수명은 발급 후
~18~21시간(같은 시각 발급된 두 키의 만료 시각이 서로 다름 — Kiwoom이 고정
기간이 아니라 다른 기준으로 만료시키는 것으로 추정)으로, 24시간보다 짧아
스케줄러가 하루 이상 재시작 없이 가동되면 반드시 재현되는 구조적 결함이었음.

**연쇄 피해:** 같은 이슈로 08-03은 별도 배포 버그(`compute_slot_krw`
ImportError, 그날 저녁 자연 해소)로 paper 잡 자체가 하루 종일 죽어있었고,
08-04는 이 토큰 만료로 또 죽어 — **2거래일 연속 손절 감시가 무력화**됨.
그 결과 08-05에 처음 정상적으로 가격 조회가 성공하자, 그동안 hard_stop_pct
10%를 훨씬 초과해(최대 -48%) 방치돼있던 포지션 21건이 한꺼번에 매도 신호를
발생시킴 — 공교롭게 같은 날 발견된 `confirm_fill` 버그와 겹쳐 매도는
됐으나 DB 미반영 상태로 남음(위 "체결 확인 절차" 항목 참고).

**수정:** `data/kiwoom_aftermarket_sync.py`의 `KiwoomClient`—
`issue_token()`이 appkey/secretkey를 인스턴스에 저장(`self._appkey`/
`self._secretkey`), `_post()`가 응답의 `return_msg`에 `"8005"`가 포함되면
저장된 credential로 자동 재발급 후 해당 요청을 1회 재시도하도록 수정(무한
루프 방지 — 재발급은 호출당 최대 1회, 재발급 후에도 실패하면 즉시 예외).
`inject_token()`으로만 인증한 경우(appkey/secretkey 미보유)는 기존처럼
즉시 예외 — 재발급 불가 상황을 조용히 삼키지 않음. 기존 429(rate limit)
재시도 로직과 동일 루프 안에서 공존하도록 리팩토링(헤더 생성을 루프
내부로 이동, 매 재시도마다 최신 토큰 반영).
`tests/test_kiwoom_client_token_refresh.py` 신규(7 테스트: 정상 케이스,
8005 감지 후 재발급+재시도 성공, credential 없을 때 즉시 예외, 재발급해도
여전히 실패 시 무한루프 없이 예외, 8005 아닌 다른 오류는 재발급 시도 안 함,
429 재시도 하위호환). 전체 스위트 894건 통과, pyright/ruff 클린.

**Effort:** M (human: ~1h / CC: ~40min)
**Priority:** P1
**Found:** 2026-08-05→06, "다음 작업 찾기" 세션 중 08-04 로그 이상치(손실 -40%대) 추적 중 발견
**주의:** 이 수정은 스케줄러 재시작 후에야 실 프로세스에 반영됨 — 커밋 후 재시작
필요(직전 재시작이 08-05 21:53 KST라 새 코드 반영 없이는 다음 만료 위험 구간이
08-06 15:20 exit-checker와 겹칠 수 있었음).

---

## P1: 한국경제(hankyung) RSS 완전 차단 — Cloudflare JS 챌린지 — ✅ 완료 (사이트맵으로 대체)

**What:** `jobs/scheduler_collect.py`가 10분마다 수집하는 `hankyung/feed/economy`,
`hankyung/feed/finance` 두 RSS 피드가 2026-08-02부터 최소 3일간(하루 288건 로그
전부) **100% 403 실패** — `logs/news_crawler.log` 정기 점검이 아니라 우연히 발견됨.
재시도(3회/회차)에도 매번 실패해 핵심 뉴스 소스 하나가 조용히 완전히 빠진 상태였음.

**원인:** UA/Referer/헤더 문제 아님 — `/feed/*` 경로만 Cloudflare가 JS 챌린지
페이지(`"Just a moment..."`, 403)로 막고 있고, 홈페이지·`robots.txt`는 동일 요청·
헤더로 전부 정상(200). `robots.txt`에도 `/feed/` disallow 규정 없음 — 정책적
차단이 아니라 WAF 룰. JS 실행이 필요한 챌린지라 httpx 헤더 조합으로는 우회 불가능.

**대안:** `https://www.hankyung.com/sitemap/latest-article.xml`(구글 뉴스 사이트맵)은
차단되지 않음 — 최근 기사 838건이 `<loc>`/`<news:title>`/`<news:publication_date>`로
제공돼 RSS가 주던 필드(title/url/published)를 그대로 대체.

**수정:** `jobs/scheduler_collect.py`에 `_parse_sitemap()`/`_parse_sitemap_dt()` 추가,
`fetch_feed()`가 `cfg["type"]`(`rss`\|`sitemap`, 기본값 `rss`)로 파서를 디스패치하도록
리팩토링(중복 재시도 루프 제거). `run_scheduler.py`의 hankyung 두 RSS 엔트리를
`type: sitemap` 단일 엔트리로 교체(economy/finance 구분은 사라지지만 원래도 둘 다
`category="korea"`로 동일 취급됐음 — 영향 없음). `tests/test_scheduler_collect.py`
신규(12 테스트: 사이트맵 날짜 파싱, url/title 추출, news 블록 없는 entry 처리,
fetch_feed의 rss/sitemap 디스패치, 신선도 필터, 403 재시도). 실제 프로덕션
헤더로 라이브 스모크 테스트 — 429건 정상 수집 확인. 전체 스위트 887건 통과,
pyright/ruff 클린.

**Effort:** S (human: ~1h / CC: ~30min)
**Priority:** P1
**Found:** 2026-08-05, "오늘 할 일 찾기" 세션 중 로그 훑다가 발견 (TODOS.md에 기록된 적 없던 이슈)

---

## P1: 체결 확인 절차(ka10076) — 첫 실거래 로그 확인 필요 — ✅ 완료 (결함 발견 및 수정)

**What:** 아래 항목("모의투자 — 매수/매도 주문 체결 확인 절차 부재")에서 구현한
`confirm_fill()`/`check_execution()`이 실제 신규 주문에서 정상 동작하는지 아직
라이브로 확인 못 함 — 라이브 검증 시도했지만 모의투자 서버가 과거 체결 이력을
보관하지 않아(오래된 주문 조회 시 빈 응답) 히스토리 기반 검증이 불가능했음.

**How to apply:** 다음 실거래(평일 09:05 KST `paper_open_entry_job` 매수 진입, 또는
15:20 KST `paper_exit_checker_job` 매도 청산) 이후 `logs/news_crawler.log`에서 확인:
1. `[paper] {ticker} 매수 체결 확인: N/M주 (주문번호=...)` 로그가 정상적으로 찍히는지
2. 미체결(`N=0`) → `buy_never_filled`로 closed 처리 + 텔레그램 경고가 실제로 발생하는지
3. 부분체결(`0<N<M`) 케이스가 있으면 실체결 수량 기준으로 `paper_positions.qty`가
   정확히 기록됐는지 DB로 교차 확인
4. 에러 없이 완료됐는지(`[paper-entry] 완료` 로그까지 도달)

**Effort:** XS (확인만, ~5분)
**Priority:** P1
**Found:** 2026-08-04, 체결확인 기능 구현 세션 — 라이브 검증이 막혀 다음 실거래로 이월

**결과 (2026-08-05, 15:20 KST `paper_exit_checker_job` 라이브 로그 확인):** 매도 21건
전부 `매도 체결 확인: 0/N주`(미확인) — 예상과 달리 "정상 동작 확인"이 아니라 **버그
발견**. `get_positions()`로 실제 브로커 잔고 교차 확인 결과 21건 전부 실제로는 100%
매도 완료(계좌 보유수량 0)였는데, `check_execution()`(ka10076)이 신규 주문 직후든
4시간 뒤 재조회든 항상 빈 체결내역(`cntr: []`)만 반환하는 것으로 확인됨 — "과거
이력만 안 남는다"가 아니라 이 계좌에서 ka10076 자체가 구조적으로 못 쓰는 TR이었음.
`paper_positions` DB에는 21건 전부 `status='open'`으로 남아 브로커 실보유(0주)와
어긋난 상태 확인 — 다음 실행에서 잔고 부족으로 재매도 실패가 예정돼 있었음(실제로
같은 날 `000500.KS`에서 "매도가능수량 부족" 에러로 선행 관측됨). 매수 쪽도 동일
로직이라, 체결된 매수가 `buy_never_filled`로 오판정될 위험도 동일하게 존재했음.

**수정:** `confirm_fill()`을 ka10076 기반에서 `get_positions()`(kt00018) 전후 보유수량
스냅샷 델타 비교로 교체 (`data/kiwoom_paper_trader.py:get_position_qty()` 신설). 호출부
(`jobs/paper_jobs.py` 매수/매도 양쪽)에서 주문 제출 **직전** 보유수량을 스냅샷해
`confirm_fill(..., qty_before=...)`로 전달 — 같은 티커를 동시에 보유한 다른 모델의
몫과 무관하게 이 주문이 실제로 바꾼 수량만 델타로 잡아냄. `check_execution()`은 응답
파싱 로직 자체는 정상이라 삭제하지 않고 유지(테스트 6건 그대로 통과), `confirm_fill()`
내부에서만 더 이상 호출 안 함. `tests/test_kiwoom_execution_check.py`의
`TestConfirmFill` 4개 테스트를 새 시그니처(`qty_before` 필수)로 재작성 + 동시보유
델타 검증 테스트 2건 추가(12개 전체 통과). pyright/ruff 클린.
DB에 남은 21건의 stale `open` 행 자체는 이번 세션에서 수동 정정하지 않음(코드 수정만
범위) — 다음 15:20 실행 시 매도 재시도 → 새 로직이 즉시 (qty_before=0 또는 이미
0이었던 잔고 확인) `filled=0`으로 정확히 판정할 것으로 예상되나, 이 경우도
"매도가능수량 부족" API 에러 자체는 여전히 발생할 수 있어 별도 관찰 필요.

**2026-08-10 추가 결함 발견 (investigate 세션, "오늘 매도 왜 안 됐는지" 질문 계기):**
위 수정(`get_position_qty()` 신설)이 그 자체로 또 다른 회귀를 안고 있었음 —
`get_positions()`(kt00018)이 종목코드를 `"A005930"`처럼 거래소 접두사를 붙여
반환하는데 `get_position_qty()`는 접두사 없는 6자리(`_to_6digit()`)와 비교해,
비교가 **항상 실패**하고 보유수량이 실제와 무관하게 늘 0으로 잡혔다. 즉 08-05
수정이 "체결 확인 무력화"라는 같은 증상을 다른 경로로 재도입한 것 — 유닛
테스트가 `get_position_qty`를 MagicMock으로 직접 스텁했기 때문에(`confirm_fill`
델타 로직만 검증) 이 내부 구현 버그는 테스트로 잡히지 않았음.

**파급 확인 (실계좌 대조):** `paper_positions.status='open'` 19건 중 5건이 실보유와
불일치 — `000500.KS`(DB 28 vs 실 7), `001210.KS`(7692 vs 1538), `483650.KS`(43 vs 27),
`006340.KS`(918 vs 0, 08-07 매도주문 0146149 완전체결 후 미확인 처리), `073240.KS`
(1605 vs 0, TP1 이후 트레일 청산이 미확인 처리). 반대 방향으로, 매수 쪽 오탐으로
브로커에만 남고 DB엔 아예 행이 없는 유령 보유가 20건(오늘 8건 포함, 예: CJ CGV
976주, F&F 81주, 삼성전자우 31주) — 이쪽은 실 체결가/일시 기록이 없어(`paper_open_entry_job`의
`update_to_closed(..., 0.0, "buy_never_filled", ...)`가 시도 시점의 시가를 보존하지
않는 별도 결함까지 겹침) 자동 복구 불가, 별도 트리아지 필요.

**수정:** `get_positions()` 파싱 단계에서 `"A"` 접두사를 정규화(`data/kiwoom_paper_trader.py`)
— `get_position_qty()`를 포함해 이 메서드를 쓰는 모든 코드가 일관된 6자리 코드로
비교하도록 함. 회귀 테스트 4건(`TestGetPositionsStkCdNormalization`) 추가 — 수정 전
실패 확인 후 복구. `paper_positions` 5건은 실계좌 대조 후 수동 정정(위 문단 수치대로
qty 정정 또는 `exit_type='reconciled_qty_drift'`로 closed 처리, exit_price는 실제
체결가 대신 정리 시점 현재가로 근사 — 정확한 과거 체결가는 이 계좌의 체결이력 조회
TR(ka10076)이 구조적으로 못 쓰는 관계로 복구 불가능).

---

## P1: 모의투자 — 매수/매도 주문 체결 확인 절차 부재

**What:** `paper_open_entry_job`/`paper_exit_checker_job`(`jobs/paper_jobs.py`)이
`place_buy()`/`place_sell()`에서 반환된 `ord_no`(주문 접수 응답)를 그대로
체결 확정으로 취급한다 — 실제 체결 여부를 확인하는 별도 조회(미체결/체결내역
TR)가 코드에 아예 없다.

**Why:** 2026-08-03 investigate 세션에서 매도 실패(24건, `kiwoom_sell_no='FAILED'`)를
고쳤는데, 같은 세션에서 매수 쪽도 같은 결함이 있는 걸 발견했다 —
`000240.KS`(compose-funnel1), `475150.KS`(compose-and1/compose-score1) 3건이
2026-06-21에 정상적인 주문번호(`0011150`/`0011287`/`0011371`)까지 받았지만
브로커 잔고엔 0주. 매도와 달리 매수는 예외조차 안 던져서 조용히 "성공"으로
기록됐다 — DB(`paper_positions`)에 `status='closed', exit_type='buy_never_filled'`로
정정 완료(2026-08-03), 프론트 `EXIT_LABEL`에 '매수미체결' 라벨 추가.

**How to apply:**
1. Kiwoom 체결내역 조회 TR(미체결/체결 조회, 예: `ka10075`/`ka10076`류 — 정확한
   TR 코드는 키움 API 문서 재확인 필요)을 `KiwoomPaperTrader`에 추가.
2. `place_buy`/`place_sell` 직후 해당 TR로 체결 확인 → 미체결/부분체결 시
   `update_to_open`/`update_to_closed` 대신 재시도 또는 대기 상태로 분기.
3. 최소한의 임시 대안: 매수 직후 `get_positions()`로 해당 종목 보유 여부를
   교차 검증 — 새 TR 없이도 "주문은 받았는데 실제로 안 샀다"는 사실은 잡아낼 수 있음.

**Effort:** M (human: ~1h / CC: ~30min)
**Priority:** P1
**Found:** 2026-08-03, paper trading 계좌 불일치 investigate 세션
**Completed:** 2026-08-04 — Kiwoom REST API 문서(p.190-192) 원문 확인 후 `ka10076`(체결요청)로
`KiwoomPaperTrader.check_execution()`/`confirm_fill()` 추가(`data/kiwoom_paper_trader.py`).
주문 직후 최대 3회(1.5s 간격) 폴링해 실제 체결 수량 확인. 매수 미체결 시 `update_to_open` 대신
즉시 `update_to_closed(exit_type='buy_never_filled')` 처리(기존 수동 정정과 동일 라벨, 프론트
변경 불필요), 부분체결 시 실체결 수량 기준으로 기록. 매도는 미체결/부분체결 시 기존 `FAILED`
분기와 동일하게 `open` 유지 후 다음 실행 재시도. 유닛테스트 10건(`tests/test_kiwoom_execution_check.py`)
추가, 전체 스위트 873건 통과, pyright/ruff 클린. 라이브 검증 중 모의투자 서버가 과거 체결 이력을
오래 보관하지 않는 걸 발견해(`ka10075`/`ka10076` 둘 다 오래된 주문 조회 시 빈 응답) `confirm_fill()`
자체가 확인 결과를 INFO 로그로 남기도록 보강 — Kiwoom 이력 보관 여부와 무관하게 우리 쪽이 감사
기록을 갖도록 함. 실제 신규 주문에 대한 검증은 다음 실거래(09:05/15:20 KST)에서 로그로 확인 필요.

---

## P1: 모의투자 — 매수 오탐(`buy_never_filled`)으로 브로커에만 남은 유령 보유 20건

**What:** `get_position_qty()` 종목코드 접두사 버그(위 항목 참고, 2026-08-10 수정)로
인해 08-05~08-10 사이 `paper_open_entry_job`이 실제로 체결된 매수 20건을
"미체결"로 오판, `update_to_closed(..., 0.0, "buy_never_filled", ...)`로 DB에서
지워버렸다. 브로커 계좌엔 실제로 주식이 남아있는데(37개 보유종목 중 20개가
DB `paper_positions`에 대응 행이 아예 없음 — 예: CJ CGV 976주, F&F 81주,
삼성전자우 31주, 안국약품 420주 등) DB 관점에선 존재하지 않는 포지션이라
평가손익·모델별 성과 집계 어디에도 안 잡힌다.

**Why:** 실체결가/체결일시가 DB에 보존되지 않아 자동 복구 불가 — 게다가
`paper_open_entry_job`(`jobs/paper_jobs.py`)이 미체결 판정 시
`update_to_closed(db_pool, _pos_id, 0.0, "buy_never_filled", _ord_no)`를
호출하는데, 이 시점에 실제로 시도했던 시가(`_open_px`, 매수 주문에 실제
사용한 가격)를 인자로 넘기지 않고 하드코딩된 `0.0`을 저장한다 — 별개의
데이터 유실 버그. compose 모델(`compose-funnel1`/`compose-score1`) 계열은
애초에 `entry_theory=0.0`으로 pending 삽입되는 경우가 많아 이론가마저
복구 불가한 경우가 다수(`028670.KS`, `138040.KS`, `316140.KS` 등). ichimoku/stage
모델 계열은 `entry_theory`에 신호 시점 가격이 남아있어 근사치로는 쓸 수
있음(`079160.KS` 5410원, `383220.KS` 80500원, `005935.KS` 191000원 등).

**How to apply:**
1. `paper_open_entry_job`의 `update_to_closed(..., 0.0, ...)` 호출을
   `update_to_closed(..., float(_open_px), ...)`로 수정 — 최소한 앞으로는
   시도가가 보존되도록.
2. 20건의 유령 보유 처리 방침 결정 필요(사용자 확인 필요, 자동 진행하지
   않음): (a) `get_positions()` 실보유 대비 `entry_theory` 또는 정리 시점
   현재가로 새 `paper_positions` 행을 소급 생성, (b) 브로커 쪽에서 실제로
   전량 매도해 계좌를 DB와 다시 맞춤, (c) 그대로 두고 향후 buy_never_filled
   오탐이 재발하지 않는지(코드 수정으로 이미 해결) 관찰만.
3. 어느 쪽이든 조치 후 `get_positions()`(37종목) vs `paper_positions
   WHERE status='open'`(수정 후 17건) 종목 수가 다시 벌어지지 않는지
   주기적 대조 스크립트화 고려.

**Effort:** M (human: ~1h 검토+결정 / CC: ~30min 구현, 방침 확정 후)
**Priority:** P1
**Found:** 2026-08-10, "오늘 모의거래 매도 로그" investigate 세션 — 실계좌 vs DB 전수 대조 중 발견

**Completed (부분, 2026-08-10):** 20건을 재조사해보니 균질하지 않았음 — 3그룹으로
분리.

1. `update_to_closed(..., 0.0, ...)` → `update_to_closed(..., float(_open_px), ...)`
   수정 완료(`jobs/paper_jobs.py`).
2. **A그룹 (7건, 신호가 단서 있음)** — `079160.KS`/`383220.KS`/`005935.KS`/
   `001525.KS`/`041830.KQ`/`192440.KQ`/`008930.KS`(stage). 실보유는 여전히
   브로커에 있으므로 **closed가 아니라 open으로 복원** — `entry_theory`를
   근사 진입가로, 브로커 실보유수량을 `qty`로 채워 `paper_positions`를
   되살림(`status='open'`). `008930.KS`의 `compose-score1` 중복행(id=97)은
   같은 티커의 실보유를 이미 stage 쪽(id=90)에 전량 귀속시켰으므로 수량을
   나눌 근거가 없어 `exit_type='reconciled_no_data'`로 라벨만 정정(closed 유지).
   Open 포지션 17→24건.
3. **B그룹 (10종목, 11행 — 가격 단서 전혀 없음)** — `003490.KS`/`008470.KQ`/
   `017650.KQ`/`028670.KS`(중복 2행)/`052460.KQ`/`138040.KS`/`252990.KQ`/
   `278470.KS`/`316140.KS`/`001540.KQ`. DB 라벨은 `exit_type='reconciled_no_data'`로
   정정 완료(실제로는 체결됐으나 진입가 복구 불가함을 명시). **브로커 쪽 전량
   매도는 아직 미실행** — 2026-08-10 16:09 KST 시도 시 `RC4058(모의투자 장종료)`로
   전건 거부됨(정규장 마감 후). 다음 평일 장중(09:00~15:30 KST)에
   `KiwoomPaperTrader.place_sell()`로 위 10종목 전량 매도 실행 필요:
   `003490.KS`=401주, `008470.KQ`=2596주, `017650.KQ`=546주, `028670.KS`=2755주,
   `052460.KQ`=965주, `138040.KS`=88주, `252990.KQ`=765주, `278470.KS`=28주,
   `316140.KS`=321주, `001540.KQ`=420주 (수량은 2026-08-10 기준 `get_positions()`
   스냅샷 — 실행 직전 재조회 권장).
4. **C그룹 (3건) — 원인 확인 완료, 부분 수정.** `000650.KS`/`001080.KS`/`003480.KS`.
   `exit_date`는 2026-08-06(created_at 2026-05-16은 포지션 최초 진입일일 뿐)
   — 08-03~08-04 이틀 연속 손절 감시가 죽어있던(별도 항목 "Kiwoom 토큰 자동
   만료" 참고) 여파로 08-05에 21건이 한꺼번에 몰려 처리된 배치 중 3건. 당시
   전량 매도주문(37/2016/1818주, 주문번호 0154518/0154356/0154323, 로그에
   실제 남아있음)을 냈지만 **실제 체결은 부분체결**(각 28/29/112주만 체결,
   9/1987/1706주는 안 팔리고 남음)이었는데, 08-06에 `kiwoom_sell_no='RECONCILED'`
   (실제 주문번호 아닌 리터럴 문자열)로 **전량 청산으로 가정 후 hard_stop_pct
   공식(entry_actual×0.9, blended_return=-10% 고정)으로 일괄 마감** — 브로커
   실보유 대조 없이 진행된 것으로 보임.

   **수정 완료(2026-08-10):** 세 행의 `qty`를 실제 체결량(28/29/112)으로,
   `kiwoom_sell_no`를 로그상 진짜 주문번호로 정정. `exit_price`/`blended_return`의
   -10% 근사치는 그대로 둠(`daily_ohlcv`에 이 3종목 08-05 시세 없어 더 정확한
   값으로 교체 불가 — 근사치임은 `exit_type='reconciled_hardstop'`으로 이미
   명시돼 있음).

   **완료(2026-08-10):** 잔여 9/1987/1706주를 새 `open` 행(id=103/104/105)으로
   복원 — 원래 entry_actual/model/hard_stop 파라미터 유지, `kiwoom_buy_no`는
   최초 매수 주문번호(0088654/0088708/0088826) 그대로 승계. 세 종목 다 현재가가
   이미 hard_stop 기준선 아래(`000650.KS` 263500→151500, `001080.KS` 4960→4090,
   `003480.KS` 5500→4840)라 다음 15:20 KST exit checker 실행에서 즉시 실제
   현재가 기준 hard_stop 청산이 걸릴 것으로 예상 — 정상 동작이면 이번엔
   `qty` 왜곡 없이 제대로 마감될지 다음 장중에 확인 필요. Open 포지션 24→27건.

---

## P2: tests/test_news_gating.py — 죽은 게이팅 로직을 테스트 중 (재작성 필요)

**What:** `tests/test_news_gating.py`(5개 테스트)가 검증하는 `_screener_tickers` 집합
기반 게이팅은 `run_scheduler.py`에서 이미 삭제됨 — 2026-05-20 `_active_stage_tickers`
전역 캐시 + `get_active_stage_tickers()` DB 함수 기반 게이팅으로 교체됨(Sprint 2,
`v0.9.3.0`). 테스트는 실제 `run_scheduler` 코드를 호출하지 않고, `summary_worker`의
옛 게이팅 알고리즘을 테스트 함수 안에 그대로 복사해 넣고 그 사본만 검증한다
(주석에 "Simulate the gating logic directly"라고 명시돼 있음). `rs._screener_tickers`는
모듈에 더 이상 존재하지 않는 속성이라 pytest에서만 우연히 동작(동적 속성 할당) —
pyright는 `reportAttributeAccessIssue`로 잡아냄.

**Why:** 이 테스트 파일은 865개 스위트 중 5개를 차지하지만 `run_scheduler.py`의
실제 게이팅 동작을 전혀 검증하지 못한다 — 프로덕션 게이팅 로직이 다시 깨져도
이 테스트는 여전히 통과한다(자기 자신의 사본만 테스트하므로). False confidence.

**How to apply:**
1. `run_scheduler.py`의 실제 게이팅 지점(`summary_worker` 내부, `_active_stage_tickers`
   참조 부분)을 찾아 호출 가능한 형태인지 확인 — 필요하면 순수 함수로 추출.
2. `_active_stage_tickers`를 직접 주입하거나 `get_active_stage_tickers()`를
   monkeypatch해서 실제 게이팅 함수를 호출하도록 테스트 재작성.
3. 지금 세션에서는 pyright 타입 에러만 기계적으로 봉합(`cross=` kwarg 제거,
   `_screener_tickers` 접근에 `type: ignore[attr-defined]`) — 행동 변경 없음,
   테스트는 여전히 죽은 로직을 검증 중.

**Effort:** S (human: ~1h / CC: ~20min)
**Priority:** P2
**Depends on:** 없음
**Found:** 2026-07-25, `/health` pyright 에러 수정 세션 중 발견
**Completed:** 2026-07-25 — `run_scheduler.py`의 게이팅 판정을 `_gate_signal(signal_syms,
screener_tickers, active_stage_tickers) -> (suppressed, in_screener, in_stage)`
순수 함수로 추출, `summary_worker()`는 이 함수를 호출하도록 변경(동작 변화 없음).
`tests/test_news_gating.py`를 전면 재작성 — 이제 이 실제 함수를 직접 import해
호출(7 테스트: 스크리너만 매치/Stage만 매치/둘 다 미스/두 캐시 모두 비어 게이팅
비활성/부분 교집합/둘 다 매치 시 플래그 동시 반환/signal_syms 빈 경우). 죽은
`_screener_tickers`/`cross=` 관련 코드 전부 제거. 전체 스위트 865→867건(순증 2),
pyright 0 에러 유지 확인.

---

## P1: kosdaq 스테이지 분류 커버리지 회복 확인 (2026-07-24 수정분 검증)

**What:** `jobs/stage_job.py`의 티커 캡 순서 버그(KOSPI가 리스트 앞을 차지해 KOSDAQ이
캡에 절대 못 듦) + `market_map` sector/symbol 오판정 버그(모든 종목이 `"KOSPI"`로
분류)를 수정(`CHANGELOG.md` `[0.10.1.18]`). 며칠 뒤 `stage_classifications`에
실제로 KOSDAQ(`.KQ`) 행이 쌓이는지, `kosdaq` 모의투자 모델에 pending이 들어오는지
확인 필요.

**Why:** `stage_classifications`는 런칭 이후 전체 기간(787건) 동안 KOSDAQ 행이
0건이었고, 그 결과 `kosdaq` 모의투자 모델(백테스트 기준 4개 모델 중 최고 성과,
val_sharpe=5.48)이 한 번도 거래를 못 했음. 로직 수정은 완료했지만 실 운영에서
의도대로 동작하는지(yfinance 60일 OHLCV fetch가 KOSDAQ 종목에서도 정상 응답하는지,
Stage1 임계값(`_S1_THRESHOLD["KOSDAQ"]=0.07`)이 실제로 히트를 만들어내는지)는
아직 라이브로 검증 안 됨.

**How to apply:**
1. 며칠(최소 1주) 뒤 아래 쿼리로 KOSDAQ 행 적재 여부 확인:
   ```sql
   SELECT stage, COUNT(*) FROM stage_classifications
   WHERE ticker LIKE '%.KQ' AND classified_date > CURRENT_DATE - 7
   GROUP BY stage;
   ```
2. Stage1 KOSDAQ 히트가 나오면 `paper_positions WHERE model='kosdaq'`에 pending이
   생기는지 확인.
3. 몇 주 뒤에도 여전히 0건이면 `classify_stage_v15`/`_check_stage1_v13` 내부에
   시장별로 추가로 갈라지는 로직이 있는지(예: `_S1_THRESHOLD` 외 다른 시장 종속
   조건) 재조사.

**Effort:** XS (확인만, 코드 변경 없음 — 재조사 필요시 별도)
**Priority:** P1
**Depends on:** 2026-07-24 `jobs/stage_job.py` 수정 배포 후 daily_stage_classifier 실행 누적

**중간 확인 (2026-07-25, 하루 경과 — 아직 판단 시점 아님):** `jobs/stage_backfill.py`에
같은 날 추가된 `--market` 필터(`c0073ae`)를 `--market KOSDAQ --max-tickers 50
--start 2026-06-01 --end 2026-07-25 --skip-existing`로 시험 실행. 필터 자체는
정상 동작(KOSPI 943 + KOSDAQ 1821 = 전체 2764종목, 정확히 분할). DB 조회 결과
`stage_classifications` 전체 KOSDAQ stage1이 이미 208건 존재 — 위 "Why"의 "0건"
서술은 `stage_backfill.py`(전종목 순회, 캡 버그 미해당)로 채워진 과거 백필 기준으로는
이미 사실이 아님. 다만 **라이브 일별 잡** 기준 최근 7일(07-18~07-25) 신규 분류는
KOSPI 1건뿐, KOSDAQ 0건 — `jobs/stage_job.py` 캡 순회 수정이 실제 스케줄러 실행에서
KOSDAQ 히트를 만들어내는지는 원래 계획대로 최소 1주 경과 후(07-31 전후) 위 쿼리로
재확인 필요. 결론 미변경, 재확인 시점만 남겨둠.

**해소 (2026-08-04):** 재확인 시점(07-31 전후)을 지나서도 라이브 일별 잡 신규 분류가
07-24~08-03(오늘 포함 11일 연속) 전체 KOSPI 포함 0건으로 이어진 게 확인됨 — 캡 순회
자체가 KOSDAQ까지 도달하는지와 무관하게, 순회 로직 전체가 히트를 거의 못 만들고
있었던 것. 이 시점에 별도로 daily_market_snap 전종목 확장 작업을 하면서 스테이지
분류기의 150종목 캡/KOSPI-KOSDAQ 순환 로직 자체를 제거하고 스크리너와 동일하게
매일 전종목(~2764종목)을 스캔하도록 변경(`jobs/stage_job.py`) — 이 항목이 다루던
"순환이 KOSDAQ까지 도달하는가"라는 질문 자체가 구조적으로 해소됨(순환이 없으므로
매일 전종목이 대상). 남은 확인 사항은 이 항목과 무관한 별개 질문("Stage1 조건 자체가
실제로 얼마나 자주 히트하는가")이라 이 TODO는 닫는다.

---

## P3: 리팩토링 Phase A~C 후속 — 심(shim) 삭제 + 범위 제외분 정리 (2026-07-15, 심 삭제 완료 2026-07-16)

**배경:** 2026-07-15 구조 리팩토링(우선순위 3단계 컷)으로 core/dates.py,
core/tor.py, core/db_sync.py, core/env.py 신설 및 analysis/backtest_engine.py
(3,360줄) → analysis/backtest/ 8모듈 분해 완료. 모든 이동은 옛 위치에
re-export 심을 남김.

**심 삭제 (스케줄러가 새 코드로 ~1주 정상 가동 후):**
- ~~`data/krx_flow_sync.py`, `analysis/chart_screener.py` 등의
  `_last_trading_day`/`_jittered_delay`/`_tor_new_identity`/`_prev_business_day`/
  `_connect` 별칭 — grep으로 잔여 importer 0 확인 후 테스트를 canonical
  경로로 갱신하고 삭제.~~ — 완료(2026-07-16). `_connect`는 애초에 외부
  importer가 없어 그대로 둠(내부 지역 별칭일 뿐).
- ~~`analysis/backtest_engine.py`(79줄 순수 심) — 소비자(telegram_bot,
  paper_jobs, strategy_compose, scripts/, tests/)를 analysis.backtest.*
  직접 import로 점진 전환 후 삭제 검토.~~ — 완료(2026-07-16). `strategy_compose.py`는
  실제로는 주석에서만 언급, 실 import 없었음(당초 소비자 목록이 다소 과장됨).
- **재평가(2026-07-16, Phase D 1차 탐색):** 처음엔 둘 다 삭제 보류 —
  5개 별칭 전부와 backtest_engine 심 전부 실제 소비자가 남아있음(별칭은
  주로 alias-regression 테스트, backtest_engine은 프로덕션 6곳도). 최초
  Effort 추정(S)을 M으로 정정. 같은 날 후속 세션에서 마이그레이션
  (테스트/소비자 canonical 전환) → zero-importer 재확인 → 삭제 완료.
  telegram_bot.py 함수-로컬 import와 test_backtest_compose_bot.py의
  mock.patch 문자열 타깃을 같은 커밋에서 lockstep 처리(따로 옮기면 패치가
  조용히 무효화되는 위험 발견). 상세는
  [refactoring-roadmap.md](refactoring-roadmap.md) Phase D 참조.

**범위 제외로 남긴 것:**
- ~~`dashboard/backend/main.py:2374` psycopg2 직접 연결 → core.db_sync 미적용~~
  — 완료(2026-07-16, 대시보드 라우터 분리 작업에서 `core.db_sync.connect()`로 전환).
- `scripts/*`의 psycopg2/DSN 중복 — 일회성 스크립트라 기회적 정리.
- ~~`jobs/ohlcv_warm.py`의 주말 스킵 로직 — last_trading_day와 의미가 다름
  (월요일 실행 시 금요일 daily_ohlcv를 채우는 회차가 없는 잠재 커버리지 갭).
  보정 전환은 동작 변경이라 별도 fix로 판단 필요 (코드에 TODO 주석 있음).~~
  — 완료(2026-07-20). DB 실측으로 갭 실재 확인(최근 한 달 금요일 행 전무)
  → 일배치를 최근 7일 캐치업(backfill_ohlcv 재사용, min_rows=1000)으로
  전환 + 결손 11일 30,432행 백필. 상세는
  [refactoring-roadmap.md](refactoring-roadmap.md) 2026-07-20 항목 참조.
- 전체 9단계 계획의 나머지: ~~텔레그램 계층 정리~~ — 완료(2026-07-17,
  2단계: notify esc/esc_code 중첩 사본 7개 통합 + telegram_bot 핸들러
  21개를 telegram/bot_handlers.py로 분리(1,272→293줄, facade + 지연
  import 속성 접근 패턴). 상세는
  [refactoring-roadmap.md](refactoring-roadmap.md) 참조),
  ~~지표/OHLCV 통합~~ — 완료(2026-07-17, jobs/stage_shared.py, Phase F),
  ~~대시보드 라우터 분리~~ — 완료(2026-07-16, `b591f19`~`54d5001` 7커밋),
  ~~stage_classifier 레거시 분리~~ — 완료(2026-07-17,
  analysis/stage_classifier_legacy.py — 구버전 디스패처 5개 + v1.0 전용 헬퍼만
  분리. v11~v14 조건 헬퍼는 v15의 누적 체인 라이브 의존성이라 본 모듈에 유지,
  상세는 [refactoring-roadmap.md](refactoring-roadmap.md) 참조),
  ~~run_scheduler 분해~~ — 완료(2026-07-17, 안전한 부분만, Phase G).

**후속(2026-07-16):** core/db.py(1,689줄) 도메인 분리 완료 — facade + 6모듈,
`96bfb3c`~`7b31706` 5커밋. 이후의 리팩토링 후속 계획(Phase D~G: 저위험 정리 →
대시보드 마무리 → 백필 플러밍 통합 → run_scheduler 분해)과 방법론 원칙은
[refactoring-roadmap.md](refactoring-roadmap.md)로 이관.

**최종(2026-07-17): Phase D~G 전부 완료.** 구조 리팩토링 로드맵 종료 —
상세 기록·잔여 case-by-case 항목은 [refactoring-roadmap.md](refactoring-roadmap.md) 참조.

**후속(2026-07-16, Phase D 완료):** test_scan_cmd.py 교체, jobs/_common.py
추출, core/db_schema.py 분리, 심 마이그레이션+삭제까지 4/4 전부 완료.
Phase D 종료 — 다음은 Phase E(대시보드 백엔드 마무리).

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
**Completed:** 2026-07-25 — `/api/report/paper`의 `model_summary`에 모델별
`win_rate`/`total_realized`(청산 포지션 기준) 필드 추가(`routers_report.py`),
`PaperPortfolio.tsx` 카드에 승률·실현누적 인라인 표시. 클릭 시 확장되는
아코디언 UI는 스코프 축소 — 기존 카드 클릭이 이미 포지션 목록 필터링 용도로
쓰이고 있어 같은 클릭에 확장까지 얹으면 UX가 충돌, 핵심 요구(통계 노출)는
인라인 표시로 충분히 달성돼 별도 확장 상태 없이 마무리. `/api/paper/curve`
호출 제거 항목은 애초에 프론트엔드가 호출한 적이 없어 해당 없음(엔드포인트
자체는 `tests/test_paper_analytics.py`가 커버하므로 유지).

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
**Completed:** 2026-07-25 — `Heatmap.tsx`에서 마운트 제거, `App.tsx` 헤더 바로 아래
공통 영역(모든 탭 상위)으로 이동. 모바일에서는 히트맵 탭의 `calc(100svh - ...)`
고정 높이 계산이 배너 높이를 반영하지 않던 문제가 있어, `App.tsx`가
`ResizeObserver`로 배너 실측 높이를 `--banner-h` CSS 변수로 노출하고
`index.css`의 `.app-mobile-layout > .heatmap-root` 계산식에 반영하도록 확장
(접기/펼치기로 배너 높이가 바뀌어도 자동 대응). `/browse`로 데스크톱·모바일,
접힘·펼침 4가지 조합 스크린샷 검증 완료 — 클리핑 없음.

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
**Completed:** 2026-07-25 — Phase 2(앱 헤더 승격)와 함께 진행. `collapsed` 상태를
`localStorage('market-banner-collapsed')`로 영속화, 우상단 chevron 버튼으로 토글.
접힌 상태는 코스피/코스닥 수치만 한 줄(라벨+등락률, 팁·한마디·배지 숨김). "완전히
닫기 vs 최소화" 결정은 최소화(수치는 항상 노출)로 확정 — 시장 맥락 완전 소실은
피하고 싶다는 Phase 2 Why(초보자가 급락장을 놓치면 안 됨)와 일치시킴.

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
- daily_ohlcv 캐시 워밍 잡: `jobs/ohlcv_warm.py` — 백필(`--start 2025-01-02`, KRX OpenAPI) + 평일 18:30 KST 일배치(run_scheduler 자동 실행, 이미 채워진 날짜 스킵). **Completed:** 2026-06-14
- Tier-1 조합전략 백필: `stage_classifications` 2025-W01~2026-W24(`jobs/stage_backfill.py`), `chart_signals` 2025-W01~2026-W24 4,144건(`jobs/screener_backfill.py`); 4종 백테스트 검증 — AND-1(샤프 1.75) / FUNNEL-1(샤프 0.74, 주력) / SCORE-1(샤프 0.62) / AND-2(신호 희소). CLI: `python scripts/run_compose.py --strategy ALL --start 2025-01-01 --end 2026-06-14`. **Completed:** 2026-06-14

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

**추가 해소 (2026-08-04):** 여기서 다루던 "150→300 단계적 확장" 자체가 무의미해짐 — 스크리너가
이미 전종목(2763개)을 yfinance 개별 호출로 매일 문제없이 처리하는 게 실증돼(3분 24초), 단계적
확장 대신 `DAILY_CLASSIFIER_TICKERS` 캡/순환 로직 자체를 제거하고 스크리너와 동일하게 매일
전종목을 스캔하도록 변경(`jobs/stage_job.py`). p99 실측 후 300으로 늘리는 절차 자체가 필요
없어짐 — 이 TODO 완전히 닫음.

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
