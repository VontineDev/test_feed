# 주봉 차트 스크리너 설정 가이드

KOSPI + KOSDAQ 전 종목(~2770개)에 Ichimoku + 이동평균선 7조건을 적용해 매주 돌파 후보를 추립니다.

---

## 전제 조건

- PostgreSQL 연결 (`DB_*` 또는 `DATABASE_URL`)
- yfinance 설치 (`pip install -r requirements.txt`)
- KRX 종목 DB 초기화 (최초 1회):

```bash
python krx_sync.py
```

`krx_sync.py`는 KRX 전 종목 이름·코드·시장 구분을 `krx_listings` 테이블에 저장합니다. 이후 매주 월요일 자동 갱신됩니다.

---

## 스크리닝 조건 (7조건)

| 조건 | 내용 |
|------|------|
| A | 이번 주 종가 > 구름 상단 (Ichimoku 돌파) |
| B | 직전 주 종가 ≤ 직전 주 구름 상단 (이번 주에 돌파) |
| C | 종가 > 20주 이동평균선 |
| D | 종가 > 60주 이동평균선 |
| E | 20주선 우상향 (이번 주 > 직전 주) |
| F | 60주선 우상향 |
| G | 종가 > 120주 이동평균선 (데이터 부족 시 기본: 통과) |

**7조건 모두 충족**해야 결과에 포함됩니다.

### Enhanced 배지

조건 A~G 통과 후 추가로 아래 두 조건을 만족하면 `is_enhanced=True` (Enhanced 배지):

- H: 전환선(9주 고저 평균) > 기준선(26주 고저 평균)
- I: 전환선·기준선 모두 우상향

Enhanced 종목은 구름 돌파에 더해 Ichimoku 내부 구조까지 강세인 종목입니다.

---

## 스크리닝 실행

### 자동 실행 (매주 일요일 20:30 KST)

`python run_scheduler.py`로 스케줄러를 실행하면 자동으로 돌아갑니다.

### 즉시 실행

```bash
# 텔레그램 명령어
/scan

# 또는 CLI
python run_scheduler.py --once screener
```

### 결과 조회

```bash
# 텔레그램 명령어
/screener

# DB 직접 조회
psql -d news_db -c "
SELECT ticker, name, close, week_of, is_enhanced, has_gapjum, sector
FROM chart_signals
WHERE week_of = (SELECT MAX(week_of) FROM chart_signals)
ORDER BY has_gapjum DESC, close DESC;"
```

---

## 성능 설정

기본 1 워커로 전 종목 스크리닝은 약 45분이 걸립니다. 프로덕션에서는 워커를 늘리세요.

```env
SCREENER_WORKERS=8
```

| 워커 수 | 예상 소요 시간 (2770종목) |
|---------|------------------------|
| 1 | ~45분 |
| 4 | ~12분 |
| 8 | ~6분 |
| 16 | ~3분 |

yfinance API 제한에 걸릴 수 있으므로 16 이상은 권장하지 않습니다.

---

## Condition G Calibration

### 문제

데이터가 부족한 신규 상장 종목(주봉 100개 미만)은 120주선(ma_120w)이 NaN입니다. 기본 설정에서는 이런 종목이 조건 G를 자동 통과합니다.

```sql
-- 이번 주 통과 종목 중 120주선이 NaN인 비율 확인
SELECT
    COUNT(*) FILTER (WHERE ma_120w IS NULL) AS null_count,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) FILTER (WHERE ma_120w IS NULL) / COUNT(*), 1) AS null_pct
FROM chart_signals
WHERE week_of = '2026-W17';   -- 확인할 주차로 교체
```

### 결정 기준

| null_pct | 판단 | 조치 |
|----------|------|------|
| ≤ 20% | 허용 범위 | 변경 없음 |
| > 20% | 데이터 부족 종목 과다 | `SCREENER_G_NAN_STRICT=1` 설정 |

### Strict 모드 활성화

`.env`에 추가:

```env
SCREENER_G_NAN_STRICT=1
```

strict 모드에서는 ma_120w가 NaN인 종목이 조건 G에서 탈락합니다. 결과 수가 줄어드므로 설정 전후 결과 수를 비교하세요.

```sql
-- strict 모드 전환 전후 비교 (직접 재실행 후 확인)
SELECT week_of, COUNT(*) FROM chart_signals GROUP BY week_of ORDER BY week_of;
```

---

## HTML 리포트 생성

스크리닝 결과를 HTML 파일로 내보냅니다.

```bash
python generate_html_report.py
```

`reports/` 디렉터리에 `screener_YYYYMMDD.html`이 생성됩니다. 브라우저에서 열면 섹터별 그룹, 스파크라인(최근 12주 종가), Enhanced 배지를 확인할 수 있습니다.

---

## 스크리너 우선 게이팅

스크리닝 통과 종목은 `_screener_tickers` 캐시에 저장됩니다. 이 캐시는 뉴스 신호 게이팅에 사용됩니다. 해당 주 스크리너를 통과한 종목의 뉴스 신호만 Telegram으로 전달됩니다.

자세한 내용은 [신호 파이프라인 설계 해설](explanation-signal-pipeline.md)을 참고하세요.

---

## 트러블슈팅

**결과가 0건인 경우:**
1. `python run_scheduler.py --once screener` 로 수동 실행 → 로그 확인
2. `krx_listings` 테이블에 데이터 있는지 확인: `SELECT COUNT(*) FROM krx_listings;`
3. yfinance 응답 확인: `python -c "import yfinance as yf; print(yf.Ticker('005930.KS').history(period='5d'))"`

**스크리닝이 너무 느린 경우:**
- `SCREENER_WORKERS` 증가
- 시간대 확인: 장 마감 직후(15:30~16:30 KST)는 yfinance 서버 부하가 높습니다. 19:00 이후 실행 권장

**Enhanced 종목이 없는 경우:**
- 시장 전반이 약세이면 전환선 > 기준선 조건을 동시 만족하는 종목이 없을 수 있습니다. 정상입니다.

---

## 누락 주차 백필

`chart_signals`에 데이터가 빠진 주차를 소급해서 채울 수 있습니다.

```bash
# 누락 주차 자동 탐지 후 백필
python jobs/screener_backfill.py

# 특정 주차만 지정
python jobs/screener_backfill.py --weeks W19 W24

# 저장 없이 탐지만 (dry-run)
python jobs/screener_backfill.py --dry-run

# 워커 수 조정 (기본 4)
python jobs/screener_backfill.py --workers 8
```

**동작 원리:**
- yfinance 3년치 주봉 OHLCV를 가져와 대상 주차 월요일 이전 행만 슬라이스
- 현재 스크리너와 동일한 7조건(A-G) 적용
- `screened_at`을 해당 주 일요일 20:30 KST로 고정해 `chart_signals`에 upsert

**주의:** yfinance는 배당락 조정 가격을 반환하므로 당시 실제 주가와 미세하게 다를 수 있습니다.

---

## 관련 문서

- [reference-env-vars.md](reference-env-vars.md) — `SCREENER_WORKERS`, `SCREENER_G_NAN_STRICT`
- [howto-stage-classifier.md](howto-stage-classifier.md) — 일봉 분류기 (스크리너와 교차)
- [explanation-signal-pipeline.md](explanation-signal-pipeline.md) — 게이팅 동작 원리
- [HowToBacktest.md](HowToBacktest.md) — 스크리너 신호 백테스트
- [name-resolution.md](name-resolution.md) — 종목명 해석 우선순위 및 신규 엔드포인트 체크리스트
