# YouTube 내러티브 백필 계획

> **목적**: 블라인드 백테스트용 과거 데이터(2026-01 ~ 2026-05) 소급 수집
> **원칙**: 방법론 확정(v0.10.0.0, 2026-05-31 tag) → 과거 데이터 수집 → 가격 대조 순서 고정

---

## 백필 범위

| 월 | 영업일 | YouTube API 쿼터 | Gemini 대기(10영상 기준) | 예상 소요 |
|----|-------|----------------|----------------------|---------|
| 2026-01 | 21일 | ~2,100 units | ~14분 | ~20분 |
| 2026-02 | 19일 | ~1,900 units | ~13분 | ~18분 |
| 2026-03 | 22일 | ~2,200 units | ~15분 | ~22분 |
| 2026-04 | 22일 | ~2,200 units | ~15분 | ~22분 |
| 2026-05 | 21일 | ~2,100 units | ~14분 | ~20분 |
| **합계** | **105일** | **~10,500 units** | **~71분** | **~102분** |

**YouTube API 쿼터**: 검색 1회 = 100 units, 일일 한도 10,000 units.
→ 5개월치(~10,500 units)는 이틀에 걸쳐 실행. 하루에 4개월까지 안전.

---

## 실행 방법

```bash
# 전체 자동 실행 (sync → fill-returns → scores, 1~5월)
python scripts/youtube_backfill_monthly.py

# 특정 월만
python scripts/youtube_backfill_monthly.py --from 2026-01 --to 2026-01

# 이어서 실행 (2월부터)
python scripts/youtube_backfill_monthly.py --from 2026-02

# 단계 분리 (문제 발생 시)
python scripts/youtube_backfill_monthly.py --step sync
python scripts/youtube_backfill_monthly.py --step fill-returns
python scripts/youtube_backfill_monthly.py --step scores
```

---

## 실행 순서 (단계 의존성)

```
1단계: sync (월별, 반드시 오래된 달 먼저)
  2026-01 → 2026-02 → 2026-03 → 2026-04 → 2026-05
  이유: attention_score rolling 5영업일 window가 이전 달 데이터 참조

2단계: fill-returns (전체 기간 한 번에)
  yfinance 배치 다운로드 → 1d/5d/20d forward return 산출
  500건 단위 처리. 잔여분 있으면 재실행.

3단계: scores (전체 날짜 순회)
  날짜별 compute_attention_scores() 호출
  이미 집계된 날짜는 upsert(덮어쓰기)로 안전
```

---

## 진행 상태

| 월 | sync | fill-returns | scores | 비고 |
|----|:----:|:-----------:|:------:|------|
| 2026-01 | 미완 | - | - | 프로세스 hang → 강제 종료 (2026-06-02) |
| 2026-02 | 미완 | - | - | |
| 2026-03 | 미완 | - | - | |
| 2026-04 | 미완 | - | - | |
| 2026-05 | 미완 | - | - | |

> 완료 시 이 표 업데이트.

---

## 쿼터 초과 시 대처

YouTube API 일일 쿼터(10,000 units) 초과 시 해당 날짜의 `fetch_video_list` 호출이 실패하고 WARNING 로그가 출력됩니다. UNIQUE 제약 덕분에 다음날 이어서 실행해도 중복 없이 재개됩니다.

```bash
# 당일 중단된 시점부터 재실행
python scripts/youtube_backfill_monthly.py --from 2026-03 --step sync
```

---

## 백테스트 기준 (완료 후)

- **실행 시점**: 2026-06-05(금) 이후 — 2026-05-29 데이터의 +5영업일
- **명령**: `python scripts/youtube_backtest.py --ret ret_5d`
- **합격 기준**: IC(ret_5d) > 0.05 AND t-stat > 1.65 AND 샘플 ≥ 100
- **결과별 대응**:
  - 합격 → `attention_score`를 `effective_confidence`에 낮은 가중치로 편입
  - IC 음수 → 역지표(청산 경계 신호)로 재설계
  - IC ≈ 0 → 채널 교체 또는 전처리 개선 후 v2 재검증

---

## 관련 문서

- [소급 수집 방법 (how-to)](howto-youtube-backfill.md)
- [YouTube 내러티브 레퍼런스](reference-youtube-narrative.md)
- [TODOS.md — P1 백테스트](TODOS.md)
