# YouTube 내러티브 백필 계획

> **목적**: 블라인드 백테스트용 과거 데이터(2026-01 ~ 2026-05) 소급 수집
> **원칙**: 방법론 확정(v0.10.0.0, 2026-05-31 tag) → 과거 데이터 수집 → 가격 대조 순서 고정

---

## ⚠️ 2026-06-03 burst 백필 시도 — 전량 실패 (IP 차단)

`youtube_backfill_monthly.py --step sync` (월별 순차, 영상당 2초 간격)로
1~5월 810개 영상을 ~25분간 연속 요청 → **810/810 전부 "IP blocked by YouTube" 에러로 실패, 저장 0건**
(`logs/backfill_monthly.log`).

같은 기간 같은 IP(`175.197.9.6`, KT 회선)로 도는 **일일 운영 잡(영상 6개/일, 09:05 KST)은 정상 동작**
(`logs/news_crawler.log` — 6/8에도 6개 중 5개 자막 수집 성공, 31건 저장).

**결론**: IP 자체가 영구 차단된 게 아니라, **burst 요청량(분당 수십 건)이 YouTube의 안티스크래핑
임계값을 넘겨 일시 차단을 유발**한다. 쿠키 인증(`docs/youtube.com_cookies.txt`)은 IP 레벨 차단에는
영향 없음 — 해결책은 **요청 빈도를 일일 운영 잡 수준으로 낮추는 것**.

→ 아래 "분산 백필" 방식으로 재설계함 (구 `--step sync`는 코드에 남아있으나 비권장).

---

## 분산 백필 설계 (권장)

**핵심**: 810개 영상을 한 번에 모으지 않고, 실증된 안전 수준(8개/회)으로 큐에서 조금씩 꺼내 처리.

### 구조
1. **`youtube_backfill_queue` 테이블** — video_id, video_date, status(pending/done/no_transcript/blocked/error), attempts
2. **`enqueue` 단계** — `fetch_video_list`(검색 API만 사용, 자막 요청 없음 → 차단 위험 없음)로
   1~5월 영상 목록을 미리 큐에 적재. 1회만 실행.
3. **`process` 단계** — 큐에서 `--batch-size`(기본 8)개를 video_date 오름차순으로 꺼내
   자막 수집 → LLM 추출 → 저장. **1회 호출 = 1배치**, 끝나면 종료.
   - IP 차단(`RequestBlocked`) 감지 시 해당 영상을 `pending`으로 유지하고 즉시 배치 중단
     → 다음 스케줄 실행에서 자동 재시도 (수동 개입 불필요)
4. **외부 트리거(Windows 작업 스케줄러)** — `process`를 하루 2~3회 호출

### 실행 방법

```bash
# 1) 큐 적재 (1회만 실행 — 검색 API만 사용, 안전)
python scripts/youtube_backfill_monthly.py --step enqueue

# 2) Windows 작업 스케줄러에 배치 처리 등록 (하루 3회: 11시/14시/17시)
schtasks /Create /TN "YTBackfillBatch" ^
  /TR "<repo>\venv\Scripts\python.exe <repo>\scripts\youtube_backfill_monthly.py --step process --batch-size 8" ^
  /SC DAILY /ST 11:00,14:00,17:00

# 큐 진행 상황 확인 (status별 카운트)
#   SELECT status, COUNT(*) FROM youtube_backfill_queue GROUP BY status;

# 3) 큐가 모두 done/no_transcript/error가 되면 작업 삭제
schtasks /Delete /TN "YTBackfillBatch" /F

# 4) 마무리 — forward return 채우기 + attention_score 재집계
python scripts/youtube_backfill_monthly.py --step fill-returns
python scripts/youtube_backfill_monthly.py --step scores --from 2026-01 --to 2026-05
```

### 예상 소요

| 배치 크기 | 1일 실행 횟수 | 1일 처리량 | 810개 완료까지 |
|---------|------------|----------|--------------|
| 8개 (= 일일 운영 잡 수준, 채택) | 3회 (11/14/17시) | 24개 | 약 34일 |

차단되더라도 데이터 손실 없음 — `pending` 상태로 큐에 남아 다음 실행에서 자동 재처리.

---

## 실행 순서 (단계 의존성)

```
1단계: enqueue → process 반복 (큐가 빌 때까지, 반드시 오래된 달부터 — video_date ASC로 자동 보장)
  이유: attention_score rolling 5영업일 window가 이전 달 데이터 참조

2단계: fill-returns (전체 기간 한 번에, 큐 소진 후)
  yfinance 배치 다운로드 → 1d/5d/20d forward return 산출

3단계: scores (전체 날짜 순회, 큐 소진 후)
  날짜별 compute_attention_scores() 호출. 이미 집계된 날짜는 upsert(덮어쓰기)로 안전.
```

---

## 진행 상태

| 단계 | 상태 | 비고 |
|------|------|------|
| burst 방식 (`--step sync`) | ❌ 폐기 | 2026-06-03 시도 — 810/810 IP 차단, 저장 0건 |
| `youtube_backfill_queue` enqueue | ✅ 완료 | 2026-06-08 — 972건 적재 (2026-01-01~05-31, 전체 pending) |
| `schtasks "YTBackfillBatch"` 등록 | ✅ 완료 | 2026-06-08 — 매일 11/14/17시, batch-size 8 (`/RI 180 /DU 0006:00`) |
| `process` 배치 처리 | 진행 중 | 첫 실행: 2026-06-09 11:00. 24개/일 → 약 40일 소요 예상 |

> `SELECT status, COUNT(*) FROM youtube_backfill_queue GROUP BY status;` 로 진행 상황 확인 후 갱신.

---

## 백테스트 기준 (분산 백필 완료 후)

- **실행 시점**: 분산 백필(`youtube_backfill_queue`)이 모두 소진되고 fill-returns/scores를
  재실행한 이후. (기존 "2026-06-05 이후" 기준은 burst 백필이 성공했다는 전제였음 — 실제로는
  1~5월 데이터가 거의 없으므로 분산 백필 완료 전에는 샘플 수(≥100) 기준을 못 채울 가능성이 큼)
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
