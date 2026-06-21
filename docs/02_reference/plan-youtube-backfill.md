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

**핵심**: 810개 영상을 한 번에 처리하지 않고, 실증된 안전 수준(8개/회)으로 큐에서 조금씩 꺼내 처리.

- `youtube_backfill_queue` 테이블에 영상 목록을 미리 적재(`enqueue`)한 뒤 외부 스케줄러가 소량씩 반복 처리(`process`)한다.
- `RequestBlocked` 감지 시 해당 영상을 `pending`으로 유지하고 배치를 즉시 중단 → 다음 실행에서 자동 재시도.
- 처리량: 8개/회 × 3회/일 = 24개/일 → 810개 기준 약 34일 소요.

실행 명령어 및 단계별 상세는 [소급 수집 방법](howto-youtube-backfill.md) 참고.

---

## 진행 상태

| 단계 | 상태 | 비고 |
|------|------|------|
| burst 방식 (`--step sync`) | ❌ 폐기 | 2026-06-03 시도 — 810/810 IP 차단, 저장 0건 |
| `youtube_backfill_queue` enqueue | ✅ 완료 | 2026-06-08 — 972건 적재 (2026-01-01~05-31) |
| `schtasks "YTBackfillBatch"` 등록 | ✅ 완료 | 2026-06-08 — 매일 11/14/17시, batch-size 8 (`/RI 180 /DU 0006:00`) |
| `process` 배치 처리 | ✅ 완료 | 큐 964건 `ok` / 8건 `no_transcript`(자막 없음) — pending 0건, 분산 백필 완전 소진 |
| `fill-returns` / `scores` | ✅ 완료 | `youtube_mention_forward_returns` 3,383행, `youtube_attention_scores` 7,004행 |
| 백테스트 (`--ret ret_5d`) | ✅ 실행됨 — **불합격** | 아래 "백테스트 결과" 참고 |

### 현재 수집 현황 (DB 직접 조회 기준 — 갱신 시 최신화 필요)

| 기간 | `youtube_mention_raw` 언급 건수 |
|------|-------------------------------|
| 2026-01 | 2,088건 |
| 2026-02 | 1,145건 |
| 2026-03 | 514건 |
| 2026-04 | 2,016건 |
| 2026-05 | 139건 |
| 2026-06 | 749건 |

총 6,651건. 분산 백필이 완전히 소진되어 1~5월 전 구간이 채워졌다.

> `schtasks "YTBackfillBatch"`는 여전히 등록되어 매일 11/14/17시에 실행 중이나, 큐가 비어 있어 매번 0건 처리하는 no-op 상태다. 더 이상 필요 없다면 `schtasks /delete /tn YTBackfillBatch`로 제거 검토.

> `SELECT status, COUNT(*) FROM youtube_backfill_queue GROUP BY status;` 로 진행 상황 확인 후 갱신.

---

## 백테스트 결과

`python scripts/youtube_backtest.py --ret ret_5d` 실행 결과 (n=2,587):

| 지표 | 값 | 합격 기준 |
|------|-----|----------|
| Spearman IC | +0.0136 | > 0.05 |
| t-stat | +0.69 | > 1.65 |
| p-value | 0.4889 | — |

**판정: [조건부]** (`scripts/youtube_backtest.py:_verdict()` 기준 — `ic > 0.01`이면 t-stat 무관하게 조건부) — rolling window·가중치 조정 후 v2 재검증 필요. `attention_score`를 아직 `effective_confidence`에 편입하지 않음.

Direction별 ret_5d 평균: buy +2.47%(n=641), neutral +3.60%(n=1,878), sell +1.58%(n=68) — buy가 neutral보다 낮아 현재 신호 추출 방식이 예측력을 갖는다고 보기 어렵다.

### 결과별 대응 (`_verdict()` 4단계 판정, 참고)

- `[합격]` IC > 0.05 AND t-stat > 1.65 → `attention_score`를 `effective_confidence`에 낮은 가중치로 편입
- `[조건부]` IC > 0.01 → rolling window·가중치 조정 후 v2 재검증 ← **현재 해당**
- `[역지표 후보]` IC < 0 → 청산/경계 신호로 재설계 검토
- `[불합격]` 그 외(0 ≤ IC ≤ 0.01) → 채널 교체 또는 전처리 개선 필요

---

## 관련 문서

- [소급 수집 방법 (how-to)](howto-youtube-backfill.md)
- [YouTube 내러티브 레퍼런스](reference-youtube-narrative.md)
- [TODOS.md — P1 백테스트](TODOS.md)
