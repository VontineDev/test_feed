# How to: YouTube 내러티브 백테스트 실행 및 결과 해석

attention_score(종목 언급 빈도·감성 집계)가 실제 주가 수익률을 예측하는지
IC(Information Coefficient, Spearman 순위 상관계수)로 검증한다.

## 전제 조건

- `youtube_mention_raw`와 `youtube_attention_scores`에 데이터가 있어야 한다
- `youtube_mention_forward_returns`에 forward return이 채워져 있어야 한다
  - `ret_5d` 검증 기준: 가장 늦은 `video_date` 기준 +5 영업일이 지나야 유효
- scipy 설치: `pip install scipy`

## 1단계: forward return 최신화

백테스트 전에 오늘 종가까지 return을 채운다.

```bash
python data/youtube_narrative_sync.py --fill-returns
```

`forward return 채우기: 0건` 이 출력되면 이미 최신 상태다.

## 2단계: 백테스트 실행

### 개별 언급 단위 IC

```bash
python scripts/youtube_backtest.py --ret ret_5d
```

`ret_5d` 대신 `ret_1d`(익일) 또는 `ret_20d`(1달)도 선택 가능하다.

### 주간 집계 → 1달 수익률 IC (권장)

개별 언급보다 노이즈가 적고 더 안정적인 측정값을 얻는다.

```bash
python scripts/youtube_backtest.py --mode weekly
```

## 3단계: 결과 해석

### IC 합격 기준

| 지표 | 합격 기준 | 의미 |
|------|----------|------|
| Spearman IC | > 0.05 | attention_score 높을수록 수익률 높은 경향 |
| t-stat | > 1.65 | 95% 신뢰수준 (단측 검정) |
| 샘플 수 (n) | ≥ 100 | 통계적으로 유의미한 최소 건수 |

### 결과별 대응

| 판정 | 조건 | 다음 액션 |
|------|------|----------|
| `[합격]` | IC > 0.05 AND t > 1.65 | `effective_confidence`에 낮은 가중치로 편입 |
| `[조건부]` | IC 0.01~0.05 | rolling window·가중치 조정 후 v2 재검증 |
| `[역지표 후보]` | IC < 0 | 청산·경계 신호로 재설계 검토 |
| `[불합격]` | IC ≈ 0 | 채널 교체 또는 전처리 개선 후 v2 재검증 |

### direction별 평균 수익률 읽기

`buy` 언급의 평균 수익률이 `neutral` > `sell` 순으로 높으면 LLM의 방향 판단이 유효하다.

```
[Direction별 평균 ret_5d]
  buy     : n= 320  avg=+0.0082 (+0.82%)
  neutral : n= 180  avg=+0.0031 (+0.31%)
  sell    : n=  40  avg=-0.0012 (-0.12%)
```

### 주간 모드의 attention_score 구간별 수익률

상위 3분위(`high_ret`)가 하위 3분위(`low_ret`)보다 높으면 attention_score의 단조 예측력이 있는 것이다.

```
[attention_score 구간별 평균 ret_20d]
  하위 3분위 (n=15): -0.0021 (-0.21%)
  상위 3분위 (n=15): +0.0143 (+1.43%)
```

## 트러블슈팅

**`데이터 없음. --backfill 먼저 실행하세요.`**
→ `youtube_mention_raw`가 비어 있다. 먼저 [소급 수집](howto-youtube-backfill.md)을 실행한다.

**`샘플 부족 (N < 100)`**
→ 백필 범위를 넓히거나(더 많은 달), 운영 수집을 더 쌓은 뒤 재실행한다.

**`IC = NaN (점수가 모두 동일 — 분산 없음)`**
→ `youtube_attention_scores`의 값이 모두 동일하다. `--compute-scores`가 정상 실행됐는지 확인한다.
  ```bash
  python data/youtube_narrative_sync.py --compute-scores
  ```

**`ModuleNotFoundError: No module named 'scipy'`**
→ `pip install scipy`로 설치한다.

## 관련

- [youtube_backtest.py 소스](../scripts/youtube_backtest.py)
- [백필 계획](plan-youtube-backfill.md)
- [설계 개념 — 블라인드 백테스트 프로토콜](explanation-youtube-narrative-design.md)
