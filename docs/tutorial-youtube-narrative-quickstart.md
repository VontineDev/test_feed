# 튜토리얼: YouTube 내러티브 파이프라인 첫 설정

삼프로TV 방송 자막을 자동 분석해 종목 언급 데이터를 수집하는 파이프라인을
처음 설정한다. 이 튜토리얼이 끝나면 `youtube_mention_raw` 테이블에 첫 데이터가
쌓이고, 스케줄러가 매일 09:05 KST에 자동 수집을 시작한다.

## 필요한 것

- Python 환경 및 의존성 설치 완료 (`pip install -r requirements.txt`)
- PostgreSQL 접속 가능 (`.env`에 `DATABASE_URL` 설정)
- **YouTube Data API v3 키** — [Google Cloud Console](https://console.cloud.google.com/)에서 발급
- **Google Gemini API 키** — [Google AI Studio](https://aistudio.google.com/)에서 발급

---

## 1단계: API 키를 .env에 추가

`.env` 파일에 두 키를 추가한다.

```
YOUTUBE_API_KEY=AIza...
GEMINI_API_KEY=AIza...
```

---

## 2단계: DB 테이블 생성

```bash
python data/youtube_narrative_sync.py --ensure-tables
```

```
INFO  [yt-sync] 테이블 확인 완료
```

세 테이블이 생성된다: `youtube_mention_raw`, `youtube_attention_scores`,
`youtube_mention_forward_returns`. 이미 존재하면 무시한다.

---

## 3단계: 어제 영상 수동 수집으로 동작 확인

스케줄러 없이 직접 실행해 파이프라인이 전체적으로 동작하는지 확인한다.

```bash
python data/youtube_narrative_sync.py
```

정상 동작하면 아래와 같은 로그가 출력된다.

```
INFO  [yt-sync] 영상 목록: 8개 (2026-06-02 ~ 2026-06-02)
INFO  [yt-sync] [1/8] 2026-06-02 삼성전자 실적 전망은?...
INFO  [yt-sync]   -> 3/5건 저장
INFO  [yt-sync] 운영 수집 완료: 12건
```

### IP 차단 오류가 나오면

```
WARNING  [yt-sync] 자막 없음 abc123: YouTube is blocking requests from your IP.
```

IP가 차단된 상태다. 두 가지 방법으로 우회할 수 있다.

**방법 A — 쿠키 파일**: Chrome에서 YouTube에 로그인한 뒤 "Get cookies.txt LOCALLY" 확장으로
Netscape 형식 쿠키를 내보내 `docs/youtube.com_cookies.txt`에 저장한다. 파일이 있으면
`_make_yt_api()`가 자동으로 로드한다.

**방법 B — 대기**: YouTube IP 차단은 보통 수 시간~1일 내에 자동 해제된다.

---

## 4단계: 데이터 확인

```sql
SELECT ticker, stock_name_raw, direction, video_date
FROM youtube_mention_raw
ORDER BY created_at DESC
LIMIT 10;
```

행이 보이면 파이프라인이 정상 작동한 것이다.

---

## 5단계: 스케줄러 등록

`run_scheduler.py`에 YouTube 잡 3개가 이미 등록되어 있다. Windows 작업 스케줄러
태스크("NewsCrawler")를 재시작하면 즉시 활성화된다.

```bat
scripts\restart_scheduler.bat
```

| 잡 ID | 실행 시각 (KST) | 동작 |
|-------|----------------|------|
| `youtube_narrative_sync` | 평일 09:05 | 전일 영상 수집 + Gemini 추출 |
| `youtube_attention_score` | 평일 09:35 | attention_score 집계 |
| `youtube_forward_return` | 평일 15:40 | yfinance forward return 채우기 |

---

## 지금 만들어진 것

- `youtube_mention_raw`: 종목 언급 원시 데이터 (video_id, ticker, direction, source_quote)
- 스케줄러가 매 평일 전일 영상을 자동 수집

**다음 단계**:
- 과거 1~5월 데이터를 소급 수집해 백테스트 샘플을 쌓으려면 → [소급 수집 방법](howto-youtube-backfill.md)
- attention_score의 예측력을 검증하려면 → [백테스트 실행 방법](howto-youtube-run-backtest.md)
- 시스템 설계 배경을 이해하려면 → [설계 개념 문서](explanation-youtube-narrative-design.md)
