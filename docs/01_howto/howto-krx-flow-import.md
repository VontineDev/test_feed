# KRX 외국인·기관 수급 데이터 임포트 방법

## 목적

`daily_flow` 테이블에 외국인·기관 순매수 이력을 적재합니다. 이 데이터는 stage classifier의 수급 강도 계산(`krx_flow_sync.py`)에 사용됩니다.

## 전제 조건

한국 ISP 또는 VPN 환경 (data.krx.co.kr은 해외 IP 차단).

## 방법 A: KRX 직접 크롤 (권장)

### 1단계: 자격증명 설정

data.krx.co.kr 아이디/비밀번호를 `.env`에 추가:

```env
KRX_ID=your_id
KRX_PW=your_password
```

### 2단계: 응답 구조 확인 (첫 실행 전 권장)

```bash
python data/krx_flow_sync.py --probe 005930
```

삼성전자 당일 응답 구조를 출력합니다. 응답 형식이 변경됐을 때 파악하기 위한 진단 명령입니다.

### 3단계: 초기 벌크 적재 (최초 1회)

```bash
# 2023년~현재까지 전체 KOSPI+KOSDAQ 수급 데이터 적재
python data/krx_flow_sync.py --start 2023-01-01 --end 2026-05-31

# 테스트: KOSPI 50종목만 먼저 확인
python data/krx_flow_sync.py --start 2026-01-01 --end 2026-05-31 --market KOSPI --max 50
```

전체 적재는 수 시간 소요됩니다 (전 종목 × 전 날짜 × API 호출 간격).

### 4단계: 스케줄러 자동 증분

`run_scheduler.py` 실행 후 평일 18:00 KST(장 마감 + KRX 게시 여유 확보)에 전일 데이터가 자동 적재됩니다. 별도 작업 불필요.

수동으로 증분 실행하려면:

```bash
python data/krx_flow_sync.py --incremental
```

---

## 방법 B: CSV 수동 임포트

data.krx.co.kr에서 직접 다운로드한 CSV를 임포트합니다.

### 1단계: CSV 다운로드

1. data.krx.co.kr 접속
2. 주식 → 투자자별 거래실적
3. 날짜 범위 설정, 전체 종목 선택 후 CSV 다운로드

### 2단계: CSV 임포트

```bash
python data/krx_flow_sync.py --csv /path/to/download.csv --backend csv
```

KRX 다운로드 CSV (한국어 헤더)를 자동으로 감지합니다:

```
날짜,종목코드,종목명,외국인순매수,기관합계
20260529,005930,삼성전자,123456,-78900
```

또는 표준 형식:

```
date,ticker,foreign_net,inst_net
2026-05-29,005930.KS,123456,-78900
```

---

## 방법 C: pykrx 백엔드 (대안)

`pip install pykrx` 설치 후 `--backend pykrx`를 지정하면 KRX 직접 크롤 대신 pykrx 라이브러리로 수급 데이터를 가져옵니다. 미설치 상태로 실행하면 에러로 종료됩니다.

```bash
python data/krx_flow_sync.py --start 2025-01-01 --backend pykrx
```

---

## 검증

```sql
-- 최근 5일 데이터 확인
SELECT trade_date, COUNT(*) as ticker_count,
       SUM(ABS(foreign_net)) as total_foreign_activity
FROM daily_flow
WHERE trade_date >= CURRENT_DATE - 5
GROUP BY trade_date
ORDER BY trade_date DESC;
```

기대값: 날짜당 KOSPI+KOSDAQ 전 종목 수 (~2,800건).

---

## 문제 해결

| 증상 | 원인 | 해결 |
|------|------|------|
| 403 에러 | 해외 IP | 한국 VPN 사용 |
| `KRX_ID/KRX_PW 미설정` | .env 누락 | 자격증명 추가 |
| 응답 파싱 실패 | API 형식 변경 | `--probe 005930`으로 응답 구조 확인 |
| CSV 헤더 인식 실패 | 다른 CSV 형식 | 수동으로 헤더를 표준 형식으로 변환 |

## 관련 문서

- [KRX 파이프라인 레퍼런스](reference-krx-pipeline.md)
