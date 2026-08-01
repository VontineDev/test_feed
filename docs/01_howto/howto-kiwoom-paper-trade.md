# 키움 모의투자 설정 방법

## 목적

키움 모의투자 서버(`mockapi.kiwoom.com`)에 연결하여 스케줄러 신호 기반 자동 주문을 테스트합니다. 실 계좌에 영향 없이 전략을 검증할 수 있습니다.

## 전제 조건

- 키움증권 계정 (HTS/MTS 가입 완료)
- `run_scheduler.py` 실행 중

## 1단계: 모의투자 API 키 발급

1. 키움 OpenAPI 신청 (openapi.kiwoom.com)
2. 모의투자 앱키/시크릿키 발급 (실 API와 별도)
3. 모의투자 계좌번호 확인 (HTS "모의투자" 메뉴에서 확인)

## 2단계: 환경 변수 설정

`.env`에 추가:

```env
KIWOOM_MOCK_APPKEY=your_mock_appkey
KIWOOM_MOCK_APPSECRET=your_mock_secretkey
KIWOOM_MOCK_ACCOUNT=12345678  # 8자리 모의투자 계좌번호
```

실 API 키(`KIWOOM_APPKEY`, `KIWOOM_SECRETKEY`)와 구분됩니다.

## 3단계: 스케줄러 재시작

```bash
python run_scheduler.py
```

시작 로그에서 초기화 성공 메시지를 확인합니다:

```
[paper] 키움 모의투자 클라이언트 초기화 완료
[paper] Exit Checker 등록 완료 (15:20 KST)
[paper] EOD 샘플러 등록 완료 (16:40 KST)
[paper] T+1 진입 잡 등록 완료 (09:05 KST)
[compose-paper] 주간 신호 적재 잡 등록 완료 (일요일 21:15 KST)
```

`KIWOOM_MOCK_APPKEY` 미설정이면:
```
[paper] KIWOOM_MOCK_APPKEY 미설정 — 모의투자 비활성
```

## 4단계: 포지션 확인

텔레그램에서 현재 모의투자 상태를 조회합니다:

```
/paper           — 오픈 포지션 목록 + 미실현 손익
/paper_perf      — 누적 성과 (승률, 평균 수익, 슬리피지)
```

또는 DB에서 직접 조회:

```sql
-- 현재 오픈 포지션 (paper_trades 테이블은 존재하지 않음 — paper_positions 하나로 관리)
SELECT ticker, model, entry_theory, entry_actual, qty, signal_date
FROM paper_positions
WHERE status != 'closed'
ORDER BY signal_date DESC;

-- 모델별 성과 (/paper_perf 명령어와 동일 쿼리, telegram_bot.py 참고)
SELECT model,
       COUNT(*) FILTER (WHERE status = 'closed')                       AS closed,
       AVG(blended_return) FILTER (WHERE status = 'closed')            AS avg_return,
       AVG(CASE WHEN blended_return > 0 AND status = 'closed' THEN 1.0
                WHEN status = 'closed' THEN 0.0 END)                   AS win_rate
FROM paper_positions
GROUP BY model;
```

## 5단계: 수동 청산

특정 종목을 즉시 청산하려면:

```
/paper_exit 005930   ← 종목코드 6자리
```

## 모델 슬롯 설정

`data/kiwoom_paper_trader.py`의 `MODEL_CONFIG`를 편집하여 모델별 슬롯 수(분산 종목 수)를 조정합니다:

```python
MODEL_CONFIG = {
    "stage":           {"max_slots": 10},
    "kosdaq":          {"max_slots": 10},
    "cross":           {"max_slots":  5},
    "ichimoku":        {"max_slots": 10},
    "compose-funnel1": {"max_slots": 10},
    "compose-and1":    {"max_slots":  5},
    "compose-score1":  {"max_slots":  5},
}
```

`max_slots` 초과 시 신규 진입이 거부됩니다.

포지션당 금액은 더 이상 고정값이 아니라 **계좌 자산 기준으로 매 실행마다 동적 계산**됩니다(2026-07-31 재설계).
`compute_slot_krw()`가 계좌 추정예탁자산(`prsm_dpst_aset_amt`)의 `1 - CASH_RESERVE_RATIO`(기본 20% 현금 비중 제외)만큼을
`ACTIVE_MODELS`(kosdaq 제외 — 신호가 전혀 생성되지 않는 별도 버그) 수로 균등 분배하고, 그 금액을 각 모델의
`max_slots`로 나눠 슬롯당 금액을 정합니다. 슬롯이 적은 모델(cross/compose-and1/compose-score1, 5개)은
슬롯당 금액이 크고, 슬롯이 많은 모델(10개)은 슬롯당 금액이 작지만, 모델별 총 배정 금액은 동일합니다.

`paper_open_entry_job`은 매수 주문 전에 "기투자금액 + 이번 주문 금액"이 배포 가능 자본(현금 비중 제외분)을
넘는지 확인하고, 넘으면 주문을 스킵합니다(다음 실행에서 재시도).

## 주의 사항

- 모의투자 서버는 KRX 거래 시간(09:00~15:30 KST)에만 주문이 체결됩니다.
- 모의투자 계좌 초기 예수금은 키움 HTS에서 확인/재설정합니다 (일반적으로 1억원).
- `paper_open_entry` 잡(09:05 KST)이 전일 신호 종목에 대해 당일 시가로 주문합니다.
- `paper_exit_checker` 잡(15:20 KST, 정규장 마감 직전)이 익절/손절 조건 충족 종목을 시장가로 청산합니다.
- `compose_paper_entry` 잡(일요일 21:15 KST)은 FUNNEL-1/AND-1/SCORE-1 조합전략 주간 신호를 DB에 적재만 하며, 이 시점에는 Kiwoom 호출이 필요 없습니다 — 실제 주문은 다음 평일 `paper_open_entry`에서 실행됩니다.

## 관련 문서

- [키움 연동 레퍼런스](reference-kiwoom.md)
- [스케줄러 레퍼런스](reference-scheduler.md)
