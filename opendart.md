# OpenDART 파이프라인 상태 문서

## 현재 상태 (2026-06-13 기준)

watchlist 19개 종목 전체 DART 수치 검증 완료.

---

## 완료된 작업 이력

### v1 — 파이프라인 초기 구축
- DART 공시 API 연동, `dart_companies` / `dart_extractions` 스키마 설계
- Top 20 + watchlist 종목 사업/분기/반기보고서 로컬 다운로드 (`data/dart_download.py`)
- Ollama 기반 LLM 추출 (`data/dart_extractor.py`) → DB 저장
- XBRL TE 태그 직접 파싱 (`extract_xbrl_quarterly`) — 분기/반기보고서 우선 사용

### v2 — 수치 버그 수정 (2026-06-12~13)

**버그 1: XBRL ADECIMAL 미적용**
- 원인: `extract_xbrl_quarterly`가 ADECIMAL 속성을 무시하고 raw 값 저장
  - `ADECIMAL="-6"` → ×10^6 미적용 (예: 두산 매출 5원 → 5조원)
- 수정: `_XBRL_ADECIMAL_RE`, `_parse_xbrl_value(adecimal)` 추가
- 영향: 분기/반기보고서 1,705건 중 다수 재추출 (`scripts/fix_xbrl_adecimal.py`)

**버그 2: XBRL `<P>` 태그 래핑 미처리**
- 원인: `_XBRL_TE_RE` 패턴 `[^<]+`가 `<TE><P>값</P></TE>` 구조에서 값 추출 실패
  - 인텔리안테크 등 일부 회사가 이 구조 사용
- 수정: `.+?` + `re.DOTALL` + `_XBRL_INNER_TAG_RE.sub("")` 후처리
- 영향: 73건 추가 재추출

**버그 3: YoY 계산 공식 역전**
- 원인: `StageHistoryPopup.tsx`에서 `(revVals[1] / revVals[0]) - 1` = `(전기/당기) - 1`
- 수정: 인덱스 swap → `(revVals[0] / revVals[1]) - 1` = `(당기/전기) - 1`

**버그 4: API 정렬 기준 오류**
- 원인: `ORDER BY extracted_at DESC` → ADECIMAL 재추출 후 과거 period가 최신 extracted_at을 갖게 돼 오래된 보고서 선택
- 수정: `ORDER BY period DESC` (대시보드 API + verify 스크립트)

**버그 5: 메타랩스 corp_name 인코딩 불일치**
- 원인: `dart_extractions.corp_name = '메타랝스'` (U+B79D) vs `dart_companies.corp_name = '메타랩스'` (U+B7A9)
- 수정: UPDATE + `reports/dart/메타랝스` → `메타랩스` 디렉토리 rename

### v3 — 인텔리안테크(189300) 추가 (2026-06-13)
- 보고서 다운로드: 2024 사업보고서 ~ 2026.1Q 분기보고서 6건
- 추출/저장 완료
- `extract_all(corp_filter=...)` 파라미터 추가 (특정 회사만 재추출 지원)

---

## watchlist 종목 수치 현황 (2026 Q1)

| 종목코드 | 회사명 | 매출 | 영업이익 | 비고 |
|---|---|---|---|---|
| 000100 | 한화에어로스페이스 | 5,268억 | 88억 | |
| 000150 | 두산 | 5.06조 | 3,408억 | |
| 000270 | 기아 | 29.50조 | 2.21조 | |
| 000660 | SK하이닉스 | 52.58조 | 37.61조 | |
| 003550 | LG | 1.80조 | 4,138억 | 지주사 별도 기준 |
| 005380 | 현대차 | 45.94조 | 2.51조 | |
| 005930 | 삼성전자 | 133.87조 | 57.23조 | DS 81.7조+DX 52.7조 |
| 009150 | 삼성전기 | 3.21조 | 2,806억 | |
| 010130 | 고려아연 | 6.07조 | 7,461억 | |
| 011170 | 롯데케미칼 | 4.99조 | 735억 | |
| 015760 | 한국전력 | 24.40조 | 3.78조 | |
| 017670 | SK텔레콤 | 4.39조 | 5,376억 | |
| 028260 | 삼성물산 | 10.47조 | 7,204억 | |
| 034730 | SK | 36.75조 | 3.67조 | |
| 035420 | NAVER | 3.24조 | 5,418억 | +16.3% YoY |
| 051900 | LG생활건강 | 1.58조 | 1,078억 | |
| 090370 | 메타랩스 | 304억 | 5억 | +259.6% YoY |
| 189300 | 인텔리안테크 | 647억 | 6억 | 흑자전환(전년 -120억) |
| 207940 | 삼성바이오로직스 | 1.26조 | 5,808억 | |

모든 수치: 연결재무제표 기준, 단위 원, DART XBRL 원본 검증 완료.

---

## 주요 파일 구조

```
data/
  dart_download.py      # DART API 보고서 다운로드
  dart_extractor.py     # XBRL 파싱 + Ollama LLM 추출 → DB 저장
  dart_sync.py          # DART API 클라이언트

scripts/
  fix_xbrl_adecimal.py      # ADECIMAL 일괄 재추출 (LLM 없이 XBRL만)
  fix_metalabs_corpname.py  # 메타랩스 corp_name 수정
  verify_dart_numbers.py    # watchlist 수치 검증
  check_big_stocks.py       # 대형주 XBRL vs DB 대조
  extract_intelian.py       # 인텔리안테크 재추출 실행기

reports/dart/
  {회사명}/
    {rcept_no}_{보고서명}/  # 압축 해제 XML
    {rcept_no}_meta.json

DB 테이블:
  dart_companies      # 종목코드 ↔ corp_code/corp_name 매핑
  dart_extractions    # 추출 결과 (revenue_json, segments_json 등)
```

---

## XBRL 주요 계정코드

**매출 (Revenue)**
- `ifrs-full_Revenue`
- `ifrs-full_RevenueFromContractsWithCustomers`
- `dart_OperatingRevenue`

**영업이익 (Operating Profit)**
- `dart_OperatingIncomeLoss`
- `ifrs-full_OperatingProfit`

**ACONTEXT 패턴**
- `CFY{년도}dFQQ_...ConsolidatedMember` → 당기 분기, 연결
- `PFY{년도}dFQQ_...ConsolidatedMember` → 전기 분기, 연결
- `ADECIMAL="-6"` → ×1,000,000 (백만원 단위 표기)
- `ADECIMAL="-3"` → ×1,000 (천원 단위 표기)
- `ADECIMAL="0"` → 원 단위 그대로
- `ADECIMAL="INF"` → 원 단위 그대로 (정밀도 무한)
