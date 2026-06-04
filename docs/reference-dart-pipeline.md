# DART 파이프라인 레퍼런스

OpenDART 전자공시 데이터를 수집·저장·추출하는 4개 모듈의 완전한 기술 레퍼런스.

## 아키텍처 개요

```
OpenDART API
    │
    ├─ dart_download.py  ─── reports/dart/{기업}/{rcept_no}_{보고서명}/  (로컬 XML)
    │
    ├─ dart_sync.py      ─── DB: dart_companies / dart_disclosures / dart_xbrl / dart_segments
    │
    └─ dart_extractor.py ─── DB: dart_extractions  (Ollama로 추출한 내러티브)
                              └─ scripts/export_dart_md.py  → dart/{date}_{corp}.md
```

---

## 환경 변수

| 변수 | 필수 | 설명 |
|------|------|------|
| `DART_API_KEY` | 필수 | OpenDART API 인증키. opendart.fss.or.kr 가입 후 발급. |
| `OLLAMA_MODEL` | 선택 | 추출 모델 (기본: `qwen3.5:9b`) |
| `OLLAMA_BASE` | 선택 | Ollama 서버 주소 (기본: `http://localhost:11434`) |
| `DATABASE_URL` | 필수 | PostgreSQL DSN (dart_sync, dart_extractor 공통) |

---

## dart_download.py

정기보고서(사업보고서·반기보고서·분기보고서) 원문 ZIP을 내려받아 압축 해제.

### CLI

```bash
python data/dart_download.py                            # 올해 Top 20 전체
python data/dart_download.py --year 2025                # 2025년
python data/dart_download.py --year 2026 --corp 005930  # 삼성전자만
python data/dart_download.py --year 2026 --type 사업보고서
python data/dart_download.py --year 2026 --dry-run      # 다운로드 없이 목록 확인
python data/dart_download.py --year 2026 --zip          # 압축 해제 대신 ZIP 보관
```

### 플래그

| 플래그 | 기본값 | 설명 |
|--------|--------|------|
| `--year YYYY` | 올해 | 수집 연도 |
| `--corp CODE` | Top 20 전체 | 6자리 종목코드 또는 8자리 DART 코드 |
| `--type TYPE` | 전체 | `사업보고서` / `반기보고서` / `분기보고서` |
| `--out DIR` | `reports/dart/` | 저장 경로 |
| `--dry-run` | off | 실제 다운로드 없이 수집 대상 출력 |
| `--zip` | off | 압축 해제 대신 ZIP 파일 그대로 저장 |

### 저장 구조

```
reports/dart/
  삼성전자/
    20260313000662_사업보고서 (2025.12)/   ← 압축 해제 XML 파일들
      BIZ_20260313_XXXXXX.xml
      ...
    20260313000662_meta.json               ← 메타데이터 (rcept_no, 날짜, 크기, 파일목록)
```

이미 `target_dir`(또는 `--zip` 시 `.zip`)와 `meta.json`이 모두 존재하면 자동 스킵.

---

## dart_sync.py

OpenDART API → DB 수집. 3개 레이어:
- `dart_companies`: DART 기업 고유번호 마스터
- `dart_disclosures`: 일별 공시 이벤트
- `dart_xbrl`: 사업보고서 XBRL 재무수치
- `dart_segments`: 사업보고서 II-2/II-4 절 Ollama 파싱 결과

### CLI

```bash
python data/dart_sync.py --seed-companies           # corp 마스터 초기 시드 (최초 1회)
python data/dart_sync.py --sync-disclosures         # 전일 공시 수집
python data/dart_sync.py --sync-disclosures --bgn-de 20260101 --end-de 20260531
python data/dart_sync.py --sync-xbrl 2025           # 2025년 XBRL 재무수치
python data/dart_sync.py --sync-segments 2025       # 2025년 세그먼트 파싱
python data/dart_sync.py --sync-segments 2025 --force  # 기존 파싱 덮어쓰기
python data/dart_sync.py --corp-code 005930         # 단일 기업 지정 (모든 잡에 공통)
```

### 주요 함수

#### `DartClient`

```python
class DartClient:
    async def fetch_corp_codes_zip() -> bytes
    async def fetch_disclosures(corp_code, bgn_de, end_de, pblntf_ty) -> list[dict]
    async def fetch_document_zip(rcept_no) -> bytes
    async def fetch_xbrl(corp_code, bsns_year, reprt_code, fs_div) -> list[dict]
```

`pblntf_ty`: `"A"` = 정기보고서, `"B"` = 주요사항보고서. 수집 대상: A+B 모두.

#### `seed_corp_companies(pool, api_key=None) -> int`

`corpCode.zip` 전체(~5만 건)를 `dart_companies`에 upsert. 최초 1회 실행 필수. 1000건 단위 배치.

#### `sync_disclosures(pool, corp_codes, bgn_de=None, end_de=None) -> int`

`bgn_de`/`end_de` 미지정 시 전일 하루치. `corp_codes` 기업별 A·B 공시 타입 각각 조회. FK 위반 방지를 위해 `_ensure_corp()`로 `dart_companies`에 corp_code 자동 시드.

#### `sync_xbrl(pool, corp_codes, bsns_year=None) -> int`

연결(CFS) + 개별(OFS) 재무제표 모두 수집. `bsns_year` 미지정 시 전년도.

#### `sync_segments(pool, corp_codes, bsns_year=None, force=False) -> int`

`parse_ok=TRUE` 레코드가 2개 이상이면 건너뜀 (force=False). `II-2`(주요제품/서비스), `II-4`(매출현황) 절 Ollama 파싱. `_MAX_SECTION_CHARS = 6000`.

### 수집 대상 (Top 20 종목코드)

```
005930(삼성전자) 000660(SK하이닉스) 207940(삼성바이오) 005380(현대차)
068270(셀트리온) 005490(POSCO홀딩스) 035420(NAVER) 051910(LG화학)
000270(기아) 028260(삼성물산) 012330(현대모비스) 066570(LG전자)
003670(포스코퓨처엠) 032830(삼성생명) 055550(신한지주) 105560(KB금융)
035720(카카오) 096770(SK이노베이션) 017670(SK텔레콤) 030200(KT)
```

---

## dart_extractor.py

로컬 XML을 읽어 Ollama로 두 가지 결과를 `dart_extractions` 테이블에 저장:
1. **서술 텍스트** (`extraction_text`) — 투자 판단용 내러티브 (프롬프트 템플릿 기반)
2. **3-Pass 구조화 JSON** (`segments_json`, `revenue_json`, `competitors_json`)

### CLI

```bash
python data/dart_extractor.py --company LG전자    # 단일 기업 (콘솔 출력, DB 없음)
python data/dart_extractor.py --all               # 전체 처리 → DB 저장
python data/dart_extractor.py --all --force       # 기존 결과 덮어쓰기
python data/dart_extractor.py --all --model gemma3:9b
python data/dart_extractor.py --migrate-schema    # dart_extractions에 3-Pass 컬럼 추가 (최초 1회)
```

### 플래그

| 플래그 | 설명 |
|--------|------|
| `--company NAME` | `reports/dart/` 하위 디렉터리명과 일치. 단일 기업, DB 저장 없음 |
| `--all` | `reports/dart/` 전체 처리. DB 저장 |
| `--force` | 이미 저장된 `(corp_name, rcept_no)` 건너뛰지 않고 덮어쓰기 |
| `--model MODEL` | Ollama 모델 (기본: `OLLAMA_MODEL` 환경변수 또는 `qwen3.5:9b`) |
| `--migrate-schema` | `dart_extractions`에 `segments_json`/`revenue_json`/`competitors_json` 컬럼 추가 |

### XML 추출 전략

```
extract_xml(xml_path) → max 20,000자  (서술 추출 + 3-Pass 공통 기반)
  ├─ anchor_xml()   헤더 앵커 발견 시 이하 50행 수집 → max 8,000자 (우선)
  └─ grep_xml()     키워드 포함 행 추출 → 나머지 예산 (char_limit - anchor_text)
```

**추출 키워드**: AI, 인공지능, 온디바이스, 데이터센터, 로봇, 매출, 영업이익, 목표 등 14개

**앵커 헤더**: 부문별 매출실적, 사업부문별 매출, 매출실적, 영업실적 등 11개

리포트 디렉터리에 복수 XML이 있으면 `{rcept_no}.xml`(suffix 없는 메인) 우선 선택.

---

### 3-Pass 구조화 추출 (`extract_structured`)

`extract_structured(http, xml_path, model)` 은 단일 XML에서 세 개의 JSON을 병렬 추출한다.

```
Pass 1 (segments)  ─ _SEGMENT_ANCHORS → SEGMENT_EXTRACTOR_PROMPT
                      → segments_json: [{segment_name, products, revenue_share_pct, note}]

Pass 2 (revenue)   ─ _REVENUE_ANCHORS → REVENUE_ANALYZER_PROMPT
                      → revenue_json: {periods, segments, consolidated, unit}
                      ※ 손익계산서 트리밍 + 합계행 보완 + 금융사 폴백 후 전달

Pass 3 (competitors) ─ extract_xml() 전체 6,000자 → COMPETITOR_EXTRACTOR_PROMPT
                        → competitors_json: [{name, relation_type, competing_segment,
                                              source_quote, confidence}]
```

세 Pass는 `asyncio.gather`로 병렬 실행된다. 하나가 실패해도 나머지는 계속된다.

#### `_anchor_best()` 우선순위 탐색 (Pass 2에 사용)

`_REVENUE_ANCHORS` 목록을 순서대로 시도하며 4단계로 최적 블록을 찾는다:

| 패스 | 조건 |
|------|------|
| 1 | 헤더 길이 ≤ 80 + `영업이익` 포함 + 목차(처음 200줄) 제외 |
| 2 | 본문 모든 길이 + `영업이익` 포함 + 목차 제외 |
| 3 | 헤더 + `영업이익` 포함 (목차 위치 포함 fallback) |
| 4 | 길이 무관 최종 fallback |

이 탐색 덕분에 "사업부문별 요약 재무현황"처럼 목차에도 등장하는 헤더가 실제 섹션 대신 목차를 히트하는 문제를 방지한다.

#### revenue 전처리 3단계

Pass 2 추출 후 Ollama에 전달하기 전에 rev_text에 3가지 전처리를 적용한다:

1. **손익계산서 트리밍**: 요약재무정보 섹션은 `재무상태표 → 손익계산서` 순으로 나열된다. "매출액" / "영업수익" 등 손익 시작 키워드가 등장하는 줄부터 잘라낸다 (재무상태표 오염 방지).
2. **영업이익 이후 30줄 절단**: 마지막 "영업이익" 줄 다음 30줄에서 자른다. 재무 표 이후의 사업 설명이 Ollama 입력에 포함되어 파싱을 방해하는 것을 막는다.
3. **합계행 보완**: rev_text에 "합계/총계" 행이 없으면 `매출실적` 표를 추가로 병합한다. 삼성전자처럼 부문별 요약에 내부거래가 포함돼 실제 연결 합계가 별도 표에 있는 경우에 대응한다.

#### 금융사 폴백

rev_text에 "매출액", "영업수익", "이자수익", "총영업이익" 등 매출 지표가 없으면 금융사 전용 앵커(`영업수익`, `총영업이익`, `이자이익` 등)로 재탐색한다. Ollama가 revenue를 빈 값으로 반환하면 regex로 `매출액(영업수익)`, `총영업이익`, `순영업이익` 패턴을 직접 추출한다.

#### `extract_all(pool, model, force, dart_dir) -> int`

- 단일 `httpx.AsyncClient` 공유
- Ollama 미응답 시 전체 중단
- 개별 기업 오류는 catch+log 후 계속 (전체 중단 없음)
- UPSERT 시 `segments_json`/`revenue_json`/`competitors_json`는 `COALESCE(EXCLUDED, 기존값)` — 새로 추출된 값이 None이어도 기존 성공 결과를 덮어쓰지 않는다
- 예상 실행 시간: 10~60분 (기업당 Ollama 응답 시간 의존)

---

## scripts/export_dart_md.py

`dart_extractions` DB 레코드를 마크다운 파일로 일괄 출력.

```bash
python scripts/export_dart_md.py
```

출력 경로: `dart/YYYYMMDD_{corp}_{period}_{type}.md`

예: `dart/20260529_삼성전자_202512_사업.md`

---

## DB 테이블

### `dart_companies`

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `corp_code` | TEXT PK | DART 8자리 고유번호 |
| `corp_name` | TEXT | 기업명 |
| `stock_code` | TEXT | KRX 6자리 종목코드 (상장기업만) |
| `updated_at` | TIMESTAMPTZ | |

### `dart_disclosures`

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `rcept_no` | TEXT PK | 접수번호 (고유) |
| `corp_code` | TEXT FK | → `dart_companies` |
| `corp_name` | TEXT | |
| `report_nm` | TEXT | 보고서명 |
| `rcept_dt` | DATE | 공시일 |
| `pblntf_ty` | TEXT | A=정기보고서, B=주요사항보고서 |
| `rm` | TEXT | 비고 |

### `dart_xbrl`

| 컬럼 | 타입 | 설명 |
|------|------|------|
| PK | (corp_code, bsns_year, reprt_code, account_nm, fs_div) | |
| `amount` | BIGINT | 당기 수치 (원) |
| `currency` | TEXT | 'KRW' |
| `fetched_at` | TIMESTAMPTZ | |

`fs_div`: `CFS`=연결, `OFS`=별도. `reprt_code`: `11011`=사업보고서, `11012`=반기보고서.

### `dart_segments`

| 컬럼 | 타입 | 설명 |
|------|------|------|
| PK | (corp_code, bsns_year, section) | `section`: `II-2` 또는 `II-4` |
| `raw_text` | TEXT | 섹션 원문 (최대 6,000자) |
| `parsed_json` | JSONB | Ollama 파싱 결과 |
| `parse_ok` | BOOL | 파싱 성공 여부 |
| `model` | TEXT | 사용 모델명 |

### `dart_extractions`

| 컬럼 | 타입 | 설명 |
|------|------|------|
| PK | (corp_name, rcept_no) | UPSERT 키 |
| `report_type` | TEXT | 사업보고서 / 분기보고서 등 |
| `period` | TEXT | 예: `2025.12` |
| `extraction_text` | TEXT | Ollama 추출 서술 텍스트 (프롬프트 템플릿 기반) |
| `model` | TEXT | 사용 모델명 |
| `xml_chars` | INT | 입력 XML 컨텍스트 크기 |
| `extracted_at` | TIMESTAMPTZ | |
| `segments_json` | JSONB | 3-Pass Pass 1 결과: `[{segment_name, products, revenue_share_pct, note}]` |
| `revenue_json` | JSONB | 3-Pass Pass 2 결과: `{periods, segments, consolidated, unit}` |
| `competitors_json` | JSONB | 3-Pass Pass 3 결과: `[{name, relation_type, competing_segment, source_quote, confidence}]` |

`segments_json`/`revenue_json`/`competitors_json`는 `--migrate-schema` 로 추가되는 컬럼이다. UPSERT 시 새 값이 NULL이면 기존 값을 보존한다 (`COALESCE`).

`revenue_json.unit` 필드: Ollama 호출 전 rev_text에서 `(단위 : XXX)` 패턴을 감지해 주입한다. 값은 `원` / `천원` / `백만원` / `억원` / `조원` / `unknown` 중 하나.

---

## 관련 문서

- [DART 파이프라인 설정 방법](howto-dart-setup.md)
- [DART 설계 설명](explanation-dart-design.md)
- [스케줄러 레퍼런스](reference-scheduler.md) — `daily_dart_disclosures`, `monthly_dart_xbrl`, `annual_dart_extractor` 잡
