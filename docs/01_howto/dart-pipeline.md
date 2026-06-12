# DART 파이프라인 개요

OpenDART 전자공시 데이터를 수집·저장·분석하는 파이프라인입니다. 코스피 Top 20 기업을 대상으로 3개 레이어로 동작합니다.

## 구조 한 눈에 보기

```
OpenDART API (https://opendart.fss.or.kr)
    │
    ├─ dart_download.py  → reports/dart/{기업}/{rcept_no}_{보고서명}/*.xml   (로컬 저장)
    │
    ├─ dart_sync.py      → dart_companies / dart_disclosures / dart_xbrl / dart_segments  (DB)
    │
    └─ dart_extractor.py → dart_extractions (서술 텍스트 + 3-Pass 구조화 JSON)          (DB)
                           └─ scripts/export_dart_md.py → dart/*.md
```

각 레이어는 독립적으로 실행할 수 있습니다. `dart_download.py`가 실패해도 `dart_sync.py`는 영향 없고, `dart_extractor.py`는 이미 다운로드된 XML을 재사용하므로 API를 다시 호출하지 않습니다.

## 3-Pass 구조화 추출

`dart_extractor.py`는 각 보고서 XML에서 세 가지 JSON을 병렬로 추출합니다:

| Pass | 결과 컬럼 | 내용 |
|------|-----------|------|
| Pass 1 | `segments_json` | 사업부문별 주요 제품, 매출 비중 |
| Pass 2 | `revenue_json` | 연도별 부문별 매출, 영업이익, 금액 단위 |
| Pass 3 | `competitors_json` | 명시적으로 언급된 경쟁사, 경쟁 관계 유형 |

## 스케줄

| 잡 | 실행 시각 (KST) | 담당 레이어 |
|----|-----------------|-------------|
| `daily_dart_disclosures` | 평일 09:00 | dart_sync: 전일 공시 이벤트 |
| `monthly_dart_xbrl` | 매월 1일 02:00 | dart_sync: XBRL 재무수치 |
| `dart_extractions_spring` | 5월 20일 03:00 | dart_extractor: 사업보고서 + 1분기 |
| `dart_extractions_autumn` | 9월 1일 03:00 | dart_extractor: 반기보고서 |
| `dart_extractions_winter` | 11월 20일 03:00 | dart_extractor: 3분기보고서 |

## 필수 환경 변수

| 변수 | 설명 |
|------|------|
| `DART_API_KEY` | OpenDART API 인증키 (opendart.fss.or.kr 발급) |
| `DATABASE_URL` | PostgreSQL DSN |
| `OLLAMA_MODEL` | 추출 모델 (기본: `qwen3.5:9b`) |

## 문서 목록

| 문서 | 설명 |
|------|------|
| [DART 파이프라인 설정 방법](howto-dart-setup.md) | 처음부터 가동하는 단계별 가이드 |
| [DART 파이프라인 레퍼런스](reference-dart-pipeline.md) | 모든 CLI, 함수, DB 스키마 |
| [DART 설계 설명](explanation-dart-design.md) | 레이어 분리, 3-Pass, 금융사 폴백 설계 근거 |
