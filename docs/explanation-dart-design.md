# DART 파이프라인 설계 설명

## 왜 3계층(download / sync / extract)인가

### 문제

OpenDART가 제공하는 데이터에는 두 가지 성질이 있다:

1. **API 기반 구조화 데이터** — XBRL 재무수치, 공시 이벤트 목록. JSON으로 받을 수 있고, DB에 직저장이 가능하다.
2. **문서 원문** — 사업보고서 본문 XML/HTML. API로 접근 가능하지만, 파싱 결과의 신뢰도를 높이려면 로컬에서 반복 실험이 필요하다.

이 두 성질을 하나의 모듈에서 처리하면, 한 쪽의 실패가 다른 쪽을 블록한다. 특히 LLM 추출(3단계)은 실험적이고 시간이 오래 걸리므로 API 수집(1단계)과 묶으면 안 된다.

### 해결: 레이어 분리

```
dart_download.py   → API에서 ZIP 다운로드, 로컬에 저장 (멱등)
dart_sync.py       → API에서 구조화 데이터 → DB 직저장 (증분)
dart_extractor.py  → 로컬 XML → Ollama → dart_extractions (재실행 가능)
```

각 레이어는 독립적으로 재실행할 수 있다. download가 실패해도 sync는 영향 없다. extractor가 모델을 바꿔 재실행해도 API를 다시 호출하지 않는다.

---

## XML 추출을 Ollama로 한 이유

### 문제: DART 사업보고서 본문은 비정형이다

XBRL(`fnlttSinglAcntAll`)은 재무수치를 표준 스키마로 제공한다. 하지만 "AI 사업 비중", "서비스 로봇 매출 성장"처럼 내러티브에 담긴 정성적 정보는 XBRL에 없다.

사업보고서 사업의 내용(II절)은 기업마다 HTML 구조가 다르고, 정규식으로 파싱하기 어렵다. 규칙 기반 접근으로는 20개 기업 × 다양한 보고서 양식을 커버하기 힘들다.

### 해결: 키워드+앵커 그레핑 → LLM 요약

```
XML 전체 (수십만 자)
    ↓ 앵커 헤더(매출실적, 사업부문별...) 발견 시 이하 50행 수집 (최대 8,000자)
    ↓ 키워드(AI, 데이터센터, 매출...) 포함 행 그레핑 (나머지 예산)
    = 최대 20,000자 컨텍스트
    ↓ Ollama qwen3.5:9b 호출 (max_tokens=2,000)
    = 투자 판단용 서술 텍스트
```

**이 방식의 트레이드오프:**
- 키워드/앵커 목록에 없는 섹션은 누락될 수 있다.
- Ollama 응답 품질은 모델과 프롬프트에 의존한다.
- 기업당 수십 초 ~ 수 분 소요.

XBRL로 대체 불가능한 정성적 정보를 자동화하는 것이 목적이므로, 불완전한 추출이라도 없는 것보다 낫다. `--force` 옵션으로 모델 교체 후 재추출이 가능하다.

---

## dart_companies FK 레이스 컨디션 해결

### 문제

일별 공시 스케줄러(`09:00 KST`)가 `dart_disclosures`에 INSERT 할 때, `dart_companies`에 없는 신규 상장 기업이 공시를 내면 FK 위반 에러가 발생한다. 에러는 로그에만 남고, 해당 공시는 조용히 유실된다.

`--seed-companies`를 매일 실행하는 방법도 있지만, 5만 건을 매일 다운로드하는 것은 API 한도 낭비다.

### 해결: INSERT 전 자동 시드

```python
# dart_sync.py, _ensure_corp()
await conn.execute("""
    INSERT INTO dart_companies (corp_code, corp_name, updated_at)
    VALUES ($1, $2, NOW())
    ON CONFLICT (corp_code) DO NOTHING
""", corp_code, corp_name)
```

공시 INSERT 직전에 `corp_code` 존재 여부를 확인하고, 없으면 최소 정보(코드+이름)만 시드한다. 다음 월별 `--seed-companies` 실행 시 `stock_code` 등 나머지 정보가 완성된다.

---

## XBRL만으로는 부족한 이유 — 세그먼트 파싱

`dart_segments`가 존재하는 이유: XBRL은 재무수치(매출 합계, 영업이익 등)를 제공하지만 세그먼트 granularity가 기업마다 다르다. 삼성전자는 DS/DX 부문을 나누지만, 어떤 기업은 단일 세그먼트로만 보고한다.

사업의 내용 II-2(주요제품/서비스), II-4(매출현황)를 HTML에서 Ollama로 파싱하면, XBRL에는 없는 제품별 매출 비중, 서비스별 성장률 등을 구조화된 JSON으로 얻을 수 있다.

---

## 관련 문서

- [DART 파이프라인 레퍼런스](reference-dart-pipeline.md)
- [DART 파이프라인 설정 방법](howto-dart-setup.md)
