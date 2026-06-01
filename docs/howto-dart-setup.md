# DART 파이프라인 설정 및 첫 실행 방법

## 목적

이 가이드를 따르면 OpenDART 전자공시 파이프라인을 처음부터 가동할 수 있습니다.
코스피 Top 20 기업의 공시 이벤트, 재무수치(XBRL), 사업보고서 원문, Ollama 내러티브 추출까지 전체 레이어가 작동하는 상태로 끝납니다.

## 전제 조건

- Python 3.11+, PostgreSQL 14+
- Ollama 실행 중 (`ollama serve` + `ollama pull qwen3.5:9b`)
- `.env` 파일에 `DATABASE_URL` 설정 완료

## 1단계: DART API 키 발급

1. [opendart.fss.or.kr](https://opendart.fss.or.kr) 회원가입
2. "인증키 신청/관리" → API 키 발급 (즉시 발급, 무료)
3. `.env`에 추가:

```env
DART_API_KEY=여기에_발급받은_키
```

## 2단계: 기업 마스터 시드 (최초 1회)

`dart_companies` 테이블에 전체 상장·비상장 기업 고유번호를 적재합니다.

```bash
python data/dart_sync.py --seed-companies
```

예상 출력:

```
10:00:00  INFO     [dart] corpCode.zip 다운로드 중...
10:00:03  INFO     [dart] 기업 목록 파싱 완료: 52341건
10:00:08  INFO     [dart] dart_companies upsert 완료: 52341건
dart_companies 시드 완료: 52341건
```

이 단계 없이 `--sync-disclosures`를 실행하면 `corp_code` FK 제약으로 공시 INSERT가 실패할 수 있습니다.

## 3단계: 사업보고서 원문 다운로드

Top 20 기업의 올해 정기보고서를 `reports/dart/`에 저장합니다.

```bash
# 다운로드 전 목록 확인 (dry-run)
python data/dart_download.py --dry-run

# 실제 다운로드
python data/dart_download.py
```

다운로드에 10~30분 소요됩니다 (API 호출 간격 1.5초, 기업당 복수 보고서 포함).
이미 존재하는 보고서는 자동 스킵됩니다.

### 특정 기업·연도만 다운로드

```bash
python data/dart_download.py --year 2025 --corp 005930   # 삼성전자 2025년
python data/dart_download.py --year 2026 --type 사업보고서  # 사업보고서만
```

## 4단계: Ollama 추출 실행

다운로드된 XML에서 Ollama로 투자 판단 내러티브를 추출합니다.

```bash
# 단일 기업 테스트 (콘솔 출력, DB 저장 없음)
python data/dart_extractor.py --company 삼성전자

# 전체 처리 (DB 저장)
python data/dart_extractor.py --all
```

예상 출력 (단일 기업):

```
[1/3] XML 추출: reports/dart/삼성전자/20260313000662_사업보고서 (2025.12)/20260313000662.xml
      anchor+grep: 18,432자
[2/3] Ollama 호출
[3/3] 결과 (18,432자 컨텍스트)

========================================================================
삼성전자 2025년 사업보고서 분석:
- AI 반도체: HBM3E 12단 양산 시작, 엔비디아 공급 확대 중...
...
========================================================================
```

전체 처리 시 10~60분 소요 (기업당 Ollama 응답 시간 의존).

## 5단계: 마크다운 파일 내보내기

`dart_extractions` DB 내용을 `dart/` 디렉터리에 MD 파일로 저장합니다.

```bash
python scripts/export_dart_md.py
```

`dart/20260529_삼성전자_202512_사업.md` 형태로 저장됩니다.

## 대안: Claude로 테마 직접 추출 (Ollama 없이)

Ollama가 없거나 특정 테마(예: AI·데이터센터·로봇)에 집중한 분석이 필요할 때, 아래 프롬프트를 Claude에 직접 붙여넣어 추출할 수 있습니다.

### 프롬프트 템플릿

```
# 역할
당신은 한국 기업 공시 보고서(DART XML) 분석 전문가입니다.
첨부된 사업보고서/분기보고서에서 지정된 테마에 관한 정량·정성 데이터를 빠짐없이 추출하세요.

# 분석 대상 테마
다음 키워드와 관련된 모든 내용을 탐색하세요:
- AI / 인공지능 / 온디바이스 AI / 생성형 AI
- 데이터센터 / AI 데이터센터 / AIDC / 냉각솔루션 / 칠러 / CDU / 액체냉각
- 로봇 / 서비스로봇 / 산업용로봇 / 홈로봇 / 액추에이터
- 위 키워드와 직접 연관된 사업부, 자회사, 제품명

# 추출 항목 (우선순위 순)

## 1. 정량 데이터 (수치가 있으면 반드시 포함)
- 매출액 및 전년 대비 성장률 (연간 / 분기)
- 영업이익 / 영업손실
- 시장점유율 (M/S)
- 수주잔고 / 수주 목표
- 투자금액 / 인수금액 / 영업권
- 중장기 목표 수치 (예: "2027년까지 매출 X조")
- 성장률 목표 (예: "전년 대비 X배 수주 목표")

## 2. 정성 데이터
- 사업부 전략 방향 및 핵심 경쟁력
- 시장 환경 분석 (성장성, 경기변동, 경쟁 구도)
- 신제품 / 신규 수주 / 파트너십 주요 내역
- 리스크 요인

## 3. 사업 구조
- 해당 사업이 속한 사업부문과 전사 매출 내 비중
- 관련 자회사 및 지분율

# 출력 형식

각 테마별로 다음 구조로 정리하세요:

### [테마명]

**실적 수치**
| 항목 | 당기 | 전기 | 증감 |
|------|------|------|------|
| (해당 항목 기입) | | | |

**전략 및 시장 현황**
- (핵심 내용 bullet로 요약)

**목표/가이던스**
- (중장기 수치 목표 기재)

**주의사항**
- 테마별 전용 매출이 별도 공시되지 않은 경우 반드시 명시
- 수치 출처(사업보고서 어느 부문 설명인지) 병기

# 처리 지침
1. XML 태그를 무시하고 텍스트 내용만 분석하세요
2. 수치가 없는 테마는 "별도 공시 없음"으로 명시하고 관련 정성 내용을 대신 기술하세요
3. 목표치와 실적치를 혼동하지 말고 명확히 구분하세요
4. 동일 수치가 여러 곳에 반복되면 한 번만 기재하세요
5. 불확실한 내용은 추정임을 표시하세요

# 보고서 정보
- 회사명: {회사명}
- 보고서 종류: {사업보고서 / 분기보고서}
- 대상 기간: {기간}

# 입력 데이터
아래는 보고서에서 관련 키워드로 사전 필터링한 텍스트입니다.
이 내용을 기반으로 위 항목을 추출하세요.

[여기에 grep 결과 또는 관련 섹션 텍스트 붙여넣기]
```

### 사용 방법

1. `export_dart_md.py`로 생성된 `dart/{회사명}.md` 파일을 열어 관련 섹션을 복사
2. 프롬프트의 `[여기에 grep 결과...]` 자리에 붙여넣기
3. `{회사명}`, `{사업보고서 / 분기보고서}`, `{기간}` 채우기
4. Claude에 전송

Ollama 파이프라인(`dart_extractor.py`)은 KEYWORDS/ANCHORS 기반으로 자동 추출하고 DB에 저장하는 반면, 이 방법은 테마를 자유롭게 지정해 단발성으로 분석할 때 적합합니다.

---

## 6단계: 공시 이벤트 수집 (스케줄러 대신 수동 실행)

스케줄러 없이 전일 공시를 수동으로 수집하려면:

```bash
python data/dart_sync.py --sync-disclosures
# 또는 특정 기간
python data/dart_sync.py --sync-disclosures --bgn-de 20260101 --end-de 20260531
```

## 7단계: 스케줄러 등록 확인

`run_scheduler.py`를 실행하면 다음 잡이 자동 등록됩니다:

| 잡 ID | 실행 시각 (KST) | 내용 |
|-------|-----------------|------|
| `daily_dart_disclosures` | 평일 09:00 | 전일 Top 20 공시 수집 |
| `monthly_dart_xbrl` | 매월 1일 09:10 | Top 20 XBRL 재무수치 갱신 |
| `annual_dart_extractor` | 연 1회 | 사업보고서 Ollama 추출 |

`DART_API_KEY` 미설정 시 잡은 경고 로그 후 자동 스킵 — 스케줄러 크래시 없음.

## 검증

```bash
# dart_companies 건수 확인
python -c "
import asyncio
from core.db import create_pool
async def check():
    pool = await create_pool()
    async with pool.acquire() as conn:
        n = await conn.fetchval('SELECT COUNT(*) FROM dart_companies')
        print(f'dart_companies: {n}건')
    await pool.close()
asyncio.run(check())
"
```

기대값: 50,000건 이상 (국내 전체 상장·비상장 기업)

## 문제 해결

| 증상 | 원인 | 해결 |
|------|------|------|
| `DART_API_KEY 환경변수가 설정되지 않았습니다` | .env 누락 | `.env`에 `DART_API_KEY=` 추가 |
| `Ollama 서버 미응답` | ollama 미실행 | `ollama serve` 실행 |
| `XML 없음: reports/dart/...` | 3단계 건너뜀 | `dart_download.py` 먼저 실행 |
| `corp_code=...에 해당하는 dart_companies 레코드 없음` | 2단계 건너뜀 | `--seed-companies` 먼저 실행 |

## 관련 문서

- [DART 파이프라인 레퍼런스](reference-dart-pipeline.md)
- [DART 설계 설명](explanation-dart-design.md)
