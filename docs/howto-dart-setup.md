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
