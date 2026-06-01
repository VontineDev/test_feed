이전 요약에 새 결정사항을 추가한 업데이트 버전입니다.

---

## 📋 OpenDART 세션 재시작 컨텍스트 (v2)

**프로젝트**: `test_feed` master 브랜치 (Kiwoom 실시간 + 뉴스 신호 대시보드)
**목표**: OpenDART 전자공시 데이터를 기존 파이프라인에 추가

---

### ✅ 확정된 결론 (3개 전제 검증 완료)

**전제 1 — 매출 품목 상세**
- DART 사업보고서 매출 품목 내러티브는 비정형 HTML, 완전 자동 파싱 불가
- XBRL(`fnlttSinglAcntAll`)로 수치 가능하나 세그먼트 granularity는 회사마다 편차 큼
- → Ollama로 자동화 가능성 판단 예정 (기존 뉴스 파싱 인프라 활용)

**전제 2 — PER/PBR/ROE → 폐기**
- DART API는 재무제표 원본만 제공, 비율 직접 계산 없음
- 컨센서스 추정치는 FnGuide/Bloomberg 유료 데이터
- pykrx가 이미 PER/PBR 커버 중

**전제 3 — DART 고유 가치**
- (a) 매출 세그먼트 내러티브
- (b) 공시 이벤트 (실적발표, 유상증자, 최대주주변경 등)

---

### ✅ 확정된 설계 방향: C) 하이브리드

| 레이어 | 도구 | 역할 |
|--------|------|------|
| 수치 데이터 | DART XBRL API | 재무수치, 세그먼트 테이블 → DB 직저장 |
| 내러티브 파싱 | Ollama | 사업의 내용 II-2(주요제품/서비스) + II-4(매출현황) 절만 타겟 파싱 |
| 공시 이벤트 | DART 공시검색 API | 실적발표 등 이벤트 감지 |

**저장 구조**: 원본 로컬 저장 → 가공 데이터 DB 저장 → 대시보드 호출
**갱신 주기**: Top 20 기업 월별 갱신 (API 한도 고려)

---

### ✅ 추가 확정 사항 (v2 신규)

**Finding 2 — API 키 관리 방식: A) `.env` + `.env.example` 표준 패턴 확정**
- `DART_API_KEY`를 `.env`에 추가, `.env.example`에 명시
- 기존 `KIWOOM_*`, `SUPABASE_*` 등과 동일 패턴으로 일관성 유지
- 코드에서 `os.environ.get("DART_API_KEY")` 단일 접근

**Finding 3 — `dart_companies` FK 레이스 컨디션: A) INSERT 전 corp_code 자동 시드 확정**
- 문제: 일별 공시 스케줄러(09:00)가 `dart_disclosures`에 쓸 때, `dart_companies`에 없는 신규 기업이면 FK 위반 에러 → 조용한 데이터 손실
- 해결: `dart_disclosures` INSERT 전에 `corp_code` 존재 여부 확인, 없으면 `ON CONFLICT DO NOTHING`으로 자동 시드
- 효과: 다음 월별 갱신 시 5개 테이블 데이터 자동 완성

---

### ⏭️ 다음에 해야 할 것 (세션 중단 시점)

Finding 3 확정 직후 중단됨. 다음 단계:

1. **Top 20 기업 실제 사업보고서 수신 테스트** — DART API로 최근 정기보고서 1건 실제 호출, 응답 사이즈 및 구조 확인
2. **Ollama 파싱 가능성 검증** — 사업의 내용 II절 HTML을 Ollama에 넣어 매출 품목/비중 추출 테스트
3. **DB 스키마 설계** — XBRL 수치 테이블 + 세그먼트 내러티브 테이블 구조 결정
4. **corp_code 자동 시드 코드 구현** — 일별 공시 잡에 Finding 3 해결책 반영