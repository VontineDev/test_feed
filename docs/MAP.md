# Project Documentation Map (Context Navigator)

> **AI 가이드**: 본 문서는 이 프로젝트의 모든 개발 문서 구조와 역할을 정리한 지도입니다.
> 사용자가 특정 기능 개발이나 수정을 요청하면, 먼저 이 지도를 읽고 **'어떤 가이드(01_howto)'와 '어떤 참조 데이터(02_reference)'를 추가로 읽어야 하는지** 판단하여 사용자에게 해당 문서의 내용을 요청하세요.

---

## 00_meta (Core System & Rules)
프로젝트의 뼈대, 아키텍처, 코딩 규칙을 정의합니다. 코드를 짜기 전 항상 기본 컨텍스트로 유지해야 합니다.
- `ARCHITECTURE.md`: 시스템 아키텍처 및 전체 기술 스택 규정
- `DESIGN.md`: 핵심 디자인 패턴 및 컴포넌트 설계 방식
- `TODOS.md`: 현재 개발 진행 상황 및 백로그 관리
- `refactoring-roadmap.md`: 구조 리팩토링 완료 기록(Phase A~C, 대시보드 라우터, core/db.py 분리) 및 향후 로드맵(Phase D~G)·방법론 원칙
- `USER_MANUAL.md`: 프로그램 실행 및 조작법
- `name-resolution.md`: 종목명 DB 조회 우선순위 및 SQL COALESCE 패턴 표준 (ticker_names → krx_listings → chart_signals → youtube_mention_raw 폴백 체인)
- `차트분석가이드.md`: 차트 분석 개념 교육 자료 — 캔들·이평선·구름대 직관 가이드 (코드 스크리닝 조건이 아닌 배경 지식용)
- `stage-screening-framework.md`: 퀀트+기술+수급 3단계(랠리초입/재매집/과열) 스크리닝 조건식 및 매수매도 타이밍 전략

## 01_howto (Development Guides)
새로운 코드를 구현하거나 기능을 확장할 때 반드시 준수해야 하는 작업 절차서입니다.
- `dart-pipeline.md`: DART 공시 데이터 파이프라인 구축 및 확장 절차
- `Dashboard.md`: UI/대시보드 컴포넌트 추가 및 차트 시각화 구현 가이드
- `howto-backtest.md`: 백테스트 엔진 구동 및 신규 전략 테스트 방법
- `HTTPS-Setup.md`: API 서버 보안 및 HTTPS 인증서 설정 절차
- `howto-dart-setup.md`: DART API 연동 및 환경 설정 가이드
- `howto-kiwoom-paper-trade.md`: 키움증권 모의투자(Paper Trading) 연동 가이드
- `howto-krx-flow-import.md`: KRX 거래대금/자금 흐름 데이터 임포트 절차
- `howto-screener.md`: 퀀트 스크리너 조건식 및 필터링 기능 구현 가이드
- `howto-stage-classifier.md`: 주가 국면 분류(Stage Classification) 알고리즘 구현 가이드
- `howto-watchlist.md`: 관심종목(Watchlist) 관리 및 알림 로직 구현 가이드
- `howto-youtube-backfill.md`: 과거 유튜브 영상 데이터 백필(Backfill) 작업 가이드
- `howto-youtube-run-backtest.md`: 유튜브 내러티브 기반 백테스트 실행 가이드
- `howto-quant-backtest.md`: TechnicalQuant.md(펀더멘털+기술적 지표) 퀀트 전략 백테스트 실행 및 필터 스윕 가이드

## 02_reference (Specs & Timetables)
정적인 스펙 데이터 및 외부 API 연동 명세입니다. 기능 구현 시 값을 참조할 때만 호출합니다.
- `krx openapi specs/`: [폴더] KRX OpenAPI 공식 데이터 스펙 모음
- `plan-youtube-backfill.md`: 유튜브 데이터 수집 범위 및 스케줄링 계획서
- `reference-dart-pipeline.md`: DART API 엔드포인트 및 응답 데이터 구조 참조
- `reference-env-vars.md`: 프로젝트 전체 환경 변수(`.env`) 정의 및 설명
- `reference-kiwoom.md`: 키움 Open API+ 핵심 메서드 및 에러 코드 참조
- `reference-krx-pipeline.md`: KRX 수집 파이프라인 데이터 모델 명세
- `reference-scheduler.md`: 배치 작업 및 크론탭(Crontab) 스케줄러 설정값
- `reference-telegram-commands.md`: 텔레그램 봇 커맨드 및 메시지 포맷 명세
- `reference-youtube-backfill-monthly.md`: 월간 유튜브 데이터 백필 주기 및 기준 데이터
- `reference-youtube-narrative.md`: 유튜브 키워드/내러티브 매핑 테이블 스펙
- `증권거래소타임테이블.md`: 국내 주식 시장 운영 시간 및 정규/시간외 데이터 수집 타임라인
- `키움 REST API 문서.pdf`: 키움증권 REST API 명세 (필요시 텍스트 추출 참조)

## 03_explanation (Domain Knowledge)
프로젝트의 핵심 비즈니스 로직과 수학적/금융적 배경지식을 설명하는 개념서입니다.
- `explanation-dart-design.md`: 공시 데이터를 활용한 퀀트 요인 분석 설계 개념
- `explanation-paper-trading.md`: 모의투자 체결 엔진의 싱크 및 슬리피지 반영 로직 설명
- `explanation-signal-pipeline.md`: 매매 시그널(수급+기술+퀀트) 생성 파이프라인 개념도
- `explanation-youtube-narrative-design.md`: 유튜브 텍스트 분석을 통한 시장 내러티브 추출 로직
- `tutorial-youtube-narrative-quickstart.md`: 유튜브 내러티브 분석 엔진 신속 시작 가이드
- `삼프로TV_자막샘플.md`: 내러티브 모델 학습 및 테스트용 텍스트 원본 샘플

## 04_assets (Generated Charts & Data)
분석 스크립트가 생성한 이미지·CSV 등 보조 자산을 보관하는 폴더입니다. 문서 본문에서 직접 참조(임베드)할 때만 채워 넣고, 참조가 끊긴 산출물은 정기적으로 정리합니다.
- *(현재 비어 있음 — 2026-06-21 기준 미참조 IC 분석 산출물 삭제. 필요 시 `scripts/youtube_ic_analysis.py` 등으로 재생성 후 본문에서 링크 추가)*

---

## Claude 가동용 워크플로우 (AI Workflow)
1. **작업 인식**: 사용자가 "키움증권 모의투자에서 주문 에러가 나"라고 하면, 이 지도에서 `01_howto/howto-kiwoom-paper-trade.md`와 `02_reference/reference-kiwoom.md`가 필요함을 인지합니다.
2. **컨텍스트 요청**: 사용자에게 "해당 작업을 위해 `howto-kiwoom-paper-trade.md` 파일의 내용을 프롬프트에 붙여넣어 주세요"라고 먼저 요청하세요.
3. **코드 생성**: 필요한 문서의 맥락이 모두 채워지면, 규칙을 준수하여 코드를 작성합니다.
