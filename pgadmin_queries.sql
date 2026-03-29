-- ================================================================
-- 뉴스 크롤러 통계 쿼리 모음
-- pgAdmin 4 → Tools → Query Tool 에서 실행
-- 각 쿼리는 독립적으로 실행 가능 (블록 선택 후 F5)
-- ================================================================


-- ================================================================
-- 1. 전체 현황 요약 (대시보드 첫 화면용)
-- ================================================================
SELECT
    (SELECT COUNT(*)                FROM news_articles)                  AS 총_기사수,
    (SELECT COUNT(*)                FROM news_articles
     WHERE  fetched_at >= NOW() - INTERVAL '24 hours')                  AS 최근24h_수집,
    (SELECT COUNT(*)                FROM news_articles
     WHERE  summary_ko IS NOT NULL
       AND  summary_ko <> '')                                            AS 요약_완료,
    (SELECT COUNT(*)                FROM news_articles
     WHERE  summary_ko IS NULL
        OR  summary_ko = '')                                             AS 요약_실패,
    (SELECT COUNT(*)                FROM trade_signals)                  AS 총_신호수,
    (SELECT COUNT(*)                FROM trade_signals
     WHERE  detected_at >= NOW() - INTERVAL '24 hours')                 AS 최근24h_신호;


-- ================================================================
-- 2. 소스 · 카테고리별 수집 통계
-- ================================================================
SELECT
    source                                      AS 소스,
    category                                    AS 카테고리,
    COUNT(*)                                    AS 기사수,
    COUNT(*) FILTER (WHERE summary_ko IS NOT NULL
                       AND summary_ko <> '')    AS 요약완료,
    ROUND(
        COUNT(*) FILTER (WHERE summary_ko IS NOT NULL
                           AND summary_ko <> '')
        * 100.0 / COUNT(*), 1
    )                                           AS 요약률_pct,
    MAX(fetched_at)                             AS 마지막_수집
FROM  news_articles
GROUP BY source, category
ORDER BY source, category;


-- ================================================================
-- 3. 시간대별 수집량 (최근 7일 기준, UTC+9 한국시간)
-- ================================================================
SELECT
    EXTRACT(HOUR FROM fetched_at AT TIME ZONE 'Asia/Seoul')::INT   AS 한국시간_시,
    COUNT(*)                                                        AS 기사수,
    ROUND(COUNT(*) * 100.0 /
          SUM(COUNT(*)) OVER (), 1)                                AS 비율_pct,
    RPAD('█', (COUNT(*) * 30 /
               MAX(COUNT(*)) OVER ())::INT, '█')                   AS 막대그래프
FROM  news_articles
WHERE fetched_at >= NOW() - INTERVAL '7 days'
GROUP BY 1
ORDER BY 1;


-- ================================================================
-- 4. 일자별 수집 추이 (최근 14일)
-- ================================================================
SELECT
    DATE(fetched_at AT TIME ZONE 'Asia/Seoul')  AS 날짜,
    COUNT(*)                                    AS 총_기사수,
    COUNT(*) FILTER (WHERE source = 'reuters')  AS reuters,
    COUNT(*) FILTER (WHERE source = 'investing') AS investing,
    COUNT(*) FILTER (WHERE source = 'cnbc')     AS cnbc
FROM  news_articles
WHERE fetched_at >= NOW() - INTERVAL '14 days'
GROUP BY 1
ORDER BY 1 DESC;


-- ================================================================
-- 5. 매매 신호 통계 (BUY / SELL / WATCH 비율)
-- ================================================================
SELECT
    direction                                   AS 방향,
    COUNT(*)                                    AS 신호수,
    ROUND(COUNT(*) * 100.0 /
          SUM(COUNT(*)) OVER (), 1)             AS 비율_pct,
    ROUND(AVG(strength), 2)                     AS 평균강도,
    MAX(strength)                               AS 최대강도,
    COUNT(*) FILTER (WHERE strength >= 4)       AS 강신호_4이상
FROM  trade_signals
GROUP BY direction
ORDER BY 신호수 DESC;


-- ================================================================
-- 6. 신호 강도 분포
-- ================================================================
SELECT
    strength                                    AS 강도,
    COUNT(*)                                    AS 건수,
    ROUND(COUNT(*) * 100.0 /
          SUM(COUNT(*)) OVER (), 1)             AS 비율_pct
FROM  trade_signals
GROUP BY strength
ORDER BY strength DESC;


-- ================================================================
-- 7. 요약 실패 수 / 비율
-- ================================================================
SELECT
    source                                      AS 소스,
    COUNT(*)                                    AS 전체,
    COUNT(*) FILTER (WHERE summary_ko IS NULL
                       OR  summary_ko = '')     AS 실패,
    COUNT(*) FILTER (WHERE summary_ko IS NOT NULL
                       AND summary_ko <> '')    AS 성공,
    ROUND(
        COUNT(*) FILTER (WHERE summary_ko IS NULL
                           OR  summary_ko = '')
        * 100.0 / COUNT(*), 1
    )                                           AS 실패율_pct
FROM  news_articles
GROUP BY source
ORDER BY 실패율_pct DESC;


-- ================================================================
-- 8. LLM 백엔드별 통계 (Ollama vs LM Studio vs 실패)
-- ================================================================

-- 8-1. 요약 백엔드
SELECT
    COALESCE(llm_backend, 'unknown')            AS 백엔드,
    COUNT(*)                                    AS 건수,
    ROUND(COUNT(*) * 100.0 /
          SUM(COUNT(*)) OVER (), 1)             AS 비율_pct
FROM  news_articles
GROUP BY llm_backend
ORDER BY 건수 DESC;

-- 8-2. 신호 감지 백엔드
SELECT
    COALESCE(llm_backend, 'unknown')            AS 백엔드,
    COUNT(*)                                    AS 건수,
    ROUND(AVG(strength), 2)                     AS 평균_신호강도
FROM  trade_signals
GROUP BY llm_backend
ORDER BY 건수 DESC;


-- ================================================================
-- 9. 최근 감지된 신호 목록 (상위 30건)
-- ================================================================
SELECT
    s.detected_at AT TIME ZONE 'Asia/Seoul'     AS 감지시각_KST,
    s.direction                                 AS 방향,
    s.strength                                  AS 강도,
    a.source                                    AS 소스,
    a.category                                  AS 카테고리,
    LEFT(a.title_en, 60)                        AS 제목,
    LEFT(s.reason, 80)                          AS 판단근거,
    s.tickers                                   AS 관련종목
FROM  trade_signals  s
JOIN  news_articles  a ON a.id = s.article_id
ORDER BY s.detected_at DESC
LIMIT 30;


-- ================================================================
-- 10. 강신호 집중 분석 (strength 4~5만)
-- ================================================================
SELECT
    s.detected_at AT TIME ZONE 'Asia/Seoul'     AS 감지시각_KST,
    s.direction                                 AS 방향,
    s.strength                                  AS 강도,
    a.source                                    AS 소스,
    LEFT(a.title_en, 70)                        AS 제목,
    LEFT(a.summary_ko, 100)                     AS 한글요약,
    s.reason                                    AS 판단근거,
    s.tickers                                   AS 관련종목,
    a.url                                       AS 원문링크
FROM  trade_signals  s
JOIN  news_articles  a ON a.id = s.article_id
WHERE s.strength >= 4
ORDER BY s.detected_at DESC
LIMIT 20;


-- ================================================================
-- 11. 종목별 신호 집계 (자주 등장하는 종목)
-- ================================================================
SELECT
    ticker                                      AS 종목_지수,
    COUNT(*)                                    AS 등장횟수,
    COUNT(*) FILTER (WHERE s.direction = 'BUY')  AS BUY,
    COUNT(*) FILTER (WHERE s.direction = 'SELL') AS SELL,
    COUNT(*) FILTER (WHERE s.direction = 'WATCH') AS WATCH,
    ROUND(AVG(s.strength), 2)                   AS 평균강도
FROM  trade_signals s,
      UNNEST(s.tickers) AS ticker
GROUP BY ticker
ORDER BY 등장횟수 DESC
LIMIT 20;


-- ================================================================
-- 12. 오늘 실시간 수집 현황 (한국시간 기준 오늘)
-- ================================================================
SELECT
    TO_CHAR(fetched_at AT TIME ZONE 'Asia/Seoul', 'HH24:MI') AS 수집시각,
    source                                      AS 소스,
    category                                    AS 카테고리,
    LEFT(title_en, 70)                          AS 제목,
    CASE WHEN summary_ko IS NOT NULL
          AND summary_ko <> ''
         THEN '✓' ELSE '✗' END                 AS 요약
FROM  news_articles
WHERE DATE(fetched_at AT TIME ZONE 'Asia/Seoul') = CURRENT_DATE
ORDER BY fetched_at DESC
LIMIT 50;
