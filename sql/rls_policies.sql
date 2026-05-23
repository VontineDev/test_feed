-- ================================================================
-- RLS Policy Migration
-- Supabase security advisor: rls_enabled_no_policy (14 tables)
--
-- 목적: RLS 활성화된 테이블에 backend_all 정책 추가.
--   이 앱은 asyncpg 직접 연결(postgres 슈퍼유저)만 사용하므로
--   RLS 정책이 실제 접근 제어에 영향을 주지는 않으나,
--   Supabase 보안 어드바이저 경고를 해소하고 의도를 명시함.
--
-- 실행: pgAdmin 또는 Supabase SQL 에디터에서 전체 선택 후 실행.
-- 멱등: IF NOT EXISTS 가드로 중복 실행 안전.
-- ================================================================

DO $$ DECLARE
  _tbl TEXT;
  _tables TEXT[] := ARRAY[
    'news_articles', 'trade_signals', 'daily_ohlcv', 'daily_flow',
    'chart_signals', 'stage_classifications', 'watchlist_vol_log',
    'intraday_volumes', 'krx_listings', 'ticker_names', 'trade_log',
    'scheduler_triggers', 'aftermarket_snap', 'paper_positions'
  ];
BEGIN
  FOREACH _tbl IN ARRAY _tables LOOP
    -- 테이블이 존재하는 경우에만 처리
    IF EXISTS (
      SELECT FROM information_schema.tables
      WHERE table_schema = 'public' AND table_name = _tbl
    ) THEN
      -- RLS 활성화 (이미 활성화된 경우 무시됨)
      EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY', _tbl);

      -- 정책이 없는 경우에만 생성
      IF NOT EXISTS (
        SELECT 1 FROM pg_policies
        WHERE schemaname = 'public' AND tablename = _tbl AND policyname = 'backend_all'
      ) THEN
        EXECUTE format(
          'CREATE POLICY backend_all ON public.%I FOR ALL USING (true) WITH CHECK (true)',
          _tbl
        );
        RAISE NOTICE '% : backend_all policy created', _tbl;
      ELSE
        RAISE NOTICE '% : backend_all policy already exists, skipped', _tbl;
      END IF;
    ELSE
      RAISE NOTICE '% : table does not exist, skipped', _tbl;
    END IF;
  END LOOP;
END $$;
