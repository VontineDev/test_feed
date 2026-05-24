import { useEffect, useState } from 'react';
import { tokens } from '../tokens';

interface IndexData {
  change_pct: number | null;
  close: number | null;
  prev_close: number | null;
}

interface MarketIndexResponse {
  market_status: 'open' | 'closed';
  is_realtime: boolean;
  kospi: IndexData | null;
  kosdaq: IndexData | null;
  sentiment: string;
  sentiment_detail: string;
  as_of: string;
}

const BEGINNER_TIP = '등락률이 +이면 오늘 그 지수가 어제보다 올랐다는 뜻입니다';

function pctColor(pct: number | null): string {
  if (pct === null) return tokens.tx.muted;
  if (pct > 0) return tokens.semantic.up;
  if (pct < 0) return tokens.semantic.down;
  return tokens.semantic.flat;
}

function pctArrow(pct: number | null): string {
  if (pct === null) return '';
  if (pct > 0) return '▲';
  if (pct < 0) return '▼';
  return '–';
}

function sentimentBadgeColor(sentiment: string): string {
  if (sentiment === '강세' || sentiment === '상승') return tokens.semantic.up;
  if (sentiment === '급락' || sentiment === '하락') return tokens.semantic.down;
  return tokens.tx.muted;
}

export default function MarketSummaryBanner() {
  const [data, setData] = useState<MarketIndexResponse | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;

    async function fetchData() {
      try {
        const res = await fetch('/api/market_index');
        if (!res.ok) throw new Error('fetch failed');
        const json: MarketIndexResponse = await res.json();
        if (!cancelled) setData(json);
      } catch {
        // silent fallback — banner hides on error
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    fetchData();
    const interval = setInterval(fetchData, 300_000); // 5분마다 갱신
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, []);

  if (loading) {
    return (
      <div style={styles.wrapper}>
        <span style={{ color: tokens.tx.muted, fontSize: 13 }}>…</span>
      </div>
    );
  }

  if (!data) return null;

  const isClosed = data.market_status === 'closed';

  if (isClosed && !data.kospi && !data.kosdaq) {
    return (
      <div style={styles.wrapper}>
        <span style={{ color: tokens.tx.secondary, fontSize: 13 }}>
          오늘은 장이 없는 날입니다
        </span>
      </div>
    );
  }

  return (
    <div style={styles.wrapper}>
      {/* 헤더 행 */}
      <div style={styles.headerRow}>
        <span style={styles.label}>오늘 시장</span>
        {isClosed && (
          <span style={{ ...styles.badge, color: tokens.tx.muted, borderColor: tokens.bd.default }}>
            장마감
          </span>
        )}
        <span style={{ ...styles.badge, color: sentimentBadgeColor(data.sentiment), borderColor: sentimentBadgeColor(data.sentiment) }}>
          {data.sentiment}
        </span>
      </div>

      {/* 지수 행 */}
      <div style={styles.indexRow}>
        {data.kospi && (
          <IndexBlock label="코스피" data={data.kospi} />
        )}
        {data.kospi && data.kosdaq && (
          <div style={styles.divider} />
        )}
        {data.kosdaq && (
          <IndexBlock label="코스닥" data={data.kosdaq} />
        )}
      </div>

      {/* 한마디 */}
      <div style={styles.detail}>{data.sentiment_detail}</div>

      {/* 초보자 팁 (모바일에서는 CSS로 숨김) */}
      <div style={styles.tip} className="market-banner-tip">
        {BEGINNER_TIP}
      </div>
    </div>
  );
}

function IndexBlock({ label, data }: { label: string; data: IndexData }) {
  const pct = data.change_pct;
  const color = pctColor(pct);
  const arrow = pctArrow(pct);
  const closeStr = data.close != null ? data.close.toLocaleString('ko-KR', { maximumFractionDigits: 2 }) : '–';
  const pctStr = pct != null ? `${arrow} ${Math.abs(pct).toFixed(2)}%` : '–';

  return (
    <div style={styles.indexBlock}>
      <span style={styles.indexLabel}>{label}</span>
      <span style={styles.indexClose}>{closeStr}</span>
      <span style={{ ...styles.indexPct, color }}>{pctStr}</span>
    </div>
  );
}

const styles: Record<string, React.CSSProperties> = {
  wrapper: {
    background: tokens.bg.panel,
    borderBottom: `1px solid ${tokens.bd.default}`,
    padding: '10px 16px',
    display: 'flex',
    flexDirection: 'column',
    gap: 4,
  },
  headerRow: {
    display: 'flex',
    alignItems: 'center',
    gap: 8,
  },
  label: {
    color: tokens.tx.secondary,
    fontSize: 12,
    fontWeight: 600,
    letterSpacing: '0.04em',
    textTransform: 'uppercase',
  },
  badge: {
    fontSize: 11,
    fontWeight: 600,
    border: '1px solid',
    borderRadius: 4,
    padding: '1px 6px',
  },
  indexRow: {
    display: 'flex',
    alignItems: 'center',
    gap: 16,
    flexWrap: 'wrap',
  },
  indexBlock: {
    display: 'flex',
    alignItems: 'baseline',
    gap: 6,
  },
  indexLabel: {
    color: tokens.tx.muted,
    fontSize: 12,
  },
  indexClose: {
    color: tokens.tx.primary,
    fontSize: 15,
    fontWeight: 600,
    fontVariantNumeric: 'tabular-nums',
  },
  indexPct: {
    fontSize: 13,
    fontWeight: 500,
    fontVariantNumeric: 'tabular-nums',
  },
  divider: {
    width: 1,
    height: 16,
    background: tokens.bd.default,
  },
  detail: {
    color: tokens.tx.secondary,
    fontSize: 12,
  },
  tip: {
    color: tokens.tx.subtle,
    fontSize: 11,
  },
};
