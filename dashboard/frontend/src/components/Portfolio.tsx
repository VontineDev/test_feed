import { useEffect, useState } from 'react'
import { tokens } from '../tokens'

interface PortfolioSummary {
  prsm_dpst_aset_amt: number | null
  tot_pur_amt:        number | null
  tot_evlt_amt:       number | null
  tot_evlt_pl:        number | null
  tot_prft_rt:        number | null
  tot_loan_amt:       number | null
  entr:               number | null
  pymn_alow_amt:      number | null
  ord_alow_amt:       number | null
}

interface Holding {
  stk_cd:        string
  stk_nm:        string
  rmnd_qty:      number | null
  trde_able_qty: number | null
  pur_pric:      number | null
  cur_prc:       number | null
  evlt_amt:      number | null
  pur_amt:       number | null
  evltv_prft:    number | null
  prft_rt:       number | null
  poss_rt:       number | null
  crd_tp_nm:     string
}

interface PortfolioData {
  summary:  PortfolioSummary
  holdings: Holding[]
  cached?:  boolean
  stale?:   boolean
  error?:   string
}

const fmt = (n: number | null | undefined, digits = 0) => {
  if (n == null) return '—'
  return n.toLocaleString('ko-KR', { maximumFractionDigits: digits })
}

const fmtPct = (n: number | null | undefined) => {
  if (n == null) return '—'
  return `${n >= 0 ? '+' : ''}${n.toFixed(2)}%`
}

const profitColor = (n: number | null | undefined) => {
  if (n == null) return tokens.tx.muted
  return n > 0 ? tokens.semantic.up : n < 0 ? tokens.semantic.down : tokens.tx.primary
}

export default function Portfolio() {
  const [data, setData]     = useState<PortfolioData | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError]   = useState<string | null>(null)

  const load = (refresh = false) => {
    setLoading(true)
    setError(null)
    fetch(`/api/portfolio${refresh ? '?refresh=true' : ''}`)
      .then(r => {
        if (!r.ok) return r.json().then(d => { throw new Error(d.detail || r.statusText) })
        return r.json()
      })
      .then(d => { setData(d); setLoading(false) })
      .catch(e => { setError(e.message); setLoading(false) })
  }

  useEffect(() => { load() }, [])

  if (loading && !data) {
    return (
      <div style={s.center}>
        <div style={s.spinner} />
        <span style={{ color: tokens.tx.muted, fontSize: 13 }}>키움 API 조회 중…</span>
      </div>
    )
  }

  if (error && !data) {
    return (
      <div style={s.center}>
        <p style={{ color: tokens.semantic.down, fontSize: 13 }}>{error}</p>
        <button style={s.refreshBtn} onClick={() => load()}>다시 시도</button>
      </div>
    )
  }

  const sum = data?.summary
  const holdings = data?.holdings ?? []

  return (
    <div style={s.root}>
      {/* 헤더 */}
      <div style={s.header}>
        <span style={s.title}>내 투자 자산</span>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          {data?.stale && <span style={s.staleTag}>캐시됨</span>}
          <button style={s.refreshBtn} onClick={() => load(true)} disabled={loading}>
            {loading ? '조회 중…' : '새로고침'}
          </button>
        </div>
      </div>

      {/* 총자산 카드 */}
      <div style={s.heroCard}>
        <div style={s.heroLabel}>추정 예탁자산</div>
        <div style={s.heroValue}>{fmt(sum?.prsm_dpst_aset_amt)}원</div>
        <div style={{ ...s.heroPct, color: profitColor(sum?.tot_evlt_pl) }}>
          {fmtPct(sum?.tot_prft_rt)} ({fmt(sum?.tot_evlt_pl)}원)
        </div>
      </div>

      {/* 요약 그리드 */}
      <div style={s.grid}>
        <StatCard label="총 매입금액"  value={fmt(sum?.tot_pur_amt) + '원'} />
        <StatCard label="총 평가금액"  value={fmt(sum?.tot_evlt_amt) + '원'} />
        <StatCard label="예수금"       value={fmt(sum?.entr) + '원'} />
        <StatCard label="출금가능금액" value={fmt(sum?.pymn_alow_amt) + '원'} />
        <StatCard label="주문가능금액" value={fmt(sum?.ord_alow_amt) + '원'} />
        <StatCard label="총 대출금"    value={fmt(sum?.tot_loan_amt) + '원'} />
      </div>

      {/* 보유 종목 */}
      <div style={s.sectionTitle}>보유 종목 ({holdings.length})</div>

      {holdings.length === 0 ? (
        <div style={{ color: tokens.tx.muted, fontSize: 13, padding: '16px 0' }}>
          보유 종목이 없습니다
        </div>
      ) : (
        <div style={s.tableWrap}>
          <table style={s.table}>
            <thead>
              <tr style={s.thead}>
                <th style={{ ...s.th, textAlign: 'left' }}>종목</th>
                <th style={s.th}>보유</th>
                <th style={s.th}>평균단가</th>
                <th style={s.th}>현재가</th>
                <th style={s.th}>평가금액</th>
                <th style={s.th}>손익</th>
                <th style={s.th}>수익률</th>
                <th style={s.th}>비중</th>
              </tr>
            </thead>
            <tbody>
              {holdings.map((h, i) => (
                <tr key={i} style={i % 2 === 0 ? s.trEven : s.trOdd}>
                  <td style={s.tdLeft}>
                    <div style={s.stkNm}>{h.stk_nm}</div>
                    <div style={s.stkCd}>{h.stk_cd}{h.crd_tp_nm ? ` · ${h.crd_tp_nm}` : ''}</div>
                  </td>
                  <td style={s.td}>{fmt(h.rmnd_qty)}</td>
                  <td style={s.td}>{fmt(h.pur_pric)}</td>
                  <td style={s.td}>{fmt(h.cur_prc)}</td>
                  <td style={s.td}>{fmt(h.evlt_amt)}</td>
                  <td style={{ ...s.td, color: profitColor(h.evltv_prft) }}>{fmt(h.evltv_prft)}</td>
                  <td style={{ ...s.td, color: profitColor(h.prft_rt) }}>{fmtPct(h.prft_rt)}</td>
                  <td style={s.td}>{h.poss_rt != null ? `${h.poss_rt.toFixed(1)}%` : '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {data?.error && (
        <p style={{ color: tokens.semantic.down, fontSize: 12, marginTop: 8 }}>{data.error}</p>
      )}
    </div>
  )
}

function StatCard({ label, value }: { label: string; value: string }) {
  return (
    <div style={s.statCard}>
      <div style={s.statLabel}>{label}</div>
      <div style={s.statValue}>{value}</div>
    </div>
  )
}

const s: Record<string, React.CSSProperties> = {
  root: {
    padding: '16px 20px',
    overflowY: 'auto',
    height: '100%',
    boxSizing: 'border-box',
  },
  center: {
    display: 'flex', flexDirection: 'column', alignItems: 'center',
    justifyContent: 'center', height: '100%', gap: 12,
  },
  spinner: {
    width: 28, height: 28,
    border: `3px solid ${tokens.bd.default}`,
    borderTopColor: tokens.accent.blue,
    borderRadius: '50%',
    animation: 'spin 0.8s linear infinite',
  },
  header: {
    display: 'flex', justifyContent: 'space-between', alignItems: 'center',
    marginBottom: 16,
  },
  title: { fontWeight: 700, fontSize: 16 },
  staleTag: {
    fontSize: 11, color: tokens.tx.muted,
    background: tokens.bg.panel, border: `1px solid ${tokens.bd.default}`,
    borderRadius: 4, padding: '2px 6px',
  },
  refreshBtn: {
    background: 'none', border: `1px solid ${tokens.bd.emphasis}`,
    color: tokens.tx.muted, borderRadius: 5, padding: '5px 12px',
    cursor: 'pointer', fontSize: 12, fontWeight: 600,
  },
  heroCard: {
    background: tokens.bg.panel, border: `1px solid ${tokens.bd.default}`,
    borderRadius: 10, padding: '20px 24px', marginBottom: 16, textAlign: 'center',
  },
  heroLabel: { fontSize: 12, color: tokens.tx.muted, marginBottom: 6 },
  heroValue: { fontSize: 28, fontWeight: 700, letterSpacing: -0.5, marginBottom: 4 },
  heroPct:   { fontSize: 14, fontWeight: 600 },
  grid: {
    display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)',
    gap: 10, marginBottom: 20,
  },
  statCard: {
    background: tokens.bg.panel, border: `1px solid ${tokens.bd.default}`,
    borderRadius: 8, padding: '12px 14px',
  },
  statLabel: { fontSize: 11, color: tokens.tx.muted, marginBottom: 4 },
  statValue: { fontSize: 14, fontWeight: 600 },
  sectionTitle: {
    fontSize: 13, fontWeight: 700, color: tokens.tx.subtle,
    marginBottom: 10, paddingBottom: 6,
    borderBottom: `1px solid ${tokens.bd.default}`,
  },
  tableWrap: { overflowX: 'auto' },
  table: { width: '100%', borderCollapse: 'collapse', fontSize: 12 },
  thead: { background: tokens.bg.panel },
  th: {
    padding: '8px 10px', color: tokens.tx.muted,
    fontWeight: 600, textAlign: 'right' as const,
    borderBottom: `1px solid ${tokens.bd.default}`,
    whiteSpace: 'nowrap',
  },
  trEven: { background: 'transparent' },
  trOdd:  { background: tokens.bg.panel + '60' },
  td: {
    padding: '8px 10px', textAlign: 'right' as const,
    borderBottom: `1px solid ${tokens.bd.default}`,
    whiteSpace: 'nowrap',
  },
  tdLeft: {
    padding: '8px 10px', textAlign: 'left' as const,
    borderBottom: `1px solid ${tokens.bd.default}`,
  },
  stkNm: { fontWeight: 600, fontSize: 13 },
  stkCd: { fontSize: 10, color: tokens.tx.muted, marginTop: 2 },
}
