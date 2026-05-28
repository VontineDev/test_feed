import { useEffect, useRef, useState } from 'react'
import { tokens } from '../tokens'
import { useRole } from '../hooks/useRole'

// ── Types ─────────────────────────────────────────────────────

interface PortfolioSummary {
  tot_pur_amt:  number | null
  tot_evlt_amt: number | null
  tot_evlt_pl:  number | null
  tot_prft_rt:  number | null
}

interface Holding {
  id:             number
  stk_cd:         string
  stk_nm:         string
  market:         'KR' | 'US'
  avg_price:      number
  qty:            number
  cur_prc:        number | null
  pur_amt:        number | null
  evlt_amt:       number | null
  evltv_prft:     number | null
  pur_amt_krw:    number | null
  evlt_amt_krw:   number | null
  evltv_prft_krw: number | null
  prft_rt:        number | null
  poss_rt:        number | null
}

interface PortfolioData {
  summary:  PortfolioSummary
  holdings: Holding[]
  usd_krw:  number
}

interface FormState {
  stk_cd:    string
  stk_nm:    string
  avg_price: string
  qty:       string
}

type CurrencyMode = 'KRW' | 'USD'

// ── Formatters ────────────────────────────────────────────────

const EMPTY_FORM: FormState = { stk_cd: '', stk_nm: '', avg_price: '', qty: '' }

const fmt = (n: number | null | undefined, digits = 0) =>
  n == null ? '—' : n.toLocaleString('ko-KR', { maximumFractionDigits: digits })

const fmtUsd = (n: number | null | undefined) =>
  n == null ? '—' : `$${n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`

const fmtPct = (n: number | null | undefined) =>
  n == null ? '—' : `${n >= 0 ? '+' : ''}${n.toFixed(2)}%`

const profitColor = (n: number | null | undefined) =>
  n == null ? tokens.tx.muted : n > 0 ? tokens.semantic.up : n < 0 ? tokens.semantic.down : tokens.tx.primary

// ── HoldingsTable ─────────────────────────────────────────────

interface TableProps {
  holdings:     Holding[]
  isAdmin:      boolean
  market:       'KR' | 'US'
  currencyMode: CurrencyMode
  usdKrw:       number
  onEdit:       (h: Holding) => void
  onDelete:     (id: number, name: string) => void
}

function HoldingsTable({ holdings, isAdmin, market, currencyMode, usdKrw, onEdit, onDelete }: TableProps) {
  const isUS    = market === 'US'
  const showUSD = isUS && currencyMode === 'USD'
  const sfx     = isUS ? (showUSD ? '($)' : '(원)') : ''

  const fmtPrice = (native: number | null) => {
    if (!isUS) return fmt(native)
    return showUSD ? fmtUsd(native) : (native == null ? '—' : fmt(Math.round(native * usdKrw)))
  }

  const fmtAmt = (native: number | null, krw: number | null) => {
    if (!isUS) return fmt(krw)
    return showUSD ? fmtUsd(native) : fmt(krw)
  }

  return (
    <div style={s.tableWrap}>
      <table style={s.table}>
        <thead>
          <tr style={s.thead}>
            <th style={{ ...s.th, textAlign: 'left' }}>종목</th>
            <th style={s.th}>보유</th>
            <th style={s.th}>{`평균단가${sfx}`}</th>
            <th style={s.th}>{`현재가${sfx}`}</th>
            <th style={s.th}>{`평가금액${sfx}`}</th>
            <th style={s.th}>{`손익${sfx}`}</th>
            <th style={s.th}>수익률</th>
            <th style={s.th}>비중</th>
            {isAdmin && <th style={s.th}>관리</th>}
          </tr>
        </thead>
        <tbody>
          {holdings.map((h, i) => (
            <tr key={h.id} style={i % 2 === 0 ? s.trEven : s.trOdd}>
              <td style={s.tdLeft}>
                <div style={s.stkNm}>{h.stk_nm}</div>
                <div style={s.stkCd}>{h.stk_cd}</div>
              </td>
              <td style={s.td}>{fmt(h.qty, 6)}</td>
              <td style={s.td}>{fmtPrice(h.avg_price)}</td>
              <td style={s.td}>
                {h.cur_prc != null
                  ? fmtPrice(h.cur_prc)
                  : <span style={{ color: tokens.tx.muted }}>—</span>}
              </td>
              <td style={s.td}>{fmtAmt(h.evlt_amt, h.evlt_amt_krw)}</td>
              <td style={{ ...s.td, color: profitColor(h.prft_rt) }}>
                {fmtAmt(h.evltv_prft, h.evltv_prft_krw)}
              </td>
              <td style={{ ...s.td, color: profitColor(h.prft_rt) }}>{fmtPct(h.prft_rt)}</td>
              <td style={s.td}>{h.poss_rt != null ? `${h.poss_rt.toFixed(1)}%` : '—'}</td>
              {isAdmin && (
                <td style={s.td}>
                  <div style={{ display: 'flex', gap: 4, justifyContent: 'flex-end' }}>
                    <button style={s.iconBtn} onClick={() => onEdit(h)}>수정</button>
                    <button style={{ ...s.iconBtn, color: tokens.semantic.down }} onClick={() => onDelete(h.id, h.stk_nm)}>삭제</button>
                  </div>
                </td>
              )}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// ── Main Component ────────────────────────────────────────────

export default function Portfolio() {
  const role    = useRole()
  const isAdmin = role === 'admin'

  const [data, setData]       = useState<PortfolioData | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError]     = useState<string | null>(null)

  const [showForm, setShowForm]     = useState(false)
  const [editingId, setEditingId]   = useState<number | null>(null)
  const [form, setForm]             = useState<FormState>(EMPTY_FORM)
  const [submitting, setSubmitting] = useState(false)
  const [formError, setFormError]   = useState<string | null>(null)
  const [lookingUp, setLookingUp]   = useState(false)
  const prevStkCd                   = useRef('')
  const [currencyMode, setCurrencyMode] = useState<CurrencyMode>('KRW')

  const load = () => {
    setLoading(true)
    setError(null)
    fetch('/api/portfolio')
      .then(r => {
        if (!r.ok) return r.json().then(d => { throw new Error(d.detail || r.statusText) })
        return r.json()
      })
      .then(d => { setData(d); setLoading(false) })
      .catch(e => { setError(e.message); setLoading(false) })
  }

  useEffect(() => { load() }, [])

  const openAdd = () => {
    setEditingId(null)
    setForm(EMPTY_FORM)
    setFormError(null)
    prevStkCd.current = ''
    setShowForm(true)
  }

  const lookupTicker = async (code: string) => {
    const trimmed = code.trim().toUpperCase()
    if (!trimmed || trimmed === prevStkCd.current) return
    prevStkCd.current = trimmed
    setLookingUp(true)
    try {
      const r = await fetch(`/api/ticker/lookup?q=${encodeURIComponent(trimmed)}`)
      if (r.ok) {
        const d = await r.json()
        setForm(f => ({ ...f, stk_nm: f.stk_nm.trim() ? f.stk_nm : d.name }))
      }
    } catch (_) {}
    setLookingUp(false)
  }

  const openEdit = (h: Holding) => {
    setEditingId(h.id)
    setForm({ stk_cd: h.stk_cd, stk_nm: h.stk_nm, avg_price: String(h.avg_price), qty: String(h.qty) })
    setFormError(null)
    prevStkCd.current = h.stk_cd.toUpperCase()
    setShowForm(true)
  }

  const handleSubmit = async () => {
    const avg = parseFloat(form.avg_price)
    const qty = parseFloat(form.qty)
    if (!form.stk_cd.trim() || !form.stk_nm.trim()) return setFormError('종목코드와 종목명을 입력하세요')
    if (!avg || avg <= 0 || !qty || qty <= 0)        return setFormError('평균단가·수량은 양수여야 합니다')

    setSubmitting(true)
    setFormError(null)
    const body   = { ticker: form.stk_cd.trim(), name: form.stk_nm.trim(), avg_price: avg, qty }
    const url    = editingId != null ? `/api/portfolio/holdings/${editingId}` : '/api/portfolio/holdings'
    const method = editingId != null ? 'PUT' : 'POST'
    try {
      const r = await fetch(url, { method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
      if (!r.ok) { const d = await r.json(); throw new Error(d.detail || r.statusText) }
      setShowForm(false)
      load()
    } catch (e: unknown) {
      setFormError(e instanceof Error ? e.message : String(e))
    } finally {
      setSubmitting(false)
    }
  }

  const handleDelete = async (id: number, name: string) => {
    if (!confirm(`"${name}" 을(를) 삭제할까요?`)) return
    try {
      const r = await fetch(`/api/portfolio/holdings/${id}`, { method: 'DELETE' })
      if (!r.ok && r.status !== 204) { const d = await r.json(); throw new Error(d.detail || r.statusText) }
      load()
    } catch (e: unknown) {
      alert(e instanceof Error ? e.message : String(e))
    }
  }

  if (loading && !data) {
    return (
      <div style={s.center}>
        <div style={s.spinner} />
        <span style={{ color: tokens.tx.muted, fontSize: 13 }}>포트폴리오 조회 중…</span>
      </div>
    )
  }

  if (error && !data) {
    return (
      <div style={s.center}>
        <p style={{ color: tokens.semantic.down, fontSize: 13 }}>{error}</p>
        <button style={s.btn} onClick={load}>다시 시도</button>
      </div>
    )
  }

  const sum        = data?.summary
  const holdings   = data?.holdings ?? []
  const usdKrw     = data?.usd_krw ?? 1350
  const krHoldings = holdings.filter(h => h.market === 'KR')
  const usHoldings = holdings.filter(h => h.market === 'US')

  return (
    <div style={s.root}>

      {/* 헤더 */}
      <div style={s.header}>
        <span style={s.title}>내 투자 자산</span>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' as const }}>
          {usHoldings.length > 0 && (
            <span style={s.rateTag}>1$ = {fmt(Math.round(usdKrw))}원</span>
          )}
          {isAdmin && <button style={s.addBtn} onClick={openAdd}>+ 종목 추가</button>}
          <button style={s.btn} onClick={load} disabled={loading}>
            {loading ? '조회 중…' : '새로고침'}
          </button>
        </div>
      </div>

      {/* 관리자 입력 폼 */}
      {isAdmin && showForm && (
        <div style={s.formCard}>
          <div style={s.formTitle}>{editingId != null ? '종목 수정' : '종목 추가'}</div>
          <div style={s.formRow}>
            <label style={s.label}>종목코드</label>
            <input
              style={s.input}
              placeholder="005930 또는 AAPL"
              value={form.stk_cd}
              onChange={e => {
                setForm(f => ({ ...f, stk_cd: e.target.value, stk_nm: '' }))
                prevStkCd.current = ''
              }}
              onBlur={e => lookupTicker(e.target.value)}
            />
            <label style={s.label}>종목명</label>
            <div style={{ flex: 1, minWidth: 100, position: 'relative' }}>
              <input
                style={{ ...s.input, width: '100%', boxSizing: 'border-box' as const, opacity: lookingUp ? 0.5 : 1 }}
                placeholder={lookingUp ? '조회 중…' : '삼성전자 / Apple Inc.'}
                value={form.stk_nm}
                onChange={e => setForm(f => ({ ...f, stk_nm: e.target.value }))}
                disabled={lookingUp}
              />
            </div>
          </div>
          <div style={s.formRow}>
            <label style={s.label}>평균단가</label>
            <input
              style={s.input}
              type="number"
              placeholder="70000 (원) 또는 185.5 ($)"
              value={form.avg_price}
              onChange={e => setForm(f => ({ ...f, avg_price: e.target.value }))}
            />
            <label style={s.label}>보유수량</label>
            <input
              style={s.input}
              type="number"
              step="any"
              placeholder="100 또는 0.5"
              value={form.qty}
              onChange={e => setForm(f => ({ ...f, qty: e.target.value }))}
            />
          </div>
          {formError && <div style={s.formErr}>{formError}</div>}
          <div style={{ display: 'flex', gap: 8, marginTop: 10 }}>
            <button style={s.submitBtn} onClick={handleSubmit} disabled={submitting}>
              {submitting ? '저장 중…' : '저장'}
            </button>
            <button style={s.btn} onClick={() => setShowForm(false)}>취소</button>
          </div>
        </div>
      )}

      {/* 총자산 카드 (항상 원화 기준) */}
      {holdings.length > 0 && (
        <>
          <div style={s.heroCard}>
            <div style={s.heroLabel}>총 평가금액 (원화 환산)</div>
            <div style={s.heroValue}>{fmt(sum?.tot_evlt_amt)}원</div>
            <div style={{ ...s.heroPct, color: profitColor(sum?.tot_evlt_pl) }}>
              {fmtPct(sum?.tot_prft_rt)} ({fmt(sum?.tot_evlt_pl)}원)
            </div>
          </div>
          <div style={s.grid}>
            <StatCard label="총 매입금액" value={fmt(sum?.tot_pur_amt) + '원'} />
            <StatCard label="총 평가금액" value={fmt(sum?.tot_evlt_amt) + '원'} />
            <StatCard label="총 손익"     value={fmt(sum?.tot_evlt_pl) + '원'} color={profitColor(sum?.tot_evlt_pl)} />
          </div>
        </>
      )}

      {/* 국내 주식 섹션 */}
      {(krHoldings.length > 0 || holdings.length === 0) && (
        <div style={s.sectionRow}>
          <span style={s.sectionTitle}>국내 주식 ({krHoldings.length})</span>
        </div>
      )}
      {holdings.length === 0 ? (
        <div style={{ color: tokens.tx.muted, fontSize: 13, padding: '8px 0 16px' }}>
          {isAdmin ? '+ 종목 추가 버튼으로 보유 종목을 입력하세요' : '보유 종목이 없습니다'}
        </div>
      ) : krHoldings.length > 0 ? (
        <HoldingsTable
          holdings={krHoldings}
          isAdmin={isAdmin}
          market="KR"
          currencyMode={currencyMode}
          usdKrw={usdKrw}
          onEdit={openEdit}
          onDelete={handleDelete}
        />
      ) : null}

      {/* 해외 주식 섹션 */}
      {usHoldings.length > 0 && (
        <>
          <div style={{ ...s.sectionRow, marginTop: 24 }}>
            <span style={s.sectionTitle}>해외 주식 ({usHoldings.length})</span>
            <div style={{ display: 'flex', gap: 3 }}>
              <button
                style={{ ...s.currBtn, ...(currencyMode === 'KRW' ? s.currBtnOn : {}) }}
                onClick={() => setCurrencyMode('KRW')}
              >원</button>
              <button
                style={{ ...s.currBtn, ...(currencyMode === 'USD' ? s.currBtnOn : {}) }}
                onClick={() => setCurrencyMode('USD')}
              >$</button>
            </div>
          </div>
          <HoldingsTable
            holdings={usHoldings}
            isAdmin={isAdmin}
            market="US"
            currencyMode={currencyMode}
            usdKrw={usdKrw}
            onEdit={openEdit}
            onDelete={handleDelete}
          />
        </>
      )}

    </div>
  )
}

// ── StatCard ──────────────────────────────────────────────────

function StatCard({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div style={s.statCard}>
      <div style={s.statLabel}>{label}</div>
      <div style={{ ...s.statValue, ...(color ? { color } : {}) }}>{value}</div>
    </div>
  )
}

// ── Styles ────────────────────────────────────────────────────

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
    marginBottom: 16, flexWrap: 'wrap', gap: 8,
  },
  title: { fontWeight: 700, fontSize: 16 },
  rateTag: {
    fontSize: 11, color: tokens.tx.muted,
    background: tokens.bg.panel, border: `1px solid ${tokens.bd.default}`,
    borderRadius: 4, padding: '3px 8px', whiteSpace: 'nowrap',
  },
  btn: {
    background: 'none', border: `1px solid ${tokens.bd.emphasis}`,
    color: tokens.tx.muted, borderRadius: 5, padding: '5px 12px',
    cursor: 'pointer', fontSize: 12, fontWeight: 600,
  },
  addBtn: {
    background: tokens.accent.blue, border: 'none',
    color: '#fff', borderRadius: 5, padding: '5px 14px',
    cursor: 'pointer', fontSize: 12, fontWeight: 700,
  },
  formCard: {
    background: tokens.bg.panel, border: `1px solid ${tokens.bd.emphasis}`,
    borderRadius: 10, padding: '16px 20px', marginBottom: 16,
  },
  formTitle: { fontWeight: 700, fontSize: 14, marginBottom: 12 },
  formRow: {
    display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8, flexWrap: 'wrap' as const,
  },
  label: { fontSize: 12, color: tokens.tx.muted, whiteSpace: 'nowrap' as const, minWidth: 52 },
  input: {
    flex: 1, minWidth: 100, padding: '6px 10px', fontSize: 13,
    background: tokens.bg.root, border: `1px solid ${tokens.bd.default}`,
    borderRadius: 5, color: tokens.tx.primary, outline: 'none',
  },
  formErr: { fontSize: 12, color: tokens.semantic.down, marginTop: 4 },
  submitBtn: {
    background: tokens.accent.blue, border: 'none',
    color: '#fff', borderRadius: 5, padding: '6px 18px',
    cursor: 'pointer', fontSize: 13, fontWeight: 700,
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
  sectionRow: {
    display: 'flex', alignItems: 'center', justifyContent: 'space-between',
    marginBottom: 10, paddingBottom: 6,
    borderBottom: `1px solid ${tokens.bd.default}`,
  },
  sectionTitle: {
    fontSize: 13, fontWeight: 700, color: tokens.tx.subtle,
  },
  currBtn: {
    background: 'none', border: `1px solid ${tokens.bd.default}`,
    color: tokens.tx.muted, borderRadius: 4, padding: '2px 9px',
    cursor: 'pointer', fontSize: 11, fontWeight: 600,
  },
  currBtnOn: {
    background: tokens.accent.blue, border: `1px solid ${tokens.accent.blue}`,
    color: '#fff',
  },
  tableWrap: { overflowX: 'auto', marginBottom: 4 },
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
  iconBtn: {
    background: 'none', border: `1px solid ${tokens.bd.default}`,
    color: tokens.tx.muted, borderRadius: 4, padding: '3px 8px',
    cursor: 'pointer', fontSize: 11,
  },
}
