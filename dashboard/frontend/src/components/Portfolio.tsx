import { useEffect, useState } from 'react'
import { tokens } from '../tokens'
import { useRole } from '../hooks/useRole'

interface PortfolioSummary {
  tot_pur_amt:  number | null
  tot_evlt_amt: number | null
  tot_evlt_pl:  number | null
  tot_prft_rt:  number | null
}

interface Holding {
  id:          number
  stk_cd:      string
  stk_nm:      string
  avg_price:   number
  qty:         number
  cur_prc:     number | null
  pur_amt:     number | null
  evlt_amt:    number | null
  evltv_prft:  number | null
  prft_rt:     number | null
  poss_rt:     number | null
}

interface PortfolioData {
  summary:  PortfolioSummary
  holdings: Holding[]
}

interface FormState {
  stk_cd:    string
  stk_nm:    string
  avg_price: string
  qty:       string
}

const EMPTY_FORM: FormState = { stk_cd: '', stk_nm: '', avg_price: '', qty: '' }

const fmt = (n: number | null | undefined, digits = 0) =>
  n == null ? '—' : n.toLocaleString('ko-KR', { maximumFractionDigits: digits })

const fmtPct = (n: number | null | undefined) =>
  n == null ? '—' : `${n >= 0 ? '+' : ''}${n.toFixed(2)}%`

const profitColor = (n: number | null | undefined) =>
  n == null ? tokens.tx.muted : n > 0 ? tokens.semantic.up : n < 0 ? tokens.semantic.down : tokens.tx.primary

export default function Portfolio() {
  const role = useRole()
  const isAdmin = role === 'admin'

  const [data, setData]         = useState<PortfolioData | null>(null)
  const [loading, setLoading]   = useState(true)
  const [error, setError]       = useState<string | null>(null)

  const [showForm, setShowForm]       = useState(false)
  const [editingId, setEditingId]     = useState<number | null>(null)
  const [form, setForm]               = useState<FormState>(EMPTY_FORM)
  const [submitting, setSubmitting]   = useState(false)
  const [formError, setFormError]     = useState<string | null>(null)

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
    setShowForm(true)
  }

  const openEdit = (h: Holding) => {
    setEditingId(h.id)
    setForm({ stk_cd: h.stk_cd, stk_nm: h.stk_nm, avg_price: String(h.avg_price), qty: String(h.qty) })
    setFormError(null)
    setShowForm(true)
  }

  const handleSubmit = async () => {
    const avg = parseFloat(form.avg_price)
    const qty = parseInt(form.qty)
    if (!form.stk_cd.trim() || !form.stk_nm.trim()) return setFormError('종목코드와 종목명을 입력하세요')
    if (!avg || avg <= 0 || !qty || qty <= 0)        return setFormError('평균단가·수량은 양수여야 합니다')

    setSubmitting(true)
    setFormError(null)
    const body = { ticker: form.stk_cd.trim(), name: form.stk_nm.trim(), avg_price: avg, qty }
    const url    = editingId != null ? `/api/portfolio/holdings/${editingId}` : '/api/portfolio/holdings'
    const method = editingId != null ? 'PUT' : 'POST'
    try {
      const r = await fetch(url, { method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
      if (!r.ok) {
        const d = await r.json()
        throw new Error(d.detail || r.statusText)
      }
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
      if (!r.ok && r.status !== 204) {
        const d = await r.json()
        throw new Error(d.detail || r.statusText)
      }
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

  const sum      = data?.summary
  const holdings = data?.holdings ?? []

  return (
    <div style={s.root}>
      {/* 헤더 */}
      <div style={s.header}>
        <span style={s.title}>내 투자 자산</span>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          {isAdmin && (
            <button style={s.addBtn} onClick={openAdd}>+ 종목 추가</button>
          )}
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
              placeholder="005930"
              value={form.stk_cd}
              onChange={e => setForm(f => ({ ...f, stk_cd: e.target.value }))}
            />
            <label style={s.label}>종목명</label>
            <input
              style={s.input}
              placeholder="삼성전자"
              value={form.stk_nm}
              onChange={e => setForm(f => ({ ...f, stk_nm: e.target.value }))}
            />
          </div>
          <div style={s.formRow}>
            <label style={s.label}>평균단가</label>
            <input
              style={s.input}
              type="number"
              placeholder="70000"
              value={form.avg_price}
              onChange={e => setForm(f => ({ ...f, avg_price: e.target.value }))}
            />
            <label style={s.label}>보유수량</label>
            <input
              style={s.input}
              type="number"
              placeholder="100"
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

      {/* 총자산 카드 */}
      {holdings.length > 0 && (
        <>
          <div style={s.heroCard}>
            <div style={s.heroLabel}>총 평가금액</div>
            <div style={s.heroValue}>{fmt(sum?.tot_evlt_amt)}원</div>
            <div style={{ ...s.heroPct, color: profitColor(sum?.tot_evlt_pl) }}>
              {fmtPct(sum?.tot_prft_rt)} ({fmt(sum?.tot_evlt_pl)}원)
            </div>
          </div>

          {/* 요약 그리드 */}
          <div style={s.grid}>
            <StatCard label="총 매입금액" value={fmt(sum?.tot_pur_amt) + '원'} />
            <StatCard label="총 평가금액" value={fmt(sum?.tot_evlt_amt) + '원'} />
            <StatCard label="총 손익"     value={fmt(sum?.tot_evlt_pl) + '원'} color={profitColor(sum?.tot_evlt_pl)} />
          </div>
        </>
      )}

      {/* 보유 종목 */}
      <div style={s.sectionTitle}>보유 종목 ({holdings.length})</div>

      {holdings.length === 0 ? (
        <div style={{ color: tokens.tx.muted, fontSize: 13, padding: '16px 0' }}>
          {isAdmin ? '+ 종목 추가 버튼으로 보유 종목을 입력하세요' : '보유 종목이 없습니다'}
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
                  <td style={s.td}>{fmt(h.qty)}</td>
                  <td style={s.td}>{fmt(h.avg_price)}</td>
                  <td style={s.td}>{h.cur_prc != null ? fmt(h.cur_prc) : <span style={{ color: tokens.tx.muted }}>—</span>}</td>
                  <td style={s.td}>{fmt(h.evlt_amt)}</td>
                  <td style={{ ...s.td, color: profitColor(h.evltv_prft) }}>{fmt(h.evltv_prft)}</td>
                  <td style={{ ...s.td, color: profitColor(h.prft_rt) }}>{fmtPct(h.prft_rt)}</td>
                  <td style={s.td}>{h.poss_rt != null ? `${h.poss_rt.toFixed(1)}%` : '—'}</td>
                  {isAdmin && (
                    <td style={s.td}>
                      <div style={{ display: 'flex', gap: 4, justifyContent: 'flex-end' }}>
                        <button style={s.iconBtn} onClick={() => openEdit(h)}>수정</button>
                        <button style={{ ...s.iconBtn, color: tokens.semantic.down }} onClick={() => handleDelete(h.id, h.stk_nm)}>삭제</button>
                      </div>
                    </td>
                  )}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

function StatCard({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div style={s.statCard}>
      <div style={s.statLabel}>{label}</div>
      <div style={{ ...s.statValue, ...(color ? { color } : {}) }}>{value}</div>
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
  label: { fontSize: 12, color: tokens.tx.muted, whiteSpace: 'nowrap' as const, minWidth: 48 },
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
  iconBtn: {
    background: 'none', border: `1px solid ${tokens.bd.default}`,
    color: tokens.tx.muted, borderRadius: 4, padding: '3px 8px',
    cursor: 'pointer', fontSize: 11,
  },
}
