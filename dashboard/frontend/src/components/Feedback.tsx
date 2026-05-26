import { useState, useRef } from 'react'
import { tokens } from '../tokens'

interface Props {
  open: boolean
  onClose: () => void
}

export default function Feedback({ open, onClose }: Props) {
  const [text, setText]         = useState('')
  const [withShot, setWithShot] = useState(true)
  const [sending, setSending]   = useState(false)
  const [sent, setSent]         = useState(false)
  const textRef = useRef<HTMLTextAreaElement>(null)

  const handleClose = () => {
    if (sending) return
    setText('')
    setSent(false)
    onClose()
  }

  const send = async () => {
    if (!text.trim() || sending) return
    setSending(true)
    try {
      let screenshot: string | null = null
      if (withShot) {
        try {
          const { default: html2canvas } = await import('html2canvas')
          const canvas = await html2canvas(document.documentElement, {
            scale: 0.6,
            useCORS: true,
            logging: false,
            ignoreElements: (el) => el.id === 'feedback-modal',
          })
          screenshot = canvas.toDataURL('image/jpeg', 0.7).split(',')[1]
        } catch {
          // 스크린샷 실패 시 텍스트만 전송
        }
      }
      const r = await fetch('/api/feedback', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ text: text.trim(), screenshot }),
      })
      if (r.ok) {
        setSent(true)
        setText('')
        setTimeout(() => { setSent(false); onClose() }, 2000)
      } else {
        alert('전송 실패 — 잠시 후 다시 시도해 주세요')
      }
    } catch {
      alert('전송 실패 — 잠시 후 다시 시도해 주세요')
    } finally {
      setSending(false)
    }
  }

  if (!open) return null

  return (
    <div style={s.backdrop} onClick={(e) => { if (e.target === e.currentTarget) handleClose() }}>
      <div id="feedback-modal" style={s.modal}>
        <div style={s.modalHdr}>
          <span style={s.modalTitle}>피드백 보내기</span>
          <button style={s.closeBtn} onClick={handleClose} aria-label="닫기">✕</button>
        </div>

        {sent ? (
          <div style={s.sentMsg}>전송되었습니다. 감사합니다!</div>
        ) : (
          <>
            <textarea
              ref={textRef}
              autoFocus
              style={s.textarea}
              placeholder="버그, 개선 제안, 불편한 점 등 무엇이든 알려주세요."
              value={text}
              onChange={(e) => setText(e.target.value)}
              rows={5}
              disabled={sending}
            />
            <label style={s.shotRow}>
              <input
                type="checkbox"
                checked={withShot}
                onChange={(e) => setWithShot(e.target.checked)}
                disabled={sending}
                style={{ marginRight: 6 }}
              />
              <span style={s.shotLabel}>현재 화면 스크린샷 첨부</span>
            </label>
            <div style={s.btnRow}>
              <button style={s.cancelBtn} onClick={handleClose} disabled={sending}>취소</button>
              <button
                style={{ ...s.sendBtn, opacity: (!text.trim() || sending) ? 0.5 : 1 }}
                onClick={send}
                disabled={!text.trim() || sending}
              >
                {sending ? '전송 중…' : '전송'}
              </button>
            </div>
          </>
        )}
      </div>
    </div>
  )
}

const s: Record<string, React.CSSProperties> = {
  backdrop: {
    position: 'fixed', inset: 0, zIndex: 2000,
    background: 'rgba(0,0,0,0.55)',
    display: 'flex', alignItems: 'center', justifyContent: 'center',
  },
  modal: {
    background: tokens.bg.panel, border: `1px solid ${tokens.bd.emphasis}`,
    borderRadius: 10, padding: 20, width: 360, maxWidth: 'calc(100vw - 32px)',
    boxShadow: '0 8px 32px rgba(0,0,0,0.6)',
  },
  modalHdr: { display: 'flex', alignItems: 'center', marginBottom: 14 },
  modalTitle: { flex: 1, fontWeight: 700, fontSize: 14, color: tokens.tx.secondary },
  closeBtn: {
    background: 'none', border: 'none', color: tokens.tx.muted,
    cursor: 'pointer', fontSize: 16, padding: '2px 4px',
  },
  textarea: {
    width: '100%', boxSizing: 'border-box' as const,
    background: tokens.bg.root, border: `1px solid ${tokens.bd.default}`,
    borderRadius: 6, color: tokens.tx.primary, fontSize: 13,
    padding: '10px 12px', resize: 'vertical' as const, outline: 'none',
    fontFamily: 'inherit',
  },
  shotRow: { display: 'flex', alignItems: 'center', marginTop: 10, cursor: 'pointer' },
  shotLabel: { fontSize: 12, color: tokens.tx.muted },
  btnRow: { display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: 14 },
  cancelBtn: {
    background: 'none', border: `1px solid ${tokens.bd.emphasis}`,
    color: tokens.tx.muted, borderRadius: 6, padding: '8px 16px',
    cursor: 'pointer', fontSize: 13,
  },
  sendBtn: {
    background: tokens.bg.active, border: `1px solid ${tokens.accent.blue}`,
    color: tokens.accent.blueLight, borderRadius: 6, padding: '8px 20px',
    cursor: 'pointer', fontSize: 13, fontWeight: 600,
    transition: 'opacity 0.15s',
  },
  sentMsg: {
    textAlign: 'center' as const, color: '#4ade80',
    fontSize: 14, fontWeight: 600, padding: '24px 0',
  },
}
