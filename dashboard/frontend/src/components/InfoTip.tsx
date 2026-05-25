import { useState } from 'react'
import { tokens } from '../tokens'

export default function InfoTip({ text, width = 240, zIndex = 100 }: { text: string; width?: number; zIndex?: number }) {
  const [show, setShow] = useState(false)
  return (
    <span style={{ position: 'relative', display: 'inline-flex', alignItems: 'center' }}>
      <span
        style={{ cursor: 'help', color: tokens.tx.subtle, fontSize: 11, marginLeft: 4, userSelect: 'none' }}
        onMouseEnter={() => setShow(true)}
        onMouseLeave={() => setShow(false)}
      >ⓘ</span>
      {show && (
        <span style={{
          position: 'absolute',
          left: '110%',
          top: '50%',
          transform: 'translateY(-50%)',
          background: tokens.bg.raised,
          border: `1px solid ${tokens.bd.emphasis}`,
          color: tokens.tx.secondary,
          fontSize: 11,
          lineHeight: 1.5,
          padding: '6px 10px',
          borderRadius: 4,
          whiteSpace: 'normal',
          width,
          zIndex,
          pointerEvents: 'none',
        }}>{text}</span>
      )}
    </span>
  )
}
