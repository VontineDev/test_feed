import Positions from './Positions'
import Scheduler from './Scheduler'

export default function PaperPortfolio() {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', overflow: 'auto' }}>
      <div style={{ flex: 1, minHeight: 200, borderBottom: '1px solid #1e293b', overflowY: 'auto' }}>
        <Positions />
      </div>
      <div style={{ flexShrink: 0 }}>
        <Scheduler />
      </div>
    </div>
  )
}
