"""
backtest_engine.py  —  통합 백테스트 엔진 (Sprint 3)
────────────────────────────────────────────────────────────
4개 모드:
  ichimoku — 주봉 이치모쿠 7조건 walk-forward 재현
  stage    — 일봉 Stage 1 가격 조건 재현 (5/5 조건, 수급은 daily_flow 있을 때)
  cross    — 이치모쿠 + Stage 1 동일 ISO 주 교차
  stage2   — Stage 1 신호 후 14일 이내 Stage 2 재진입 조건 재현

지표:
  승률 (7d/28d/91d), 평균/중앙값 수익률, KOSPI 초과수익률,
  샤프비율 (연환산), MDD (equity curve)

비용 기본값 (KRX 2025):
  매수 수수료 0.014% + 매도 수수료 0.014% + 증권거래세 0.180% + 농특세 0.002%
  ≈ 왕복 0.210%

CLI 사용법:
  python run_backtest.py --mode ichimoku --start 2025-01-01 --end 2026-01-01
  python run_backtest.py --mode stage    --start 2025-01-01 --end 2026-01-01 --market KOSDAQ
  python run_backtest.py --mode cross    --start 2025-01-01 --end 2026-01-01 --max 100
"""

from __future__ import annotations

import logging
import math
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace as _dc_replace
from datetime import date, datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import pandas as pd

from chart_screener import calc_ichimoku

logger = logging.getLogger(__name__)
_KST = ZoneInfo("Asia/Seoul")

# ── 거래 비용 ─────────────────────────────────────────────────────
_TX_BUY  = 0.000140                              # 증권사 매수 수수료
_TX_SELL = 0.000140 + 0.001800 + 0.000020        # 수수료 + 증권거래세 + 농특세
TX_COST_DEFAULT: float = _TX_BUY + _TX_SELL      # ≈ 0.0021 (0.210%)

_S1_THRESHOLD = {"KOSPI": 0.05, "KOSDAQ": 0.07}  # Stage 1 일봉 상승률 기준

MODE_KOR: dict[str, str] = {
    "ichimoku": "이치모쿠(주봉)",
    "stage":    "3단계(일봉)",
    "cross":    "이치모쿠×3단계",
    "stage2":   "Stage 2(일봉)",
}


# ── 데이터 클래스 ─────────────────────────────────────────────────

@dataclass
class BacktestConfig:
    mode: str                     # "ichimoku" | "stage" | "cross"
    start: date
    end: date
    market: str = "ALL"           # "KOSPI" | "KOSDAQ" | "ALL"
    tx_cost_rt: float = TX_COST_DEFAULT
    max_tickers: int = 200        # 0 = 전종목 (수십 분 소요)
    rf_rate_annual: float = 0.030  # 무위험수익률 (한국국채 3% 기준)
    workers: int = 8
    dsn: Optional[str] = None     # PostgreSQL DSN. 설정 시 daily_ohlcv 캐시 사용
    hold_weeks: Optional[int] = None  # None = 표준 1/4/13w; N = N주 보유 수익률 추가 계산

    def __post_init__(self) -> None:
        if self.mode not in ("ichimoku", "stage", "cross", "stage2"):
            raise ValueError(f"mode는 ichimoku|stage|cross|stage2 중 하나여야 합니다: {self.mode!r}")
        if self.start >= self.end:
            raise ValueError("start는 end보다 이전이어야 합니다")
        if self.market not in ("KOSPI", "KOSDAQ", "ALL"):
            raise ValueError(f"market은 KOSPI|KOSDAQ|ALL 중 하나여야 합니다: {self.market!r}")
        if self.hold_weeks is not None and self.hold_weeks < 1:
            raise ValueError(f"hold_weeks는 1 이상이어야 합니다: {self.hold_weeks!r}")


@dataclass
class SignalRecord:
    ticker: str
    name: str
    signal_date: date
    close_at_signal: float
    mode: str                     # "ichimoku" | "stage" | "cross"
    market: str                   # "KOSPI" | "KOSDAQ"
    return_7d:  Optional[float] = None
    return_28d: Optional[float] = None
    return_91d: Optional[float] = None
    excess_7d:  Optional[float] = None
    excess_28d: Optional[float] = None
    excess_91d: Optional[float] = None
    return_custom: Optional[float] = None  # BacktestConfig.hold_weeks 지정 시 채워짐
    excess_custom: Optional[float] = None
    # 업종·MDD·매도 신호·단계 진행 (run_backtest에서 채워짐)
    sector:      str             = ""    # KIND 업종명 (빈 문자열 = 미확인)
    mdd_91d:     Optional[float] = None  # 진입일 기준 91일 MDD (≤ 0)
    s2_date:     Optional[date]  = None  # S1 신호 후 14일 이내 S2 조건 충족일
    s3_date:     Optional[date]  = None  # S2 이후 조정 고점 돌파 + RSI≥70 (과열 재가속)
    sell_date:   Optional[date]  = None  # MA20 이탈 또는 손절 발생일
    sell_reason: str             = ""    # "MA20 이탈" | "손절 -N%" | "보유 중"
    sell_return: Optional[float] = None  # 매도 시점 수익률 (거래비용 차감)
    hold_days:   Optional[int]   = None  # 신호일~매도일 달력일수


@dataclass
class GroupMetrics:
    n: int = 0
    win_rate_7d:        Optional[float] = None
    win_rate_28d:       Optional[float] = None
    win_rate_91d:       Optional[float] = None
    avg_return_28d:     Optional[float] = None
    median_return_28d:  Optional[float] = None
    avg_return_91d:     Optional[float] = None
    avg_excess_28d:     Optional[float] = None
    avg_excess_91d:     Optional[float] = None
    sharpe_7d:          Optional[float] = None  # 연환산 샤프비율 (7d 보유)
    sharpe_28d:         Optional[float] = None  # 연환산 샤프비율 (28d 보유)
    sharpe_91d:         Optional[float] = None  # 연환산 샤프비율 (91d 보유)
    mdd:                Optional[float] = None  # 최대낙폭 (equity curve)
    # 사용자 지정 보유 기간 (hold_weeks 설정 시)
    hold_days_custom:     Optional[int]   = None
    win_rate_custom:      Optional[float] = None
    avg_return_custom:    Optional[float] = None
    median_return_custom: Optional[float] = None
    avg_excess_custom:    Optional[float] = None
    sharpe_custom:        Optional[float] = None
    # 매도 신호 기반 집계 (MA20 이탈 / 손절)
    win_rate_sell:        Optional[float] = None
    avg_return_sell:      Optional[float] = None
    median_return_sell:   Optional[float] = None
    avg_hold_days:        Optional[float] = None
    s2_progression_rate:  Optional[float] = None  # S1→S2 진행 비율
    s3_progression_rate:  Optional[float] = None  # S2→S3 진행 비율
    avg_mdd_91d:          Optional[float] = None  # 종목별 MDD(91d) 평균


@dataclass
class BacktestResult:
    config: BacktestConfig
    signals: list[SignalRecord]
    overall: GroupMetrics
    computed_at: str
    note: str = ""

    def to_telegram_report(self) -> str:
        """텔레그램 전송용 요약 리포트 (4096자 이내)."""
        cfg = self.config
        m   = self.overall

        def pct(v: Optional[float], dp: int = 1) -> str:
            return f"{v * 100:+.{dp}f}%" if v is not None else "N/A"

        def val(v: Optional[float], dp: int = 2) -> str:
            return f"{v:.{dp}f}" if v is not None else "N/A"

        mode_kor = MODE_KOR.get(cfg.mode, cfg.mode)

        lines = [
            f"📊 백테스트 — {mode_kor}",
            f"📅 {cfg.start} ~ {cfg.end}  {cfg.market}",
            f"🔢 신호 {m.n}건  비용 {cfg.tx_cost_rt * 100:.3f}% RT",
            "",
            "수익률 (7d / 28d / 91d)",
            f"  승률: {pct(m.win_rate_7d)} / {pct(m.win_rate_28d)} / {pct(m.win_rate_91d)}",
            f"  평균 28d: {pct(m.avg_return_28d)}  중앙값: {pct(m.median_return_28d)}",
            f"  평균 91d: {pct(m.avg_return_91d)}",
            f"  KOSPI 초과 28d: {pct(m.avg_excess_28d)}",
            "",
            "위험 지표",
            f"  샤프비율 7d: {val(m.sharpe_7d)}  28d: {val(m.sharpe_28d)}  91d: {val(m.sharpe_91d)}",
            f"  MDD: {pct(m.mdd)}",
        ]
        if m.hold_days_custom is not None:
            hw = m.hold_days_custom // 7
            lines += [
                "",
                f"사용자 지정 보유 {hw}주({m.hold_days_custom}d)",
                f"  승률: {pct(m.win_rate_custom)}  평균: {pct(m.avg_return_custom)}  중앙값: {pct(m.median_return_custom)}",
                f"  KOSPI 초과: {pct(m.avg_excess_custom)}  샤프비율: {val(m.sharpe_custom)}",
            ]
        if m.win_rate_sell is not None:
            lines += [
                "",
                "매도 신호 (MA20 이탈 / 손절 -8%)",
                f"  승률: {pct(m.win_rate_sell)}  평균: {pct(m.avg_return_sell)}  보유: {val(m.avg_hold_days, 1)}d",
            ]
        if m.s2_progression_rate is not None:
            lines.append(f"  S1→S2 진행: {pct(m.s2_progression_rate, 1)}")
        if m.s3_progression_rate is not None:
            lines.append(f"  S2→S3 진행: {pct(m.s3_progression_rate, 1)}")
        if self.note:
            lines += ["", f"⚠ {self.note}"]
        return "\n".join(lines)

    def to_text_report(self) -> str:
        """CLI 출력용 상세 텍스트 리포트."""
        cfg = self.config
        m   = self.overall

        def pct(v: Optional[float], dp: int = 2) -> str:
            return f"{v * 100:+.{dp}f}%" if v is not None else "N/A"

        def val(v: Optional[float], dp: int = 3) -> str:
            return f"{v:.{dp}f}" if v is not None else "N/A"

        mode_kor = MODE_KOR.get(cfg.mode, cfg.mode)

        lines = [
            "=" * 62,
            f"  백테스트 리포트 — {mode_kor}",
            f"  기간: {cfg.start} ~ {cfg.end}  |  시장: {cfg.market}",
            f"  거래비용(RT): {cfg.tx_cost_rt * 100:.3f}%  |  무위험률: {cfg.rf_rate_annual * 100:.1f}%",
            "=" * 62,
            f"  총 신호 수: {m.n}건",
            "",
            "[ 수익률 ]",
            f"  승률  7d : {pct(m.win_rate_7d, 1)}",
            f"  승률 28d : {pct(m.win_rate_28d, 1)}"
            f"   평균: {pct(m.avg_return_28d)}   중앙값: {pct(m.median_return_28d)}",
            f"  승률 91d : {pct(m.win_rate_91d, 1)}"
            f"   평균: {pct(m.avg_return_91d)}",
            "",
            "[ KOSPI 초과수익률 ]",
            f"  28d: {pct(m.avg_excess_28d)}   91d: {pct(m.avg_excess_91d)}",
            "",
            "[ 위험 지표 ]",
            f"  샤프비율  7d 연환산: {val(m.sharpe_7d)}",
            f"  샤프비율 28d 연환산: {val(m.sharpe_28d)}",
            f"  샤프비율 91d 연환산: {val(m.sharpe_91d)}",
            f"  MDD:               {pct(m.mdd)}",
        ]
        if m.hold_days_custom is not None:
            hw = m.hold_days_custom // 7
            lines += [
                "",
                f"[ 사용자 지정 보유 {hw}주({m.hold_days_custom}d) ]",
                f"  승률  : {pct(m.win_rate_custom, 1)}",
                f"  평균  : {pct(m.avg_return_custom)}   중앙값: {pct(m.median_return_custom)}",
                f"  KOSPI 초과: {pct(m.avg_excess_custom)}",
                f"  샤프비율 연환산: {val(m.sharpe_custom)}",
            ]
        if m.win_rate_sell is not None:
            lines += [
                "",
                "[ 매도 신호 — MA20 이탈 / 손절 -8% ]",
                f"  승률  : {pct(m.win_rate_sell, 1)}",
                f"  평균  : {pct(m.avg_return_sell)}   중앙값: {pct(m.median_return_sell)}",
                f"  평균 보유일: {val(m.avg_hold_days, 1)}일",
            ]
        if m.s2_progression_rate is not None:
            lines.append(f"  S1→S2 진행률: {pct(m.s2_progression_rate, 1)}")
        if m.s3_progression_rate is not None:
            lines.append(f"  S2→S3 진행률: {pct(m.s3_progression_rate, 1)}")
        lines += [
            "",
            f"  산출일시: {self.computed_at}",
        ]
        if self.note:
            lines += ["", f"주의: {self.note}"]
        lines.append("=" * 62)
        return "\n".join(lines)

    def top_bottom_telegram_text(self, n: int = 5) -> str:
        """Top/bottom N stocks by 28d return — plain text for Telegram append."""
        scored = [(s, s.return_28d) for s in self.signals if s.return_28d is not None]
        if not scored:
            return ""
        scored.sort(key=lambda x: x[1], reverse=True)

        def _line(rank: int, sig: SignalRecord, ret: float) -> str:
            name = sig.name[:10] if len(sig.name) > 10 else sig.name
            return f"  {rank}. {name} {ret * 100:+.1f}% ({sig.signal_date})"

        top_n = min(n, len(scored))
        bot_n = min(n, len(scored))
        lines = [f"\n🏆 상위 {top_n}종목 (28d)"]
        for i, (sig, ret) in enumerate(scored[:top_n], 1):
            lines.append(_line(i, sig, ret))
        lines.append(f"\n📉 하위 {bot_n}종목 (28d)")
        for i, (sig, ret) in enumerate(scored[-bot_n:], 1):
            lines.append(_line(i, sig, ret))
        return "\n".join(lines)

    def to_html_report(self, output_path: str) -> str:
        """HTML 리포트 생성 + 저장. 종목별 수익률 테이블 + 분포 차트 포함. 파일 경로 반환."""
        import html as _html
        from pathlib import Path

        cfg = self.config
        m   = self.overall

        def pct(v: Optional[float], dp: int = 1) -> str:
            return f"{v * 100:+.{dp}f}%" if v is not None else "—"

        def fmt(v: Optional[float], dp: int = 2) -> str:
            return f"{v:.{dp}f}" if v is not None else "—"

        mode_kor = MODE_KOR.get(cfg.mode, cfg.mode)

        # ── 28d 분포 SVG ─────────────────────────────────────────
        r28s = [s.return_28d for s in self.signals if s.return_28d is not None]
        edges  = [(-9, -0.20), (-0.20, -0.10), (-0.10, 0.0),
                  (0.0, 0.10), (0.10, 0.20), (0.20, 9)]
        labels = ["<-20%", "-20~-10%", "-10~0%", "0~10%", "10~20%", ">20%"]
        colors = ["#b03060", "#d06080", "#806070", "#608060", "#40a060", "#209040"]
        counts: list[int] = []
        for lo, hi in edges:
            if hi == 9:
                counts.append(sum(1 for r in r28s if r >= lo))
            else:
                counts.append(sum(1 for r in r28s if lo <= r < hi))
        max_c = max(counts) if any(c > 0 for c in counts) else 1
        BW, BH = 380, 110
        bw = BW / len(counts) - 4
        svg_parts: list[str] = []
        for i, (c, lab, col) in enumerate(zip(counts, labels, colors)):
            bh_px = (c / max_c) * (BH - 28) if max_c else 0
            x = i * (BW / len(counts)) + 2
            y = BH - 22 - bh_px
            svg_parts.append(
                f'<rect x="{x:.0f}" y="{y:.0f}" width="{bw:.0f}" '
                f'height="{bh_px:.0f}" fill="{col}"/>'
                f'<text x="{x + bw/2:.0f}" y="{BH - 5}" text-anchor="middle" '
                f'font-size="9" fill="#7d8590">{lab}</text>'
            )
            if c > 0:
                svg_parts.append(
                    f'<text x="{x + bw/2:.0f}" y="{y - 3:.0f}" '
                    f'text-anchor="middle" font-size="10" fill="#e6edf3">{c}</text>'
                )
        dist_svg = (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{BW}" height="{BH}" '
            f'style="overflow:visible">{"".join(svg_parts)}</svg>'
        ) if r28s else "<span class='muted'>28d 데이터 없음</span>"

        # ── 종목 테이블 행 ────────────────────────────────────────
        def _ret_td(v: Optional[float]) -> str:
            dv  = v * 10000 if v is not None else -99999
            cls = "pos" if v is not None and v > 0 else ("neg" if v is not None else "")
            return f'<td class="num {cls}" data-v="{dv}">{pct(v)}</td>'

        _STAGE_LABEL = {"stage": ("S1", "s1"), "stage2": ("S2", "s2"),
                        "cross": ("교차", "cross"), "ichimoku": ("이치", "ichi")}
        _SELL_CLS    = {"MA20 이탈": "sell-ma", "손절": "sell-sl"}

        def _mdd_td(v: Optional[float]) -> str:
            if v is None:
                return '<td class="num muted" data-v="-1">—</td>'
            dv  = v * 10000
            cls = "neg" if v < 0 else ""
            txt = f"{v * 100:.1f}%"
            return f'<td class="num {cls}" data-v="{dv}">{txt}</td>'

        table_rows: list[str] = []
        for sig in self.signals:
            mkt_b = "KS" if sig.market == "KOSPI" else "KQ"
            ep    = f"{sig.close_at_signal:,.0f}"

            sector_txt = _html.escape(sig.sector[:12]) if sig.sector else "—"
            sector_td  = f'<td class="muted">{sector_txt}</td>'

            stage_lbl, stage_cls = _STAGE_LABEL.get(sig.mode, (sig.mode, ""))
            stage_td = f'<td><span class="badge {stage_cls}">{stage_lbl}</span></td>'

            s2_td = (f'<td data-v="{sig.s2_date.isoformat()}">{sig.s2_date}</td>'
                     if sig.s2_date else '<td class="muted" data-v="">—</td>')

            s3_td = (f'<td data-v="{sig.s3_date.isoformat()}">{sig.s3_date}</td>'
                     if sig.s3_date else '<td class="muted" data-v="">—</td>')

            sell_cls = next((v for k, v in _SELL_CLS.items() if sig.sell_reason.startswith(k)), "")
            sell_td  = (f'<td><span class="badge {sell_cls}">{_html.escape(sig.sell_reason)}</span></td>'
                        if sell_cls else f'<td class="muted">{_html.escape(sig.sell_reason)}</td>')

            sd_td = (f'<td data-v="{sig.sell_date.isoformat()}">{sig.sell_date}</td>'
                     if sig.sell_date else '<td class="muted" data-v="">—</td>')

            hd_td = (f'<td class="num" data-v="{sig.hold_days}">{sig.hold_days}d</td>'
                     if sig.hold_days is not None else '<td class="num muted" data-v="-1">—</td>')

            table_rows.append(
                f"<tr>"
                f'<td data-v="{sig.signal_date.isoformat()}">{sig.signal_date}</td>'
                f'<td>{_html.escape(sig.name)}</td>'
                + sector_td
                + f'<td class="muted">{sig.ticker} <span class="badge">{mkt_b}</span></td>'
                + stage_td
                + f'<td class="num" data-v="{sig.close_at_signal}">{ep}</td>'
                + _ret_td(sig.return_7d)
                + _ret_td(sig.return_28d)
                + _ret_td(sig.return_91d)
                + _ret_td(sig.excess_28d)
                + s2_td
                + s3_td
                + sell_td
                + sd_td
                + hd_td
                + _ret_td(sig.sell_return)
                + _mdd_td(sig.mdd_91d)
                + "</tr>"
            )

        # CSS class helpers for summary cards
        def _cls(v: Optional[float], threshold: float = 0.0) -> str:
            return "pos" if (v or 0.0) >= threshold else "neg"

        n_sigs  = len(self.signals)
        note_esc = _html.escape(self.note) if self.note else ""

        html_out = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>백테스트 리포트 — {_html.escape(mode_kor)} {cfg.start}~{cfg.end}</title>
<style>
:root {{
  --bg:#0d1117; --bg-s:#161b22; --text:#e6edf3; --muted:#7d8590;
  --border:#30363d; --accent:#58a6ff; --pos:#3fb950; --neg:#f85149;
  --ui:system-ui,-apple-system,'Segoe UI',sans-serif;
  --mono:'JetBrains Mono','Consolas',monospace;
}}
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
body{{background:var(--bg);color:var(--text);font-family:var(--ui);font-size:14px;padding:24px}}
header{{border-bottom:1px solid var(--border);padding-bottom:12px;margin-bottom:20px}}
header h1{{font-size:18px;font-weight:600;margin-bottom:4px}}
.meta{{color:var(--muted);font-size:12px}}
h2{{font-size:14px;font-weight:600;margin:24px 0 8px;color:var(--accent)}}
.cards{{display:flex;flex-wrap:wrap;gap:12px;margin-bottom:24px}}
.card{{background:var(--bg-s);border:1px solid var(--border);border-radius:6px;padding:12px 16px;min-width:130px}}
.card .lbl{{font-size:11px;color:var(--muted);margin-bottom:4px}}
.card .val{{font-size:20px;font-weight:600;font-family:var(--mono)}}
.pos{{color:var(--pos)}} .neg{{color:var(--neg)}} .muted{{color:var(--muted)}}
table{{border-collapse:collapse;width:100%;font-family:var(--mono);font-size:13px;margin-top:8px}}
th,td{{padding:6px 10px;border-bottom:1px solid var(--border);white-space:nowrap}}
th{{background:var(--bg-s);color:var(--muted);font-weight:500;cursor:pointer;user-select:none}}
th:hover{{color:var(--accent)}} th.num,td.num{{text-align:right}}
.badge{{font-size:10px;background:var(--bg-s);border:1px solid var(--border);border-radius:3px;padding:1px 4px}}
.s1{{color:#58a6ff;border-color:#58a6ff}} .s2{{color:#3fb950;border-color:#3fb950}}
.cross{{color:#d29922;border-color:#d29922}} .ichi{{color:#a371f7;border-color:#a371f7}}
.sell-ma{{color:#f85149;border-color:#f85149}} .sell-sl{{color:#b03060;border-color:#b03060}}
.tbl-wrap{{overflow-x:auto}}
footer{{margin-top:24px;padding-top:12px;border-top:1px solid var(--border);color:var(--muted);font-size:11px}}
</style>
</head>
<body>
<header>
  <h1>백테스트 리포트 &nbsp;│&nbsp; {_html.escape(mode_kor)}</h1>
  <p class="meta">기간: {cfg.start} ~ {cfg.end} &nbsp;│&nbsp; 시장: {cfg.market} &nbsp;│&nbsp; 신호: {m.n}건 &nbsp;│&nbsp; 거래비용(RT): {cfg.tx_cost_rt * 100:.3f}% &nbsp;│&nbsp; 산출: {self.computed_at[:16]}</p>
</header>

<h2>요약</h2>
<div class="cards">
  <div class="card"><div class="lbl">신호 수</div><div class="val">{m.n}</div></div>
  <div class="card"><div class="lbl">승률 7d</div><div class="val {_cls(m.win_rate_7d, 0.5)}">{pct(m.win_rate_7d)}</div></div>
  <div class="card"><div class="lbl">승률 28d</div><div class="val {_cls(m.win_rate_28d, 0.5)}">{pct(m.win_rate_28d)}</div></div>
  <div class="card"><div class="lbl">승률 91d</div><div class="val {_cls(m.win_rate_91d, 0.5)}">{pct(m.win_rate_91d)}</div></div>
  <div class="card"><div class="lbl">평균수익 28d</div><div class="val {_cls(m.avg_return_28d)}">{pct(m.avg_return_28d)}</div></div>
  <div class="card"><div class="lbl">평균수익 91d</div><div class="val {_cls(m.avg_return_91d)}">{pct(m.avg_return_91d)}</div></div>
  <div class="card"><div class="lbl">KOSPI 초과 28d</div><div class="val {_cls(m.avg_excess_28d)}">{pct(m.avg_excess_28d)}</div></div>
  <div class="card"><div class="lbl">샤프비율 28d</div><div class="val">{fmt(m.sharpe_28d)}</div></div>
  <div class="card"><div class="lbl">MDD</div><div class="val neg">{pct(m.mdd)}</div></div>
  {'<div class="card"><div class="lbl">MA20/손절 승률</div><div class="val ' + _cls(m.win_rate_sell, 0.5) + '">' + pct(m.win_rate_sell) + '</div></div>' if m.win_rate_sell is not None else ''}
  {'<div class="card"><div class="lbl">평균 매도수익</div><div class="val ' + _cls(m.avg_return_sell) + '">' + pct(m.avg_return_sell) + '</div></div>' if m.avg_return_sell is not None else ''}
  {'<div class="card"><div class="lbl">평균 보유일</div><div class="val">' + fmt(m.avg_hold_days, 1) + '일</div></div>' if m.avg_hold_days is not None else ''}
  {'<div class="card"><div class="lbl">S1→S2 진행률</div><div class="val">' + pct(m.s2_progression_rate) + '</div></div>' if m.s2_progression_rate is not None else ''}
  {'<div class="card"><div class="lbl">S2→S3 진행률</div><div class="val">' + pct(m.s3_progression_rate) + '</div></div>' if m.s3_progression_rate is not None else ''}
  {'<div class="card"><div class="lbl">평균 종목MDD(91d)</div><div class="val neg">' + f"{(m.avg_mdd_91d or 0)*100:.1f}%" + '</div></div>' if m.avg_mdd_91d is not None else ''}
</div>

<h2>28d 수익률 분포</h2>
<div style="margin-bottom:24px">{dist_svg}</div>

<h2>종목별 결과 ({n_sigs}건)</h2>
<div class="tbl-wrap">
<table id="tbl" data-sc="-1" data-sd="asc">
<thead><tr>
  <th onclick="sort(0,false)">신호일</th>
  <th onclick="sort(1,false)">종목명</th>
  <th onclick="sort(2,false)">업종</th>
  <th onclick="sort(3,false)">티커</th>
  <th onclick="sort(4,false)">단계</th>
  <th class="num" onclick="sort(5,true)">진입가</th>
  <th class="num" onclick="sort(6,true)">7d</th>
  <th class="num" onclick="sort(7,true)">28d</th>
  <th class="num" onclick="sort(8,true)">91d</th>
  <th class="num" onclick="sort(9,true)">초과(28d)</th>
  <th onclick="sort(10,false)">S2진행일</th>
  <th onclick="sort(11,false)">S3진행일</th>
  <th onclick="sort(12,false)">매도신호</th>
  <th onclick="sort(13,false)">매도일</th>
  <th class="num" onclick="sort(14,true)">보유일</th>
  <th class="num" onclick="sort(15,true)">매도수익</th>
  <th class="num" onclick="sort(16,true)">MDD(91d)</th>
</tr></thead>
<tbody>
{chr(10).join(table_rows)}
</tbody>
</table>
</div>
<footer>생성: {self.computed_at}{(' &nbsp;│&nbsp; ' + note_esc) if note_esc else ''}</footer>
<script>
function sort(col,num){{
  const t=document.getElementById('tbl');
  const p=parseInt(t.dataset.sc);
  const d=(p===col&&t.dataset.sd==='asc')?'desc':'asc';
  t.dataset.sc=col; t.dataset.sd=d;
  const rows=[...t.querySelectorAll('tbody tr')];
  rows.sort((a,b)=>{{
    const av=a.cells[col].dataset.v??a.cells[col].textContent.trim();
    const bv=b.cells[col].dataset.v??b.cells[col].textContent.trim();
    const c=num?(parseFloat(av)||-1e9)-(parseFloat(bv)||-1e9):av.localeCompare(bv,'ko');
    return d==='asc'?c:-c;
  }});
  const tb=t.querySelector('tbody');
  rows.forEach(r=>tb.appendChild(r));
  t.querySelectorAll('th').forEach((h,i)=>{{
    h.textContent=h.textContent.replace(/ [▲▼]$/,'');
    if(i===col)h.textContent+=d==='asc'?' ▲':' ▼';
  }});
}}
</script>
</body>
</html>"""

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        Path(output_path).write_text(html_out, encoding="utf-8")
        return output_path


# ── 유틸 함수 ─────────────────────────────────────────────────────

def _week_label(d: date) -> str:
    """ISO 주차 레이블. 예: '2025-W03'"""
    iso = d.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


def _build_price_lookup(df: pd.DataFrame) -> dict[date, float]:
    """DataFrame → {날짜: 종가} dict."""
    result: dict[date, float] = {}
    for ts, row in df.iterrows():
        d = ts.date() if hasattr(ts, "date") else ts
        if not pd.isna(row["Close"]):
            result[d] = float(row["Close"])
    return result


def _nearest_price(
    lookup: dict[date, float], target: date, max_days: int = 7
) -> Optional[float]:
    """target부터 최대 max_days 이내 가장 가까운 미래 거래일 종가 반환."""
    for offset in range(max_days + 1):
        p = lookup.get(target + timedelta(days=offset))
        if p is not None:
            return p
    return None


def _compute_sharpe(
    returns: list[float], hold_days: int, rf_annual: float
) -> Optional[float]:
    """연환산 샤프비율. hold_days 보유 기준 신호 단위 수익률 사용."""
    if len(returns) < 3:
        return None
    periods_per_year = 252.0 / hold_days
    rf_per_period    = rf_annual / periods_per_year
    mean_r = statistics.mean(returns)
    std_r  = statistics.stdev(returns)
    if std_r == 0.0:
        return None
    return (mean_r - rf_per_period) / std_r * math.sqrt(periods_per_year)


def _compute_mdd(returns: list[float]) -> Optional[float]:
    """신호 순서대로 누적한 equity curve의 최대낙폭(MDD).

    equal-weight, 순차 포지션 가정. 수익률 목록은 날짜 순 정렬 후 전달.
    """
    if not returns:
        return None
    equity = peak = 1.0
    max_dd = 0.0
    for r in returns:
        equity *= (1.0 + r)
        if equity > peak:
            peak = equity
        dd = (peak - equity) / peak
        if dd > max_dd:
            max_dd = dd
    return -max_dd


def _compute_rsi(closes: pd.Series, period: int = 14) -> pd.Series:
    """RSI(period) — Wilder 지수이동평균 방식."""
    delta    = closes.diff()
    gain     = delta.clip(lower=0)
    loss     = (-delta).clip(lower=0)
    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()
    rs       = avg_gain / avg_loss.replace(0.0, float("nan"))
    return 100.0 - 100.0 / (1.0 + rs)


def _compute_group_metrics(
    signals: list[SignalRecord], rf_annual: float, hold_weeks: Optional[int] = None
) -> GroupMetrics:
    """신호 목록에서 집계 지표 계산."""
    if not signals:
        return GroupMetrics()

    m = GroupMetrics(n=len(signals))

    r7s   = [s.return_7d  for s in signals if s.return_7d  is not None]
    r28s  = [s.return_28d for s in signals if s.return_28d is not None]
    r91s  = [s.return_91d for s in signals if s.return_91d is not None]
    ex28s = [s.excess_28d for s in signals if s.excess_28d is not None]
    ex91s = [s.excess_91d for s in signals if s.excess_91d is not None]

    if r7s:
        m.win_rate_7d = sum(1 for r in r7s if r > 0) / len(r7s)
    if r28s:
        m.win_rate_28d      = sum(1 for r in r28s if r > 0) / len(r28s)
        m.avg_return_28d    = statistics.mean(r28s)
        m.median_return_28d = statistics.median(r28s)
    if r91s:
        m.win_rate_91d   = sum(1 for r in r91s if r > 0) / len(r91s)
        m.avg_return_91d = statistics.mean(r91s)
    if ex28s:
        m.avg_excess_28d = statistics.mean(ex28s)
    if ex91s:
        m.avg_excess_91d = statistics.mean(ex91s)

    m.sharpe_7d  = _compute_sharpe(r7s,  hold_days=7,  rf_annual=rf_annual)
    m.sharpe_28d = _compute_sharpe(r28s, hold_days=28, rf_annual=rf_annual)
    m.sharpe_91d = _compute_sharpe(r91s, hold_days=91, rf_annual=rf_annual)
    m.mdd        = _compute_mdd(r28s)

    if hold_weeks is not None:
        hold_days = hold_weeks * 7
        m.hold_days_custom = hold_days
        rcs  = [s.return_custom for s in signals if s.return_custom is not None]
        excs = [s.excess_custom for s in signals if s.excess_custom is not None]
        if rcs:
            m.win_rate_custom      = sum(1 for r in rcs if r > 0) / len(rcs)
            m.avg_return_custom    = statistics.mean(rcs)
            m.median_return_custom = statistics.median(rcs)
        if excs:
            m.avg_excess_custom = statistics.mean(excs)
        m.sharpe_custom = _compute_sharpe(rcs, hold_days=hold_days, rf_annual=rf_annual)

    # 매도 신호 기반 집계
    sell_rets = [s.sell_return for s in signals if s.sell_return is not None]
    if sell_rets:
        m.win_rate_sell      = sum(1 for r in sell_rets if r > 0) / len(sell_rets)
        m.avg_return_sell    = statistics.mean(sell_rets)
        m.median_return_sell = statistics.median(sell_rets)
    hold_days_list = [s.hold_days for s in signals if s.hold_days is not None]
    if hold_days_list:
        m.avg_hold_days = statistics.mean(hold_days_list)
    s1_sigs = [s for s in signals if s.mode == "stage"]
    if s1_sigs:
        m.s2_progression_rate = sum(1 for s in s1_sigs if s.s2_date is not None) / len(s1_sigs)
    s2_sigs = [s for s in signals if s.s2_date is not None]
    if s2_sigs:
        m.s3_progression_rate = sum(1 for s in s2_sigs if s.s3_date is not None) / len(s2_sigs)
    mdd_list = [s.mdd_91d for s in signals if s.mdd_91d is not None]
    if mdd_list:
        m.avg_mdd_91d = statistics.mean(mdd_list)

    return m


# ── 데이터 수집 ──────────────────────────────────────────────────

def _fetch_single_ohlcv(
    ticker: str, fetch_start: date, fetch_end: date
) -> Optional[pd.DataFrame]:
    """단일 종목 일봉 OHLCV 수집 (yfinance)."""
    try:
        import yfinance as yf
        tkr = yf.Ticker(ticker)
        df  = tkr.history(
            start=fetch_start.isoformat(),
            end=fetch_end.isoformat(),
            interval="1d",
            auto_adjust=True,
        )
        if df.empty or df["Close"].notna().sum() < 30:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
        df.index = pd.to_datetime(df.index, utc=True)
        return df
    except Exception as e:
        logger.debug("[백테스트] %s 수집 실패: %s", ticker, e)
        return None


def _fetch_index(
    symbol: str, fetch_start: date, fetch_end: date
) -> Optional[pd.DataFrame]:
    """지수 일봉 데이터 수집 (벤치마크 비교용)."""
    try:
        import yfinance as yf
        tkr = yf.Ticker(symbol)
        df  = tkr.history(
            start=fetch_start.isoformat(),
            end=fetch_end.isoformat(),
            interval="1d",
            auto_adjust=True,
        )
        if df.empty:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
        df.index = pd.to_datetime(df.index, utc=True)
        return df
    except Exception as e:
        logger.warning("[백테스트] %s 지수 수집 실패: %s", symbol, e)
        return None


def _batch_fetch_ohlcv(
    tickers: list[str], fetch_start: date, fetch_end: date, workers: int
) -> dict[str, pd.DataFrame]:
    """티커 목록을 병렬로 OHLCV 수집."""
    result: dict[str, pd.DataFrame] = {}

    def _fetch(t: str) -> tuple[str, Optional[pd.DataFrame]]:
        return t, _fetch_single_ohlcv(t, fetch_start, fetch_end)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_fetch, t): t for t in tickers}
        for fut in as_completed(futures):
            t, df = fut.result()
            if df is not None:
                result[t] = df

    logger.info("[백테스트] OHLCV %d/%d 수집 완료", len(result), len(tickers))
    return result


# ── 신호 재현 ─────────────────────────────────────────────────────

def _replay_ichimoku(
    ticker: str,
    name: str,
    daily_df: pd.DataFrame,
    market: str,
    config: BacktestConfig,
) -> list[SignalRecord]:
    """일봉 → 주봉 리샘플링 후 이치모쿠 7조건 walk-forward 재현.

    조건:
      A: close > cloud_top                (구름 상향 돌파)
      B: prev_close <= prev_cloud_top     (전 주 구름 내/하부)
      C: close > MA20w
      D: close > MA60w
      E: MA20w > prev_MA20w              (우상향)
      F: MA60w > prev_MA60w
      G: close > MA120w (데이터 부족 시 통과)
    """
    # KRX는 월~금 → 주봉 = 금요일 마감가 기준
    weekly = daily_df.resample("W-FRI", closed="right", label="right").agg({
        "Open":   "first",
        "High":   "max",
        "Low":    "min",
        "Close":  "last",
        "Volume": "sum",
    }).dropna(subset=["Close"])

    # Ichimoku 선행스팬B: 52주 lookback 최소 요건
    if len(weekly) < 62:
        return []

    weekly = calc_ichimoku(weekly)
    weekly["ma_20w"]  = weekly["Close"].rolling(20,  min_periods=20).mean()
    weekly["ma_60w"]  = weekly["Close"].rolling(60,  min_periods=60).mean()
    weekly["ma_120w"] = weekly["Close"].rolling(120, min_periods=100).mean()

    signals: list[SignalRecord] = []

    for i in range(1, len(weekly)):
        row_date = weekly.index[i].date()
        if row_date < config.start or row_date > config.end:
            continue

        cur  = weekly.iloc[i]
        prev = weekly.iloc[i - 1]

        # 필수 컬럼 NaN 체크
        required_cur  = ["Close", "cloud_top", "ma_20w", "ma_60w"]
        required_prev = ["Close", "cloud_top"]
        if any(pd.isna(cur.get(c)) for c in required_cur):
            continue
        if any(pd.isna(prev.get(c)) for c in required_prev):
            continue

        close     = float(cur["Close"])
        cloud_top = float(cur["cloud_top"])
        ma20      = float(cur["ma_20w"])
        ma60      = float(cur["ma_60w"])
        prev_close = float(prev["Close"])
        prev_ct    = float(prev["cloud_top"])

        prev_ma20 = prev.get("ma_20w")
        prev_ma60 = prev.get("ma_60w")
        ma120     = cur.get("ma_120w")

        cond_A = close > cloud_top
        cond_B = prev_close <= prev_ct
        cond_C = close > ma20
        cond_D = close > ma60
        cond_E = prev_ma20 is not None and not pd.isna(prev_ma20) and ma20 > float(prev_ma20)
        cond_F = prev_ma60 is not None and not pd.isna(prev_ma60) and ma60 > float(prev_ma60)
        cond_G = ma120 is None or pd.isna(ma120) or close > float(ma120)

        if cond_A and cond_B and cond_C and cond_D and cond_E and cond_F and cond_G:
            signals.append(SignalRecord(
                ticker=ticker,
                name=name,
                signal_date=row_date,
                close_at_signal=close,
                mode="ichimoku",
                market=market,
            ))

    return signals


def _replay_stage(
    ticker: str,
    name: str,
    daily_df: pd.DataFrame,
    market: str,
    config: BacktestConfig,
    flow_lookup: Optional[dict[tuple[str, date], tuple[Optional[int], Optional[int]]]] = None,
) -> list[SignalRecord]:
    """일봉 Stage 1 가격 조건 walk-forward 재현 (5/5 조건).

    조건 1: 일일 상승률 ≥ 5%(KOSPI) / 7%(KOSDAQ)
    조건 2: 거래량 ≥ 2× 20일 평균
    조건 3: close > MA20 AND MA60
    조건 4: 52주 고점 대비 괴리율 ≤ 20%
    조건 5: 수급 — 외국인 또는 기관 순매수 > 0
             (flow_lookup 제공 시 적용. 해당 날짜 데이터 없으면 조건 생략)
    """
    threshold = _S1_THRESHOLD.get(market, 0.05)

    df     = daily_df.copy()
    closes = df["Close"]
    vols   = df["Volume"]

    df["ma_20"] = closes.rolling(20, min_periods=20).mean()
    df["ma_60"] = closes.rolling(60, min_periods=60).mean()

    signals: list[SignalRecord] = []

    # i=0는 change_pct 계산 불가, i<21은 20일 거래량 평균 불가
    for i in range(21, len(df)):
        row_date = df.index[i].date()
        if row_date < config.start or row_date > config.end:
            continue

        cur  = df.iloc[i]
        prev = df.iloc[i - 1]

        if pd.isna(cur["Close"]) or pd.isna(prev["Close"]) or float(prev["Close"]) <= 0:
            continue

        close_today = float(cur["Close"])
        close_prev  = float(prev["Close"])

        # 조건 1: 상승률
        change_pct = (close_today - close_prev) / close_prev
        if change_pct < threshold:
            continue

        # 조건 2: 거래량
        if pd.isna(cur["Volume"]):
            continue
        vol_today = float(cur["Volume"])
        avg_vol20 = float(vols.iloc[i - 20:i].mean())
        if avg_vol20 <= 0 or vol_today < 2.0 * avg_vol20:
            continue

        # 조건 3: MA20 / MA60
        if pd.isna(cur["ma_20"]) or pd.isna(cur["ma_60"]):
            continue
        if close_today <= float(cur["ma_20"]) or close_today <= float(cur["ma_60"]):
            continue

        # 조건 4: 52주 고점 괴리율
        closes_52 = closes.iloc[max(0, i - 251): i + 1].dropna()
        if closes_52.empty:
            continue
        week52_high = float(closes_52.max())
        if week52_high <= 0 or (week52_high - close_today) / week52_high > 0.20:
            continue

        # 조건 5: 수급 (외국인·기관 순매수)
        # flow_lookup이 제공됐고 해당 날짜 데이터가 있을 때만 필터링.
        # 데이터 없는 날짜는 통과 (과거 데이터 미수집 구간 보호).
        if flow_lookup is not None:
            flow = flow_lookup.get((ticker, row_date))
            if flow is not None:
                f_net, i_net = flow
                if not (
                    (f_net is not None and f_net > 0)
                    or (i_net is not None and i_net > 0)
                ):
                    continue

        signals.append(SignalRecord(
            ticker=ticker,
            name=name,
            signal_date=row_date,
            close_at_signal=close_today,
            mode="stage",
            market=market,
        ))

    return signals


def _apply_cross_filter(signals: list[SignalRecord]) -> list[SignalRecord]:
    """Cross 모드: 동일 ISO 주에 ichimoku + stage 모두 발동한 티커의 stage 신호 반환."""
    from collections import defaultdict

    # 이치모쿠 신호가 있는 (ticker, week) 집합
    ichimoku_weeks: dict[str, set[str]] = defaultdict(set)
    for s in signals:
        if s.mode == "ichimoku":
            ichimoku_weeks[s.ticker].add(_week_label(s.signal_date))

    # 동일 주에 이치모쿠도 있는 stage 신호만 cross로 승격
    # (ticker, week) 당 가장 이른 신호 1건만 유지 — 같은 주에 Stage 1이 복수 발동해도 1건
    seen: set[tuple[str, str]] = set()
    cross: list[SignalRecord] = []
    for s in sorted(signals, key=lambda x: x.signal_date):
        if s.mode != "stage":
            continue
        week = _week_label(s.signal_date)
        if week not in ichimoku_weeks.get(s.ticker, set()):
            continue
        key = (s.ticker, week)
        if key in seen:
            continue
        seen.add(key)
        cross.append(SignalRecord(
            ticker=s.ticker,
            name=s.name,
            signal_date=s.signal_date,
            close_at_signal=s.close_at_signal,
            mode="cross",
            market=s.market,
        ))

    return cross


def _replay_stage2(
    ticker: str,
    name: str,
    daily_df: pd.DataFrame,
    market: str,
    config: BacktestConfig,
) -> list[SignalRecord]:
    """Stage 1 신호 후 14 캘린더일 이내 Stage 2 조건 walk-forward 재현.

    Stage 2 조건 (S1 신호일 다음날부터 14일 이내 매일 검사):
      C1: close가 S1 종가 대비 −5% ~ −20% 되돌림 (0.80 ≤ close/s1_close ≤ 0.95)
      C2: close ≥ MA20 × 0.95
      C3: 거래량 비율(vol_today / vol_s1) ∈ [0.30, 0.60]  — 조용한 눌림목
      C4: 기관 연속 매수 — 과거 데이터 없음, 건너뜀 (3/4 조건 재현)

    S1 1건당 S2 최대 1건(첫 번째 충족일). 같은 날짜에 복수의 S1이 S2를 가리키면
    가장 이른 S1 기준 신호만 유지.
    """
    # S1 재현: S2 윈도우 확보를 위해 start를 21일 앞으로 확장
    s1_cfg = _dc_replace(config, mode="stage", start=config.start - timedelta(days=21))
    s1_signals = _replay_stage(ticker, name, daily_df, market, s1_cfg)
    if not s1_signals:
        return []

    # 날짜 → 행 인덱스 매핑
    idx_map: dict[date, int] = {}
    for i, ts in enumerate(daily_df.index):
        d = ts.date() if hasattr(ts, "date") else ts
        idx_map[d] = i

    df = daily_df.copy()
    df["ma_20"] = df["Close"].rolling(20, min_periods=20).mean()

    s2_signals: list[SignalRecord] = []
    seen_dates: set[date] = set()  # 동일 날짜 중복 신호 방지

    for s1 in s1_signals:
        s1_idx = idx_map.get(s1.signal_date)
        if s1_idx is None:
            continue

        s1_close = s1.close_at_signal
        if s1_close <= 0:
            continue

        s1_row = df.iloc[s1_idx]
        if pd.isna(s1_row["Volume"]):
            continue
        vol_s1 = float(s1_row["Volume"])
        if vol_s1 <= 0:
            continue

        cutoff = s1.signal_date + timedelta(days=14)

        for j in range(s1_idx + 1, len(df)):
            ts = df.index[j]
            row_date = ts.date() if hasattr(ts, "date") else ts
            if row_date > cutoff:
                break
            if row_date < config.start or row_date > config.end:
                continue
            if row_date in seen_dates:
                continue

            cur = df.iloc[j]
            if pd.isna(cur["Close"]) or pd.isna(cur["Volume"]):
                continue

            c_today = float(cur["Close"])
            v_today = float(cur["Volume"])
            ma20    = cur["ma_20"]

            # C1: -5% ~ -20% 되돌림
            if not (0.80 <= c_today / s1_close <= 0.95):
                continue
            # C2: close ≥ MA20 × 0.95
            if pd.isna(ma20) or c_today < float(ma20) * 0.95:
                continue
            # C3: 거래량 비율 [0.30, 0.60]
            if not (0.30 <= v_today / vol_s1 <= 0.60):
                continue
            # C4: 건너뜀

            seen_dates.add(row_date)
            s2_signals.append(SignalRecord(
                ticker=ticker,
                name=name,
                signal_date=row_date,
                close_at_signal=c_today,
                mode="stage2",
                market=market,
            ))
            break  # S1 1건당 S2 최대 1건

    return s2_signals


_STOP_LOSS_PCT: float = 0.08  # 기본 손절 기준 (−8%)


def _compute_sell_signals_and_s2(
    signals: list[SignalRecord],
    ohlcv_map: dict[str, "pd.DataFrame"],
    tx_cost_rt: float,
    stop_loss_pct: float = _STOP_LOSS_PCT,
) -> None:
    """매도 신호(MA20 이탈 / 손절) 및 S1→S2 진행일 인-플레이스 계산.

    매도 우선순위: 손절(close ≤ entry×(1−stop_loss_pct)) > MA20 이탈(close < MA20).
    같은 날에 두 조건이 동시에 충족되면 손절을 기록.
    S2 감지는 Stage 1(mode="stage") 신호에만 적용.
    """
    from collections import defaultdict

    by_ticker: dict[str, list[SignalRecord]] = defaultdict(list)
    for sig in signals:
        by_ticker[sig.ticker].append(sig)

    for ticker, sigs in by_ticker.items():
        raw_df = ohlcv_map.get(ticker)
        if raw_df is None:
            continue

        df = raw_df.copy()
        df["ma_20"]     = df["Close"].rolling(20, min_periods=20).mean()
        df["rsi_14"]    = _compute_rsi(df["Close"])
        df["avg_vol30"] = df["Volume"].rolling(30, min_periods=30).mean()
        df["high_10d"]  = df["High"].shift(1).rolling(10, min_periods=10).max()
        df["pct_chg"]   = df["Close"].pct_change(fill_method=None)

        idx_map: dict[date, int] = {}
        for i, ts in enumerate(df.index):
            d = ts.date() if hasattr(ts, "date") else ts
            idx_map[d] = i

        for sig in sigs:
            entry_idx = idx_map.get(sig.signal_date)
            if entry_idx is None:
                continue

            entry_price = sig.close_at_signal
            if entry_price <= 0:
                continue
            stop_price = entry_price * (1 - stop_loss_pct)

            s1_vol: float = 0.0
            if sig.mode == "stage":
                v = df.iloc[entry_idx]["Volume"]
                s1_vol = float(v) if not pd.isna(v) else 0.0

            s2_cutoff      = sig.signal_date + timedelta(days=14)
            mdd_window_end = sig.signal_date + timedelta(days=91)
            s2_found       = False
            peak_for_mdd   = entry_price
            max_dd_frac    = 0.0

            for j in range(entry_idx + 1, len(df)):
                ts       = df.index[j]
                row_date = ts.date() if hasattr(ts, "date") else ts
                cur      = df.iloc[j]

                if pd.isna(cur["Close"]):
                    continue

                close    = float(cur["Close"])
                vol      = float(cur["Volume"])  if not pd.isna(cur["Volume"])  else 0.0
                ma20     = float(cur["ma_20"])   if not pd.isna(cur["ma_20"])   else None
                rsi14    = float(cur["rsi_14"])  if not pd.isna(cur["rsi_14"])  else None
                avg30    = float(cur["avg_vol30"]) if not pd.isna(cur["avg_vol30"]) else None
                high10d  = float(cur["high_10d"]) if not pd.isna(cur["high_10d"]) else None
                pct_chg  = float(cur["pct_chg"]) if not pd.isna(cur["pct_chg"]) else None

                # MDD(91d) 추적
                if row_date <= mdd_window_end:
                    if close > peak_for_mdd:
                        peak_for_mdd = close
                    dd = (peak_for_mdd - close) / peak_for_mdd
                    if dd > max_dd_frac:
                        max_dd_frac = dd

                # S2 진행 감지 (Stage 1 신호 × 14일 이내)
                if sig.mode == "stage" and not s2_found and row_date <= s2_cutoff:
                    if ma20 is not None and s1_vol > 0:
                        ratio     = close / entry_price
                        vol_ratio = vol / s1_vol
                        if (0.80 <= ratio <= 0.95
                                and close >= ma20 * 0.95
                                and 0.30 <= vol_ratio <= 0.60):
                            sig.s2_date = row_date
                            s2_found    = True

                # S3 감지 (S2 이후, 조정 고점 돌파 + RSI≥70 + 거래량)
                # 조건 5(외인+기관 동시 순매수)는 과거 데이터 없어 건너뜀
                if (s2_found and sig.s3_date is None
                        and sig.s2_date is not None and row_date > sig.s2_date):
                    if (pct_chg  is not None and pct_chg  >= 0.05   # C2: +5%
                            and rsi14   is not None and rsi14   >= 70    # C3: RSI≥70
                            and high10d is not None and close > high10d  # C1: 10일 고가 돌파
                            and avg30   is not None and avg30   >  0
                            and vol >= 1.5 * avg30):                     # C4: 1.5× vol30
                        sig.s3_date = row_date

                # 매도 신호 (손절 > MA20 이탈)
                if sig.sell_date is None:
                    if close <= stop_price:
                        sig.sell_date   = row_date
                        sig.sell_reason = f"손절 -{stop_loss_pct * 100:.0f}%"
                        sig.sell_return = (close / entry_price - 1.0) - tx_cost_rt
                        sig.hold_days   = (row_date - sig.signal_date).days
                    elif ma20 is not None and close < ma20:
                        sig.sell_date   = row_date
                        sig.sell_reason = "MA20 이탈"
                        sig.sell_return = (close / entry_price - 1.0) - tx_cost_rt
                        sig.hold_days   = (row_date - sig.signal_date).days

                # 조기 종료: 매도 완료 + S2 윈도우 만료 + S3 완료(또는 S2 없음)
                past_s2_window = s2_found or row_date > s2_cutoff
                s3_done        = not s2_found or sig.s3_date is not None
                if sig.sell_date is not None and past_s2_window and s3_done:
                    break

            if sig.sell_date is None:
                sig.sell_reason = "보유 중"
            sig.mdd_91d = -max_dd_frac  # 음수 표기 (0이면 낙폭 없음)


def _fill_returns(
    sig: SignalRecord,
    stock_lookup: dict[date, float],
    kospi_lookup: dict[date, float],
    tx_cost_rt: float,
    hold_weeks: Optional[int] = None,
) -> None:
    """신호에 수익률 및 초과수익률 채우기 (거래비용 차감 포함).

    hold_weeks가 지정되면 N주(N*7일) 보유 수익률을 return_custom/excess_custom에도 채운다.
    표준 기간(1/4/13w)이더라도 return_custom에 중복 저장하므로 리포트 로직이 단순해진다.
    """
    base = sig.close_at_signal
    if base == 0:
        return

    def _ret(days: int) -> Optional[float]:
        price = _nearest_price(stock_lookup, sig.signal_date + timedelta(days=days))
        return (price / base - 1.0) - tx_cost_rt if price is not None else None

    def _kospi_ret(days: int) -> Optional[float]:
        k0 = _nearest_price(kospi_lookup, sig.signal_date)
        k1 = _nearest_price(kospi_lookup, sig.signal_date + timedelta(days=days))
        if k0 is None or k1 is None or k0 == 0:
            return None
        return k1 / k0 - 1.0

    sig.return_7d  = _ret(7)
    sig.return_28d = _ret(28)
    sig.return_91d = _ret(91)

    k7  = _kospi_ret(7)
    k28 = _kospi_ret(28)
    k91 = _kospi_ret(91)

    sig.excess_7d  = sig.return_7d  - k7  if sig.return_7d  is not None and k7  is not None else None
    sig.excess_28d = sig.return_28d - k28 if sig.return_28d is not None and k28 is not None else None
    sig.excess_91d = sig.return_91d - k91 if sig.return_91d is not None and k91 is not None else None

    if hold_weeks is not None:
        hold_days = hold_weeks * 7
        sig.return_custom = _ret(hold_days)
        kc = _kospi_ret(hold_days)
        sig.excess_custom = (
            sig.return_custom - kc
            if sig.return_custom is not None and kc is not None
            else None
        )


# ── 메인 실행 ─────────────────────────────────────────────────────

def run_backtest(config: BacktestConfig) -> BacktestResult:
    """백테스트 메인 함수. CLI 및 Telegram 봇에서 동기 호출."""
    from chart_screener import get_all_tickers

    logger.info(
        "[백테스트] 모드=%s 기간=%s~%s 시장=%s 최대티커=%s",
        config.mode, config.start, config.end, config.market, config.max_tickers or "전종목",
    )

    # 1. 업종 매핑 + 티커 목록
    from chart_screener import fetch_kind_sector_map
    sector_map  = fetch_kind_sector_map()
    all_tickers = get_all_tickers(sector_map=sector_map if sector_map else None)
    if config.market == "KOSPI":
        tickers = [(t, n, s) for t, n, s in all_tickers if t.endswith(".KS")]
    elif config.market == "KOSDAQ":
        tickers = [(t, n, s) for t, n, s in all_tickers if t.endswith(".KQ")]
    else:
        tickers = all_tickers

    if config.max_tickers > 0:
        tickers = tickers[:config.max_tickers]

    logger.info("[백테스트] 대상 티커 %d개", len(tickers))

    # 2. 데이터 수집 범위
    #   전방: MA120w(주봉 120주=840일) + 여유 → 2년(760일) lookback
    #   후방: 최대 보유 91일 + 여유 14일 (hold_weeks 지정 시 그 기간으로 확장)
    fetch_start = config.start - timedelta(days=760)
    hold_buffer = (config.hold_weeks * 7 + 14) if config.hold_weeks else 0
    fetch_end   = config.end + timedelta(days=max(105, hold_buffer))

    # 3. OHLCV 병렬 수집
    if config.dsn:
        from ohlcv_cache import batch_fetch_cached, fetch_index_cached
        ticker_pairs = [
            (t, "KOSDAQ" if t.endswith(".KQ") else "KOSPI")
            for t, _, _ in tickers
        ]
        ohlcv_map = batch_fetch_cached(
            ticker_pairs, fetch_start, fetch_end,
            config.workers, config.dsn, _fetch_single_ohlcv,
        )
        kospi_df = fetch_index_cached(
            "^KS11", "IDX", fetch_start, fetch_end,
            config.dsn, _fetch_index,
        )
    else:
        ticker_syms = [t for t, _, _ in tickers]
        ohlcv_map   = _batch_fetch_ohlcv(ticker_syms, fetch_start, fetch_end, config.workers)
        kospi_df    = _fetch_index("^KS11", fetch_start, fetch_end)

    # 4. KOSPI 벤치마크 조회
    kospi_lookup = _build_price_lookup(kospi_df) if kospi_df is not None else {}

    # 5. 수급 데이터 사전 로드 (stage/cross 모드 + DSN 있을 때)
    flow_lookup: Optional[dict] = None
    if config.dsn and config.mode in ("stage", "cross"):
        try:
            from ohlcv_cache import load_flow_data
            ticker_syms = [t for t, _, _ in tickers]
            flow_lookup = load_flow_data(config.dsn, ticker_syms, config.start, config.end)
            logger.info("[백테스트] 수급 데이터 로드: %d건", len(flow_lookup))
        except Exception as e:
            logger.warning("[백테스트] 수급 데이터 로드 실패 (조건 5 생략): %s", e)

    # 6. 신호 재현
    all_signals: list[SignalRecord] = []

    for ticker, name, _ in tickers:
        df = ohlcv_map.get(ticker)
        if df is None or df.empty:
            continue
        mkt = "KOSDAQ" if ticker.endswith(".KQ") else "KOSPI"

        if config.mode in ("ichimoku", "cross"):
            all_signals.extend(_replay_ichimoku(ticker, name, df, mkt, config))
        if config.mode in ("stage", "cross"):
            all_signals.extend(_replay_stage(ticker, name, df, mkt, config, flow_lookup))
        if config.mode == "stage2":
            all_signals.extend(_replay_stage2(ticker, name, df, mkt, config))

    # 7. Cross 필터
    if config.mode == "cross":
        all_signals = _apply_cross_filter(all_signals)

    # 8. 수익률 계산
    stock_lookup_cache: dict[str, dict[date, float]] = {}
    for sig in all_signals:
        df = ohlcv_map.get(sig.ticker)
        if df is None:
            continue
        if sig.ticker not in stock_lookup_cache:
            stock_lookup_cache[sig.ticker] = _build_price_lookup(df)
        _fill_returns(sig, stock_lookup_cache[sig.ticker], kospi_lookup, config.tx_cost_rt, config.hold_weeks)

    # 8.5. 업종 정보 주입
    ticker_sector: dict[str, str] = {t: s for t, _n, s in tickers}
    for sig in all_signals:
        sig.sector = ticker_sector.get(sig.ticker, "")

    # 8.6. 매도 신호·MDD·S2/S3 진행일 계산
    _compute_sell_signals_and_s2(all_signals, ohlcv_map, config.tx_cost_rt)

    # 9. 날짜 순 정렬 → MDD equity curve가 시간 순서대로 누적
    all_signals.sort(key=lambda s: s.signal_date)

    # 10. 집계 지표
    overall = _compute_group_metrics(all_signals, config.rf_rate_annual, config.hold_weeks)

    note = ""
    if config.mode in ("stage", "cross"):
        if flow_lookup is not None:
            note = f"Stage 1: 수급 조건(외국인·기관 순매수) 적용 — {len(flow_lookup)}건 기준"
        else:
            note = "Stage 1: 수급 조건(외국인·기관 순매수) 제외 — daily_flow 데이터 없음"
    elif config.mode == "stage2":
        note = "Stage 2: 기관 연속 매수(C4) 제외 — 과거 수급 데이터 없음 (3/4 조건 재현)"

    logger.info(
        "[백테스트] 완료 — 신호:%d 승률28d:%s",
        overall.n,
        f"{overall.win_rate_28d * 100:.1f}%" if overall.win_rate_28d is not None else "N/A",
    )

    return BacktestResult(
        config=config,
        signals=all_signals,
        overall=overall,
        computed_at=datetime.now(_KST).isoformat(),
        note=note,
    )
