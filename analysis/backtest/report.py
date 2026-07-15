"""백테스트 리포트 빌더 (models.BacktestResult 메서드에서 추출, Phase C).

텔레그램/텍스트/Top-Bottom/HTML 4종. BacktestResult는 얇은 위임 메서드로
기존 인터페이스(result.to_telegram_report() 등)를 유지한다.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import pandas as pd

from analysis.backtest.config import MODE_KOR

if TYPE_CHECKING:
    from analysis.backtest.models import BacktestResult, SignalRecord  # noqa: F401


def to_telegram_report(result: "BacktestResult") -> str:
    """텔레그램 전송용 요약 리포트 (4096자 이내)."""
    cfg = result.config
    m   = result.overall

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
        _exit_label = (
            f"분할청산 (TP1 {cfg.tp1_pct*100:.0f}% / 트레일 {cfg.trail_pct*100:.0f}% / 손절 {cfg.hard_stop_pct*100:.0f}%)"
            if cfg.tp1_pct > 0 or cfg.trail_pct > 0
            else f"매도 신호 (MA20 이탈 / 손절 {cfg.hard_stop_pct*100:.0f}%)"
        )
        lines += [
            "",
            _exit_label,
            f"  승률: {pct(m.win_rate_sell)}  평균: {pct(m.avg_return_sell)}  보유: {val(m.avg_hold_days, 1)}d",
        ]
    if m.s2_progression_rate is not None:
        lines.append(f"  S1→S2 진행: {pct(m.s2_progression_rate, 1)}")
    if m.s3_progression_rate is not None:
        lines.append(f"  S2→S3 진행: {pct(m.s3_progression_rate, 1)}")
    if result.note:
        lines += ["", f"⚠ {result.note}"]
    return "\n".join(lines)

def to_text_report(result: "BacktestResult") -> str:
    """CLI 출력용 상세 텍스트 리포트."""
    cfg = result.config
    m   = result.overall

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
        f"  산출일시: {result.computed_at}",
    ]
    if result.note:
        lines += ["", f"주의: {result.note}"]
    lines.append("=" * 62)
    return "\n".join(lines)

def top_bottom_telegram_text(result: "BacktestResult", n: int = 5) -> str:
    """Top/bottom N stocks by 28d return — plain text for Telegram append."""
    scored = [(s, s.return_28d) for s in result.signals if s.return_28d is not None]
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

def to_html_report(result: "BacktestResult", output_path: str) -> str:
    """HTML 리포트 생성 + 저장. 종목별 수익률 테이블 + 분포 차트 포함. 파일 경로 반환."""
    import html as _html
    from pathlib import Path

    cfg = result.config
    m   = result.overall

    def pct(v: Optional[float], dp: int = 1) -> str:
        return f"{v * 100:+.{dp}f}%" if v is not None else "—"

    def fmt(v: Optional[float], dp: int = 2) -> str:
        return f"{v:.{dp}f}" if v is not None else "—"

    mode_kor = MODE_KOR.get(cfg.mode, cfg.mode)

    # ── 28d 분포 SVG ─────────────────────────────────────────
    r28s = [s.return_28d for s in result.signals if s.return_28d is not None]
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

    _STAGE_LABEL = {
        "stage":      ("S1",   "s1"),
        "stage2":     ("S2",   "s2"),
        "stage_v11":  ("S1.1", "s1"),
        "stage2_v11": ("S2.1", "s2"),
        "stage_v12":  ("S1.2", "s1"),
        "stage2_v12": ("S2.2", "s2"),
        "stage_v13":  ("S1.3", "s1"),
        "stage2_v13": ("S2.3", "s2"),
        "cross":      ("교차", "cross"),
        "ichimoku":   ("이치", "ichi"),
    }
    _SELL_CLS    = {
        "MA20 이탈": "sell-ma",
        "손절":      "sell-sl",
        "구름 이탈": "sell-cloud",
        "전환<기준": "sell-dc",
    }

    def _mdd_td(v: Optional[float]) -> str:
        if v is None:
            return '<td class="num muted" data-v="-1">—</td>'
        dv  = v * 10000
        cls = "neg" if v < 0 else ""
        txt = f"{v * 100:.1f}%"
        return f'<td class="num {cls}" data-v="{dv}">{txt}</td>'

    # ── 현재가 일괄 조회 ─────────────────────────────────────────
    import yfinance as yf
    cur_prices: dict[str, float] = {}
    _unique_tks = list({s.ticker for s in result.signals if s.ticker})
    if _unique_tks:
        try:
            _raw = yf.download(_unique_tks, period="5d", progress=False,
                               auto_adjust=True)
            if not _raw.empty and "Close" in _raw.columns:
                _close = _raw["Close"]
                if isinstance(_close, pd.Series):
                    _s = _close.dropna()
                    if not _s.empty:
                        cur_prices[_unique_tks[0]] = float(_s.iloc[-1])
                else:
                    for _tk in _unique_tks:
                        if _tk in _close.columns:
                            _s = _close[_tk].dropna()
                            if not _s.empty:
                                cur_prices[_tk] = float(_s.iloc[-1])
        except Exception:
            pass

    # ── 필터용 고유값 수집 ─────────────────────────────────────
    _uniq_sectors = sorted(
        {s.sector[:12] for s in result.signals if s.sector}, key=str.lower
    )
    _uniq_exits = sorted(
        {(s.final_exit_type if s.final_exit_type else s.sell_reason)
         for s in result.signals
         if (s.final_exit_type or s.sell_reason)},
        key=str.lower,
    )
    _uniq_stages = sorted(
        {_STAGE_LABEL.get(s.mode, (s.mode, ""))[0] for s in result.signals}
    )

    table_rows: list[str] = []
    for sig in result.signals:
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

        # 청산 유형 — blended 모드면 final_exit_type, 아니면 sell_reason
        exit_type = sig.final_exit_type if sig.final_exit_type else sig.sell_reason
        exit_cls  = next((v for k, v in _SELL_CLS.items() if exit_type.startswith(k)), "")
        exit_td   = (f'<td><span class="badge {exit_cls}">{_html.escape(exit_type)}</span></td>'
                     if exit_cls else f'<td class="muted">{_html.escape(exit_type)}</td>')

        # 1차 TP 날짜
        tp1_td = (f'<td data-v="{sig.tp1_date.isoformat()}">{sig.tp1_date}</td>'
                  if sig.tp1_date else '<td class="muted" data-v="">—</td>')

        # 최종 청산일 (final_exit_date 우선, 없으면 sell_date)
        final_date = sig.final_exit_date or sig.sell_date
        final_sd_td = (f'<td data-v="{final_date.isoformat()}">{final_date}</td>'
                       if final_date else '<td class="muted" data-v="">—</td>')

        hd_td = (f'<td class="num" data-v="{sig.hold_days}">{sig.hold_days}d</td>'
                 if sig.hold_days is not None else '<td class="num muted" data-v="-1">—</td>')

        # 최종 수익: blended_return 우선, 없으면 sell_return
        blended = sig.blended_return if sig.blended_return is not None else sig.sell_return

        # 현재가 / 현재손익
        _cur = cur_prices.get(sig.ticker)
        _cur_fmt = f"{_cur:,.0f}" if _cur is not None else "—"
        _cur_dv  = int(_cur) if _cur is not None else -1
        _cur_ret = (_cur / sig.close_at_signal - 1) if _cur is not None else None

        table_rows.append(
            f"<tr>"
            f'<td data-v="{sig.signal_date.isoformat()}">{sig.signal_date}</td>'
            f'<td>{_html.escape(sig.name)}</td>'
            + sector_td
            + f'<td class="muted">{sig.ticker} <span class="badge">{mkt_b}</span></td>'
            + stage_td
            + f'<td class="num" data-v="{sig.close_at_signal}">{ep}</td>'
            + f'<td class="num" data-v="{_cur_dv}">{_cur_fmt}</td>'
            + _ret_td(_cur_ret)
            + _ret_td(sig.return_7d)
            + _ret_td(sig.return_28d)
            + _ret_td(sig.return_91d)
            + _ret_td(sig.excess_28d)
            + s2_td
            + s3_td
            + exit_td
            + tp1_td
            + final_sd_td
            + hd_td
            + _ret_td(sig.tp1_ret)
            + _ret_td(blended)
            + _mdd_td(sig.mdd_91d)
            + "</tr>"
        )

    # CSS class helpers for summary cards
    def _cls(v: Optional[float], threshold: float = 0.0) -> str:
        return "pos" if (v or 0.0) >= threshold else "neg"

    n_sigs  = len(result.signals)
    note_esc = _html.escape(result.note) if result.note else ""

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
.sell-cloud{{color:#79c0ff;border-color:#79c0ff}} .sell-dc{{color:#d29922;border-color:#d29922}}
.tbl-wrap{{overflow-x:auto}}
footer{{margin-top:24px;padding-top:12px;border-top:1px solid var(--border);color:var(--muted);font-size:11px}}
.filter-bar{{background:var(--bg-s);border:1px solid var(--border);border-radius:6px;padding:10px 14px;margin-bottom:10px}}
.filter-row{{display:flex;flex-wrap:wrap;gap:10px;align-items:flex-end}}
.fi{{display:flex;flex-direction:column;gap:3px}}
.fi label{{font-size:11px;color:var(--muted)}}
.fi input,.fi select{{background:var(--bg);border:1px solid var(--border);color:var(--text);border-radius:4px;padding:3px 7px;font-size:12px;font-family:var(--mono);min-width:110px;transition:border-color .15s}}
.fi input:focus,.fi select:focus{{outline:none;border-color:var(--accent)}}
.fi input.fi-on,.fi select.fi-on{{border-color:var(--accent);background:rgba(88,166,255,.06)}}
.stg-btns,.wl-btns{{display:flex;gap:4px}}
.stg-btn,.wl-btn{{background:var(--bg);border:1px solid var(--border);color:var(--muted);border-radius:4px;padding:3px 10px;cursor:pointer;font-size:12px;font-family:var(--mono);transition:all .15s}}
.stg-btn.stg-on{{border-color:var(--accent);color:var(--accent)}}
.wl-btn.wl-on.wl-all{{border-color:var(--muted);color:var(--text)}}
.wl-btn.wl-on.wl-win{{border-color:var(--pos);color:var(--pos);background:rgba(63,185,80,.08)}}
.wl-btn.wl-on.wl-loss{{border-color:var(--neg);color:var(--neg);background:rgba(248,81,73,.08)}}
.btn-reset{{background:var(--bg);border:1px solid var(--border);color:var(--muted);border-radius:4px;padding:4px 12px;cursor:pointer;font-size:12px;align-result:flex-end}}
.btn-reset:hover{{border-color:var(--accent);color:var(--accent)}}
.result-info{{align-result:flex-end;font-family:var(--mono);line-height:1.2}}
#vis-count{{font-size:20px;font-weight:700;color:var(--accent)}}
.cnt-tot{{font-size:11px;color:var(--muted)}}
</style>
</head>
<body>
<header>
  <h1>백테스트 리포트 &nbsp;│&nbsp; {_html.escape(mode_kor)}</h1>
  <p class="meta">기간: {cfg.start} ~ {cfg.end} &nbsp;│&nbsp; 시장: {cfg.market} &nbsp;│&nbsp; 신호: {m.n}건 &nbsp;│&nbsp; 거래비용(RT): {cfg.tx_cost_rt * 100:.3f}% &nbsp;│&nbsp; 산출: {result.computed_at[:16]}</p>
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
<div class="filter-bar">
<div class="filter-row">
  <div class="fi"><label>종목명</label><input type="text" id="f-name" placeholder="검색..." oninput="applyFilter()"></div>
  <div class="fi"><label>업종</label><select id="f-sector" onchange="applyFilter()"><option value="">전체</option>{''.join(f'<option>{_html.escape(s)}</option>' for s in _uniq_sectors)}</select></div>
  {'<div class="fi"><label>신호유형</label><div class="stg-btns">' + "".join(f'<button class="stg-btn stg-on" data-stg="{_html.escape(sl)}" onclick="toggleStage(this)">{_html.escape(sl)}</button>' for sl in _uniq_stages) + "</div></div>" if len(_uniq_stages) > 1 else ""}
  <div class="fi"><label>수익 / 손실</label><div class="wl-btns"><button class="wl-btn wl-on wl-all" data-wl="" onclick="setWL(this)">전체</button><button class="wl-btn wl-win" data-wl="win" onclick="setWL(this)">수익</button><button class="wl-btn wl-loss" data-wl="loss" onclick="setWL(this)">손실</button></div></div>
  <div class="fi"><label>청산유형</label><select id="f-exit" onchange="applyFilter()"><option value="">전체</option>{''.join(f'<option>{_html.escape(e)}</option>' for e in _uniq_exits)}</select></div>
  <div class="fi"><label>최종수익(%)</label><div style="display:flex;gap:4px;align-items:center"><input type="number" id="f-ret-min" step="1" placeholder="최소" oninput="applyFilter()" style="width:65px"><span style="color:var(--muted)">~</span><input type="number" id="f-ret-max" step="1" placeholder="최대" oninput="applyFilter()" style="width:65px"></div></div>
  <div class="fi"><label>보유일</label><div style="display:flex;gap:4px;align-items:center"><input type="number" id="f-hd-min" step="1" placeholder="최소" oninput="applyFilter()" style="width:60px"><span style="color:var(--muted)">~</span><input type="number" id="f-hd-max" step="1" placeholder="최대" oninput="applyFilter()" style="width:60px"></div></div>
  <button class="btn-reset" onclick="resetFilter()">초기화</button>
  <div class="result-info"><span id="vis-count">{n_sigs}</span><br><span class="cnt-tot">/ {n_sigs}건 표시</span></div>
</div>
</div>
<div class="tbl-wrap">
<table id="tbl" data-sc="-1" data-sd="asc">
<thead><tr>
  <th onclick="sort(0,false)">신호일</th>
  <th onclick="sort(1,false)">종목명</th>
  <th onclick="sort(2,false)">업종</th>
  <th onclick="sort(3,false)">티커</th>
  <th onclick="sort(4,false)">단계</th>
  <th class="num" onclick="sort(5,true)">진입가</th>
  <th class="num" onclick="sort(6,true)">현재가</th>
  <th class="num" onclick="sort(7,true)">현재손익</th>
  <th class="num" onclick="sort(8,true)">7d</th>
  <th class="num" onclick="sort(9,true)">28d</th>
  <th class="num" onclick="sort(10,true)">91d</th>
  <th class="num" onclick="sort(11,true)">초과(28d)</th>
  <th onclick="sort(12,false)">S2진행일</th>
  <th onclick="sort(13,false)">S3진행일</th>
  <th onclick="sort(14,false)">청산유형</th>
  <th onclick="sort(15,false)">1차TP일</th>
  <th onclick="sort(16,false)">최종청산일</th>
  <th class="num" onclick="sort(17,true)">보유일</th>
  <th class="num" onclick="sort(18,true)">1차TP수익</th>
  <th class="num" onclick="sort(19,true)">최종수익(blended)</th>
  <th class="num" onclick="sort(20,true)">MDD(91d)</th>
</tr></thead>
<tbody>
{chr(10).join(table_rows)}
</tbody>
</table>
</div>
<footer>생성: {result.computed_at}{(' &nbsp;│&nbsp; ' + note_esc) if note_esc else ''}</footer>
<script>
function sort(col,num){{
  const t=document.getElementById('tbl');
  const p=parseInt(t.dataset.sc);
  const d=(p===col&&t.dataset.sd==='asc')?'desc':'asc';
  t.dataset.sc=col; t.dataset.sd=d;
  const rows=[...t.querySelectorAll('tbody tr')].filter(r=>r.style.display!=='none');
  const hidden=[...t.querySelectorAll('tbody tr')].filter(r=>r.style.display==='none');
  rows.sort((a,b)=>{{
const av=a.cells[col].dataset.v??a.cells[col].textContent.trim();
const bv=b.cells[col].dataset.v??b.cells[col].textContent.trim();
const c=num?(parseFloat(av)||-1e9)-(parseFloat(bv)||-1e9):av.localeCompare(bv,'ko');
return d==='asc'?c:-c;
  }});
  const tb=t.querySelector('tbody');
  rows.forEach(r=>tb.appendChild(r));
  hidden.forEach(r=>tb.appendChild(r));
  t.querySelectorAll('th').forEach((h,i)=>{{
h.textContent=h.textContent.replace(/ [▲▼]$/,'');
if(i===col)h.textContent+=d==='asc'?' ▲':' ▼';
  }});
}}
let _wl='';
function setWL(btn){{
  _wl=btn.dataset.wl;
  document.querySelectorAll('.wl-btn').forEach(b=>b.classList.remove('wl-on'));
  btn.classList.add('wl-on');
  applyFilter();
}}
function toggleStage(btn){{
  btn.classList.toggle('stg-on');
  applyFilter();
}}
function applyFilter(){{
  const name=(document.getElementById('f-name').value||'').toLowerCase();
  const sector=document.getElementById('f-sector').value;
  const exit=document.getElementById('f-exit').value;
  const stages=[...document.querySelectorAll('.stg-btn.stg-on')].map(b=>b.dataset.stg);
  const allStages=document.querySelectorAll('.stg-btn').length===0;
  const retMin=document.getElementById('f-ret-min').value;
  const retMax=document.getElementById('f-ret-max').value;
  const hdMin=document.getElementById('f-hd-min').value;
  const hdMax=document.getElementById('f-hd-max').value;
  ['f-name','f-sector','f-exit','f-ret-min','f-ret-max','f-hd-min','f-hd-max'].forEach(id=>{{
const el=document.getElementById(id);
if(el)el.classList.toggle('fi-on',el.value!=='');
  }});
  let vis=0;
  document.querySelectorAll('#tbl tbody tr').forEach(row=>{{
const c=row.cells;
const rv=(parseFloat(c[19].dataset.v)||-1e9)/10000*100;
const ok=(
  (!name||c[1].textContent.toLowerCase().includes(name))&&
  (!sector||c[2].textContent.trim()===sector)&&
  (allStages||stages.length===0||stages.some(s=>c[4].textContent.includes(s)))&&
  (!exit||c[14].textContent.includes(exit))&&
  (!_wl||(_wl==='win'&&rv>0)||(_wl==='loss'&&rv<=0))&&
  (retMin===''||rv>=parseFloat(retMin))&&
  (retMax===''||rv<=parseFloat(retMax))&&
  (hdMin===''||(parseFloat(c[17].dataset.v)||0)>=parseFloat(hdMin))&&
  (hdMax===''||(parseFloat(c[17].dataset.v)||0)<=parseFloat(hdMax))
);
row.style.display=ok?'':'none';
if(ok)vis++;
  }});
  document.getElementById('vis-count').textContent=vis;
}}
function resetFilter(){{
  _wl='';
  document.getElementById('f-name').value='';
  document.getElementById('f-sector').value='';
  document.getElementById('f-exit').value='';
  document.querySelectorAll('.stg-btn').forEach(b=>b.classList.add('stg-on'));
  document.querySelectorAll('.wl-btn').forEach(b=>b.classList.remove('wl-on'));
  const allBtn=document.querySelector('.wl-btn.wl-all');
  if(allBtn)allBtn.classList.add('wl-on');
  ['f-ret-min','f-ret-max','f-hd-min','f-hd-max'].forEach(id=>document.getElementById(id).value='');
  applyFilter();
}}
</script>
</body>
</html>"""

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_text(html_out, encoding="utf-8")
    return output_path

