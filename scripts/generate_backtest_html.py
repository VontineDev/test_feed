"""
generate_backtest_html.py  —  백테스트 결과 HTML 리포트 생성

Input:  summary dict from chart_backtest.build_summary()
Output: complete HTML string  (caller writes to file)
"""
from __future__ import annotations

import html as _html
import math
from datetime import datetime
from typing import Any, Optional
from zoneinfo import ZoneInfo

_KST = ZoneInfo("Asia/Seoul")

_CSS = """
:root {
  --bg:           #0d1117;
  --bg-surface:   #161b22;
  --bg-hover:     #1c2128;
  --text-primary: #e6edf3;
  --text-muted:   #7d8590;
  --border:       #30363d;
  --accent:       #58a6ff;
  --green:        #3fb950;
  --red:          #f85149;
  --gold:         #d29922;
  --font-ui: system-ui, -apple-system, 'Segoe UI', sans-serif;
  --font-mono: 'JetBrains Mono', 'Consolas', monospace;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  background: var(--bg);
  color: var(--text-primary);
  font-family: var(--font-ui);
  font-size: 14px;
  line-height: 1.5;
  padding: 24px;
}
h1 { font-size: 20px; font-weight: 600; margin-bottom: 4px; }
.meta { color: var(--text-muted); font-size: 13px; margin-bottom: 24px; }
.meta span { margin-right: 16px; }
.section-title {
  font-size: 15px;
  font-weight: 600;
  color: var(--accent);
  margin: 28px 0 10px;
  border-bottom: 1px solid var(--border);
  padding-bottom: 6px;
}
.period-label { color: var(--text-muted); font-size: 12px; margin-left: 8px; font-weight: 400; }
table {
  width: 100%;
  border-collapse: collapse;
  font-size: 13px;
  margin-bottom: 8px;
}
thead tr { background: var(--bg-surface); }
th {
  text-align: right;
  padding: 8px 12px;
  font-weight: 500;
  color: var(--text-muted);
  border-bottom: 1px solid var(--border);
  white-space: nowrap;
}
th:first-child { text-align: left; }
td {
  text-align: right;
  padding: 7px 12px;
  border-bottom: 1px solid var(--border);
  font-variant-numeric: tabular-nums;
}
td:first-child { text-align: left; font-weight: 500; }
tr:hover td { background: var(--bg-hover); }
.kospi-row td { color: var(--text-muted); }
.pos { color: var(--green); }
.neg { color: var(--red); }
.neu { color: var(--text-muted); }
.chip {
  display: inline-block;
  padding: 2px 7px;
  border-radius: 12px;
  font-size: 11px;
  font-weight: 600;
  font-family: var(--font-mono);
}
.chip-all    { background:#1a2332; color: var(--accent); }
.chip-gap    { background:#1a331a; color: var(--green); }
.chip-reg    { background:#2a2a2a; color: var(--text-muted); }
.chip-kospi  { background:#2a2a1a; color: var(--gold); }
.low-sample  { color: var(--gold); font-size: 11px; margin-left: 4px; }
.sparkline-wrap {
  margin-top: 4px;
}
.spark-row {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 6px;
}
.spark-label {
  width: 60px;
  font-size: 11px;
  color: var(--text-muted);
  flex-shrink: 0;
}
.spark-bars { display: flex; gap: 2px; align-items: flex-end; height: 32px; }
.spark-bar {
  width: 14px;
  border-radius: 2px 2px 0 0;
  min-height: 2px;
}
.spark-bar.pos { background: var(--green); opacity: 0.8; }
.spark-bar.neg { background: var(--red); opacity: 0.8; }
.disclaimer {
  margin-top: 32px;
  padding: 12px;
  background: var(--bg-surface);
  border-radius: 6px;
  color: var(--text-muted);
  font-size: 12px;
  line-height: 1.6;
}
"""


def _e(s: Any) -> str:
    return _html.escape(str(s))


def _pct(v: Optional[float], sign: bool = True) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "<span class='neu'>—</span>"
    pct = v * 100
    cls = "pos" if pct > 0 else ("neg" if pct < 0 else "neu")
    prefix = "+" if sign and pct > 0 else ""
    return f"<span class='{cls}'>{prefix}{pct:.1f}%</span>"


def _rate(v: Optional[float]) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "<span class='neu'>—</span>"
    pct = v * 100
    cls = "pos" if pct >= 55 else ("neg" if pct < 45 else "neu")
    return f"<span class='{cls}'>{pct:.0f}%</span>"


def _count(n: int) -> str:
    low = n < 20
    s = f"<span>{n:,}</span>"
    if low:
        s += "<span class='low-sample'>※</span>"
    return s


def _make_table(groups: dict, show_kospi: dict | None = None) -> str:
    rows = []
    chips = {
        "전체": ("chip-all", "전체"),
        "정배열": ("chip-gap", "정배열"),
        "일반": ("chip-reg", "일반"),
    }

    for key in ("전체", "정배열", "일반"):
        g = groups.get(key, {})
        n = g.get("signal_count", 0)
        chip_cls, chip_label = chips[key]

        wr1 = _rate(g.get("win_rate_1w"))
        wr4 = _rate(g.get("win_rate_4w"))
        ar1 = _pct(g.get("avg_return_1w"))
        ar4 = _pct(g.get("avg_return_4w"))
        ex4 = _pct(g.get("excess_return_4w"))

        rows.append(
            f"<tr>"
            f"<td><span class='chip {chip_cls}'>{_e(chip_label)}</span></td>"
            f"<td>{_count(n)}</td>"
            f"<td>{wr1}</td>"
            f"<td>{wr4}</td>"
            f"<td>{ar1}</td>"
            f"<td>{ar4}</td>"
            f"<td>{ex4}</td>"
            f"</tr>"
        )

    if show_kospi is not None:
        k1 = _pct(show_kospi.get("return_1w"))
        k4 = _pct(show_kospi.get("return_4w"))
        rows.append(
            f"<tr class='kospi-row'>"
            f"<td><span class='chip chip-kospi'>KOSPI</span></td>"
            f"<td>—</td><td>—</td><td>—</td>"
            f"<td>{k1}</td>"
            f"<td>{k4}</td>"
            f"<td>—</td>"
            f"</tr>"
        )

    header = (
        "<thead><tr>"
        "<th>구분</th><th>신호수</th>"
        "<th>1주 승률</th><th>4주 승률</th>"
        "<th>1주 평균</th><th>4주 평균</th><th>4주 초과수익</th>"
        "</tr></thead>"
    )
    return f"<table>{header}<tbody>{''.join(rows)}</tbody></table>"


def _make_sparkline(monthly_data: dict) -> str:
    months = sorted(monthly_data.keys())
    if not months:
        return ""

    series: dict[str, list[tuple[str, Optional[float]]]] = {
        "전체": [],
        "정배열": [],
    }

    for m in months:
        mdata = monthly_data[m]
        groups = mdata.get("groups", {})
        label = mdata.get("label", m)
        for key in ("전체", "정배열"):
            g = groups.get(key, {})
            series[key].append((label, g.get("avg_return_4w")))

    def _bars(values: list[tuple[str, Optional[float]]]) -> str:
        nums = [v for _, v in values if v is not None]
        if not nums:
            return "<span style='color:var(--text-muted);font-size:11px'>데이터 없음</span>"
        max_abs = max(abs(n) for n in nums) or 1
        bars = []
        for label, v in values:
            if v is None:
                h = 4
                cls = "neu"
                style = f"height:{h}px;background:var(--border);"
            else:
                h = max(2, int(abs(v) / max_abs * 28))
                cls = "pos" if v > 0 else "neg"
                style = f"height:{h}px;"
            title = f"{label}: {v * 100:+.1f}%" if v is not None else f"{label}: —"
            bars.append(f"<div class='spark-bar {cls}' style='{style}' title='{_e(title)}'></div>")
        return f"<div class='spark-bars'>{''.join(bars)}</div>"

    rows_html = []
    labels_map = {"전체": "전체", "정배열": "정배열"}
    for key, label in labels_map.items():
        rows_html.append(
            f"<div class='spark-row'>"
            f"<span class='spark-label'>{_e(label)}</span>"
            f"{_bars(series[key])}"
            f"</div>"
        )

    month_ticks = "  ".join(
        f"<span style='font-size:10px;color:var(--text-muted)'>{m[5:]}</span>"
        for m in months
    )

    return (
        "<div class='sparkline-wrap'>"
        + "".join(rows_html)
        + f"<div style='margin-left:68px;margin-top:2px;display:flex;gap:2px;flex-wrap:wrap'>{month_ticks}</div>"
        + "</div>"
    )


def generate_backtest_html(summary: dict) -> str:
    generated_at = summary.get("generated_at", "")
    effective_weeks = summary.get("effective_weeks", 0)
    months = summary.get("months", {})
    annual = summary.get("annual", {})

    try:
        ts = datetime.fromisoformat(generated_at).astimezone(_KST).strftime("%Y-%m-%d %H:%M KST")
    except Exception:
        ts = generated_at

    total_signals = annual.get("groups", {}).get("전체", {}).get("signal_count", 0)

    # 최근 1개월 — 마지막 월 데이터
    latest_month_key = max(months.keys()) if months else None
    latest_month = months.get(latest_month_key, {}) if latest_month_key else {}
    latest_label = latest_month.get("label", "최근 1개월")
    latest_period = latest_month.get("period", "")
    latest_groups = latest_month.get("groups", {})

    # KOSPI 기준 수익률은 excess에서 역산하지 않고 '—'으로 표시
    # (KOSPI 원시 수익률은 현재 summary에 없음)

    annual_label = annual.get("label", f"최근 1년 ({effective_weeks}주 분석)")
    annual_period = annual.get("period", "")
    annual_groups = annual.get("groups", {})

    spark_html = _make_sparkline(months)

    body = f"""
<h1>주봉 스크리닝 백테스트</h1>
<div class='meta'>
  <span>최종 업데이트: {_e(ts)}</span>
  <span>실제 분석 기간: {_e(effective_weeks)}주</span>
  <span>분석 신호 수: {_e(f"{total_signals:,}")}건</span>
</div>

<div class='section-title'>
  최근 1개월 ({_e(latest_label)})
  <span class='period-label'>{_e(latest_period)}</span>
</div>
{_make_table(latest_groups)}

<div class='section-title'>
  {_e(annual_label)}
  <span class='period-label'>{_e(annual_period)}</span>
</div>
{_make_table(annual_groups)}

<div class='section-title'>월별 4주 평균 수익률 추이</div>
{spark_html}

<div class='disclaimer'>
  <strong>주의사항</strong><br>
  ・ 실제 분석 기간은 yfinance 3Y 데이터 + 120주 이동평균 최소 요건으로 약 {_e(effective_weeks)}주입니다. "1년" 레이블은 목표 기간이며 데이터 축적에 따라 자동 연장됩니다.<br>
  ・ 백테스트는 과거 데이터 기반으로 미래 수익을 보장하지 않습니다. 거래 비용, 슬리피지, 유동성은 미반영입니다.<br>
  ・ 초과수익률 = 신호 종목 수익률 − KOSPI(^KS11) 동기간 수익률. KOSPI 데이터 미조회 시 '—' 표시.<br>
  ・ ※ 표시는 해당 월 신호 수가 20건 미만으로 통계적 신뢰도가 낮습니다.
</div>
"""

    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>주봉 스크리닝 백테스트</title>
<style>{_CSS}</style>
</head>
<body>{body}</body>
</html>"""
