"""
generate_html_report.py — 주봉 차트 스크리닝 HTML 리포트 생성

Input:  list[ScreenResult] from run_weekly_screen()
Output: complete HTML string  (caller writes to file)
"""
from __future__ import annotations

import base64
import html as _html
import logging
import math
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from analysis.chart_screener import ScreenResult, current_week_of
from analysis.screener_filters import ALL_PRESETS, filter_summary

logger = logging.getLogger(__name__)
_KST = ZoneInfo("Asia/Seoul")

_BUNDLE_DIR = Path(__file__).parent / "web" / "screener_design"
_CSS_FILE = _BUNDLE_DIR / "colors_and_type.css"
_FONT_DIR = _BUNDLE_DIR / "fonts"

_FALLBACK_CSS = """
/* fallback — design bundle (web/screener_design/colors_and_type.css) not found */
:root {
  --bg:           #0d1117;
  --bg-surface:   #161b22;
  --text-primary: #e6edf3;
  --text-muted:   #7d8590;
  --border:       #30363d;
  --accent:       #58a6ff;
  --font-ui:   system-ui, -apple-system, 'Segoe UI', sans-serif;
  --font-mono: 'JetBrains Mono', 'Consolas', monospace;
}
"""


def _fmt(v: float | None) -> str:
    """Format float with thousand-comma, or '—' for None/NaN."""
    if v is None:
        return "—"
    if isinstance(v, float) and math.isnan(v):
        return "—"
    return f"{v:,.0f}"


def _fmt_ratio(v: float | None, decimals: int = 1) -> str:
    """Format a ratio/percentage, or '—' for None/NaN."""
    if v is None:
        return "—"
    if isinstance(v, float) and math.isnan(v):
        return "—"
    return f"{v:.{decimals}f}"


_BADGE_STYLES = {
    "저평가_우량주": "background:#1a4a1a;color:#4caf50;",
    "성장주":        "background:#1a2a4a;color:#58a6ff;",
    "배당주":        "background:#4a3a00;color:#ffd700;",
    "가격건전성":    "background:#2a2a2a;color:#aaaaaa;",
}
_BADGE_SHORT = {
    "저평가_우량주": "저평가",
    "성장주":        "성장",
    "배당주":        "배당",
    "가격건전성":    "건전",
}


def _filter_badges(r: ScreenResult) -> str:
    parts = []
    for name, preset in ALL_PRESETS.items():
        if preset.predicate(r):
            style = _BADGE_STYLES.get(name, "")
            label = _BADGE_SHORT.get(name, name)
            parts.append(
                f"<span style='font-size:10px;padding:1px 5px;border-radius:3px;{style}'>{label}</span>"
            )
    return "&nbsp;".join(parts)


def _embed_fonts(css: str, font_dir: Path) -> str:
    """Replace url('../fonts/*.woff2') references with inline base64 data URIs."""
    def replacer(m: re.Match) -> str:
        try:
            path = font_dir / Path(m.group(1)).name
            b64 = base64.b64encode(path.read_bytes()).decode()
            return f"url(data:font/woff2;base64,{b64})"
        except Exception as e:
            logger.warning("[HTML리포트] 폰트 파일 없음: %s", e)
            return m.group(0)
    return re.sub(r"url\('?\.\.\/fonts\/([^')]+\.woff2)'?\)", replacer, css)


def _load_css() -> str:
    """Load design bundle CSS, embed fonts, fall back to minimal CSS on error."""
    if not _CSS_FILE.exists():
        logger.warning("[HTML리포트] 디자인 번들 없음: %s — 시스템 폰트로 대체", _CSS_FILE)
        return _FALLBACK_CSS
    try:
        css = _CSS_FILE.read_text(encoding="utf-8")
        return _embed_fonts(css, _FONT_DIR)
    except Exception as e:
        logger.warning("[HTML리포트] CSS 로드 실패: %s — 시스템 폰트로 대체", e)
        return _FALLBACK_CSS


def _sparkline(history: list[float]) -> str:
    """Inline SVG polyline for 12-week close price trend. Returns '' if insufficient data."""
    if len(history) < 2:
        return ""
    lo, hi = min(history), max(history)
    if hi == lo:
        return ""
    W, H = 60, 20
    pts = " ".join(
        f"{round(i * W / (len(history) - 1), 1)},{round(H - (v - lo) / (hi - lo) * H, 1)}"
        for i, v in enumerate(history)
    )
    return (
        f"<svg width='{W}' height='{H}' style='vertical-align:middle;display:block'>"
        f"<polyline points='{pts}' fill='none' stroke='#4a9' stroke-width='1.5'/>"
        f"</svg>"
    )


def _group_by_sector(results: list[ScreenResult]) -> dict[str, list[ScreenResult]]:
    groups: dict[str, list[ScreenResult]] = {}
    for r in results:
        key = r.sector.strip() or "기타"
        groups.setdefault(key, []).append(r)
    return dict(sorted(groups.items()))


def _flow_badge(v: float | None) -> str:
    """Colored badge for net buy value: green > 0, red < 0, gray None."""
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return f"<span style='color:#555;font-size:11px'>—</span>"
    color = "#4caf50" if v > 0 else "#f44336"
    sign = "+" if v > 0 else ""
    return (
        f"<span style='color:{color};font-size:11px;font-weight:600'>"
        f"{sign}{v:,.0f}</span>"
    )


def _row(r: ScreenResult) -> str:
    name = _html.escape(r.name)
    if r.has_gapjum:
        name = f"{name} ★"
    sector = _html.escape(r.sector) if r.sector else "기타"
    per_str = _fmt_ratio(r.per, 1) if r.per is not None else "—"
    pbr_str = _fmt_ratio(r.pbr, 2) if r.pbr is not None else "—"
    div_str = (_fmt_ratio(r.dividend_yield, 1) + "%") if r.dividend_yield is not None else "—"
    foreign_str = (_fmt_ratio(r.foreign_rate, 1) + "%") if r.foreign_rate is not None else "—"
    badges = _filter_badges(r)
    return (
        f"    <tr>"
        f"<td>{name}</td>"
        f"<td>{sector}</td>"
        f"<td class='num'>{_fmt(r.close)}</td>"
        f"<td class='num'>{_fmt(r.ma_20w)}</td>"
        f"<td class='num'>{_fmt(r.ma_60w)}</td>"
        f"<td class='num'>{_fmt(r.ma_120w)}</td>"
        f"<td class='num'>{_fmt(r.cloud_top)}</td>"
        f"<td class='num'>{per_str}</td>"
        f"<td class='num'>{pbr_str}</td>"
        f"<td class='num'>{div_str}</td>"
        f"<td class='num'>{foreign_str}</td>"
        f"<td class='num'>{_flow_badge(r.foreign_net_buy)}</td>"
        f"<td class='num'>{_flow_badge(r.inst_net_buy)}</td>"
        f"<td>{_sparkline(r.close_history)}</td>"
        f"<td>{badges}</td>"
        f"</tr>\n"
    )


def _table(rows: list[ScreenResult]) -> str:
    body = "".join(_row(r) for r in sorted(rows, key=lambda x: -x.close))
    return (
        "  <table>\n"
        "    <thead><tr>\n"
        "      <th scope='col'>종목명</th>\n"
        "      <th scope='col'>업종</th>\n"
        "      <th scope='col' class='num'>종가</th>\n"
        "      <th scope='col' class='num'>20주선</th>\n"
        "      <th scope='col' class='num'>60주선</th>\n"
        "      <th scope='col' class='num'>120주선</th>\n"
        "      <th scope='col' class='num'>구름상단</th>\n"
        "      <th scope='col' class='num'>PER</th>\n"
        "      <th scope='col' class='num'>PBR</th>\n"
        "      <th scope='col' class='num'>배당</th>\n"
        "      <th scope='col' class='num'>외국인%</th>\n"
        "      <th scope='col' class='num'>외인순매수</th>\n"
        "      <th scope='col' class='num'>기관순매수</th>\n"
        "      <th scope='col'>추세</th>\n"
        "      <th scope='col'>필터</th>\n"
        "    </tr></thead>\n"
        "    <tbody>\n"
        f"{body}"
        "    </tbody>\n"
        "  </table>\n"
    )


def _sector_section(results: list[ScreenResult]) -> str:
    """Render a sector-grouped view as a second section below the main table."""
    groups = _group_by_sector(results)
    parts = ["<h2>업종별</h2>\n"]
    for sector, rows in groups.items():
        count = len(rows)
        parts.append(f"<h3>{_html.escape(sector)} ({count}종목)</h3>\n")
        parts.append(_table(rows))
    return "".join(parts)


def generate_html(results: list[ScreenResult]) -> str:
    """
    Generate a complete standalone HTML screener report string.
    Pure function — no I/O except CSS/font loading from web/screener_design/.
    """
    css = _load_css()

    week_of = current_week_of()
    now_kst = datetime.now(_KST).strftime("%Y-%m-%d %H:%M KST")
    total = len(results)
    gapjum_count = sum(1 for r in results if r.has_gapjum)
    f_summary = filter_summary(results)

    gapjum_rows = [r for r in results if r.has_gapjum]
    normal_rows = [r for r in results if not r.has_gapjum]

    # ── content sections ─────────────────────────────────────────────
    content = ""

    if total == 0:
        content = (
            f"<p class='empty-state'>이번 주 조건 통과 종목이 없습니다.<br>"
            f"<span class='muted'>(주: {_html.escape(week_of)})</span></p>\n"
        )
    else:
        if gapjum_rows:
            content += f"<h2>★ 정배열</h2>\n"
            content += _table(gapjum_rows)
        if normal_rows:
            content += f"<h2>일반</h2>\n"
            content += _table(normal_rows)
        content += _sector_section(results)

    # ── print styles ─────────────────────────────────────────────────
    print_css = """
@media print {
  body { background: #ffffff; color: #000000; }
  table { border-collapse: collapse; }
  th, td { border: 1px solid #cccccc; }
}
"""

    # ── inline style block ───────────────────────────────────────────
    page_css = """
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

body {
  background: var(--bg, #0d1117);
  color: var(--text-primary, #e6edf3);
  font-family: var(--font-ui, system-ui, sans-serif);
  font-size: 14px;
  padding: 24px;
}

header {
  border-bottom: 1px solid var(--border, #30363d);
  padding-bottom: 12px;
  margin-bottom: 20px;
}
header h1 { font-size: 18px; font-weight: 600; margin-bottom: 4px; }
header .meta { color: var(--text-muted, #7d8590); font-size: 12px; }

h2 {
  font-size: 14px;
  font-weight: 600;
  margin: 24px 0 8px;
  color: var(--accent, #58a6ff);
}

h3 {
  font-size: 13px;
  font-weight: 500;
  margin: 16px 0 6px;
  color: var(--text-muted, #7d8590);
}

table {
  border-collapse: collapse;
  width: 100%;
  font-family: var(--font-mono, 'Consolas', monospace);
  font-size: 13px;
}
th, td {
  padding: 6px 12px;
  border-bottom: 1px solid var(--border, #30363d);
  white-space: nowrap;
}
th {
  background: var(--bg-surface, #161b22);
  color: var(--text-muted, #7d8590);
  font-weight: 500;
  text-align: left;
}
th.num, td.num { text-align: right; }
td { text-align: left; }

.empty-state {
  text-align: center;
  padding: 48px;
  color: var(--text-muted, #7d8590);
  line-height: 1.8;
}
.muted { color: var(--text-muted, #7d8590); }

footer {
  margin-top: 24px;
  padding-top: 12px;
  border-top: 1px solid var(--border, #30363d);
  color: var(--text-muted, #7d8590);
  font-size: 11px;
}
"""

    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>주봉 차트 스크리닝 리포트 {_html.escape(week_of)}</title>
<style>
{css}
{page_css}
{print_css}
</style>
</head>
<body>
<header>
  <h1>주봉 차트 스크리닝 리포트 &nbsp;│&nbsp; {_html.escape(week_of)}</h1>
  <p class="meta">생성: {_html.escape(now_kst)} &nbsp;│&nbsp; 통과: {total}종목 (정배열 {gapjum_count}종목)</p>
  <p class="meta" style="margin-top:4px">필터: 저평가 우량주 {f_summary.get("저평가_우량주", 0)}개 &nbsp;│&nbsp; 성장주 {f_summary.get("성장주", 0)}개 &nbsp;│&nbsp; 배당주 {f_summary.get("배당주", 0)}개 &nbsp;│&nbsp; 가격건전성 {f_summary.get("가격건전성", 0)}개</p>
</header>
<main>
{content}
</main>
<footer>
  ※ 120주선 &mdash; = 시계열 데이터 부족 (100주 미만) &mdash; 조건 G는 NaN-pass(통과)로 처리됨
</footer>
</body>
</html>
"""


if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
    )
    from chart_screener import run_weekly_screen

    results = run_weekly_screen()
    html_str = generate_html(results)
    Path("reports/screener").mkdir(parents=True, exist_ok=True)
    out = Path("reports/screener") / f"screener_{datetime.now(_KST).strftime('%Y%m%d_%H%M')}.html"
    out.write_text(html_str, encoding="utf-8")
    print(f"리포트 저장: {out}  ({len(results)}종목)", file=sys.stderr)
