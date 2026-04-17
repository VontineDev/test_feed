"""
generate_report.py — 차트 스크리닝 결과를 UTF-8 파일로 저장
"""
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from chart_screener import run_weekly_screen

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
)

_KST = ZoneInfo("Asia/Seoul")

results = run_weekly_screen()

now_kst = datetime.now(_KST).strftime("%Y-%m-%d %H:%M KST")
gapjum = [r for r in results if r.has_gapjum]
no_gapjum = [r for r in results if not r.has_gapjum]

lines = []
lines.append("=" * 70)
lines.append(f"  주봉 차트 스크리닝 리포트")
lines.append(f"  생성일시: {now_kst}")
lines.append(f"  통과 종목: {len(results)}개  (★ 정배열: {len(gapjum)}개 / 일반: {len(no_gapjum)}개)")
lines.append("=" * 70)
lines.append("")
lines.append("【조건】")
lines.append("  A. 이번 주 종가 > 구름 상단 (구름 상향 돌파)")
lines.append("  B. 직전 주 종가 ≤ 직전 주 구름 상단 (돌파 직전 구름 내/하부)")
lines.append("  C. 이번 주 종가 > 20주선")
lines.append("  D. 이번 주 종가 > 60주선")
lines.append("  E. 20주선 우상향")
lines.append("  F. 60주선 우상향")
lines.append("  ★ 가점: 20주선 > 60주선 (정배열)")
lines.append("")

if gapjum:
    lines.append("─" * 70)
    lines.append(f"  ★ 정배열 (20주선 > 60주선)  —  {len(gapjum)}종목")
    lines.append("─" * 70)
    lines.append(f"  {'종목명':<18} {'티커':<14} {'종가':>10} {'20주선':>10} {'60주선':>10} {'구름상단':>10}")
    lines.append("  " + "-" * 66)
    for r in sorted(gapjum, key=lambda x: -x.close):
        lines.append(
            f"  {r.name:<18} {r.ticker:<14} "
            f"{r.close:>10,.0f} {r.ma_20w:>10,.0f} {r.ma_60w:>10,.0f} {r.cloud_top:>10,.0f}"
        )

lines.append("")
if no_gapjum:
    lines.append("─" * 70)
    lines.append(f"  일반 (60주선 ≥ 20주선)  —  {len(no_gapjum)}종목")
    lines.append("─" * 70)
    lines.append(f"  {'종목명':<18} {'티커':<14} {'종가':>10} {'20주선':>10} {'60주선':>10} {'구름상단':>10}")
    lines.append("  " + "-" * 66)
    for r in sorted(no_gapjum, key=lambda x: -x.close):
        lines.append(
            f"  {r.name:<18} {r.ticker:<14} "
            f"{r.close:>10,.0f} {r.ma_20w:>10,.0f} {r.ma_60w:>10,.0f} {r.cloud_top:>10,.0f}"
        )

lines.append("")
lines.append("=" * 70)

report_path = f"screening_report_{datetime.now(_KST).strftime('%Y%m%d_%H%M')}.txt"
with open(report_path, "w", encoding="utf-8") as f:
    f.write("\n".join(lines))

print(f"리포트 저장 완료: {report_path}")
print(f"통과 종목: {len(results)}개")
