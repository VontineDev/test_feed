"""
screener_filters.py — 밸류에이션·수급 필터 프리셋

chart_screener.run_weekly_screen() 결과에 후처리 필터 적용.
차트 스크리닝 통과 종목에 한해 Naver 펀더멘털을 조회하므로
전체 2770종목 조회보다 훨씬 빠름 (~200종목 × 0.1s ≈ 20s).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from analysis.chart_screener import ScreenResult


def _val(x: float | None, fallback: float) -> float:
    """None-safe field access — returns fallback when data is missing."""
    return x if x is not None else fallback


@dataclass
class FilterPreset:
    name: str
    label_ko: str
    predicate: Callable[["ScreenResult"], bool]


저평가_우량주 = FilterPreset(
    name="저평가_우량주",
    label_ko="저평가 우량주",
    predicate=lambda r: (
        _val(r.pbr, 99.0) < 1.5
        and _val(r.per, 99.0) < 20
        and _val(r.eps, 0.0) > 0
    ),
)

성장주 = FilterPreset(
    name="성장주",
    label_ko="성장주",
    predicate=lambda r: (
        _val(r.eps, 0.0) > 0
        and 15 <= _val(r.per, 0.0) <= 50
        and _val(r.volume_ratio_4w, 0.0) >= 1.0
    ),
)

배당주 = FilterPreset(
    name="배당주",
    label_ko="배당주",
    predicate=lambda r: (
        _val(r.dividend_yield, 0.0) >= 2.5
        and _val(r.eps, 0.0) > 0
    ),
)

가격건전성 = FilterPreset(
    name="가격건전성",
    label_ko="가격 건전성",
    predicate=lambda r: _val(r.volume_ratio_4w, 0.0) >= 0.8,
)

ALL_PRESETS: dict[str, FilterPreset] = {
    p.name: p for p in [저평가_우량주, 성장주, 배당주, 가격건전성]
}


def apply_filters(
    results: list["ScreenResult"],
    presets: list[FilterPreset],
    combine: str = "intersection",
) -> list["ScreenResult"]:
    """
    results: output of run_weekly_screen()
    presets: list of FilterPreset objects to apply
    combine: "intersection" (AND, default) | "union" (OR)
    """
    if not presets:
        return results
    if combine == "union":
        return [r for r in results if any(p.predicate(r) for p in presets)]
    return [r for r in results if all(p.predicate(r) for p in presets)]


def filter_summary(results: list["ScreenResult"]) -> dict[str, int]:
    """Returns {preset_name: count} for all presets, for use in reports."""
    return {name: sum(1 for r in results if p.predicate(r)) for name, p in ALL_PRESETS.items()}
