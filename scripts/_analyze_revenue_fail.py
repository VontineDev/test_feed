"""실패 기업 XML에서 _REVENUE_ANCHORS 매치 현황과 실제 매출 섹션 위치 분석."""
import re, sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

TAG_RE = re.compile(r"<[^>]+>")

REVENUE_ANCHORS = [
    "사업부문별 요약 재무 현황", "사업부문별 요약", "요약 재무 현황", "사업부문별 실적",
    "부문별 매출실적", "사업부문별 매출", "매출실적", "세그먼트별 매출", "사업별 매출", "매출현황",
]
REVENUE_KEYWORDS = ["매출", "영업이익", "매출액", "순매출", "연결매출"]

def strip(line):
    return TAG_RE.sub("", line).strip()

def analyze_corp(corp_name: str, dart_dir: Path):
    corp_dir = dart_dir / corp_name
    if not corp_dir.exists():
        print(f"디렉터리 없음: {corp_dir}")
        return

    # 사업보고서 우선
    report_dirs = sorted(corp_dir.iterdir())
    target = None
    for d in reversed(report_dirs):
        if "사업보고서" in d.name and "_meta" not in d.name and d.is_dir():
            target = d
            break
    if target is None:
        target = report_dirs[-1] if report_dirs else None
    if target is None or not target.is_dir():
        print(f"보고서 없음: {corp_name}")
        return

    # 메인 XML 선택
    rcept_no = target.name.split("_")[0]
    xml_path = target / f"{rcept_no}.xml"
    if not xml_path.exists():
        xmls = sorted(target.glob("*.xml"), key=lambda p: len(p.name))
        xml_path = xmls[0] if xmls else None
    if not xml_path:
        print(f"XML 없음: {corp_name}")
        return

    lines = []
    with open(xml_path, encoding="utf-8", errors="ignore") as f:
        for raw in f:
            s = strip(raw)
            if s:
                lines.append(s)

    print(f"\n{'='*60}")
    print(f"[{corp_name}]  {target.name}")
    print(f"XML: {xml_path.name}  총 {len(lines):,}줄")

    # 1) REVENUE_ANCHORS 매치 현황
    matched = []
    for i, line in enumerate(lines):
        for anc in REVENUE_ANCHORS:
            if anc in line:
                matched.append((i, anc, line[:100]))
    print(f"\n_REVENUE_ANCHORS 매치: {len(matched)}건")
    for i, anc, ln in matched[:10]:
        print(f"  line {i:5d} [{anc}]: {ln[:80]}")

    # 2) 매출 키워드 등장 위치 (헤더 후보)
    print(f"\n매출 키워드 등장 위치 (앞 5건):")
    kw_hits = []
    for i, line in enumerate(lines):
        if any(kw in line for kw in REVENUE_KEYWORDS) and len(line) < 50:
            kw_hits.append((i, line))
    for i, ln in kw_hits[:8]:
        ctx = lines[i:i+4]
        print(f"  line {i:5d}: {ln[:60]}")
        for c in ctx[1:]:
            print(f"           → {c[:70]}")

    # 3) 실제 매출 테이블 주변 (50자 이하 짧은 줄 + 숫자 다음줄)
    print(f"\n숫자 테이블 후보 (매출 근처):")
    NUM_RE = re.compile(r"^\d[\d,]+$")
    for i, line in enumerate(lines):
        if any(kw in line for kw in ["매출", "영업이익"]) and len(line) < 30:
            # 주변 10줄 출력
            ctx = lines[max(0,i-2):i+10]
            print(f"  --- line {i} ---")
            for c in ctx:
                print(f"  {c[:100]}")
            print()
            if len([x for x in lines if any(kw in x for kw in ["매출"])]) > 20:
                break  # 너무 많으면 처음 몇 개만


DART_DIR = Path("reports/dart")
for corp in ["삼표시멘트", "에코프로", "풍산"]:
    analyze_corp(corp, DART_DIR)
