"""삼성전자 XML에서 연결 손익 표 위치 탐색."""
import sys, re
sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8")
from data.dart_extractor import _pick_main_xml, _parse_report_dir, DART_DIR, extract_xml

corp_dir = DART_DIR / "삼성전자"
report_dirs = sorted(d for d in corp_dir.iterdir() if d.is_dir())
target = list(reversed(report_dirs))[0]  # 최신
rcept_no, rtype, period = _parse_report_dir(target.name)
xml_path = _pick_main_xml(target, rcept_no)

print(f"[{period}] {rtype} | {xml_path.name}")

# XML 전체 텍스트 읽어서 라인 검색
text = extract_xml(xml_path)
lines = text.splitlines()
print(f"전체 라인: {len(lines)}개\n")

# "매출액"이 나오는 라인들 중 연결 손익 표 구조인 것 탐색
for i, l in enumerate(lines):
    stripped = l.strip()
    if stripped in ("매출액", "Ⅰ. 매출액", "I. 매출액") or stripped.startswith("매출액"):
        if i > 5:
            context_before = "\n".join(lines[max(0,i-5):i])
            context_after  = "\n".join(lines[i:i+15])
            # 백만원 단위인 것만 찾기 (큰 숫자 패턴)
            if any(re.search(r"\d{8,}", l2) for l2 in lines[i:i+10]):
                print(f"=== 라인 {i}: '{stripped}' ===")
                print("앞5줄:", context_before[-200:])
                print("뒤15줄:", context_after[:400])
                print()
                if i > 100:  # 앞부분 아닌 곳에서 처음 히트
                    break
