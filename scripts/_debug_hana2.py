"""하나금융지주 분기 - 영업이익 데이터 섹션 헤더 찾기."""
import re, sys
from pathlib import Path
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, ".")
from data.dart_extractor import _pick_main_xml, DART_DIR

TAG_RE = re.compile(r"<[^>]+>")
def strip(l): return TAG_RE.sub("", l).strip()

corp_dir = DART_DIR / "하나금융지주"
rcept_no = "20250515002336"

xml_path = None
for d in corp_dir.iterdir():
    if d.is_dir() and rcept_no in d.name:
        xml_path = _pick_main_xml(d, rcept_no)
        break

lines = []
with open(xml_path, encoding="utf-8", errors="ignore") as f:
    for raw in f:
        s = strip(raw)
        if s:
            lines.append(s)

# line 244 (첫 영업이익) 앞 50줄에서 헤더 찾기
print("=== line 244 앞 50줄 헤더(≤80자) ===")
for i, l in enumerate(lines[194:250], start=194):
    if len(l) <= 80:
        print(f"  line {i}: {l}")

# line 8316 (요약재무정보 실제 섹션) 앞 30줄
print("\n=== line 8316 앞 30줄 ===")
for i, l in enumerate(lines[8270:8330], start=8270):
    if len(l) <= 120:
        print(f"  line {i} ({len(l)}자): {l[:100]}")

# 요약재무정보 모든 히트
print("\n=== '요약재무정보' 모든 히트 ===")
for i, l in enumerate(lines):
    if "요약재무정보" in l:
        print(f"  line {i} ({len(l)}자): {l[:80]}")

# 요약연결 관련 헤더
print("\n=== '요약연결' 관련 헤더 (≤80자) ===")
for i, l in enumerate(lines):
    if "요약연결" in l and len(l) <= 80:
        print(f"  line {i}: {l}")
