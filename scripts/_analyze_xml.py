import re, sys
sys.stdout.reconfigure(encoding="utf-8")

TAG_RE = re.compile(r"<[^>]+>")
def strip(line):
    return TAG_RE.sub("", line).strip()

xml_path = r"reports\dart\삼성전자\20260515002181_분기보고서 (2026.03)\20260515002181.xml"
lines = []
with open(xml_path, encoding="utf-8", errors="ignore") as f:
    for raw in f:
        s = strip(raw)
        if s:
            lines.append(s)

print(f"총 {len(lines)}줄\n")

# 1) 영업이익 포함 줄 컨텍스트
print("=" * 60)
print("[영업이익 등장 위치]")
print("=" * 60)
hits = []
for i, line in enumerate(lines):
    if "영업이익" in line:
        ctx = lines[max(0, i-2):i+4]
        hits.append((i, ctx))

for i, ctx in hits[:25]:
    print(f"--- line {i} ---")
    for c in ctx:
        print(" ", c[:120])
    print()

# 2) _REVENUE_ANCHORS가 실제로 매치되는 줄
print("=" * 60)
print("[_REVENUE_ANCHORS 매치 위치]")
print("=" * 60)
REVENUE_ANCHORS = ["부문별 매출실적","사업부문별 매출","매출실적","세그먼트별 매출","사업별 매출","매출현황"]
for i, line in enumerate(lines):
    for a in REVENUE_ANCHORS:
        if a in line:
            ctx = lines[i:i+6]
            print(f"앵커='{a}' line {i}:")
            for c in ctx:
                print(" ", c[:120])
            print()
            break
