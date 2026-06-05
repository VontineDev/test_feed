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

# "사업부문별 요약" 앵커가 있는 섹션 (line 1089) 전후 60줄
print("=" * 60)
print("[라. 사업부문별 요약 재무 현황 섹션 — lines 1085~1200]")
print("=" * 60)
for i, line in enumerate(lines[1085:1200], start=1085):
    print(f"{i:5d}: {line[:120]}")

print()
print("=" * 60)
print("[consolidated 영업이익 섹션 — lines 1300~1320]")
print("=" * 60)
for i, line in enumerate(lines[1300:1320], start=1300):
    print(f"{i:5d}: {line[:120]}")
