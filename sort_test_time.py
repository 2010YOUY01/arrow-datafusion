import re, sys

PAT = re.compile(r'^\s*PASS\s+\[\s*([0-9]+\.[0-9]+)s\]\s+(.+)$')

records = []                              # (seconds, original‑line)
for line in sys.stdin:
    m = PAT.match(line)
    if m:
        records.append((float(m.group(1)), line.rstrip()))

for secs, text in sorted(records, key=lambda r: r[0], reverse=True):
    print(f'{secs:10.3f}s  {text}')