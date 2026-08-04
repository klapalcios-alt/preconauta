import json
from pathlib import Path
p = Path('events.json')
if not p.exists():
    print('events.json not found')
    raise SystemExit(1)
ev = json.loads(p.read_text(encoding='utf-8-sig'))
pres = [e.get('tid') for e in ev if str(e.get('league')).strip().lower()=='presencial']
print(len(pres))
for t in pres:
    print(t)
