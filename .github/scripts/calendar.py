"""Wandelt den Forex-Factory-Wochenfeed in data/calendar.json für NoTilt um (nur hohe und mittlere Wirkung)."""
import json, sys
src, dst = sys.argv[1], sys.argv[2]
raw = json.load(open(src, encoding='utf-8'))
if not isinstance(raw, list) or not raw:
    sys.exit('Feed leer oder unerwartetes Format – alte Datei bleibt')
ev = []
for e in raw:
    imp = str(e.get('impact', ''))
    if imp not in ('High', 'Medium') or not e.get('date') or not e.get('title'):
        continue
    ev.append({'t': e['date'], 'title': str(e['title'])[:80], 'cur': str(e.get('country', ''))[:4], 'imp': imp,
               'fc': str(e.get('forecast') or '')[:16], 'pv': str(e.get('previous') or '')[:16]})
if not ev:
    sys.exit('Keine Termine gefunden – alte Datei bleibt')
ev.sort(key=lambda x: x['t'])
json.dump({'source': 'Forex Factory', 'events': ev}, open(dst, 'w', encoding='utf-8'), ensure_ascii=False, separators=(',', ':'))
print(len(ev), 'Termine')
