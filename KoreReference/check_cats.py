import httpx
from bs4 import BeautifulSoup

r = httpx.get('http://127.0.0.1:8888/content/wikipedia_en_all_maxi_2025-08/A/Proton', timeout=10)
soup = BeautifulSoup(r.text, 'html.parser')

for sel in ['#mw-normal-catlinks', '#mw-catlinks', '.catlinks', '.mw-normal-catlinks']:
    el = soup.select_one(sel)
    found = repr(str(el)[:300]) if el else 'NOT FOUND'
    print(sel + ': ' + found)

print()
for tag in soup.find_all(True):
    tid = tag.get('id', '')
    tcls = ' '.join(tag.get('class', []))
    if 'cat' in tid.lower() or 'cat' in tcls.lower():
        print('Found: id=' + repr(tid) + ' class=' + repr(tcls) + ' -> ' + repr(str(tag)[:200]))
