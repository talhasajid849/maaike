"""
Run: python rp_algolia_test.py
Tests the real RP API endpoint discovered from browser traffic.
"""
import json, sys
from urllib.parse import unquote
from pathlib import Path

try:
    from curl_cffi import requests
    session = requests.Session(impersonate="chrome110")
except ImportError:
    import requests as _r
    session = _r.Session()

cookies_raw = json.loads(Path("cookies/robertparker.json").read_text())
token, client_id = None, None
for c in cookies_raw:
    session.cookies.set(c["name"], c["value"], domain=c.get("domain","www.robertparker.com"))
    if c["name"] == "RPWA_AUTH":
        auth = json.loads(unquote(c["value"]))
        token, client_id = auth.get("token"), auth.get("clientId")

session.headers["Authorization"] = f"Bearer {token}"
session.headers["X-Client-Id"] = client_id
session.headers["Referer"] = "https://www.robertparker.com/"
session.headers["Origin"] = "https://www.robertparker.com"

BASE_API = "https://api.robertparker.com/v2"

print("=== TEST 1: Algolia search via api.robertparker.com ===")
for query in ["Krug 1995", "Krug", "L'Eglise-Clinet 2015", "Batard-Montrachet"]:
    r = session.get(f"{BASE_API}/v2/algolia",
                    params={"q": query, "sort": "latest_review", "type": "wine"},
                    timeout=15)
    ct = r.headers.get("Content-Type","?")[:50]
    print(f"\n  Query: {query!r}")
    print(f"  HTTP {r.status_code}, {len(r.content)} bytes, CT={ct}")
    if r.status_code == 200 and len(r.content) > 100:
        try:
            data = r.json()
            hits = data.get("data", {}).get("hits", [])
            nb   = data.get("data", {}).get("nbHits", 0)
            print(f"  nbHits={nb}, hits returned={len(hits)}")
            if hits:
                h = hits[0]
                print(f"  First hit: {json.dumps({k:v for k,v in h.items() if k in ('name','vintage','id','_id','wineId','slug','score','rating','tastings')}, indent=2)[:400]}")
        except Exception as e:
            print(f"  JSON parse error: {e}")
            print(f"  Body: {r.text[:200]}")
    else:
        print(f"  Body: {r.text[:200]}")

print("\n=== TEST 2: Try different Algolia param names ===")
params_variants = [
    {"query": "Krug", "sort": "latest_review", "type": "wine"},
    {"q": "Krug", "sort": "latest_review", "type": "wine", "vintage": "1995"},
    {"q": "Krug", "type": "wine"},
    {"q": "Krug 1995 Champagne"},
]
for params in params_variants:
    r = session.get(f"{BASE_API}/v2/algolia", params=params, timeout=10)
    try:
        data = r.json()
        nb = data.get("data", {}).get("nbHits", 0)
        print(f"  params={params} → nbHits={nb}")
    except:
        print(f"  params={params} → {r.text[:80]}")

print("\n=== TEST 3: Check other endpoints on api.robertparker.com ===")
for path in ["/v2/wines/search", "/v2/search", "/wines/search", "/search"]:
    try:
        r = session.get(f"https://api.robertparker.com{path}",
                        params={"q": "Krug", "type": "wine"}, timeout=8)
        print(f"  {path}: HTTP {r.status_code}, {len(r.content)}b, CT={r.headers.get('Content-Type','?')[:40]}")
        if r.status_code == 200 and b'<' not in r.content[:5]:
            print(f"    → {r.text[:100]}")
    except Exception as e:
        print(f"  {path}: {e}")