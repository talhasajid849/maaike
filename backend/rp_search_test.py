"""
Run: python rp_search_test.py
Finds the correct way to do a targeted wine search.
"""
import json
from urllib.parse import unquote
from pathlib import Path

try:
    from curl_cffi import requests
    session = requests.Session(impersonate="chrome110")
except ImportError:
    import requests as _r; session = _r.Session()

cookies_raw = json.loads(Path("cookies/robertparker.json").read_text())
token = None
for c in cookies_raw:
    session.cookies.set(c["name"], c["value"], domain=c.get("domain","www.robertparker.com"))
    if c["name"] == "RPWA_AUTH":
        auth = json.loads(unquote(c["value"]))
        token = auth.get("token")

headers = {
    "x-api-key":          "7ZPWPBFIRE2JLR6JBV5SCZPW54ZZSGGY",
    "authorization":      f"Bearer {token}",
    "authorizationtoken": "allow",
    "content-type":       "application/json",
    "cache-control":      "no-cache, no-store, must-revalidate",
    "pragma":             "no-cache",
    "expires":            "0",
    "referer":            "https://www.robertparker.com/",
}
URL = "https://api.robertparker.com/v2/v2/algolia?sort=latest_review&type=wine"

def search(label, body):
    r = session.post(URL, headers=headers, json=body, timeout=15)
    try:
        data = r.json()
        hits = data.get("data",{}).get("hits",[])
        nb = data.get("data",{}).get("nbHits",0)
        names = [h.get("name","?")[:50] for h in hits[:3]]
        print(f"  {label}: nbHits={nb}, first 3: {names}")
        if hits:
            h = hits[0]
            print(f"    keys in hit[0]: {list(h.keys())[:15]}")
    except:
        print(f"  {label}: HTTP {r.status_code} — {r.text[:80]}")

print("=== Finding correct search approach ===\n")

# Test: query in the query field (what we originally tried but with camelCase)
search("query field only", {
    "query": "Krug 1995",
    "facetFilters": [["type:wine"]],
    "filters": "rating_computed:50 TO 100",
    "hitsPerPage": 10, "page": 0, "facets": ["*"], "sortFacetValuesBy": "count"
})

# Test: query field + vintage filter
search("query field + vintage facet", {
    "query": "Krug",
    "facetFilters": [["type:wine"], ["vintage:1995"]],
    "filters": "rating_computed:50 TO 100",
    "hitsPerPage": 10, "page": 0, "facets": ["*"], "sortFacetValuesBy": "count"
})

# Test: Voerzio Barolo 2013 - one of the wines we tested
search("Voerzio Barolo query field", {
    "query": "Voerzio Barolo 2013",
    "facetFilters": [["type:wine"]],
    "filters": "rating_computed:50 TO 100",
    "hitsPerPage": 10, "page": 0, "facets": ["*"], "sortFacetValuesBy": "count"
})

# Test: just Voerzio
search("Voerzio only", {
    "query": "Voerzio",
    "facetFilters": [["type:wine"]],
    "filters": "rating_computed:50 TO 100",
    "hitsPerPage": 10, "page": 0, "facets": ["*"], "sortFacetValuesBy": "count"
})

# Test: what does a hit actually look like? Show full first hit
print("\n=== Full structure of first hit for 'Krug' ===")
r = session.post(URL, headers=headers, json={
    "query": "Krug",
    "facetFilters": [["type:wine"]],
    "filters": "rating_computed:50 TO 100",
    "hitsPerPage": 3, "page": 0, "facets": ["*"], "sortFacetValuesBy": "count"
}, timeout=15)
data = r.json()
hits = data.get("data",{}).get("hits",[])
if hits:
    print(json.dumps(hits[0], indent=2)[:1500])