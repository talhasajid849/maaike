"""
Run: python rp_api_test.py
Tests the algolia endpoint directly, showing exactly what we send.
"""
import json, sys
from urllib.parse import unquote
from pathlib import Path

try:
    from curl_cffi import requests
    session = requests.Session(impersonate="chrome110")
    print("Using curl_cffi")
except ImportError:
    import requests as _r
    session = _r.Session()
    print("Using requests")

cookies_raw = json.loads(Path("cookies/robertparker.json").read_text())
token, client_id = None, None
for c in cookies_raw:
    session.cookies.set(c["name"], c["value"],
                       domain=c.get("domain", "www.robertparker.com"))
    if c["name"] == "RPWA_AUTH":
        auth = json.loads(unquote(c["value"]))
        token, client_id = auth.get("token"), auth.get("clientId")

# Exact headers from browser capture
headers = {
    "x-api-key":                    "7ZPWPBFIRE2JLR6JBV5SCZPW54ZZSGGY",
    "authorization":                f"Bearer {token}",
    "authorizationtoken":           "allow",
    "content-type":                 "application/json",
    "cache-control":                "no-cache, no-store, must-revalidate",
    "pragma":                       "no-cache",
    "expires":                      "0",
    "access-control-allow-headers": "*",
    "referer":                      "https://www.robertparker.com/",
}

URL = "https://api.robertparker.com/v2/v2/algolia?sort=latest_review&type=wine"

print(f"\nToken present: {bool(token)}")
print(f"Token prefix: {token[:30] if token else 'NONE'}")

# Test 1: exact body from browser
body1 = {
    "query": "",
    "facet_filters": [["q:Krug 1995"], ["type:wine"]],
    "filters": "rating_computed:50 TO 100",
    "hits_per_page": 50,
    "page": 0,
    "facets": ["*"],
    "sort_facet_values_by": "count"
}
print(f"\n--- TEST 1: Exact browser body ---")
print(f"Sending: {json.dumps(body1)}")
r = session.post(URL, headers=headers, json=body1, timeout=15)
print(f"HTTP {r.status_code}, {len(r.content)} bytes")
print(f"Response: {r.text[:200]}")

# Test 2: same but snake_case vs camelCase
body2 = {
    "query": "",
    "facetFilters": [["q:Krug 1995"], ["type:wine"]],
    "filters": "rating_computed:50 TO 100",
    "hitsPerPage": 50,
    "page": 0,
    "facets": ["*"],
    "sortFacetValuesBy": "count"
}
print(f"\n--- TEST 2: camelCase keys ---")
r = session.post(URL, headers=headers, json=body2, timeout=15)
print(f"HTTP {r.status_code}, {len(r.content)} bytes")
print(f"Response: {r.text[:200]}")

# Test 3: no query params in URL (just POST to base endpoint)
URL2 = "https://api.robertparker.com/v2/v2/algolia"
print(f"\n--- TEST 3: No URL params ---")
r = session.post(URL2, headers=headers, json=body1, timeout=15)
print(f"HTTP {r.status_code}, {len(r.content)} bytes")
print(f"Response: {r.text[:200]}")

# Test 4: send the query as URL param too
URL3 = "https://api.robertparker.com/v2/v2/algolia?sort=latest_review&type=wine&q=Krug+1995"
print(f"\n--- TEST 4: query also in URL ---")
r = session.post(URL3, headers=headers, json=body1, timeout=15)
print(f"HTTP {r.status_code}, {len(r.content)} bytes")
print(f"Response: {r.text[:200]}")

# Test 5: minimal - just the api key, no auth
headers_minimal = {
    "x-api-key": "7ZPWPBFIRE2JLR6JBV5SCZPW54ZZSGGY",
    "content-type": "application/json",
    "referer": "https://www.robertparker.com/",
}
print(f"\n--- TEST 5: Minimal headers (api-key only) ---")
r2 = requests.Session(impersonate="chrome110") if 'curl_cffi' in sys.modules else session
r_resp = r2.post(URL, headers=headers_minimal, json=body1, timeout=15)
print(f"HTTP {r_resp.status_code}, {len(r_resp.content)} bytes")
print(f"Response: {r_resp.text[:200]}")

# Test 6: check what 400 response headers say
print(f"\n--- TEST 6: Response headers on 400 ---")
r = session.post(URL, headers=headers, json=body1, timeout=15)
print("Response headers:")
for k, v in r.headers.items():
    print(f"  {k}: {v}")