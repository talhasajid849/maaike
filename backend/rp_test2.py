"""
Run: python rp_test2.py
Tests Sec-Fetch headers - the likely cause of blocking.
"""
import json, sys
from urllib.parse import unquote
from pathlib import Path

try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    print("curl_cffi not found"); sys.exit(1)

cookies = json.loads(Path("cookies/robertparker.json").read_text())
token, client_id = None, None
for c in cookies:
    if c["name"] == "RPWA_AUTH":
        auth = json.loads(unquote(c["value"]))
        token, client_id = auth.get("token"), auth.get("clientId")

def make_session(extra_headers=None):
    s = cffi_requests.Session(impersonate="chrome110")
    for c in cookies:
        s.cookies.set(c["name"], c["value"], domain=c.get("domain","www.robertparker.com"))
    s.headers["Authorization"] = f"Bearer {token}"
    s.headers["X-Client-Id"] = client_id
    s.headers["Referer"] = "https://www.robertparker.com/wines"
    if extra_headers:
        s.headers.update(extra_headers)
    return s

def test(label, session):
    r = session.get("https://www.robertparker.com/api/search",
                    params={"q": "Krug", "type": "wine"}, timeout=20)
    blocked = len(r.content) == 3627
    ct = r.headers.get("Content-Type","?")[:40]
    status = "✗ BLOCKED" if blocked else f"✓ GOT DATA ({len(r.content)} bytes)"
    print(f"  {label:50s}: {status}  CT={ct}")
    if not blocked:
        print(f"    → {r.text[:150]}")
    return not blocked

print("\n=== Testing Sec-Fetch headers ===\n")

# These are the headers a browser XHR sends to same-origin API
xhr_headers = {
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Dest": "empty",
}
nav_headers = {
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-Dest": "document",
}

tests = [
    ("No extra headers (baseline)",            {}),
    ("XHR Sec-Fetch headers",                  xhr_headers),
    ("XHR + Origin header",                    {**xhr_headers, "Origin": "https://www.robertparker.com"}),
    ("XHR + X-Requested-With",                 {**xhr_headers, "X-Requested-With": "XMLHttpRequest"}),
    ("Navigate headers (like page load)",      nav_headers),
]

found = False
for label, headers in tests:
    s = make_session(headers)
    if test(label, s):
        found = True
        print(f"\n  ✓✓✓ WINNER: '{label}' works!\n")
        break

if not found:
    print("\n  All Sec-Fetch variants blocked. Testing direct wine URL...\n")
    # Try fetching the known wine URL directly
    s = make_session(xhr_headers)
    r = s.get("https://www.robertparker.com/wines/bHY9M9dg3MeKEJyWv/pommard-1er-cru-les-rugiens-hauts-2023", timeout=20)
    ct = r.headers.get("Content-Type","?")[:50]
    print(f"  Direct wine page: HTTP {r.status_code}, {len(r.content)} bytes, CT={ct}")
    if len(r.content) > 5000:
        print(f"  → Got real page! First 300 chars:")
        # Look for score in content
        import re
        text = r.text
        scores = re.findall(r'"score["\s]*:\s*"?(\d{2,3})"?', text[:5000])
        ratings = re.findall(r'"rating["\s]*:\s*"?(\d{2,3})"?', text[:5000])
        print(f"  → Score matches: {scores}")
        print(f"  → Rating matches: {ratings}")
        # Check for JSON-LD
        jsonld = re.findall(r'<script type="application/ld\+json">(.*?)</script>', text, re.DOTALL)
        print(f"  → JSON-LD blocks found: {len(jsonld)}")
        if jsonld:
            print(f"  → First JSON-LD: {jsonld[0][:300]}")
    elif len(r.content) == 3627:
        print(f"  → Also blocked (3627 bytes)")
    else:
        print(f"  → {r.text[:200]}")