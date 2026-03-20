"""
Run: python rp_test3.py
Final diagnosis: proves IP/cookie blocking and tests the one remaining option.
"""
import json, sys, re
from urllib.parse import unquote
from pathlib import Path

try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    import requests as cffi_requests

cookies_raw = json.loads(Path("cookies/robertparker.json").read_text())
token, client_id = None, None
all_cookies = {}
for c in cookies_raw:
    all_cookies[c["name"]] = c["value"]
    if c["name"] == "RPWA_AUTH":
        auth = json.loads(unquote(c["value"]))
        token, client_id = auth.get("token"), auth.get("clientId")

print("=== TEST A: No cookies, no auth — does RP return something different? ===")
try:
    s = cffi_requests.Session(impersonate="chrome110")
    r = s.get("https://www.robertparker.com/api/search",
              params={"q": "Krug", "type": "wine"}, timeout=15)
    print(f"  HTTP {r.status_code}, {len(r.content)} bytes, CT={r.headers.get('Content-Type','?')[:50]}")
    print(f"  First 100: {r.text[:100]}")
except Exception as e:
    print(f"  Error: {e}")

print("\n=== TEST B: Try robertparker.com homepage — does it load? ===")
try:
    s = cffi_requests.Session(impersonate="chrome110")
    r = s.get("https://www.robertparker.com/", timeout=15)
    print(f"  HTTP {r.status_code}, {len(r.content)} bytes")
    title = re.search(r'<title>(.*?)</title>', r.text)
    print(f"  Title: {title.group(1) if title else 'none'}")
    if "Just a moment" in r.text or "cf-browser-verification" in r.text:
        print("  ✗ CLOUDFLARE CHALLENGE PAGE — IP is flagged")
    elif "Robert Parker" in r.text:
        print("  ✓ Got real RP homepage")
    else:
        print(f"  ? Unknown content: {r.text[:200]}")
except Exception as e:
    print(f"  Error: {e}")

print("\n=== TEST C: RP's actual API endpoint from the browser screenshot ===")
print("  (The browser called /api/wines/bHY9M9dg3MeKEJyWv directly)")
try:
    s = cffi_requests.Session(impersonate="chrome110")
    for c in cookies_raw:
        s.cookies.set(c["name"], c["value"], domain=c.get("domain","www.robertparker.com"))
    s.headers["Authorization"] = f"Bearer {token}"
    s.headers["X-Client-Id"] = client_id
    # Try the wine detail endpoint that returned 9.2kB in the browser
    r = s.get("https://www.robertparker.com/api/wines/bHY9M9dg3MeKEJyWv", timeout=15)
    print(f"  HTTP {r.status_code}, {len(r.content)} bytes, CT={r.headers.get('Content-Type','?')[:50]}")
    if len(r.content) > 4000 and b'<' not in r.content[:10]:
        print(f"  ✓ JSON! First 200: {r.text[:200]}")
    else:
        print(f"  ✗ Blocked. First 100: {r.text[:100]}")
except Exception as e:
    print(f"  Error: {e}")

print("\n=== TEST D: What does the 3627-byte response actually contain? ===")
try:
    s = cffi_requests.Session(impersonate="chrome110")
    r = s.get("https://www.robertparker.com/api/search",
              params={"q": "Krug", "type": "wine"}, timeout=15)
    text = r.text
    # Check for Cloudflare challenge
    if "Just a moment" in text:
        print("  → CLOUDFLARE JAVASCRIPT CHALLENGE (IP flagged, must solve CAPTCHA)")
    elif "cf-browser-verification" in text or "cf_clearance" in text:
        print("  → CLOUDFLARE BROWSER VERIFICATION")
    elif "Access denied" in text:
        print("  → ACCESS DENIED page")
    elif "log in" in text.lower() or "sign in" in text.lower():
        print("  → LOGIN PAGE (cookies not being sent/accepted)")
    else:
        # Show full content
        print(f"  → Unknown block. Full content ({len(text)} chars):")
        print(text[:500])
        # Check response headers for clues
        print(f"\n  Response headers:")
        for k, v in r.headers.items():
            print(f"    {k}: {v}")
except Exception as e:
    print(f"  Error: {e}")
    