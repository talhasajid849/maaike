"""
Run this in your backend terminal:
  cd D:\Maaike\backend
  python rp_test.py

This will tell us EXACTLY why RP is still blocking us.
"""
import json, sys
from urllib.parse import unquote
from pathlib import Path

print("=" * 60)
print("RP DIAGNOSTIC TEST")
print("=" * 60)

# Step 1: Check curl_cffi import and version
try:
    import curl_cffi
    print(f"\n✓ curl_cffi version: {curl_cffi.__version__}")
    from curl_cffi import requests as cffi_requests
    print("✓ curl_cffi.requests imported OK")
except ImportError as e:
    print(f"\n✗ curl_cffi import FAILED: {e}")
    sys.exit(1)

# Step 2: Load cookies
cookie_file = Path("cookies/robertparker.json")
if not cookie_file.exists():
    print(f"\n✗ Cookie file not found: {cookie_file}")
    sys.exit(1)

cookies = json.loads(cookie_file.read_text())
token = None
client_id = None
for c in cookies:
    if c["name"] == "RPWA_AUTH":
        try:
            auth = json.loads(unquote(c["value"]))
            token = auth.get("token")
            client_id = auth.get("clientId")
        except: pass

print(f"\n✓ Token: {token[:30]}..." if token else "\n✗ No token found")
print(f"✓ ClientId: {client_id}" if client_id else "✗ No clientId")

# Step 3: Test 1 - plain GET to RP with Chrome impersonation
print("\n--- TEST 1: curl_cffi with impersonate='chrome110' ---")
try:
    session = cffi_requests.Session(impersonate="chrome110")
    # Set cookies
    for c in cookies:
        session.cookies.set(c["name"], c["value"],
                           domain=c.get("domain","www.robertparker.com"))
    # Auth headers only
    if token:
        session.headers["Authorization"] = f"Bearer {token}"
    if client_id:
        session.headers["X-Client-Id"] = client_id
    session.headers["Referer"] = "https://www.robertparker.com/"

    r = session.get("https://www.robertparker.com/api/search",
                    params={"q": "Krug 1995", "type": "wine"},
                    timeout=20)
    ct = r.headers.get("Content-Type","?")
    print(f"  HTTP {r.status_code}, {len(r.content)} bytes, CT={ct}")
    if len(r.content) == 3627:
        print("  ✗ BLOCKED: still 3627 bytes HTML shell")
    elif b'<html' in r.content[:100]:
        print(f"  ✗ HTML response (not JSON). First 200 chars:")
        print(f"  {r.text[:200]}")
    else:
        print(f"  ✓ JSON response! First 200 chars:")
        print(f"  {r.text[:200]}")
except Exception as e:
    print(f"  ✗ Exception: {e}")

# Step 4: Test 2 - try different Chrome versions
print("\n--- TEST 2: Try different Chrome impersonation targets ---")
for target in ["chrome110", "chrome120", "chrome124", "chrome131", "chrome"]:
    try:
        s = cffi_requests.Session(impersonate=target)
        for c in cookies:
            s.cookies.set(c["name"], c["value"],
                         domain=c.get("domain","www.robertparker.com"))
        if token: s.headers["Authorization"] = f"Bearer {token}"
        if client_id: s.headers["X-Client-Id"] = client_id
        r = s.get("https://www.robertparker.com/api/search",
                  params={"q": "Krug", "type": "wine"}, timeout=15)
        ct = r.headers.get("Content-Type","?")[:40]
        blocked = len(r.content) == 3627
        print(f"  {target:20s}: HTTP {r.status_code}, {len(r.content):5d} bytes, CT={ct} {'✗ BLOCKED' if blocked else '✓ DIFFERENT SIZE'}")
        if not blocked:
            print(f"    → {r.text[:100]}")
            break
    except Exception as e:
        print(f"  {target:20s}: ERROR {str(e)[:60]}")

# Step 5: Check what headers curl_cffi actually sends
print("\n--- TEST 3: Check what headers curl_cffi sends ---")
try:
    s = cffi_requests.Session(impersonate="chrome110")
    r = s.get("https://httpbin.org/headers", timeout=10)
    if r.ok:
        data = r.json()
        headers = data.get("headers", {})
        print("  Headers curl_cffi sent to httpbin:")
        for k, v in sorted(headers.items()):
            print(f"    {k}: {v[:80]}")
    else:
        print(f"  httpbin returned {r.status_code}")
except Exception as e:
    print(f"  httpbin test failed: {e}")

print("\n" + "=" * 60)
print("Copy and paste ALL output above and share it")
print("=" * 60)