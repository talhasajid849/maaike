"""
Run: python rp_playwright_test.py
Tests Playwright interception directly - tells us what the browser actually sees.
"""
import asyncio, json
from urllib.parse import unquote
from pathlib import Path

async def main():
    from playwright.async_api import async_playwright

    cookies_raw = json.loads(Path("cookies/robertparker.json").read_text())
    token = None
    for c in cookies_raw:
        if c["name"] == "RPWA_AUTH":
            auth = json.loads(unquote(c["value"]))
            token = auth.get("token")

    pw_cookies = []
    for c in cookies_raw:
        domain = c.get("domain", "www.robertparker.com")
        pw_cookies.append({
            "name": c["name"], "value": c["value"],
            "domain": domain, "path": c.get("path", "/"),
            "secure": c.get("secure", True),
            "httpOnly": c.get("httpOnly", False),
        })

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=["--no-sandbox"])
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        )
        await context.add_cookies(pw_cookies)

        # Inject auth header for ALL requests
        await context.set_extra_http_headers({
            "Authorization": f"Bearer {token}",
        })

        page = await context.new_page()
        all_responses = []

        async def on_response(response):
            url = response.url
            status = response.status
            ct = response.headers.get("content-type", "?")
            size = 0
            body_preview = ""
            try:
                body = await response.body()
                size = len(body)
                if b'<' not in body[:5] and size > 10:
                    body_preview = body[:100].decode('utf-8', errors='replace')
            except:
                pass
            if "robertparker.com" in url:
                all_responses.append((url, status, ct[:40], size, body_preview))

        page.on("response", on_response)

        print("Navigating to search page...")
        try:
            await page.goto(
                "https://www.robertparker.com/search/wine?q=Krug+1995&type=wine",
                wait_until="domcontentloaded",
                timeout=20000
            )
            print("DOM loaded, waiting for network...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"goto error: {e}")

        print(f"\nAll robertparker.com responses ({len(all_responses)}):")
        for url, status, ct, size, preview in sorted(all_responses, key=lambda x: -x[3]):
            print(f"  [{status}] {size:6d}b  {ct:40s}  {url[-80:]}")
            if preview:
                print(f"           → {preview}")

        # Check if we're logged in
        title = await page.title()
        print(f"\nPage title: {title}")

        # Check page content for login indicators
        content = await page.content()
        if "log in" in content.lower() or "sign in" in content.lower():
            print("⚠ Page may be showing login prompt")
        if "Krug" in content:
            print("✓ 'Krug' found in page content")
        else:
            print("✗ 'Krug' not found in page content")

        # Try direct API call from within the page context (same-origin)
        print("\nTrying fetch from within page context (same-origin)...")
        try:
            result = await page.evaluate("""
                async () => {
                    const r = await fetch('/api/search?q=Krug+1995&type=wine', {
                        headers: {
                            'Accept': 'application/json',
                        }
                    });
                    const ct = r.headers.get('content-type');
                    const text = await r.text();
                    return {status: r.status, ct: ct, size: text.length, preview: text.substring(0, 200)};
                }
            """)
            print(f"  In-page fetch: HTTP {result['status']}, {result['size']} bytes, CT={result['ct']}")
            print(f"  Preview: {result['preview']}")
        except Exception as e:
            print(f"  In-page fetch failed: {e}")

        await browser.close()

asyncio.run(main())