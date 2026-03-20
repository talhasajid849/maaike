"""
Run: python rp_capture_test.py
Captures EVERY request header the browser sends to api.robertparker.com
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
        page = await context.new_page()

        captured_requests = []

        async def on_request(request):
            if "api.robertparker.com" in request.url:
                captured_requests.append({
                    "url": request.url,
                    "method": request.method,
                    "headers": dict(request.headers),
                })

        async def on_response(response):
            if "api.robertparker.com" in response.url:
                try:
                    body = await response.body()
                    # Find matching request
                    for req in captured_requests:
                        if req["url"] == response.url and "body" not in req:
                            req["response_status"] = response.status
                            req["response_size"] = len(body)
                            req["response_preview"] = body[:300].decode('utf-8', errors='replace')
                            break
                except:
                    pass

        page.on("request", on_request)
        page.on("response", on_response)

        print("Loading search page and waiting for API calls...")
        try:
            await page.goto(
                "https://www.robertparker.com/search/wine?q=Krug&type=wine",
                wait_until="domcontentloaded", timeout=20000
            )
            await asyncio.sleep(6)  # wait for JS to execute and make API calls
        except Exception as e:
            print(f"Navigation error: {e}")

        print(f"\n{'='*60}")
        print(f"Captured {len(captured_requests)} requests to api.robertparker.com:")
        for req in captured_requests:
            print(f"\n  URL: {req['url']}")
            print(f"  Method: {req['method']}")
            print(f"  Status: {req.get('response_status','?')} | Size: {req.get('response_size','?')}b")
            print(f"  Headers sent:")
            for k, v in sorted(req['headers'].items()):
                # Truncate long values
                v_display = v[:80] + "..." if len(v) > 80 else v
                print(f"    {k}: {v_display}")
            if req.get('response_preview'):
                print(f"  Response: {req['response_preview']}")

        await browser.close()

asyncio.run(main())