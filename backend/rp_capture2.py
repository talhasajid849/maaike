"""
Run: python rp_capture2.py
Captures the EXACT request body sent to api.robertparker.com/v2/v2/algolia
"""
import asyncio, json
from urllib.parse import unquote
from pathlib import Path

async def main():
    from playwright.async_api import async_playwright

    cookies_raw = json.loads(Path("cookies/robertparker.json").read_text())
    pw_cookies = []
    for c in cookies_raw:
        pw_cookies.append({
            "name": c["name"], "value": c["value"],
            "domain": c.get("domain", "www.robertparker.com"),
            "path": c.get("path", "/"),
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

        captured = []

        async def on_request(request):
            if "algolia" in request.url:
                body = None
                try:
                    body = request.post_data
                except:
                    pass
                captured.append({
                    "url": request.url,
                    "method": request.method,
                    "headers": dict(request.headers),
                    "body": body,
                })

        async def on_response(response):
            if "algolia" in response.url:
                try:
                    body = await response.body()
                    for r in captured:
                        if r["url"] == response.url and "response" not in r:
                            r["response_status"] = response.status
                            r["response"] = body[:500].decode('utf-8', errors='replace')
                except:
                    pass

        page.on("request", on_request)
        page.on("response", on_response)

        # Navigate to search WITH a real query
        print("Loading search with query 'Krug 1995'...")
        try:
            await page.goto(
                "https://www.robertparker.com/search/wine?q=Krug+1995&type=wine",
                wait_until="domcontentloaded", timeout=20000
            )
            await asyncio.sleep(6)
        except Exception as e:
            print(f"Navigation: {e}")

        print(f"\nCaptured {len(captured)} algolia requests:\n")
        for req in captured:
            print(f"  URL: {req['url']}")
            print(f"  Method: {req['method']}")
            print(f"  Status: {req.get('response_status','?')}")
            print(f"  Request Body: {req.get('body','(empty)')}")
            print(f"  Response: {req.get('response','?')}")
            print(f"  Headers:")
            for k, v in sorted(req['headers'].items()):
                print(f"    {k}: {v[:100]}")
            print()

        await browser.close()

asyncio.run(main())