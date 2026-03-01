#!/usr/bin/env python3
"""
MAAIKE Phase 1 — JancisRobinson.com Review Extractor
Search strategy: LWIN7 → wine name variants → name without vintage
Extracts: score, full tasting note, reviewer, drink window, review count
"""
from __future__ import annotations

import base64
import json
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

JR_ORIGIN  = "https://www.jancisrobinson.com"
JR_MSEARCH = "https://searchserver.jancisrobinson.com/elasticsearch_index_main_tasting_notes/_msearch"

# ─── JWT Helpers ──────────────────────────────────────────────────────────────

def _b64d(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))

def _jwt_payload(token: str) -> dict:
    try:
        return json.loads(_b64d(token.split(".")[1]).decode("utf-8", errors="replace"))
    except Exception:
        return {}

def jwt_days_remaining(token: str) -> int:
    try:
        payload = _jwt_payload(token)
        return (int(payload.get("exp", 0)) - int(datetime.now(timezone.utc).timestamp())) // 86400
    except Exception:
        return -1

def _jwt_bool(token: str, field: str) -> bool:
    return bool(_jwt_payload(token).get(field, False))


# ─── LWIN Helpers ─────────────────────────────────────────────────────────────

def parse_lwin(lwin: str) -> Dict[str, str]:
    """Parse full LWIN into LWIN7, vintage, qty, size."""
    if not lwin:
        return {}
    raw = lwin.upper().strip()
    # Strip LWIN prefix
    if raw.startswith("LWIN"):
        digits = raw[4:]
    else:
        digits = raw
    if len(digits) < 7:
        return {}
    return {
        "lwin7":   digits[:7],
        "lwin11":  digits[:11] if len(digits) >= 11 else digits,
        "vintage": digits[7:11] if len(digits) >= 11 else "",
        "qty":     digits[11:13] if len(digits) >= 13 else "",
        "size_ml": str(int(digits[13:18])) if len(digits) >= 18 and digits[13:18].isdigit() else "",
    }


# ─── Session ──────────────────────────────────────────────────────────────────

def load_session(cookie_path: str = "real_cookies.json") -> requests.Session:
    with open(cookie_path, encoding="utf-8") as f:
        cookies = json.load(f)

    jr = next((c for c in cookies if c.get("name") == "jrAccessRole"), None)
    if not jr:
        raise RuntimeError("jrAccessRole cookie missing")

    tok  = jr.get("value", "")
    days = jwt_days_remaining(tok)
    has_sess = any(c.get("name", "").upper().startswith(("SESS", "SSESS")) for c in cookies)

    print("=" * 56)
    print("  MAAIKE session loaded")
    print(f"  JWT valid        : {days} days")
    print(f"  Member           : {_jwt_bool(tok, 'isMember')}")
    print(f"  Tasting access   : {_jwt_bool(tok, 'canAccessTastingNotes')}")
    print(f"  Drupal session   : {'YES' if has_sess else 'MISSING — scores may show as XX'}")
    print("=" * 56)

    if days < 0:
        raise SystemExit("JWT expired. Refresh cookies from browser.")

    s = requests.Session()
    for c in cookies:
        n = c.get("name"); v = c.get("value")
        d = c.get("domain") or ".jancisrobinson.com"
        p = c.get("path", "/")
        if "jancisrobinson" in d and not d.startswith("."):
            d = ".jancisrobinson.com"
        s.cookies.set(n, v, domain=d, path=p)

    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/132.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": JR_ORIGIN,
        "Referer": JR_ORIGIN + "/tastings",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "X-Requested-With": "XMLHttpRequest",
    })
    return s


# ─── Name Variants ────────────────────────────────────────────────────────────

def _clean_name_variants(name: str) -> List[str]:
    base = name.strip().lower()
    base = re.sub(r"[''`]", "", base)
    base = re.sub(r"\.", "", base)
    base = re.sub(r"\s+", " ", base)
    base = re.sub(r"\s*\(?\b(19|20)\d{2}\b\)?$", "", base).strip()

    variants = {base}

    for prefix in ["chateau", "chateau", "ch ", "domaine", "dom ", "clos ", "maison ", "cave "]:
        p = prefix.rstrip()
        if base.startswith(p + " "):
            variants.add(base[len(p)+1:].strip())
        elif base.startswith(p):
            variants.add(base[len(p):].strip())

    for suffix in [" pere et fils", " freres", " et fils", " & fils"]:
        if base.endswith(suffix):
            variants.add(base[:-len(suffix)].strip())

    return list({v.title() for v in variants if v})


# ─── ES Payloads ──────────────────────────────────────────────────────────────

_SRC = ["url","title","score_number","score_modifier","note",
        "drink_date_from","drink_date_to","appellation","colour",
        "date_tasted","wine_name","vintage","lwin"]

def _payload_lwin(lwin7: str, vintage: str = "") -> str:
    must = [{"prefix": {"lwin": lwin7}}, {"term": {"status": "published"}}]
    if vintage:
        must.append({"match_phrase": {"vintage": vintage}})
    body = {"query":{"bool":{"must":must}},"size":20,"from":0,
            "sort":[{"date_tasted":{"order":"desc"}},{"_score":{"order":"desc"}}],
            "_source":_SRC}
    return "{}\n" + json.dumps(body, separators=(",",":")) + "\n"

def _payload_name(query: str, vintage: str = "") -> str:
    must = [{"match_phrase":{"wine_name":query}},
            {"range":{"score_number":{"gte":0}}},
            {"term":{"status":"published"}}]
    if vintage:
        must.append({"match_phrase":{"vintage":vintage}})
    body = {"query":{"bool":{"must":must}},"size":20,"from":0,
            "sort":[{"date_tasted":{"order":"desc"}},{"_score":{"order":"desc"}}],
            "_source":_SRC}
    return "{}\n" + json.dumps(body, separators=(",",":")) + "\n"


# ─── ES Request ───────────────────────────────────────────────────────────────

def _do_msearch(session: requests.Session, payload: str) -> List[Dict]:
    try:
        r = session.post(JR_MSEARCH, data=payload,
                         headers={"Content-Type":"application/x-ndjson"}, timeout=15)
        r.raise_for_status()
        data = r.json()
    except requests.HTTPError as e:
        print(f"    [HTTP {e.response.status_code}] {e.response.text[:200]}")
        return []
    except Exception as e:
        print(f"    [ES ERR] {type(e).__name__}: {e}")
        return []

    hits = []
    for resp in data.get("responses", []):
        if not isinstance(resp, dict):
            continue
        for h in resp.get("hits", {}).get("hits", []):
            src = h.get("_source")
            if isinstance(src, str):
                try: src = json.loads(src)
                except: continue
            if isinstance(src, dict):
                hits.append(src)
    return hits


# ─── Smart Multi-Strategy Search ─────────────────────────────────────────────

def jr_msearch(session: requests.Session, wine_name: str, vintage: str, lwin: str = "") -> List[Dict]:
    """
    1. LWIN7 + vintage
    2. LWIN7 alone
    3. Name variants + vintage
    4. Name variants alone
    """
    lwin_info = parse_lwin(lwin) if lwin else {}
    lwin7     = lwin_info.get("lwin7", "")

    if lwin7:
        print(f"    [LWIN7={lwin7}] vintage={vintage or 'any'}")
        hits = _do_msearch(session, _payload_lwin(lwin7, vintage))
        if hits:
            print(f"    -> {len(hits)} hit(s) via LWIN7")
            return hits[:20]
        if vintage:
            hits = _do_msearch(session, _payload_lwin(lwin7, ""))
            if hits:
                print(f"    -> {len(hits)} hit(s) via LWIN7 (no vintage)")
                return hits[:20]

    variants = _clean_name_variants(wine_name)
    for v in variants:
        print(f"    [NAME='{v}'] vintage={vintage or 'any'}")
        hits = _do_msearch(session, _payload_name(v, vintage))
        if hits:
            print(f"    -> {len(hits)} hit(s) via name")
            return hits[:20]

    if vintage:
        for v in variants:
            hits = _do_msearch(session, _payload_name(v, ""))
            if hits:
                print(f"    -> {len(hits)} hit(s) via name (no vintage)")
                return hits[:20]

    print("    -> 0 results")
    return []


# ─── HTML Full-Page Parser ────────────────────────────────────────────────────

def _fetch_full_page(session: requests.Session, url: str) -> Dict:
    if not url:
        return {}
    try:
        r = session.get(url, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print(f"    [HTML ERR] {url} -> {e}")
        return {}

    soup   = BeautifulSoup(r.text, "html.parser")
    result: Dict[str, Any] = {}

    # Score
    se = soup.find("div", class_="tastingNoteScore")
    if se:
        sp = se.find("span")
        if sp:
            try:
                result["score_20"] = float(sp.get_text(strip=True))
                result["score"]    = round(result["score_20"] * 5.0, 1)
            except ValueError:
                pass

    # Full note
    nd = (soup.find("div", class_="tastingNotePage__body")
          or soup.find("div", class_="tasting-note-body")
          or soup.find("div", class_="field--name-field-tasting-note"))
    if nd:
        note = " ".join(p.get_text(strip=True) for p in nd.find_all("p") if p.get_text(strip=True))
        note = re.sub(r"\s+", " ", note).strip()
        if note:
            result["tasting_note"] = note

    # Reviewer
    rl = soup.find("a", href=re.compile(r"/author/|/writers/"))
    if rl:
        result["reviewer"] = rl.get_text(strip=True)

    # Drink window
    dt = soup.find(string=re.compile(r"\b(19|20)\d{2}\s*[-\u2013]\s*(19|20)\d{2}\b"))
    if dt:
        m = re.search(r"(\d{4})\s*[-\u2013]\s*(\d{4})", dt)
        if m:
            result["drink_from"] = int(m.group(1))
            result["drink_to"]   = int(m.group(2))

    return result


# ─── Field Helpers ────────────────────────────────────────────────────────────

def _first(v: Any) -> str:
    if isinstance(v, list):
        return next((str(x).strip() for x in v if x), "")
    return str(v).strip() if v else ""

def _parse_year(s: str) -> Optional[int]:
    m = re.search(r"\b(19\d{2}|20[0-3]\d)\b", str(s))
    if not m:
        return None
    year = int(m.group(1))
    return year if year > 1900 else None  # 1900 is JR sentinel for "no data"

def _parse_date(s: str) -> str:
    s = str(s or "").strip()
    if not s:
        return ""
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").strftime("%d %b %Y")
    except Exception:
        return s

def _parse_score(src: Dict) -> Optional[float]:
    raw = _first(src.get("score_number", []))
    if not raw or raw in ("XX", "x", ""):
        return None
    try:
        score = float(raw)
    except ValueError:
        return None
    mod = _first(src.get("score_modifier", []))
    if mod == "+":    score += 0.3
    elif mod == "++": score += 0.6
    return score


# ─── Main Entry Point ─────────────────────────────────────────────────────────

def search_wine(session: requests.Session, wine_name: str, vintage: str, lwin: str = "") -> List[Dict]:
    """
    Search JancisRobinson for a wine.
    Returns list of reviews sorted by score desc.
    Each review includes: score_20, tasting_note, reviewer, drink_from/to, review_url
    """
    es_hits = jr_msearch(session, wine_name, vintage, lwin)
    reviews: List[Dict] = []

    for src in es_hits:
        if not isinstance(src, dict):
            continue

        url_raw = src.get("url")
        if isinstance(url_raw, list):
            url_raw = url_raw[0] if url_raw else ""
        url = urljoin(JR_ORIGIN, str(url_raw or "").strip("[]'\" "))

        es_score = _parse_score(src)
        rev: Dict[str, Any] = {
            "score_20":     es_score,
            "score":        round(es_score * 5.0, 1) if es_score is not None else None,
            "reviewer":     "",
            "drink_from":   _parse_year(_first(src.get("drink_date_from", []))),
            "drink_to":     _parse_year(_first(src.get("drink_date_to", []))),
            "tasting_note": _first(src.get("note", [])),
            "region":       _first(src.get("appellation", [])),
            "colour":       _first(src.get("colour", [])),
            "date_tasted":  _parse_date(src.get("date_tasted")),
            "wine_name_jr": str(src.get("wine_name") or "").strip(),
            "review_url":   url,
            "jr_lwin":      _first(src.get("lwin", [])),
        }

        if url:
            html = _fetch_full_page(session, url)
            if html.get("score_20") is not None:
                rev["score_20"] = html["score_20"]
                rev["score"]    = html.get("score")
            if html.get("tasting_note"):
                rev["tasting_note"] = html["tasting_note"]
            if html.get("reviewer"):
                rev["reviewer"] = html["reviewer"]
            if html.get("drink_from") and not rev["drink_from"]:
                rev["drink_from"] = html["drink_from"]
            if html.get("drink_to") and not rev["drink_to"]:
                rev["drink_to"] = html["drink_to"]

        if rev.get("score_20") is not None or rev.get("tasting_note"):
            reviews.append(rev)

        time.sleep(0.25)

    reviews.sort(key=lambda x: x.get("score_20") or 0, reverse=True)
    print(f"    -> {len(reviews)} usable, {sum(1 for r in reviews if r.get('score_20') is not None)} scored")
    return reviews


# ─── CLI Test ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    wine    = sys.argv[1] if len(sys.argv) > 1 else "Giuseppe Rinaldi, Barolo, Brunate"
    vintage = sys.argv[2] if len(sys.argv) > 2 else "2021"
    lwin    = sys.argv[3] if len(sys.argv) > 3 else "LWIN110419020211200750"

    print(f"\nTest: {wine} {vintage}")
    print(f"LWIN parsed: {parse_lwin(lwin)}\n")

    s  = load_session("real_cookies.json")
    rs = search_wine(s, wine, vintage, lwin)

    print(f"\n{'='*70}\n{len(rs)} result(s)\n{'='*70}")
    for r in rs:
        print(f"\n★ {r.get('score_20')}/20  |  {r.get('wine_name_jr', wine)}")
        print(f"  Reviewer   : {r.get('reviewer') or '—'}")
        print(f"  Drink      : {r.get('drink_from','?')} – {r.get('drink_to','?')}")
        print(f"  JR LWIN    : {r.get('jr_lwin','—')}")
        note = r.get("tasting_note") or ""
        print(f"  Note       : {note[:300]}{'...' if len(note)>300 else ''}")
        print(f"  URL        : {r.get('review_url','')}")
        print("─" * 70)