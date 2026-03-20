"""
sources/robertparker.py
=======================
Robert Parker Wine Advocate scraper.

ARCHITECTURE (reverse-engineered from browser traffic):
  - www.robertparker.com Ã¢â€ â€™ serves only index.html (pure React SPA)
  - api.robertparker.com Ã¢â€ â€™ all data, requires x-api-key header
  - Search: POST api.robertparker.com/v2/v2/algolia?sort=latest_review&type=wine
    Body: { "query": "<text>", "facetFilters": [["type:wine"],["vintage:YYYY"]], ... }
    NOTE: camelCase keys required; snake_case returns HTTP 400
  - Score/note are in hit["tasting_notes_history"][0] Ã¢â‚¬â€ no separate detail call needed
  - Detail: GET api.robertparker.com/v2/v2/wines/<id> (used as fallback only)

Score scale: /100
"""

import json
import re
import time
import os
import logging
import random
import unicodedata
from pathlib import Path
from urllib.parse import unquote, urlparse

try:
    from curl_cffi import requests
    _CFFI = True
except ImportError:
    import requests
    _CFFI = False

logger = logging.getLogger("maaike.rp")

RP_API_KEY  = "7ZPWPBFIRE2JLR6JBV5SCZPW54ZZSGGY"
BASE_API    = "https://api.robertparker.com/v2/v2"
COOKIE_FILE = "cookies/robertparker.json"
ALGOLIA_URL = f"{BASE_API}/algolia"
ALGOLIA_PARAMS = {"sort": "latest_review", "type": "wine"}
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
RP_LLM_MATCH_ENABLED = os.environ.get("RP_LLM_MATCH_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
RP_LLM_MODEL = os.environ.get("RP_LLM_MODEL", "gpt-4.1-mini").strip() or "gpt-4.1-mini"
RP_LLM_MIN_CONF = float(os.environ.get("RP_LLM_MIN_CONF", "0.70"))
RP_LLM_VERIFY_ENABLED = os.environ.get("RP_LLM_VERIFY_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
RP_LLM_VERIFY_MIN_CONF = float(os.environ.get("RP_LLM_VERIFY_MIN_CONF", "0.90"))
RP_STRICT_MODE = os.environ.get("RP_STRICT_MODE", "1").strip().lower() in ("1", "true", "yes", "on")
RP_STRICT_MIN_RANK = float(os.environ.get("RP_STRICT_MIN_RANK", "62"))
RP_STRICT_AMBIGUITY_DELTA = float(os.environ.get("RP_STRICT_AMBIGUITY_DELTA", "8"))


# Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬ Session Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬

def load_session(cookie_file: str = COOKIE_FILE):
    path = Path(cookie_file)
    if not path.exists():
        raise FileNotFoundError(f"Cookie file not found: {cookie_file}")

    cookies = json.loads(path.read_text())
    rp_auth = {}
    for c in cookies:
        if c["name"] == "RPWA_AUTH":
            try:
                rp_auth = json.loads(unquote(c["value"]))
            except Exception as e:
                logger.warning("Could not parse RPWA_AUTH: %s", e)

    if _CFFI:
        session = requests.Session(impersonate="chrome110")
    else:
        session = requests.Session()

    for c in cookies:
        session.cookies.set(
            c["name"], c["value"],
            domain=c.get("domain", "www.robertparker.com"),
            path=c.get("path", "/"),
        )

    session.headers.update({
        "x-api-key":                    RP_API_KEY,
        "authorizationtoken":           "allow",
        "content-type":                 "application/json",
        "cache-control":                "no-cache, no-store, must-revalidate",
        "pragma":                       "no-cache",
        "expires":                      "0",
        "access-control-allow-headers": "*",
        "Referer":                      "https://www.robertparker.com/",
    })

    if rp_auth.get("token"):
        session.headers["Authorization"] = f"Bearer {rp_auth['token']}"
        logger.info("RP: session loaded userId=%s expires=%s",
                    rp_auth.get("userId"), rp_auth.get("tokenExpiry"))
    else:
        logger.warning("RP: no auth token in RPWA_AUTH cookie")

    session._rp_auth = rp_auth
    return session


def check_session(session) -> dict:
    return {
        "source":    "robertparker",
        "has_token": "Authorization" in session.headers,
    }


# Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬ Public entry point Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬

def search_wine(
    session,
    name:      str,
    vintage:   int  = None,
    lwin:      str  = None,
    sleep_sec: float = 3.0,
    search_hints: dict | None = None,
) -> list[dict]:
    sleep_sec = max(1.5, min(sleep_sec, 360.0))
    rp_url = str((search_hints or {}).get("rp_search_url") or "").strip()
    if rp_url:
        direct = _fetch_review_from_url(session, rp_url)
        if direct:
            hit_name = direct.get("wine_name_src", "")
            if _name_matches(name, hit_name) and _vintage_matches(vintage, direct.get("vintage_src"), hit_name):
                logger.info("RP direct URL accepted: %r", rp_url)
                return [direct]
            logger.info("RP direct URL rejected by name/vintage guard: query=%r url=%r hit=%r", name, rp_url, hit_name)

    queries   = _build_rp_queries(name, vintage)

    for q in queries:
        logger.info("RP trying: %r", q)
        _jitter(sleep_sec * 0.4)

        hits = _algolia_search(session, q, vintage)
        if not hits:
            continue

        candidates = []

        for hit in hits[:20]:
            review = _parse_hit(hit)
            if not review:
                continue
            hit_name = review.get("wine_name_src", "")
            if not _name_matches(name, hit_name):
                logger.info("RP name mismatch - skipping: query=%r hit=%r", name, hit_name)
                continue
            if not _vintage_matches(vintage, review.get("vintage_src"), hit_name):
                logger.info(
                    "RP vintage mismatch - skipping: query_vintage=%r hit_vintage=%r hit=%r",
                    vintage, review.get("vintage_src"), hit_name
                )
                continue
            if _hard_reject_candidate(name, hit_name):
                logger.info("RP hard reject by collision rule: query=%r hit=%r", name, hit_name)
                continue

            rank = _candidate_rank(name, hit_name)
            logger.info("RP candidate rank %.2f for hit=%r", rank, hit_name)
            candidates.append({
                "rank": rank,
                "hit_name": hit_name,
                "review": review,
            })

        if candidates:
            candidates.sort(key=lambda c: c["rank"], reverse=True)
            best_review = candidates[0]["review"]
            best_name = candidates[0]["hit_name"]
            best_score = candidates[0]["rank"]

            llm_pick = _llm_pick_candidate(name, vintage, candidates[:8])
            if llm_pick is not None:
                best_review = llm_pick["review"]
                best_name = llm_pick["hit_name"]
                best_score = llm_pick["rank"]

            if RP_STRICT_MODE:
                if _is_ambiguous_top_candidates(candidates, name):
                    logger.info("RP strict abstain: ambiguous top candidates for query=%r", name)
                    continue
                if not _strict_accept_candidate(name, best_name, best_score):
                    logger.info(
                        "RP strict abstain: candidate below strict gate query=%r hit=%r rank=%.2f",
                        name, best_name, best_score
                    )
                    continue
            if not _llm_verify_exact_match(name, vintage, best_name):
                logger.info("RP LLM verify abstain: query=%r hit=%r", name, best_name)
                continue

            logger.info(
                "RP selected %r (rank %.2f) -> %.1f/100 (%s)",
                best_name,
                best_score,
                best_review["score_native"],
                best_review.get("reviewer", "?"),
            )
            return [best_review]

        logger.info("RP search %r -> %d hits but none passed name check", q, len(hits))

    return []


def _rp_wine_id_from_url(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    try:
        path = urlparse(raw).path or ""
    except Exception:
        path = raw
    m = re.search(r"/wines/([^/?#]+)", path)
    return m.group(1).strip() if m else ""


def _fetch_review_from_url(session, url: str) -> dict | None:
    wine_id = _rp_wine_id_from_url(url)
    if not wine_id:
        return None
    try:
        r = session.get(f"{BASE_API}/wines/{wine_id}", timeout=20)
    except Exception as e:
        logger.warning("RP detail request failed %r: %s", url, e)
        return None
    if not r.ok:
        logger.warning("RP detail HTTP %s for %r: %s", r.status_code, url, r.text[:120])
        return None
    try:
        data = r.json()
    except Exception as e:
        logger.warning("RP detail JSON error %r: %s", url, e)
        return None

    payload = data.get("data") if isinstance(data, dict) else None
    if isinstance(payload, dict):
        payload.setdefault("_id", wine_id)
        review = _parse_hit(payload)
        if review:
            review["review_url"] = str(url or review.get("review_url") or "").strip()
            return review
    return None
# Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬ Algolia search Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬

def _algolia_search(session, query: str, vintage) -> list[dict]:
    """
    POST /v2/v2/algolia?sort=latest_review&type=wine
    IMPORTANT: camelCase keys required; query goes in "query" field (not facetFilters).
    """
    facet_filters = [["type:wine"]]
    if vintage:
        facet_filters.append([f"vintage:{vintage}"])

    body = {
        "query":             query,
        "facetFilters":      facet_filters,
        "filters":           "rating_computed:50 TO 100",
        "hitsPerPage":       20,
        "page":              0,
        "facets":            ["*"],
        "sortFacetValuesBy": "count",
    }

    try:
        r = session.post(
            ALGOLIA_URL,
            params=ALGOLIA_PARAMS,
            json=body,
            timeout=20,
        )
    except Exception as e:
        logger.warning("RP algolia request failed %r: %s", query, e)
        return []

    logger.info("RP algolia %r Ã¢â€ â€™ HTTP %s, %d bytes",
                query, r.status_code, len(r.content))

    if not r.ok:
        logger.warning("RP algolia HTTP %s for %r: %s",
                       r.status_code, query, r.text[:100])
        return []

    try:
        data = r.json()
    except Exception as e:
        logger.warning("RP algolia JSON error %r: %s", query, e)
        return []

    if not data.get("success"):
        logger.warning("RP algolia failed %r: %s", query, data.get("message","?"))
        return []

    hits = data.get("data", {}).get("hits", [])
    nb   = data.get("data", {}).get("nbHits", 0)
    logger.info("RP algolia %r Ã¢â€ â€™ %d total hits, %d returned", query, nb, len(hits))

    # Only trust results when the search is specific enough
    if nb > 50 and len(query.split()) < 2:
        logger.info("RP algolia %r Ã¢â€ â€™ too many hits (%d), skipping", query, nb)
        return []

    return hits


# Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬ Hit parser Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬

def _parse_hit(hit: dict) -> dict | None:
    """Parse score + note from an Algolia hit's tasting_notes_history."""
    notes = hit.get("tasting_notes_history") or []
    if not notes:
        # Try fallback score fields at top level
        score = _extract_score(hit)
        if score is None:
            return None
        notes_text = ""
        reviewer   = None
        pub_date   = None
    else:
        # Find the best (highest-scored) note
        best = _best_note(notes)
        if best is None:
            return None
        score = _parse_rating_display(best.get("rating_display",""))
        if score is None:
            score = _extract_score(best)
        if score is None:
            return None
        notes_text = best.get("content") or best.get("note") or ""
        reviewer_obj = best.get("reviewer") or {}
        reviewer = (reviewer_obj.get("name") if isinstance(reviewer_obj, dict)
                    else str(reviewer_obj) if reviewer_obj else None)
        pub_date = best.get("published_at") or best.get("publishDate")
        if pub_date and isinstance(pub_date, (int, float)):
            # millisecond epoch Ã¢â€ â€™ ISO date string
            import datetime
            pub_date = datetime.datetime.utcfromtimestamp(
                pub_date / 1000).strftime("%Y-%m-%d")

    slug = hit.get("slug", "")
    wine_id = hit.get("_id", "")
    url = (f"https://www.robertparker.com/wines/{wine_id}/{slug}"
           if slug else f"https://www.robertparker.com/wines/{wine_id}")

    # Paywall check
    if notes_text and ("become a member" in notes_text.lower()
                       or "subscribe" in notes_text.lower()):
        notes_text = ""

    drink_from, drink_to = _parse_drink_window(
        hit.get("drink_date") or hit.get("drinkDate") or
        (notes[0].get("drink_date") if notes else None) or "")

    wine_name = (hit.get("display_name") or hit.get("name") or "")
    producer  = hit.get("producer") or ""
    if isinstance(producer, dict):
        producer = producer.get("name","")
    full_name = f"{producer} {wine_name}".strip() if producer else wine_name

    return {
        "score_native":  float(score),
        "note":          notes_text,
        "reviewer":      reviewer,
        "drink_from":    drink_from,
        "drink_to":      drink_to,
        "date_tasted":   pub_date,
        "vintage_src":   hit.get("vintage"),
        "review_url":    url,
        "colour":        _parse_colour(hit.get("color_class") or hit.get("colour")),
        "wine_name_src": full_name or wine_name,
        "score_label":   _rp_label(score),
    }


def _best_note(notes: list) -> dict | None:
    best, best_score = None, -1.0
    for n in notes:
        s = _parse_rating_display(n.get("rating_display",""))
        if s is None:
            s = _extract_score(n)
        if s is not None and s > best_score:
            best_score, best = s, n
    return best


def _parse_rating_display(raw: str) -> float | None:
    """Parse RP rating_display: '96', '93-95', '92+', '89+?', '(91-93)' Ã¢â€ â€™ float."""
    if not raw:
        return None
    s = str(raw).strip().strip("()")
    # Range: "93-95" or "93Ã¢â‚¬â€œ95"
    m = re.search(r"(\d{2,3})\s*[-Ã¢â‚¬â€œ]\s*(\d{2,3})", s)
    if m:
        mid = round((int(m.group(1)) + int(m.group(2))) / 2, 1)
        return mid if 50 <= mid <= 100 else None
    # Single with modifier: "92+" or "89+?"
    m2 = re.search(r"(\d{2,3})", s)
    if m2:
        f = float(m2.group(1))
        return f if 50 <= f <= 100 else None
    return None


def _extract_score(obj: dict) -> float | None:
    for k in ("score","rating","rounded_score","pointScore","points",
              "ratingComputed","rating_computed","rating_display"):
        v = obj.get(k)
        if v is not None:
            r = _parse_rating_display(str(v))
            if r is not None:
                return r
    lo = obj.get("scoreMin") or obj.get("ratingMin")
    hi = obj.get("scoreMax") or obj.get("ratingMax")
    if lo is not None and hi is not None:
        try:
            return round((float(lo) + float(hi)) / 2, 1)
        except Exception:
            pass
    return None


def _parse_drink_window(s) -> tuple:
    if not s:
        return None, None
    nums = re.findall(r"\d{4}", str(s))
    if len(nums) >= 2:
        return int(nums[0]), int(nums[1])
    if len(nums) == 1:
        return int(nums[0]), None
    return None, None


def _parse_colour(raw) -> str | None:
    if not raw:
        return None
    r = raw.strip().lower()
    if "red"   in r: return "Red"
    if "white" in r: return "White"
    if "ros"   in r: return "RosÃƒÂ©"
    if "spark" in r or "champagne" in r: return "Sparkling"
    return raw.title()


def _rp_label(score) -> str | None:
    if score is None: return None
    s = float(score)
    if s >= 96: return "Extraordinary"
    if s >= 90: return "Outstanding"
    if s >= 80: return "Above Average to Excellent"
    if s >= 70: return "Average"
    return "Below Average"


# Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬ Query builder Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬

def _build_rp_queries(name: str, vintage) -> list[str]:
    queries     = []
    full_clean  = re.sub(r"\b(?:Magnum|Dbl\.\s*Magnum)\b", "", name, flags=re.IGNORECASE).strip()
    parts       = [p.strip() for p in name.split(",")]
    parts       = [re.sub(r"\b\d{4}\b|\bMagnum\b|\bDbl\.\s*Magnum\b", "", p,
                          flags=re.IGNORECASE).strip()
                   for p in parts]
    parts       = [p for p in parts if p and len(p) > 2]
    vintage_str = str(vintage) if vintage else ""

    # Website-style keyword query first, closest to manual URL search behavior.
    if full_clean:
        if vintage_str and vintage_str not in full_clean:
            queries.append(f"{full_clean} {vintage_str}")
        queries.append(full_clean)

    if len(parts) >= 2:
        p0 = parts[0]
        p1 = parts[1]
        p2 = parts[2] if len(parts) > 2 else ""

        if _is_producer(p0):
            producer, wine, climat = p0, p1, p2
        else:
            wine, producer, climat = p0, p1, p2

        prod_short = re.sub(
            r"^(?:Domaine|Chateau|ChÃƒÂ¢teau|Ch\.|Dom\.|Azienda\s+Agricola"
            r"|Castello|Weingut|Bodegas|Quinta)\s+",
            "", producer, flags=re.IGNORECASE).strip()
        prod_last  = _last_name(prod_short)

        climat_clean = re.sub(
            r"\b(?:\d{4}|Grand|Premier|Cru|Classe|Riserva|GG|NV)\b",
            "", climat, flags=re.IGNORECASE).strip()

        if vintage_str:
            queries.append(f"{prod_last} {wine} {vintage_str}")
            if climat_clean and len(climat_clean) > 3:
                queries.append(f"{prod_last} {climat_clean} {vintage_str}")
        # Also try with full producer short name + wine (no vintage) as fallback
        queries.append(f"{prod_last} {wine}")
        if vintage_str:
            queries.append(f"{prod_short} {vintage_str}")
        queries.append(prod_short)

    else:
        clean = re.sub(r"\b(?:Magnum|Dbl\.\s*Magnum|\d{4})\b", "",
                       name, flags=re.IGNORECASE).strip()
        if vintage_str:
            queries.append(f"{clean} {vintage_str}")
        queries.append(clean)

    seen, result = set(), []
    for q in queries:
        q = " ".join(q.split()).strip()
        if q and len(q) > 2 and q not in seen:
            seen.add(q)
            result.append(q)
    return result[:6]


def _is_producer(text: str) -> bool:
    low = text.lower().strip()
    prefixes = ["domaine","chateau","chÃƒÂ¢teau","ch.","dom.","clos","maison",
                "cave","tenuta","castello","weingut","bodegas","quinta","mas",
                "azienda"]
    if any(low.startswith(p) for p in prefixes):
        return True
    avoid = ["grand","premier","cru","rouge","blanc","rosÃƒÂ©","vieilles",
             "vignes","classico","amarone","barolo","brunello","sauternes",
             "pomerol","margaux","saint","gevrey","chambolle","batard",
             "bÃƒÂ¢tard","montrachet","1er","riserva","toscana","rioja",
             "languedoc","alsace","champagne","bordeaux","morgon","rully"]
    words = text.split()
    if len(words) <= 3 and all(w[0].isupper() for w in words if w):
        if not any(kw in low for kw in avoid):
            return True
    return False


def _last_name(prod_short: str) -> str:
    if not prod_short:
        return prod_short
    prod_short = re.sub(
        r"\s+(?:Pere\s+et\s+Fils|et\s+Fils|&\s*Fils|Fils|et\s+Fille"
        r"|Freres|Heritiers)\s*$",
        "", prod_short, flags=re.IGNORECASE).strip()
    if "&" in prod_short:
        after = prod_short.split("&")[-1].strip().split()
        return after[-1] if after else prod_short
    words = prod_short.split()
    if len(words) <= 1:
        return prod_short
    if words[0].lower().rstrip("'") in ("d","de","du","des","le","la","les","l"):
        return prod_short
    return words[-1]


def _jitter(sleep_sec: float):
    time.sleep(max(0.3, sleep_sec + random.uniform(-0.2, 0.2)))


# Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬ Name similarity Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬

# Common words that carry no discriminating power Ã¢â‚¬â€ excluded from token matching
_STOP = {
    "chateau","chÃƒÂ¢teau","domaine","clos","maison","cave","mas","tenuta",
    "castello","weingut","bodegas","quinta","azienda","grand","premier",
    "cru","classe","riserva","rouge","blanc","rosÃƒÂ©","rose","vieilles",
    "vignes","les","des","the","and","von","van","del","dei","den",
    "magnum","dbl","double","jeroboam",
}

def _name_tokens(s: str) -> set:
    """Lowercase tokens longer than 2 chars, with stop-words and years removed."""
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"\b(19|20)\d{2}\b", "", s)          # strip vintage years
    s = re.sub(r"[^a-z0-9\s]", " ", s)              # remove punctuation
    return {t for t in s.split() if len(t) > 2 and t not in _STOP}


def _name_matches(query_name: str, hit_name: str, threshold: float = 0.45) -> bool:
    """
    Returns True if the RP result wine name is a plausible match for our wine.

    For 3-part names (Producer, Appellation, Cru/CuvÃƒÂ©e) the cru tokens MUST
    appear in the hit Ã¢â‚¬â€ this prevents "Conterno Barolo Monfortino" from
    matching "Conterno Barolo Francia" just because they share producer+appellation.

    For 1Ã¢â‚¬â€œ2 part names the standard token-overlap threshold applies.

    Examples:
      query "Charles Noellat, Nuits-Saint-Georges, Blanc"
        RP "Charles Noellat Nuits-Saint-Georges Blanc"        Ã¢â€ â€™ 100% Ã¢Å“â€œ
        RP "Charles Noellat Richebourg"                       Ã¢â€ â€™ 40%  Ã¢Å“â€”
      query "Giacomo Conterno, Barolo, Monfortino Riserva"
        RP "Giacomo Conterno Barolo Monfortino Riserva"       Ã¢â€ â€™ climat match Ã¢Å“â€œ
        RP "Giacomo Conterno Barolo Francia"                  Ã¢â€ â€™ climat miss  Ã¢Å“â€”
    """
    h_tok = _name_tokens(hit_name)

    # 3-part name: climat/cuvÃƒÂ©e tokens (3rd section) must appear in the hit
    parts = [p.strip() for p in query_name.split(",") if p.strip()]
    if len(parts) >= 3:
        climat_tok = _name_tokens(parts[2])
        if climat_tok and not (climat_tok & h_tok):
            return False   # cru not found in hit Ã¢â€ â€™ wrong wine

    q_tok = _name_tokens(query_name)
    if not q_tok:
        return True
    overlap = len(q_tok & h_tok)
    if overlap == 0:
        return False
    return overlap / len(q_tok) >= threshold


def _name_matches(query_name: str, hit_name: str, threshold: float = 0.45) -> bool:
    """
    Return True if RP hit name plausibly matches query wine name.

    Relaxed fallback for 3-part names so aliases/diacritics do not get rejected
    (e.g. Bouard vs Bouard), while still requiring solid producer overlap.
    """
    h_tok = _name_tokens(hit_name)
    q_tok = _name_tokens(query_name)
    if not q_tok:
        return True

    overlap = len(q_tok & h_tok)
    if overlap == 0:
        return False

    ratio = overlap / len(q_tok)
    if ratio >= threshold:
        return True

    parts = [p.strip() for p in query_name.split(",") if p.strip()]
    if len(parts) >= 3:
        producer_tok = _name_tokens(parts[0])
        climat_tok = _name_tokens(parts[2])
        has_cuvee = bool(climat_tok & h_tok) if climat_tok else True
        producer_ratio = (len(producer_tok & h_tok) / len(producer_tok)) if producer_tok else 0
        if has_cuvee:
            return ratio >= 0.35
        return ratio >= 0.35 and producer_ratio >= 0.5

    return ratio >= 0.35


def _soft_overlap_count(q_tok: set, h_tok: set) -> int:
    count = 0
    for qt in q_tok:
        matched = False
        for ht in h_tok:
            if qt == ht:
                matched = True
                break
            if len(qt) >= 5 and len(ht) >= 3 and (qt.endswith(ht) or ht.endswith(qt) or qt in ht or ht in qt):
                matched = True
                break
        if matched:
            count += 1
    return count


def _name_matches(query_name: str, hit_name: str, threshold: float = 0.45) -> bool:
    """Name match with soft token overlap for RP aliases/diacritics."""
    h_tok = _name_tokens(hit_name)
    q_tok = _name_tokens(query_name)
    if not q_tok:
        return True

    overlap = _soft_overlap_count(q_tok, h_tok)
    if overlap == 0:
        return False

    ratio = overlap / len(q_tok)
    if ratio >= threshold:
        return True

    parts = [p.strip() for p in query_name.split(",") if p.strip()]
    if len(parts) >= 3:
        producer_tok = _name_tokens(parts[0])
        climat_tok = _name_tokens(parts[2])
        has_cuvee = _soft_overlap_count(climat_tok, h_tok) > 0 if climat_tok else True
        producer_ratio = (_soft_overlap_count(producer_tok, h_tok) / len(producer_tok)) if producer_tok else 0
        if has_cuvee:
            return ratio >= 0.35
        return ratio >= 0.3 and producer_ratio >= 0.5

    return ratio >= 0.25


_SUBCUVEE_TOKENS = {
    "plus", "lion", "reserve", "riserva", "selection", "cuvee",
    "special", "old", "vines", "vineyard", "grand", "premier", "bricco",
}


def _candidate_rank(query_name: str, hit_name: str) -> float:
    """Rank acceptable RP candidates; higher is better."""
    q_tok = _name_tokens(query_name)
    h_tok = _name_tokens(hit_name)
    if not q_tok or not h_tok:
        return -9999.0

    overlap = _soft_overlap_count(q_tok, h_tok)
    if overlap <= 0:
        return -9999.0

    ratio = overlap / len(q_tok)
    score = ratio * 100.0

    parts = [p.strip() for p in query_name.split(",") if p.strip()]
    if parts:
        producer_tok = _name_tokens(parts[0])
        if producer_tok:
            score += (_soft_overlap_count(producer_tok, h_tok) / len(producer_tok)) * 20.0

    # Prefer fewer extra tokens in hit names.
    extra_tokens = [t for t in h_tok if t not in q_tok]
    score -= len(extra_tokens) * 0.6

    # If query does not request a sub-cuvee, penalize sub-cuvee hits like "Le Plus" / "Le Lion".
    query_has_subcuvee = any(t in q_tok for t in _SUBCUVEE_TOKENS)
    if not query_has_subcuvee:
        sub_hits = sum(1 for t in extra_tokens if t in _SUBCUVEE_TOKENS)
        score -= sub_hits * 12.0

    return score


def _extract_year(text) -> int | None:
    if text is None:
        return None
    m = re.search(r"\b(19|20)\d{2}\b", str(text))
    return int(m.group(0)) if m else None


def _vintage_matches(query_vintage, hit_vintage, hit_name: str) -> bool:
    """Strict vintage check when query vintage is present."""
    qy = _extract_year(query_vintage)
    if qy is None:
        return True

    hy = _extract_year(hit_vintage)
    if hy is None:
        hy = _extract_year(hit_name)

    # If RP did not expose a year on the hit, do not hard-fail.
    if hy is None:
        return True

    return hy == qy


# Final strict override to avoid drift from older duplicate versions above.
def _name_matches(query_name: str, hit_name: str, threshold: float = 0.50) -> bool:
    """Strict name matcher with producer guard and cuvee handling."""
    h_tok = _name_tokens(hit_name)
    q_tok = _name_tokens(query_name)
    if not q_tok:
        return True

    overlap = _soft_overlap_count(q_tok, h_tok)
    if overlap == 0:
        return False

    ratio = overlap / len(q_tok)

    parts = [p.strip() for p in query_name.split(",") if p.strip()]
    producer_tok = _name_tokens(parts[0]) if parts else set()
    producer_ratio = (_soft_overlap_count(producer_tok, h_tok) / len(producer_tok)) if producer_tok else 1.0

    # Never accept weak producer match; this cuts many wrong wines.
    if producer_ratio < 0.5:
        return False

    if len(parts) >= 3:
        climat_tok = _name_tokens(parts[2])
        has_cuvee = _soft_overlap_count(climat_tok, h_tok) > 0 if climat_tok else True
        if has_cuvee:
            return ratio >= 0.45
        return ratio >= 0.40 and producer_ratio >= 0.7

    return ratio >= threshold


# Final strict ranker override.
def _candidate_rank(query_name: str, hit_name: str) -> float:
    q_tok = _name_tokens(query_name)
    h_tok = _name_tokens(hit_name)
    if not q_tok or not h_tok:
        return -9999.0

    overlap = _soft_overlap_count(q_tok, h_tok)
    if overlap <= 0:
        return -9999.0

    ratio = overlap / len(q_tok)
    score = ratio * 100.0

    parts = [p.strip() for p in query_name.split(",") if p.strip()]
    producer_tok = _name_tokens(parts[0]) if parts else set()
    if producer_tok:
        pr = _soft_overlap_count(producer_tok, h_tok) / len(producer_tok)
        score += pr * 30.0
        if pr < 0.5:
            score -= 80.0

    extra_tokens = [t for t in h_tok if t not in q_tok]
    score -= len(extra_tokens) * 1.0

    query_has_subcuvee = any(t in q_tok for t in _SUBCUVEE_TOKENS)
    if not query_has_subcuvee:
        sub_hits = sum(1 for t in extra_tokens if t in _SUBCUVEE_TOKENS)
        score -= sub_hits * 20.0

    return score


def _llm_pick_candidate(query_name: str, query_vintage, candidates: list[dict]) -> dict | None:
    """
    Optional GPT reranker for ambiguous RP candidates.
    Returns selected candidate dict or None to keep rule-based pick.
    """
    if not (RP_LLM_MATCH_ENABLED and OPENAI_API_KEY):
        return None
    if len(candidates) < 2:
        return None

    payload = []
    for i, c in enumerate(candidates):
        r = c.get("review") or {}
        payload.append({
            "idx": i,
            "name": c.get("hit_name") or "",
            "score": r.get("score_native"),
            "reviewer": r.get("reviewer") or "",
            "date_tasted": r.get("date_tasted") or "",
            "rank": round(float(c.get("rank") or 0.0), 3),
        })

    prompt = {
        "query": {
            "name": query_name,
            "vintage": query_vintage,
        },
        "candidates": payload,
        "task": "Choose exact same wine only. Prefer base wine over sub-cuvee unless query asks sub-cuvee.",
        "rules": [
            "Vintage must match query when query vintage exists.",
            "Producer tokens must strongly match.",
            "If uncertain, return choose_idx = -1.",
        ],
        "output": {"choose_idx": "int", "confidence": "0..1", "reason": "short"},
    }

    data = {
        "model": RP_LLM_MODEL,
        "input": [
            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": "You are a strict wine matching judge. Respond with JSON only.",
                    }
                ],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": json.dumps(prompt, ensure_ascii=True),
                    }
                ],
            },
        ],
        "temperature": 0,
        "max_output_tokens": 180,
    }

    try:
        resp = requests.post(
            "https://api.openai.com/v1/responses",
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json=data,
            timeout=20,
        )
        if not resp.ok:
            logger.warning("RP LLM rerank HTTP %s: %s", resp.status_code, resp.text[:200])
            return None

        out = _extract_llm_text(resp.json())
        if not out:
            return None
        m = re.search(r"\{[\s\S]*\}", out)
        parsed = json.loads(m.group(0) if m else out)

        idx = int(parsed.get("choose_idx", -1))
        conf = float(parsed.get("confidence", 0.0))
        reason = str(parsed.get("reason", ""))[:200]

        if idx < 0 or idx >= len(candidates):
            logger.info("RP LLM rerank abstained: %s", reason)
            return None
        if conf < RP_LLM_MIN_CONF:
            logger.info("RP LLM rerank low confidence %.2f (< %.2f): %s", conf, RP_LLM_MIN_CONF, reason)
            return None

        logger.info("RP LLM rerank picked idx=%d conf=%.2f reason=%s", idx, conf, reason)
        return candidates[idx]
    except Exception as e:
        logger.warning("RP LLM rerank failed: %s", e)
        return None


def _extract_llm_text(obj: dict) -> str:
    if not isinstance(obj, dict):
        return ""
    if isinstance(obj.get("output_text"), str) and obj.get("output_text"):
        return obj["output_text"]

    chunks = []
    for item in obj.get("output", []) or []:
        for c in item.get("content", []) or []:
            if c.get("type") in ("output_text", "text") and isinstance(c.get("text"), str):
                chunks.append(c["text"])
    return "\n".join(chunks).strip()


def _is_ambiguous_top_candidates(candidates: list[dict]) -> bool:
    if len(candidates) < 2:
        return False
    top = float(candidates[0].get("rank") or 0.0)
    second = float(candidates[1].get("rank") or 0.0)
    return (top - second) < RP_STRICT_AMBIGUITY_DELTA


def _strict_accept_candidate(query_name: str, hit_name: str, rank: float) -> bool:
    """Precision-first acceptance gate. False means abstain -> not found."""
    if rank < RP_STRICT_MIN_RANK:
        return False

    q_tok = _name_tokens(query_name)
    h_tok = _name_tokens(hit_name)
    if not q_tok or not h_tok:
        return False

    overlap = _soft_overlap_count(q_tok, h_tok)
    ratio = overlap / len(q_tok) if q_tok else 0.0
    if ratio < 0.45:
        return False

    parts = [p.strip() for p in query_name.split(",") if p.strip()]
    producer_tok = _name_tokens(parts[0]) if parts else set()
    producer_ratio = (_soft_overlap_count(producer_tok, h_tok) / len(producer_tok)) if producer_tok else 0.0
    if producer_tok and producer_ratio < 0.80:
        return False

    if len(parts) >= 3:
        cuvee_tok = _name_tokens(parts[2])
        if cuvee_tok:
            q_has_sub = any(t in q_tok for t in _SUBCUVEE_TOKENS)
            h_has_sub = any(t in h_tok for t in _SUBCUVEE_TOKENS)
            if q_has_sub and not h_has_sub:
                return False

    return True


# Final override: keep strict behavior but avoid dropping valid same-producer/base-wine hits.
def _name_matches(query_name: str, hit_name: str, threshold: float = 0.50) -> bool:
    q_tok = _name_tokens(query_name)
    h_tok = _name_tokens(hit_name)
    if not q_tok:
        return True
    if not h_tok:
        return False

    overlap = _soft_overlap_count(q_tok, h_tok)
    if overlap == 0:
        return False

    ratio = overlap / len(q_tok)
    if ratio >= threshold:
        return True

    q_prod = _producer_head_tokens(query_name)
    producer_ratio = (_soft_overlap_count(q_prod, h_tok) / len(q_prod)) if q_prod else 0.0
    if q_prod and producer_ratio < 0.70:
        return False

    parts = [p.strip() for p in query_name.split(",") if p.strip()]
    if len(parts) >= 3:
        cuvee_tok = _name_tokens(parts[2])
        if cuvee_tok and _soft_overlap_count(cuvee_tok, h_tok) == 0:
            return False

    # Producer-strong fallback for query formats where appellation text is present
    # in query but not repeated in RP hit title.
    return ratio >= 0.35 and producer_ratio >= 0.90


def _strict_accept_candidate(query_name: str, hit_name: str, rank: float) -> bool:
    if rank < RP_STRICT_MIN_RANK:
        return False

    q_tok = _name_tokens(query_name)
    h_tok = _name_tokens(hit_name)
    if not q_tok or not h_tok:
        return False

    overlap = _soft_overlap_count(q_tok, h_tok)
    ratio = overlap / len(q_tok)
    if ratio < 0.35:
        return False

    q_prod = _producer_head_tokens(query_name)
    producer_ratio = (_soft_overlap_count(q_prod, h_tok) / len(q_prod)) if q_prod else 0.0
    if q_prod and producer_ratio < 0.70:
        return False

    # If overall overlap is moderate, require very strong producer agreement.
    if ratio < 0.45 and producer_ratio < 0.90:
        return False

    parts = [p.strip() for p in query_name.split(",") if p.strip()]
    if len(parts) >= 3:
        cuvee_tok = _name_tokens(parts[2])
        if cuvee_tok and _soft_overlap_count(cuvee_tok, h_tok) == 0:
            return False

    return True


def _llm_verify_exact_match(query_name: str, query_vintage, hit_name: str) -> bool:
    """Final GPT accept/reject gate for selected match."""
    if not RP_LLM_VERIFY_ENABLED:
        return True
    if not OPENAI_API_KEY:
        return True
    # Deterministically very strong matches don't need LLM veto.
    q_tok = _name_tokens(query_name)
    h_tok = _name_tokens(hit_name)
    if q_tok and h_tok:
        overlap = _soft_overlap_count(q_tok, h_tok)
        ratio = overlap / len(q_tok)
        q_prod = _producer_head_tokens(query_name)
        pr = (_soft_overlap_count(q_prod, h_tok) / len(q_prod)) if q_prod else 0.0
        if pr >= 0.90 and ratio >= 0.75:
            return True

    prompt = {
        "query": {
            "name": query_name,
            "vintage": query_vintage,
        },
        "candidate": {
            "name": hit_name,
        },
        "task": "Decide if candidate is the same exact wine as query.",
        "rules": [
            "Reject if producer is different.",
            "Reject if cuvee/appellation is different.",
            "Reject if vintage differs when query has vintage.",
            "If uncertain, reject.",
        ],
        "output": {"accept": "bool", "confidence": "0..1", "reason": "short"},
    }

    data = {
        "model": RP_LLM_MODEL,
        "input": [
            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": "You are a strict wine identity verifier. Return JSON only.",
                    }
                ],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": json.dumps(prompt, ensure_ascii=True),
                    }
                ],
            },
        ],
        "temperature": 0,
        "max_output_tokens": 140,
    }

    try:
        resp = requests.post(
            "https://api.openai.com/v1/responses",
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json=data,
            timeout=20,
        )
        if not resp.ok:
            logger.warning("RP LLM verify HTTP %s: %s", resp.status_code, resp.text[:200])
            return False

        out = _extract_llm_text(resp.json())
        if not out:
            return False

        m = re.search(r"\{[\s\S]*\}", out)
        parsed = json.loads(m.group(0) if m else out)
        accept = bool(parsed.get("accept", False))
        conf = float(parsed.get("confidence", 0.0))
        reason = str(parsed.get("reason", ""))[:180]

        if not accept:
            logger.info("RP LLM verify rejected: %s", reason)
            return False
        if conf < RP_LLM_VERIFY_MIN_CONF:
            logger.info("RP LLM verify low confidence %.2f (< %.2f): %s", conf, RP_LLM_VERIFY_MIN_CONF, reason)
            return False

        logger.info("RP LLM verify accepted conf=%.2f reason=%s", conf, reason)
        return True
    except Exception as e:
        logger.warning("RP LLM verify failed: %s", e)
        return False


# Manual calibration rules from first-100 benchmark.
_PRODUCER_COLLISION_DENY = [
    ("krug", "charles krug"),
]


def _producer_head_tokens(name: str) -> set:
    part = (name or "").split(",")[0]
    return _name_tokens(part)


def _hard_reject_candidate(query_name: str, hit_name: str) -> bool:
    qn = (query_name or "").lower()
    hn = (hit_name or "").lower()
    for q_need, h_forbid in _PRODUCER_COLLISION_DENY:
        if q_need in qn and h_forbid in hn:
            return True
    return False


# Override with stricter vintage handling for manual RP sheet behavior.
def _vintage_matches(query_vintage, hit_vintage, hit_name: str) -> bool:
    qy = _extract_year(query_vintage)
    if qy is None:
        return True

    hv_raw = str(hit_vintage or "").strip().lower()
    if hv_raw in ("nv", "n/v", "non vintage", "non-vintage"):
        return False

    hy = _extract_year(hit_vintage)
    if hy is None:
        hy = _extract_year(hit_name)
    if hy is None:
        return True
    return hy == qy


# Override ambiguity test: do not abstain when top is clearly base-wine style.
def _is_ambiguous_top_candidates(candidates: list[dict], query_name: str = "") -> bool:
    if len(candidates) < 2:
        return False

    top = float(candidates[0].get("rank") or 0.0)
    second = float(candidates[1].get("rank") or 0.0)
    delta = top - second

    top_name = str(candidates[0].get("hit_name") or "")
    second_name = str(candidates[1].get("hit_name") or "")
    q_tok = _name_tokens(query_name)
    t_tok = _name_tokens(top_name)
    s_tok = _name_tokens(second_name)

    # If top has no extra sub-cuvee token but second has one, keep top.
    top_sub = any(t in _SUBCUVEE_TOKENS for t in (t_tok - q_tok))
    sec_sub = any(t in _SUBCUVEE_TOKENS for t in (s_tok - q_tok))
    if (not top_sub) and sec_sub:
        return False

    return delta < RP_STRICT_AMBIGUITY_DELTA


# Final active matcher override.
def _name_matches(query_name: str, hit_name: str, threshold: float = 0.50) -> bool:
    q_tok = _name_tokens(query_name)
    h_tok = _name_tokens(hit_name)
    if not q_tok:
        return True
    if not h_tok:
        return False

    overlap = _soft_overlap_count(q_tok, h_tok)
    if overlap == 0:
        return False

    parts = [p.strip() for p in query_name.split(",") if p.strip()]
    if len(parts) >= 3:
        app_tok = _name_tokens(parts[1])
        if app_tok and _soft_overlap_count(app_tok, h_tok) == 0:
            return False
    if len(parts) >= 3:
        cuvee_tok = _name_tokens(parts[2])
        if cuvee_tok and _soft_overlap_count(cuvee_tok, h_tok) == 0:
            return False

    ratio = overlap / len(q_tok)
    if ratio >= threshold:
        return True

    q_prod = _producer_head_tokens(query_name)
    producer_ratio = (_soft_overlap_count(q_prod, h_tok) / len(q_prod)) if q_prod else 0.0
    if q_prod and producer_ratio < 0.70:
        return False

    return ratio >= 0.35 and producer_ratio >= 0.90


def _strict_accept_candidate(query_name: str, hit_name: str, rank: float) -> bool:
    if rank < RP_STRICT_MIN_RANK:
        return False

    q_tok = _name_tokens(query_name)
    h_tok = _name_tokens(hit_name)
    if not q_tok or not h_tok:
        return False

    overlap = _soft_overlap_count(q_tok, h_tok)
    ratio = overlap / len(q_tok)
    if ratio < 0.35:
        return False

    q_prod = _producer_head_tokens(query_name)
    producer_ratio = (_soft_overlap_count(q_prod, h_tok) / len(q_prod)) if q_prod else 0.0
    if q_prod and producer_ratio < 0.70:
        return False

    if ratio < 0.45 and producer_ratio < 0.90:
        return False

    parts = [p.strip() for p in query_name.split(",") if p.strip()]
    if len(parts) >= 3:
        app_tok = _name_tokens(parts[1])
        if app_tok and _soft_overlap_count(app_tok, h_tok) == 0:
            return False
    if len(parts) >= 3:
        cuvee_tok = _name_tokens(parts[2])
        if cuvee_tok and _soft_overlap_count(cuvee_tok, h_tok) == 0:
            return False

    return True
