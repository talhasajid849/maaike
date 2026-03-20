#!/usr/bin/env python3
"""
MAAIKE Phase 1 â€” JancisRobinson.com Review Extractor
Search strategy: LWIN7 â†’ wine name variants â†’ name without vintage
Extracts: score, full tasting note, reviewer, drink window, review count
"""
from __future__ import annotations

import base64
import json
import logging
import os
import re
import sys
import time
import unicodedata
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, unquote, urlencode, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

JR_ORIGIN  = "https://www.jancisrobinson.com"
JR_MSEARCH = "https://searchserver.jancisrobinson.com/elasticsearch_index_main_tasting_notes/_msearch"
logger = logging.getLogger("maaike.jr")

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
JR_LLM_MATCH_ENABLED = os.environ.get("JR_LLM_MATCH_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
JR_LLM_MODEL = os.environ.get("JR_LLM_MODEL", "gpt-4.1-mini").strip() or "gpt-4.1-mini"
JR_LLM_MIN_CONF = float(os.environ.get("JR_LLM_MIN_CONF", "0.78"))
JR_LLM_VERIFY_ENABLED = os.environ.get("JR_LLM_VERIFY_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
JR_LLM_VERIFY_MIN_CONF = float(os.environ.get("JR_LLM_VERIFY_MIN_CONF", "0.82"))
JR_STRICT_MODE = os.environ.get("JR_STRICT_MODE", "1").strip().lower() in ("1", "true", "yes", "on")
JR_STRICT_MIN_RANK = float(os.environ.get("JR_STRICT_MIN_RANK", "55"))
JR_STRICT_AMBIGUITY_DELTA = float(os.environ.get("JR_STRICT_AMBIGUITY_DELTA", "7"))
JR_PLAUSIBLE_MIN_RANK = float(os.environ.get("JR_PLAUSIBLE_MIN_RANK", "60"))
SEARCH_MAX_QUERY_VARIANTS = int(os.environ.get("SEARCH_MAX_QUERY_VARIANTS", "8"))
SEARCH_MAX_QUERY_VARIANTS_WITH_HINTS = int(os.environ.get("SEARCH_MAX_QUERY_VARIANTS_WITH_HINTS", "6"))
JR_HTML_MAX_TERMS_WITH_HINTS = int(os.environ.get("JR_HTML_MAX_TERMS_WITH_HINTS", "4"))
JR_DEBUG_SEARCH = os.environ.get("JR_DEBUG_SEARCH", "0").strip().lower() in ("1", "true", "yes", "on")
MATCH_REQUIRE_EXACT_VINTAGE = os.environ.get("MATCH_REQUIRE_EXACT_VINTAGE", "1").strip().lower() in ("1", "true", "yes", "on")
MATCH_WINE_COVERAGE = float(os.environ.get("MATCH_WINE_COVERAGE", "0.60"))
MATCH_REMAINING_APPELLATION_COVERAGE = float(os.environ.get("MATCH_REMAINING_APPELLATION_COVERAGE", "0.60"))
MATCH_TAIL_REQUIRED_RATIO = float(os.environ.get("MATCH_TAIL_REQUIRED_RATIO", "1.00"))
MATCH_RELAXED_COVERAGE = float(os.environ.get("MATCH_RELAXED_COVERAGE", "0.55"))
MATCH_LEAD_WINE_FALLBACK_COVERAGE = float(os.environ.get("MATCH_LEAD_WINE_FALLBACK_COVERAGE", "0.75"))


def _jr_debug(msg: str) -> None:
    if JR_DEBUG_SEARCH:
        print(f"    [JR DEBUG] {msg}")

# â”€â”€â”€ JWT Helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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


# â”€â”€â”€ LWIN Helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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


# â”€â”€â”€ Session â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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
    print(f"  Drupal session   : {'YES' if has_sess else 'MISSING â€” scores may show as XX'}")
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


# â”€â”€â”€ Name Matching â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

_JR_STOP = {
    "chateau","chÃ¢teau","domaine","clos","maison","cave","grand","premier",
    "cru","classe","riserva","rouge","blanc","rosÃ©","rose","vieilles","vignes",
    "les","des","the","and","von","van","del","dei",
    "azienda","agricola","tenuta","societa","bodega","bodegas","weingut",
}

# â”€â”€â”€ Region / Classification Terms â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# When the 3rd comma-part of a wine name is one of these, it is a region/
# classification â€” NOT a specific cru/vineyard.  In that case we treat the
# name as a 2-part name (Producer, WineName) and use the 2nd part for search.
_REGION_TERMS = {
    # Generic wine classifications
    "vdf", "vdt", "igt", "doc", "docg", "aoc", "aop", "do", "dop", "ava",
    "classico", "superiore", "reserva",
    # German / Austrian wine regions
    "mosel", "pfalz", "franken", "rheingau", "rheinhessen", "nahe", "ahr",
    "saar", "wachau", "kamptal", "kremstal", "steiermark",
    # Spanish regions
    "rioja", "ribera del duero", "priorat", "montsant", "bierzo", "rias baixas",
    "rueda", "penedes",
    # Italian regions
    "tuscany", "toscana", "piemonte", "piedmont", "veneto", "sicilia", "sicily",
    "umbria",
    # French regions
    "bordeaux", "bourgogne", "champagne", "alsace", "provence",
    "cotes de provence", "cotes du rhone", "roussillon", "languedoc",
    # French sub-appellations / AOC
    "haut-medoc", "medoc", "saint-julien", "pauillac", "margaux",
    "saint-estephe", "saint-emilion", "saint-emilion grand cru",
    "pomerol", "lalande-de-pomerol", "lalande de pomerol",
    "pessac-leognan", "graves", "sauternes", "barsac",
    "fronsac", "canon-fronsac", "blaye", "bourg", "castillon",
    # RhÃ´ne / Southern France
    "cote-rotie", "cote rotie", "condrieu", "cornas", "hermitage",
    "crozes-hermitage", "gigondas", "vacqueyras", "chateauneuf-du-pape",
    "bandol", "cassis",
    # Beaujolais crus
    "moulin-a-vent", "moulin a vent", "morgon", "fleurie", "brouilly",
    "chiroubles", "chenas", "julienas", "regnie", "saint-amour", "cote de brouilly",
    # Italian sub-appellations / DOC
    "bolgheri", "bolgheri superiore", "chianti classico",
    "etna bianco", "etna rosso",
    # American AVAs / regions
    "rutherford", "oakville", "napa valley", "napa", "stags leap district",
    "howell mountain", "mount veeder", "diamond mountain", "spring mountain",
    "central coast", "sonoma", "willamette valley", "anderson valley",
    "paso robles",
    # Southern hemisphere
    "tupungato", "mendoza", "barossa", "mclaren vale", "margaret river",
    "marlborough", "hawkes bay",
    # Spirits regions
    "speyside", "highland", "highlands", "islay", "lowlands",
}

def _is_region_term(s: str) -> bool:
    """Return True if s looks like a wine region/classification, not a specific cru."""
    return s.strip().lower() in _REGION_TERMS

def _normalize(s: str) -> str:
    """Strip accents: 'FolatiÃ¨res' â†’ 'Folatieres'."""
    return unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode("ascii")

def _name_tokens(s: str) -> set:
    """Meaningful lowercase tokens (>2 chars, no stop-words, no years, no accents)."""
    s = _normalize(s).lower()
    s = re.sub(r"\b(19|20)\d{2}\b", "", s)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    return {t for t in s.split() if len(t) > 2 and t not in _JR_STOP}

def _name_matches_jr(query_name: str, jr_name: str, threshold: float = 0.45) -> bool:
    """
    Returns True when the JR wine name overlaps enough with our query name.

    For 3-part names (Producer, Appellation, Cru/Climat) JR's ES wine_name
    field stores ONLY the cru â€” not the producer or appellation.  So the cru
    token is the decisive identifier: present â†’ accept, absent â†’ reject.
    Falling through to the threshold check is wrong because the full query
    has 3â€“4 tokens while JR's wine_name has only 1 (the cru).

    For 1â€“2 part names uses token-overlap threshold.
    """
    h_tok = _name_tokens(jr_name)
    parts = [p.strip() for p in query_name.split(",") if p.strip()]

    # 3+ comma-separated names are generally "Producer, Appellation, Cru/Region".
    # We require producer overlap for safety, then require overlap with either:
    # - the cru/appellation candidate part; or
    # - a fallback overlap ratio for edge cases.
    # This blocks false positives where JR returns generic regional wines.
    if len(parts) >= 3:
        producer_tok = _name_tokens(parts[0])
        candidate = parts[1] if _is_region_term(parts[-1]) else parts[-1]
        candidate_tok = _name_tokens(candidate)
        if not candidate_tok and len(parts) >= 2:
            # If the last part is too generic (e.g. "Blanc"), fall back to
            # the middle part to preserve specificity.
            candidate_tok = _name_tokens(parts[1])
        if candidate_tok and not (candidate_tok & h_tok):
            return False
        producer_overlap = bool(producer_tok & h_tok) if producer_tok else False
        if producer_overlap:
            return True
        if candidate_tok:
            # JR sometimes omits/abbreviates producer tokens in wine_name.
            # Accept a strong candidate (cru/appellation) match if candidate
            # has at least 2 meaningful tokens and most of them overlap.
            cand_ratio = len(candidate_tok & h_tok) / max(1, len(candidate_tok))
            if len(candidate_tok) >= 2 and cand_ratio >= 0.66:
                return True
            # For single-token candidates (e.g. "Silex"), keep producer guard.
            if len(candidate_tok) == 1:
                return False
    elif len(parts) == 2:
        left_tok = _name_tokens(parts[0])
        right_tok = _name_tokens(parts[1])
        left_overlap = _soft_overlap_count(left_tok, h_tok)
        right_overlap = _soft_overlap_count(right_tok, h_tok)
        left_ratio = (left_overlap / len(left_tok)) if left_tok else 0.0
        right_ratio = (right_overlap / len(right_tok)) if right_tok else 0.0

        # Reject broad appellation-only matches when the producer/cuvee side is missing.
        if left_tok and right_tok and left_overlap == 0 and right_ratio >= 0.5:
            return False

        # Relax abbreviated 2-part names like "Salvioni Brunello" or
        # "Salvioni La Cerbaiola" when the distinctive left side matches well.
        if left_tok and (
            (left_ratio >= 0.5 and right_ratio >= 0.34) or
            (left_ratio >= 0.67 and left_overlap >= 2)
        ):
            return True

    q_tok = _name_tokens(query_name)
    if not q_tok or not h_tok:
        return False
    overlap = len(q_tok & h_tok)
    if overlap == 0:
        return False
    return overlap / len(q_tok) >= threshold


_JR_SUBCUVEE_TOKENS = {
    "reserve", "riserva", "selection", "cuvee", "special",
    "plus", "lion", "bricco", "vieilles", "vignes",
}


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


def _producer_head_tokens(name: str) -> set:
    part = (name or "").split(",")[0]
    return _name_tokens(part)


def _query_specific_tokens(name: str) -> set:
    parts = [p.strip() for p in (name or "").split(",") if p.strip()]
    if len(parts) >= 3:
        c = parts[1] if _is_region_term(parts[-1]) else parts[-1]
        return _name_tokens(c)
    if len(parts) >= 2:
        return _name_tokens(parts[1])
    return set()


def _jr_candidate_rank(query_name: str, match_text: str, src: Dict, vintage: str, our_lwin7: str) -> float:
    q_tok = _name_tokens(query_name)
    h_tok = _name_tokens(match_text)
    if not q_tok or not h_tok:
        return -9999.0

    parsed_query = _parse_query_structured(query_name, vintage)
    producer_text = _first(src.get("producer", []))
    wine_text = _first(src.get("wine_name", [])) or _first(src.get("title", []))
    appellation_text = _first(src.get("appellation", []))
    wine_plus_app = " ".join(p for p in [wine_text, appellation_text] if p).strip()

    producer_cov = _token_coverage(parsed_query.get("producer", ""), producer_text or match_text) if parsed_query.get("producer") else 0.0
    lead_cov = _token_coverage(parsed_query.get("lead", ""), match_text)
    wine_cov = _token_coverage(parsed_query.get("wine", ""), wine_text or match_text) if parsed_query.get("wine") else 0.0
    app_cov = _token_coverage(parsed_query.get("appellation", ""), wine_plus_app or match_text) if parsed_query.get("appellation") else 0.0
    tail_query = " ".join(parsed_query.get("tail_parts") or [])
    tail_cov = _token_coverage(tail_query, wine_plus_app or match_text) if tail_query else 0.0

    overlap = _soft_overlap_count(q_tok, h_tok)
    ratio = overlap / len(q_tok)
    score = ratio * 40.0

    if parsed_query.get("producer"):
        score += producer_cov * 55.0
        if producer_cov == 0:
            score -= 45.0
    else:
        score += lead_cov * 25.0

    if parsed_query.get("wine"):
        score += wine_cov * 30.0
    if parsed_query.get("appellation"):
        score += app_cov * 18.0
    if tail_query:
        score += tail_cov * 26.0

    if parsed_query.get("producer") and producer_cov == 0 and max(wine_cov, app_cov, tail_cov) < 0.34:
        score -= 40.0
    if parsed_query.get("producer") and producer_cov == 0 and app_cov >= 0.60 and wine_cov == 0:
        score -= 55.0

    hit_vintage = str(_first(src.get("vintage", [])) or "").strip()
    if vintage and hit_vintage:
        score += 14.0 if hit_vintage == str(vintage).strip() else -120.0

    hit_lwin = _first(src.get("lwin", []))
    if our_lwin7 and hit_lwin and len(hit_lwin) >= 7:
        score += 20.0 if hit_lwin[:7] == our_lwin7 else -80.0

    hit_year = _parse_year(_parse_date(src.get("date_tasted")))
    if hit_year:
        score += min(6.0, max(0.0, (hit_year - 2000) * 0.2))

    return score


def _jr_plausible_match(query_name: str, match_text: str, src: Dict[str, Any], vintage: str, our_lwin7: str) -> bool:
    if not match_text:
        return False
    if _name_matches_jr(query_name, match_text):
        return True
    return _jr_candidate_rank(query_name, match_text, src or {}, vintage, our_lwin7) >= JR_PLAUSIBLE_MIN_RANK


def _jr_is_ambiguous_top(candidates: List[Dict]) -> bool:
    if len(candidates) < 2:
        return False
    top = float(candidates[0].get("rank") or 0.0)
    second = float(candidates[1].get("rank") or 0.0)
    return (top - second) < JR_STRICT_AMBIGUITY_DELTA


def _jr_strict_accept_candidate(query_name: str, match_text: str, rank: float) -> bool:
    if rank < JR_STRICT_MIN_RANK:
        return False

    q_tok = _name_tokens(query_name)
    h_tok = _name_tokens(match_text)
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

    q_specific = _query_specific_tokens(query_name)
    if q_specific and _soft_overlap_count(q_specific, h_tok) == 0:
        return False

    return True


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


def _jr_query_identity(query_name: str, query_vintage: str) -> Dict[str, str]:
    cleaned = re.sub(r"\s+", " ", str(query_name or "")).strip()
    cleaned = re.sub(r"\s*\(?\b(19|20)\d{2}\b\)?$", "", cleaned).strip()
    parts = [p.strip() for p in cleaned.split(",") if p.strip()]
    producer = parts[0] if parts else ""
    title_parts = parts[1:] if len(parts) > 1 else []
    appellation = ""
    specific = ""

    if len(parts) >= 3:
        if _is_region_term(parts[-1].lower()):
            appellation = parts[-1]
            specific = parts[1]
        else:
            appellation = parts[1]
            specific = parts[-1]
    elif len(parts) == 2:
        if _is_region_term(parts[1].lower()):
            appellation = parts[1]
        else:
            specific = parts[1]

    title = ", ".join(title_parts) if title_parts else cleaned
    return {
        "name": cleaned,
        "producer": producer,
        "title": title,
        "appellation": appellation,
        "specific": specific,
        "vintage": str(query_vintage or "").strip(),
    }


def _normalize_search_vintage(vintage: str, wine_name: str = "", lwin: str = "") -> str:
    raw = str(vintage or "").strip()
    if raw.upper() in ("NV", "N/V"):
        return "NV"
    m = re.search(r"\b(19\d{2}|20[0-3]\d)\b", raw)
    if m:
        return m.group(1)
    m = re.search(r"\b(19\d{2}|20[0-3]\d)\b", str(wine_name or ""))
    if m:
        return m.group(1)
    parsed = parse_lwin(lwin or "")
    lv = str(parsed.get("vintage") or "").strip()
    if re.fullmatch(r"(19\d{2}|20[0-3]\d)", lv):
        return lv
    return ""


def _normalize_search_name(wine_name: str, vintage: str = "") -> str:
    name = re.sub(r"\s+", " ", str(wine_name or "")).strip()
    name = re.sub(r"\s*\([^)]*\)\s*$", "", name).strip()
    if vintage and vintage not in ("NV", "N/V"):
        name = re.sub(rf"(?:,?\s+|\s*\()\b{re.escape(vintage)}\b(?:\))?\s*$", "", name, flags=re.I).strip(" ,")
    name = re.sub(r",?\s*\b(?:NV|N/V)\b\s*$", "", name, flags=re.I).strip(" ,")
    name = re.sub(r"(?:,|\s)\d{5,}\s*$", "", name).strip(" ,")
    return name


def _hint_query_from_url(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
        params = parse_qs(parsed.query or "")
        term = (params.get("search-full") or [""])[0]
        term = unquote(term).strip().strip('"').strip("'")
        return re.sub(r"\s+", " ", term).strip()
    except Exception:
        return ""


def _hint_identity_name(search_hints: Optional[Dict[str, Any]], vintage: str = "") -> str:
    hints = search_hints or {}
    parts = [
        str(hints.get("jr_producer") or "").strip(),
        str(hints.get("jr_wine_name") or "").strip(),
        str(hints.get("jr_appellation") or "").strip(),
    ]
    return _normalize_search_name(", ".join([p for p in parts if p]), vintage)


def _query_needs_variant_expansion(query: str) -> bool:
    text = str(query or "").strip()
    if not text:
        return False
    if re.fullmatch(r"(?:lwin)?\d{7,}", text, flags=re.I):
        return False
    return "," in text or "'" in text or any(text.lower().startswith(prefix) for prefix in ("chateau ", "domaine ", "azienda ", "tenuta ", "clos "))


def _build_match_contexts(base_name: str, vintage: str, search_hints: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    names: List[str] = []
    seen: set[str] = set()

    def _add(name: str) -> None:
        candidate = _normalize_search_name(name, vintage)
        key = candidate.lower()
        if candidate and key not in seen:
            seen.add(key)
            names.append(candidate)

    hint_name = _hint_identity_name(search_hints, vintage)
    url_name = _hint_query_from_url((search_hints or {}).get("jr_search_url") or "")

    _add(hint_name)
    _add(url_name)
    _add(base_name)

    parsed = [_parse_query_structured(name, vintage) for name in names]
    primary_name = names[0] if names else _normalize_search_name(base_name, vintage)
    return {
        "query_names": names or [primary_name],
        "parsed_queries": parsed or [_parse_query_structured(primary_name, vintage)],
        "primary_name": primary_name,
    }


def _jr_candidate_rank_any(
    query_names: List[str],
    match_text: str,
    src: Dict[str, Any],
    vintage: str,
    our_lwin7: str,
) -> float:
    return max(
        (_jr_candidate_rank(query_name, match_text, src, vintage, our_lwin7) for query_name in query_names if query_name),
        default=-9999.0,
    )


def _jr_plausible_match_any(
    query_names: List[str],
    match_text: str,
    src: Dict[str, Any],
    vintage: str,
    our_lwin7: str,
) -> bool:
    return any(_jr_plausible_match(query_name, match_text, src, vintage, our_lwin7) for query_name in query_names if query_name)


def _build_search_queries(base_name: str, vintage: str, lwin: str = "", search_hints: Optional[Dict[str, Any]] = None) -> List[str]:
    hints = search_hints or {}
    parts = [p.strip() for p in str(base_name or "").split(",") if p.strip()]
    queries: List[str] = []

    def _add(q: str) -> None:
        q = re.sub(r"\s+", " ", str(q or "")).strip().strip(",")
        if q:
            queries.append(q)

    _add(_hint_query_from_url(hints.get("jr_search_url") or ""))

    hint_parts = [
        str(hints.get("jr_producer") or "").strip(),
        str(hints.get("jr_wine_name") or "").strip(),
        str(hints.get("jr_appellation") or "").strip(),
    ]
    _add(", ".join([p for p in hint_parts if p]))

    if base_name and vintage and vintage != "NV":
        _add(f"{base_name} {vintage}")
    _add(base_name)

    if len(parts) == 2 and _is_region_term(parts[1]):
        if vintage and vintage != "NV":
            _add(f"{parts[0]} {vintage}")
        _add(parts[0])

    seen: set[str] = set()
    ordered: List[str] = []
    for q in queries:
        key = q.lower()
        if key in seen:
            continue
        seen.add(key)
        ordered.append(q)
        if len(ordered) >= 3:
            break
    return ordered


def _parse_query_structured(query_name: str, query_vintage: str) -> Dict[str, Any]:
    cleaned = _normalize_search_name(query_name, query_vintage)
    parts = [p.strip() for p in cleaned.split(",") if p.strip()]
    producer = ""
    wine = ""
    appellation = ""

    if len(parts) >= 3:
        producer, wine, appellation = parts[0], parts[1], parts[2]
    elif len(parts) == 2:
        producer = parts[0]
        if _is_region_term(parts[1]):
            appellation = parts[1]
        else:
            wine = parts[1]
    elif parts:
        wine = parts[0]

    lead = producer or wine or cleaned
    tail_parts = [p for p in [wine, appellation] if p]
    return {
        "lead": lead,
        "producer": producer,
        "wine": wine,
        "appellation": appellation,
        "year": str(query_vintage or "").strip(),
        "tail_parts": tail_parts,
    }


def _token_coverage(query_text: str, candidate_text: str) -> float:
    q_tok = _name_tokens(query_text)
    h_tok = _name_tokens(candidate_text)
    if not q_tok:
        return 1.0
    if not h_tok:
        return 0.0
    return _soft_overlap_count(q_tok, h_tok) / len(q_tok)


def _candidate_prerank_score(parsed_query: Dict[str, Any], candidate: Dict[str, Any]) -> float:
    score = float(candidate.get("rank") or 0.0)
    match_text = str(candidate.get("match_text") or "")
    producer = str(candidate.get("producer") or "")
    wine_name = str(candidate.get("wine_name") or "")
    title = str(candidate.get("title") or "")
    appellation = str(candidate.get("appellation") or "")
    year = str(parsed_query.get("year") or "").strip()
    cand_year = str(candidate.get("vintage") or "").strip()
    wine_text = wine_name or title

    if year and cand_year:
        score += 18.0 if cand_year == year else -120.0

    if parsed_query.get("producer"):
        prod_cov = _token_coverage(parsed_query["producer"], producer or match_text)
        score += prod_cov * 55.0
        if prod_cov == 0:
            score -= 45.0
    else:
        score += _token_coverage(parsed_query.get("lead", ""), match_text) * 25.0

    if parsed_query.get("wine"):
        wine_cov = _token_coverage(parsed_query["wine"], wine_text or match_text)
        score += wine_cov * 30.0
    else:
        wine_cov = 0.0

    if parsed_query.get("appellation"):
        app_cov = _token_coverage(parsed_query["appellation"], f"{wine_text} {appellation}".strip())
        score += app_cov * 18.0
    else:
        app_cov = 0.0

    tail_cov = _token_coverage(" ".join(parsed_query.get("tail_parts") or []), f"{wine_text} {appellation}".strip())
    score += tail_cov * 26.0

    if parsed_query.get("producer"):
        prod_cov = _token_coverage(parsed_query["producer"], producer or match_text)
        if prod_cov == 0 and app_cov >= 0.60 and wine_cov == 0:
            score -= 60.0
        elif prod_cov == 0 and max(wine_cov, app_cov, tail_cov) < 0.34:
            score -= 40.0

    hit_year = _parse_year(candidate.get("date_tasted") or "")
    if hit_year:
        score += min(6.0, max(0.0, (hit_year - 2000) * 0.2))
    return score


def _jr_candidate_passes(parsed_queries: List[Dict[str, Any]], candidate: Dict[str, Any]) -> bool:
    if not parsed_queries:
        _jr_debug("candidate rejected: no parsed queries")
        return False

    scores = [_candidate_prerank_score(parsed, candidate) for parsed in parsed_queries]
    best_score = max(scores) if scores else -9999.0
    if best_score < JR_PLAUSIBLE_MIN_RANK:
        _jr_debug(
            f"candidate rejected: score={best_score:.2f} < min={JR_PLAUSIBLE_MIN_RANK:.2f} "
            f"hit={str(candidate.get('match_text') or '')[:120]!r}"
        )
        return False

    best_query = parsed_queries[scores.index(best_score)]
    match_text = str(candidate.get("match_text") or "")
    producer = str(candidate.get("producer") or "")
    wine_name = str(candidate.get("wine_name") or "")
    title = str(candidate.get("title") or "")
    appellation = str(candidate.get("appellation") or "")
    wine_plus_app = " ".join(p for p in [wine_name or title, appellation] if p).strip()

    producer_cov = _token_coverage(best_query.get("producer", ""), producer or match_text) if best_query.get("producer") else 0.0
    wine_cov = _token_coverage(best_query.get("wine", ""), wine_name or title or match_text) if best_query.get("wine") else 0.0
    app_cov = _token_coverage(best_query.get("appellation", ""), wine_plus_app or match_text) if best_query.get("appellation") else 0.0
    tail_query = " ".join(best_query.get("tail_parts") or [])
    tail_cov = _token_coverage(tail_query, wine_plus_app or match_text) if tail_query else 0.0

    if best_query.get("producer"):
        if producer_cov == 0 and max(wine_cov, app_cov, tail_cov) < 0.34:
            _jr_debug(
                f"candidate rejected: no producer overlap and weak tail "
                f"(producer={producer_cov:.2f}, wine={wine_cov:.2f}, app={app_cov:.2f}, tail={tail_cov:.2f}) "
                f"hit={match_text[:120]!r}"
            )
            return False
        if producer_cov == 0 and app_cov >= 0.60 and wine_cov == 0:
            _jr_debug(
                f"candidate rejected: region-only false positive "
                f"(producer={producer_cov:.2f}, wine={wine_cov:.2f}, app={app_cov:.2f}, tail={tail_cov:.2f}) "
                f"hit={match_text[:120]!r}"
            )
            return False
        passed = producer_cov >= 0.34 or max(wine_cov, app_cov, tail_cov) >= 0.65
        _jr_debug(
            f"candidate {'accepted' if passed else 'rejected'}: "
            f"producer={producer_cov:.2f} wine={wine_cov:.2f} app={app_cov:.2f} tail={tail_cov:.2f} "
            f"score={best_score:.2f} hit={match_text[:120]!r}"
        )
        return passed

    lead_cov = _token_coverage(best_query.get("lead", ""), match_text)
    passed = lead_cov >= 0.45
    _jr_debug(
        f"candidate {'accepted' if passed else 'rejected'}: "
        f"lead={lead_cov:.2f} score={best_score:.2f} hit={match_text[:120]!r}"
    )
    return passed


def _strict_match_structured(parsed_query: Dict[str, Any], candidate: Dict[str, Any]) -> bool:
    year = str(parsed_query.get("year") or "").strip()
    cand_year = str(candidate.get("vintage") or "").strip()
    if MATCH_REQUIRE_EXACT_VINTAGE and year and cand_year and cand_year != year:
        return False

    match_text = str(candidate.get("match_text") or "")
    producer = str(candidate.get("producer") or "")
    wine_name = str(candidate.get("wine_name") or "")
    appellation = str(candidate.get("appellation") or "")
    wine_plus_app = f"{wine_name} {appellation}".strip()

    if parsed_query.get("producer"):
        prod_cov = _token_coverage(parsed_query["producer"], producer or match_text)
        lead_fallback = _token_coverage(parsed_query["lead"], match_text)
        if prod_cov < 0.50 and lead_fallback < MATCH_LEAD_WINE_FALLBACK_COVERAGE:
            return False

    if parsed_query.get("wine"):
        if _token_coverage(parsed_query["wine"], wine_name or match_text) < MATCH_WINE_COVERAGE:
            return False

    tails = parsed_query.get("tail_parts") or []
    if tails:
        covered = sum(
            1 for part in tails
            if _token_coverage(part, wine_plus_app or match_text) >= MATCH_REMAINING_APPELLATION_COVERAGE
        )
        if covered / len(tails) < MATCH_TAIL_REQUIRED_RATIO:
            return False

    return True


def _relaxed_match_structured(parsed_query: Dict[str, Any], candidate: Dict[str, Any]) -> bool:
    year = str(parsed_query.get("year") or "").strip()
    cand_year = str(candidate.get("vintage") or "").strip()
    if year and cand_year and cand_year != year:
        return False

    match_text = str(candidate.get("match_text") or "")
    producer = str(candidate.get("producer") or "")
    wine_name = str(candidate.get("wine_name") or "")
    appellation = str(candidate.get("appellation") or "")
    wine_plus_app = f"{wine_name} {appellation}".strip()

    if parsed_query.get("producer") and _token_coverage(parsed_query["producer"], producer or match_text) == 0:
        return False

    tail_query = " ".join(parsed_query.get("tail_parts") or [])
    if tail_query and _token_coverage(tail_query, wine_plus_app or match_text) < MATCH_RELAXED_COVERAGE:
        return False

    return True


def _jr_candidate_identity(candidate: Dict[str, Any], idx: Optional[int] = None) -> Dict[str, Any]:
    out = {
        "match_text": str(candidate.get("match_text") or "").strip(),
        "producer": str(candidate.get("producer") or "").strip(),
        "wine_name": str(candidate.get("wine_name") or "").strip(),
        "title": str(candidate.get("title") or "").strip(),
        "appellation": str(candidate.get("appellation") or "").strip(),
        "vintage": str(candidate.get("vintage") or "").strip(),
        "lwin": str(candidate.get("lwin") or "").strip(),
        "rank": round(float(candidate.get("rank") or 0.0), 3),
        "review_url": str(candidate.get("review_url") or "").strip(),
    }
    if idx is not None:
        out["idx"] = idx
    return out


def _jr_llm_pick_candidate(query_name: str, query_vintage: str, candidates: List[Dict]) -> Optional[Dict]:
    if not (JR_LLM_MATCH_ENABLED and OPENAI_API_KEY):
        return None
    if len(candidates) < 2:
        return None

    payload = [_jr_candidate_identity(c, idx=i) for i, c in enumerate(candidates[:12])]
    query = _jr_query_identity(query_name, query_vintage)

    prompt = {
        "query": query,
        "candidates": payload,
        "task": "Choose exact same wine only. If uncertain choose_idx=-1.",
        "rules": [
            "Vintage must match when query vintage exists.",
            "Compare producer, wine title, and appellation/cuvee identity.",
            "Do not reward generic or partial matches.",
            "Do not guess.",
        ],
        "output": {"choose_idx": "int", "confidence": "0..1", "reason": "short"},
    }
    data = {
        "model": JR_LLM_MODEL,
        "input": [
            {"role": "system", "content": [{"type": "input_text", "text": "You are a strict wine matching judge. Respond with JSON only."}]},
            {"role": "user", "content": [{"type": "input_text", "text": json.dumps(prompt, ensure_ascii=True)}]},
        ],
        "temperature": 0,
        "max_output_tokens": 180,
    }

    try:
        resp = requests.post(
            "https://api.openai.com/v1/responses",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            json=data,
            timeout=20,
        )
        if not resp.ok:
            logger.warning("JR LLM rerank HTTP %s: %s", resp.status_code, resp.text[:200])
            return None
        out = _extract_llm_text(resp.json())
        if not out:
            return None
        m = re.search(r"\{[\s\S]*\}", out)
        parsed = json.loads(m.group(0) if m else out)
        idx = int(parsed.get("choose_idx", -1))
        conf = float(parsed.get("confidence", 0.0))
        reason = str(parsed.get("reason", ""))[:200]

        if idx < 0 or idx >= len(payload):
            logger.info("JR LLM rerank abstained: %s", reason)
            return None
        if conf < JR_LLM_MIN_CONF:
            logger.info("JR LLM rerank low confidence %.2f (< %.2f): %s", conf, JR_LLM_MIN_CONF, reason)
            return None
        logger.info("JR LLM rerank picked idx=%d conf=%.2f reason=%s", idx, conf, reason)
        return candidates[idx]
    except Exception as e:
        logger.warning("JR LLM rerank failed: %s", e)
        return None


def _jr_llm_verify_exact_match(query_name: str, query_vintage: str, candidate: Dict[str, Any]) -> bool:
    if not JR_LLM_VERIFY_ENABLED:
        return True
    if not OPENAI_API_KEY:
        return True

    match_text = str(candidate.get("match_text") or "").strip()
    q_tok = _name_tokens(query_name)
    h_tok = _name_tokens(match_text)
    if q_tok and h_tok:
        ratio = _soft_overlap_count(q_tok, h_tok) / len(q_tok)
        q_prod = _producer_head_tokens(query_name)
        prod_ratio = (_soft_overlap_count(q_prod, h_tok) / len(q_prod)) if q_prod else 0.0
        if ratio >= 0.85 and prod_ratio >= 0.90:
            return True

    query = _jr_query_identity(query_name, query_vintage)
    prompt = {
        "query": query,
        "candidate": _jr_candidate_identity(candidate),
        "task": "Decide if candidate is the same exact wine as query.",
        "rules": [
            "If uncertain, return accept=false.",
            "Respect producer identity and specific title/cuvee/appellation.",
            "Reject broader appellation matches when the query is a more specific wine.",
        ],
        "output": {"accept": "boolean", "confidence": "0..1", "reason": "short"},
    }
    data = {
        "model": JR_LLM_MODEL,
        "input": [
            {"role": "system", "content": [{"type": "input_text", "text": "You are a strict wine identity verifier. Respond with JSON only."}]},
            {"role": "user", "content": [{"type": "input_text", "text": json.dumps(prompt, ensure_ascii=True)}]},
        ],
        "temperature": 0,
        "max_output_tokens": 140,
    }
    try:
        resp = requests.post(
            "https://api.openai.com/v1/responses",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            json=data,
            timeout=20,
        )
        if not resp.ok:
            logger.warning("JR LLM verify HTTP %s: %s", resp.status_code, resp.text[:200])
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
            logger.info("JR LLM verify rejected: %s", reason)
            return False
        if conf < JR_LLM_VERIFY_MIN_CONF:
            logger.info("JR LLM verify low confidence %.2f (< %.2f): %s", conf, JR_LLM_VERIFY_MIN_CONF, reason)
            return False
        logger.info("JR LLM verify accepted conf=%.2f reason=%s", conf, reason)
        return True
    except Exception as e:
        logger.warning("JR LLM verify failed: %s", e)
        return False


def _jr_select_candidate(query_name: str, query_vintage: str, candidates: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not candidates:
        return None

    ordered = sorted(candidates, key=lambda c: float(c.get("rank") or -9999.0), reverse=True)
    picked = ordered[0]
    llm_pick = _jr_llm_pick_candidate(query_name, query_vintage, ordered[:8])
    if llm_pick:
        picked = llm_pick

    if not JR_STRICT_MODE:
        return picked

    if _jr_is_ambiguous_top(ordered[:2]) and not llm_pick:
        logger.info("JR strict abstain: ambiguous top candidates for query=%r", query_name)
        return None

    if not _jr_strict_accept_candidate(query_name, str(picked.get("match_text") or ""), float(picked.get("rank") or 0.0)):
        logger.info(
            "JR strict abstain: candidate below strict gate query=%r hit=%r rank=%.2f",
            query_name,
            picked.get("match_text"),
            float(picked.get("rank") or 0.0),
        )
        return None

    if not _jr_llm_verify_exact_match(query_name, query_vintage, picked):
        logger.info("JR LLM verify abstain: query=%r hit=%r", query_name, picked.get("match_text"))
        return None

    return picked


# â”€â”€â”€ Name Variants â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€



def _clean_name_variants(name: str) -> List[str]:
    """
    JR-friendly variant builder.
    Keeps apostrophe-aware producer names and adds short producer prefixes
    like "Ch" / "Dom" that JR often uses in search and titles.
    """
    base = str(name or "").strip().lower()
    base = re.sub(r"\.", "", base)
    base = re.sub(r"\s+", " ", base)
    base = re.sub(r"\s*\(?\b(19|20)\d{2}\b\)?$", "", base).strip()

    prefixes = [
        "chateau ", "chÃƒÂ¢teau ", "ch ", "domaine ", "dom ", "clos ", "maison ", "cave ",
        "azienda agricola ", "azienda ", "az agricola ", "az ", "tenuta ",
        "societa agricola ", "societa ", "soc agricola ", "weingut ", "bodegas ", "casa ",
    ]
    prefix_aliases = {
        "chateau ": ["ch "],
        "chÃƒÂ¢teau ": ["ch "],
        "domaine ": ["dom "],
        "dom ": ["domaine "],
    }
    ordered: List[str] = []
    seen: set[str] = set()

    def _add_variant(text: str) -> None:
        candidate = re.sub(r"\s+", " ", str(text or "").strip().lower())
        candidate = re.sub(r"\s*,\s*", ", ", candidate).strip(" ,")
        if not candidate:
            return
        if candidate not in seen:
            seen.add(candidate)
            ordered.append(candidate)
        ascii_candidate = _normalize(candidate).lower().strip()
        if ascii_candidate and ascii_candidate not in seen:
            seen.add(ascii_candidate)
            ordered.append(ascii_candidate)

    _add_variant(base)
    _add_variant(re.sub(r"['`]", "", base))
    _add_variant(re.sub(r"['`]", " ", base))

    for candidate in list(ordered):
        for prefix in prefixes:
            if candidate.startswith(prefix):
                stripped = candidate[len(prefix):].strip()
                _add_variant(stripped)
                for alias in prefix_aliases.get(prefix, []):
                    _add_variant(alias + stripped)
                if stripped.startswith("d'") or stripped.startswith("d "):
                    _add_variant(stripped[2:].strip())

    for candidate in list(ordered):
        if candidate.startswith("d'") or candidate.startswith("d "):
            _add_variant(candidate[2:].strip())

    for candidate in list(ordered):
        for suffix in [" pere et fils", " freres", " et fils", " & fils"]:
            if candidate.endswith(suffix):
                _add_variant(candidate[:-len(suffix)].strip())
        parts_local = [p.strip() for p in candidate.split(",") if p.strip()]
        if parts_local:
            producer_part = parts_local[0]
            for suffix in [" pere et fils", " freres", " et fils", " & fils"]:
                if producer_part.endswith(suffix):
                    trimmed = producer_part[:-len(suffix)].strip()
                    rebuilt = ", ".join([trimmed] + parts_local[1:])
                    _add_variant(rebuilt)
                    _add_variant(trimmed)

    parts = [p.strip() for p in base.split(",") if p.strip()]
    if len(parts) >= 3:
        producer = parts[0]
        cru = parts[1] if _is_region_term(parts[-1]) else parts[-1]
        short_prod = producer
        for pfx in prefixes:
            if short_prod.startswith(pfx):
                short_prod = short_prod[len(pfx):].strip()
                break
        _add_variant(f"{short_prod} {cru}".strip())
        if short_prod.startswith("d'") or short_prod.startswith("d "):
            _add_variant(f"{short_prod[2:].strip()} {cru}".strip())
    elif len(parts) == 2:
        left, right = parts
        short_left = left
        for pfx in prefixes:
            if short_left.startswith(pfx):
                short_left = short_left[len(pfx):].strip()
                break
        if _is_region_term(right.lower()):
            _add_variant(short_left)
            if short_left.startswith("d'") or short_left.startswith("d "):
                _add_variant(short_left[2:].strip())
        else:
            _add_variant(short_left)
            _add_variant(f"{left} {right}".strip())
            _add_variant(f"{short_left} {right}".strip())
            if short_left.startswith("d'") or short_left.startswith("d "):
                _add_variant(short_left[2:].strip())

    return [v.title() for v in ordered if v]


def _hit_match_text(src: Dict) -> str:
    """
    Build a robust match text from ES source fields.
    JR can return blank wine_name while title/appellation still carry identity.
    """
    producer = _first(src.get("producer", []))
    wine_name = _first(src.get("wine_name", []))
    title = _first(src.get("title", []))
    appellation = _first(src.get("appellation", []))
    parts = []
    seen = set()
    for p in [producer, wine_name, title, appellation]:
        pv = str(p).strip()
        if not pv:
            continue
        key = pv.lower()
        if key in seen:
            continue
        seen.add(key)
        parts.append(pv)
    return " ".join(parts).strip()


# â”€â”€â”€ ES Payloads â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

_SRC = ["url","title","producer","score_number","score_modifier","note",
        "drink_date_from","drink_date_to","appellation","colour",
        "date_tasted","wine_name","vintage","lwin"]

def _payload_lwin(lwin7: str, vintage: str = "") -> str:
    must = [{"prefix": {"lwin": lwin7}}]
    should = []
    if vintage:
        should.append({"match_phrase": {"vintage": vintage}})
    body = {"query":{"bool":{"must":must}},"size":20,"from":0,
            "sort":[{"date_tasted":{"order":"desc"}},{"_score":{"order":"desc"}}],
            "_source":_SRC}
    if should:
        body["query"]["bool"]["should"] = should
    return "{}\n" + json.dumps(body, separators=(",",":")) + "\n"

def _payload_name(query: str, vintage: str = "") -> str:
    must = [{
        "simple_query_string": {
            "query": query,
            "default_operator": "and",
        }
    }]
    should = [{
        "simple_query_string": {
            "query": f"\"{query}\"",
            "boost": 4,
        }
    }]
    if vintage:
        should.append({"match_phrase": {"vintage": vintage}})
    body = {"query":{"bool":{"must":must}},"size":20,"from":0,
            "sort":[{"date_tasted":{"order":"desc"}},{"_score":{"order":"desc"}}],
            "_source":_SRC}
    if should:
        body["query"]["bool"]["should"] = should
    return "{}\n" + json.dumps(body, separators=(",",":")) + "\n"

def _payload_name_match(query: str, vintage: str = "") -> str:
    """
    match query (operator=and): all terms must appear in wine_name, any order.
    Used when JR inserts articles/prepositions into cru names, e.g.:
      our DB:  'Les Rouges Dessus'
      JR name: 'Les Rouges du Dessus'  â† phrase match fails, any-order match succeeds.
    """
    must = [{
        "simple_query_string": {
            "query": query,
            "default_operator": "or",
        }
    }]
    should = [{
        "simple_query_string": {
            "query": query,
            "default_operator": "and",
            "boost": 2,
        }
    }]
    if vintage:
        should.append({"match_phrase": {"vintage": vintage}})
    body = {"query": {"bool": {"must": must}}, "size": 20, "from": 0,
            "sort": [{"date_tasted": {"order": "desc"}}, {"_score": {"order": "desc"}}],
            "_source": _SRC}
    if should:
        body["query"]["bool"]["should"] = should
    return "{}\n" + json.dumps(body, separators=(",", ":")) + "\n"


# â”€â”€â”€ ES Request â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _do_msearch(session: requests.Session, payload: str) -> List[Dict]:
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            r = session.post(
                JR_MSEARCH,
                data=payload,
                headers={"Content-Type": "application/x-ndjson"},
                timeout=15,
            )
            r.raise_for_status()
            data = r.json()
            if JR_DEBUG_SEARCH:
                responses = data.get("responses", []) if isinstance(data, dict) else []
                total = None
                if responses and isinstance(responses[0], dict):
                    total = responses[0].get("hits", {}).get("total")
                _jr_debug(f"msearch status={r.status_code} responses={len(responses)} total={total}")
            break
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            body = (e.response.text[:200] if e.response is not None else str(e)) if e else ""
            transient = status in (429, 500, 502, 503, 504)
            if transient and attempt < max_attempts - 1:
                wait = 0.5 * (attempt + 1)
                print(f"    [HTTP {status}] retrying in {wait:.1f}s")
                time.sleep(wait)
                continue
            print(f"    [HTTP {status}] {body}")
            return []
        except (requests.Timeout, requests.ConnectionError) as e:
            if attempt < max_attempts - 1:
                wait = 0.5 * (attempt + 1)
                print(f"    [ES RETRY] {type(e).__name__}: {e} (retry in {wait:.1f}s)")
                time.sleep(wait)
                continue
            print(f"    [ES ERR] {type(e).__name__}: {e}")
            return []
        except Exception as e:
            print(f"    [ES ERR] {type(e).__name__}: {e}")
            return []
    else:
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
    if JR_DEBUG_SEARCH:
        preview = []
        for src in hits[:3]:
            preview.append({
                "producer": _first(src.get("producer", [])),
                "wine_name": _first(src.get("wine_name", [])),
                "title": _first(src.get("title", [])),
                "appellation": _first(src.get("appellation", [])),
                "vintage": _first(src.get("vintage", [])),
            })
        _jr_debug(f"msearch hits={len(hits)} preview={preview}")
    return hits


# â”€â”€â”€ Smart Multi-Strategy Search â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€



# â”€â”€â”€ HTML Full-Page Parser â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def jr_msearch(
    session: requests.Session,
    wine_name: str,
    vintage: str,
    lwin: str = "",
    search_hints: Optional[Dict[str, Any]] = None,
) -> List[Dict]:
    """
    Minimal JR ES search with compact name-only queries.
    """
    collected: List[Dict] = []
    seen_keys: set[str] = set()
    query_names = list(_build_search_queries(wine_name, vintage, "", search_hints=search_hints))
    _jr_debug(f"search start wine={wine_name!r} vintage={vintage!r} queries={query_names!r}")

    def _hit_key(src: Dict[str, Any]) -> str:
        if not isinstance(src, dict):
            return ""
        return "|".join([
            _first(src.get("url", [])),
            _first(src.get("producer", [])),
            _first(src.get("wine_name", [])),
            _first(src.get("title", [])),
            _first(src.get("vintage", [])),
        ]).strip("|")

    def _add_hits(hits_list: List[Dict], label: str) -> None:
        added = 0
        for hit in hits_list:
            key = _hit_key(hit)
            if key and key in seen_keys:
                continue
            if key:
                seen_keys.add(key)
            collected.append(hit)
            added += 1
            if len(collected) >= 24:
                break
        if added:
            print(f"    -> {added} new hit(s) via {label}")
            if JR_DEBUG_SEARCH:
                _jr_debug(f"added via {label}: {[(_first(h.get('producer', [])), _first(h.get('wine_name', [])), _first(h.get('vintage', []))) for h in hits_list[:5]]}")

    def _has_relevant_hit(hits_list: List[Dict]) -> bool:
        relevant = any(
            _jr_plausible_match_any(query_names, _hit_match_text(h), h, vintage, "")
            for h in hits_list
        )
        if JR_DEBUG_SEARCH:
            sample = []
            for h in hits_list[:5]:
                sample.append({
                    "match_text": _hit_match_text(h),
                    "vintage": _first(h.get("vintage", [])),
                    "plausible": _jr_plausible_match_any(query_names, _hit_match_text(h), h, vintage, ""),
                })
            _jr_debug(f"relevant_hit={relevant} sample={sample}")
        return relevant

    for query in query_names:
        print(f"    [NAME='{query}'] vintage={vintage or 'any'}")
        hits = _do_msearch(session, _payload_name(query, vintage))
        if hits and _has_relevant_hit(hits):
            _add_hits(hits, "name")
            break

        print(f"    [NAME-MATCH(any)='{query}'] vintage={vintage or 'any'}")
        hits = _do_msearch(session, _payload_name_match(query, vintage))
        if hits and _has_relevant_hit(hits):
            _add_hits(hits, "any-order name")
            break

    base_nm = re.sub(r"\s*\(?\b(19|20)\d{2}\b\)?$", "", wine_name.lower()).strip()
    parts_nm = [p.strip() for p in base_nm.split(",") if p.strip()]
    if False:
        prod = parts_nm[0]
        cru = parts_nm[1] if _is_region_term(parts_nm[-1]) else parts_nm[-1]
        for pfx in ["domaine ", "dom ", "chateau ", "chÃƒÂ¢teau ", "ch ", "clos ", "maison ", "cave "]:
            if prod.startswith(pfx):
                prod = prod[len(pfx):].strip()
                break

        prod_cru = f"{prod} {cru}".title()
        print(f"    [NAME-MATCH='{prod_cru}'] vintage={vintage}")
        hits = _do_msearch(session, _payload_name_match(prod_cru, vintage))
        if hits and _has_relevant_hit(hits):
            _add_hits(hits, "prod+cru (any-order match)")

        cru_q = cru.title()
        print(f"    [NAME-MATCH(cru)='{cru_q}'] vintage={vintage}")
        hits = _do_msearch(session, _payload_name_match(cru_q, vintage))
        if hits and _has_relevant_hit(hits):
            _add_hits(hits, "cru-only (any-order match)")

    if False:
        for v in variants[:1]:
            hits = _do_msearch(session, _payload_name(v, ""))
            if hits and _has_relevant_hit(hits):
                _add_hits(hits, "name (no vintage)")

        for v in variants[:1]:
            hits = _do_msearch(session, _payload_name_match(v, ""))
            if hits and _has_relevant_hit(hits):
                _add_hits(hits, "any-order name (no vintage)")

    if not collected:
        print("    -> 0 results")
        return []

    print(f"    -> {len(collected)} total unique result(s)")
    return collected[:20]


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


def _extract_tastings_rows(soup: BeautifulSoup) -> List[Any]:
    selectors = [
        "div.tspTable__body div.tspTable__grid.tspTable__body-row",
        "div.tspTable__grid.tspTable__body-row",
        "div[class*='tspTable__body-row']",
    ]
    for selector in selectors:
        rows = soup.select(selector)
        if rows:
            return rows
    return []


def _debug_tastings_page(response: requests.Response, soup: BeautifulSoup, rows: List[Any], label: str) -> None:
    stats = ""
    stats_el = soup.select_one("div.tspResults__stats")
    if stats_el:
        stats = stats_el.get_text(" ", strip=True)
    search_value = ""
    search_input = soup.select_one("input[aria-label='search-full']")
    if search_input:
        search_value = str(search_input.get("value") or "").strip()
    page_title = ""
    if soup.title:
        page_title = soup.title.get_text(" ", strip=True)

    print(
        f"    [JR HTML DEBUG] {label} status={response.status_code} "
        f"url={response.url} title={page_title!r} search={search_value!r} "
        f"stats={stats!r} rows={len(rows)}"
    )

    if rows:
        return

    body_text = soup.get_text(" ", strip=True)
    body_text = re.sub(r"\s+", " ", body_text).strip()[:240]
    if body_text:
        print(f"    [JR HTML DEBUG] snippet={body_text!r}")


# â”€â”€â”€ Field Helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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


def _jr_es_candidate(query_names: List[str], vintage: str, our_lwin7: str, src: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(src, dict):
        return None

    match_text = _hit_match_text(src)
    hit_lwin = _first(src.get("lwin", []))
    rank = _jr_candidate_rank_any(query_names, match_text, src, vintage, our_lwin7)
    parsed_queries = [_parse_query_structured(query_name, vintage) for query_name in query_names if query_name]
    name_match = any(match_text and _name_matches_jr(query_name, match_text) for query_name in query_names if query_name)
    if match_text and not name_match:
        rank -= 35.0

    lwin_match = True
    if our_lwin7 and hit_lwin and len(hit_lwin) >= 7 and hit_lwin[:7] != our_lwin7:
        lwin_match = False
        rank -= 80.0

    logger.info("JR candidate rank %.2f for hit=%r", rank, match_text)
    _jr_debug(
        f"candidate raw rank={rank:.2f} name_match={name_match} lwin_match={lwin_match} "
        f"producer={_first(src.get('producer', []))!r} wine={_first(src.get('wine_name', []))!r} "
        f"appellation={_first(src.get('appellation', []))!r} vintage={_first(src.get('vintage', []))!r}"
    )

    candidate = {
        "src": src,
        "review": None,
        "match_text": match_text,
        "rank": rank,
        "producer": _first(src.get("producer", [])),
        "wine_name": _first(src.get("wine_name", [])),
        "title": _first(src.get("title", [])),
        "appellation": _first(src.get("appellation", [])),
        "vintage": _first(src.get("vintage", [])),
        "lwin": hit_lwin,
        "name_match": name_match,
        "lwin_match": lwin_match,
        "review_url": "",
    }
    if not _jr_candidate_passes(parsed_queries, candidate):
        return None

    url_raw = src.get("url")
    if isinstance(url_raw, list):
        url_raw = url_raw[0] if url_raw else ""
    candidate["review_url"] = urljoin(JR_ORIGIN, str(url_raw or "").strip("[]'\" "))
    _jr_debug(
        f"candidate selected review_url={candidate['review_url']!r} match_text={match_text[:120]!r}"
    )
    return candidate


def _jr_review_from_es_candidate(
    session: requests.Session,
    candidate: Dict[str, Any],
    fetch_full: bool = True,
) -> Optional[Dict[str, Any]]:
    src = candidate.get("src") or {}
    jr_match_text = str(candidate.get("match_text") or "").strip()
    url = str(candidate.get("review_url") or "").strip()
    es_score = _parse_score(src)

    rev: Dict[str, Any] = {
        "score_20":     es_score,
        "score":        round(es_score * 5.0, 1) if es_score is not None else None,
        "reviewer":     "",
        "drink_from":   _parse_year(_first(src.get("drink_date_from", []))),
        "drink_to":     _parse_year(_first(src.get("drink_date_to", []))),
        "tasting_note": _first(src.get("note", [])),
        "region":       str(candidate.get("appellation") or "").strip(),
        "colour":       _first(src.get("colour", [])),
        "date_tasted":  _parse_date(src.get("date_tasted")),
        "wine_name_jr": jr_match_text,
        "review_url":   url,
        "jr_lwin":      str(candidate.get("lwin") or "").strip(),
    }

    if fetch_full and url:
        html = _fetch_full_page(session, url)
        if html.get("score_20") is not None:
            rev["score_20"] = html["score_20"]
            rev["score"] = html.get("score")
        if html.get("tasting_note"):
            rev["tasting_note"] = html["tasting_note"]
        if html.get("reviewer"):
            rev["reviewer"] = html["reviewer"]
        if html.get("drink_from") and not rev["drink_from"]:
            rev["drink_from"] = html["drink_from"]
        if html.get("drink_to") and not rev["drink_to"]:
            rev["drink_to"] = html["drink_to"]

    if rev.get("score_20") is not None or rev.get("tasting_note"):
        return rev
    return None


def _search_tastings_page(
    session: requests.Session,
    wine_name: str,
    vintage: str,
    search_hints: Optional[Dict[str, Any]] = None,
) -> List[Dict]:
    """
    Fallback: parse JR tastings search HTML (same UI page users check manually).
    Used when ES msearch returns no usable hits.
    """
    match_context = _build_match_contexts(wine_name, vintage, search_hints)

    def _collect_from_term(term: str) -> List[Dict]:
        def _fetch(term_value: str, label: str) -> Tuple[Optional[BeautifulSoup], List[Any]]:
            q = urlencode({"search-full": term_value})
            url = f"{JR_ORIGIN}/tastings?{q}"
            try:
                r = session.get(
                    url,
                    timeout=20,
                    headers={
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                        "Accept-Language": "en-US,en;q=0.9",
                        "Cache-Control": "no-cache",
                        "Pragma": "no-cache",
                        "Referer": JR_ORIGIN + "/tastings",
                        "User-Agent": (
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/136.0.0.0 Safari/537.36"
                        ),
                    },
                    allow_redirects=True,
                )
                r.raise_for_status()
                soup = BeautifulSoup(r.text, "html.parser")
                rows = _extract_tastings_rows(soup)
                _debug_tastings_page(r, soup, rows, label)
                return soup, rows
            except Exception as e:
                print(f"    [JR HTML SEARCH ERR] {type(e).__name__}: {e}")
                return None, []

        # Match the manual browser flow: exact quoted search first.
        soup, rows = _fetch(f"\"{term}\"", f"quoted term={term!r}")
        if soup is None:
            return []

        if not rows:
            soup2, rows2 = _fetch(term, f"plain term={term!r}")
            if soup2 is not None:
                soup = soup2
                rows = rows2
        if rows:
            print(f"    [JR HTML] term={term!r} -> {len(rows)} row(s)")
        out_local: List[Dict] = []
        parsed_queries = match_context["parsed_queries"]
        for row in rows:
            items = row.select("div.tspTable__body-row-item")
            if len(items) < 7:
                continue
            producer = items[0].get_text(" ", strip=True)
            wine = items[1].get_text(" ", strip=True)
            hit_vintage = items[2].get_text(" ", strip=True)
            appellation = items[3].get_text(" ", strip=True)
            date_tasted_raw = items[4].get_text(" ", strip=True)
            drink_window = items[5].get_text(" ", strip=True)
            score_raw = items[6].get_text(" ", strip=True)

            if vintage and hit_vintage and str(hit_vintage).strip() != str(vintage).strip():
                continue

            match_text = " ".join(p for p in [producer, wine, appellation] if p).strip()
            if not match_text:
                continue
            score_20: Optional[float] = None
            if score_raw:
                m = re.search(r"(\d+(?:\.\d+)?)", score_raw)
                if m:
                    try:
                        score_20 = float(m.group(1))
                        if "+" in score_raw:
                            score_20 += 0.3
                    except ValueError:
                        score_20 = None

            drink_from = drink_to = None
            m_dw = re.search(r"(\d{4})\s*[-\u2013]\s*(\d{4})", drink_window)
            if m_dw:
                drink_from = int(m_dw.group(1))
                drink_to = int(m_dw.group(2))

            note = ""
            link = row.select_one("div.tspTable__body-row-description a.button")
            review_url = urljoin(JR_ORIGIN, link["href"]) if link and link.get("href") else ""
            desc = row.select_one("div.tspTable__body-row-description div")
            if desc:
                note = re.sub(r"\s+", " ", desc.get_text(" ", strip=True)).strip()

            rev: Dict[str, Any] = {
                "score_20": score_20,
                "score": round(score_20 * 5.0, 1) if score_20 is not None else None,
                "reviewer": "",
                "drink_from": drink_from,
                "drink_to": drink_to,
                "tasting_note": note,
                "region": appellation,
                "colour": "",
                "date_tasted": _parse_date(date_tasted_raw),
                "wine_name_jr": match_text,
                "review_url": review_url,
                "jr_lwin": "",
            }

            if rev.get("score_20") is not None or rev.get("tasting_note"):
                candidate = {
                    "src": None,
                    "review": rev,
                    "match_text": match_text,
                    "rank": _jr_candidate_rank_any(
                        match_context["query_names"],
                        match_text,
                        {"vintage": [hit_vintage] if hit_vintage else [], "date_tasted": date_tasted_raw},
                        vintage,
                        "",
                    ),
                    "producer": producer,
                    "wine_name": wine,
                    "title": wine,
                    "appellation": appellation,
                    "vintage": str(hit_vintage or "").strip(),
                    "lwin": "",
                    "review_url": review_url,
                    "date_tasted": _parse_date(date_tasted_raw),
                }
                if _jr_candidate_passes(parsed_queries, candidate):
                    out_local.append(candidate)
        return out_local

    base = re.sub(r"\s+", " ", wine_name).strip()
    terms = list(_build_search_queries(base, vintage, "", search_hints=search_hints))
    seen = set()
    all_hits: List[Dict] = []
    for term in terms:
        t = term.strip()
        if not t or t.lower() in seen:
            continue
        seen.add(t.lower())
        print(f"    [JR HTML SEARCH] term={t!r}")
        hits = _collect_from_term(t)
        if hits:
            all_hits.extend(hits)
            break

    if not all_hits:
        return []

    print(f"    -> {len(all_hits)} hit(s) via JR tastings HTML fallback")
    all_hits.sort(key=lambda x: (float(x.get("rank") or -9999.0), str(x.get("date_tasted") or "")), reverse=True)
    reviews: List[Dict[str, Any]] = []
    seen_urls: set[str] = set()
    for idx, candidate in enumerate(all_hits):
        review = dict(candidate.get("review") or {})
        review_url = str(candidate.get("review_url") or review.get("review_url") or "").strip()
        if review_url and review_url in seen_urls:
            continue
        if review_url:
            seen_urls.add(review_url)
        if idx == 0 and review_url:
            html = _fetch_full_page(session, review_url)
            if html.get("score_20") is not None:
                review["score_20"] = html["score_20"]
                review["score"] = html.get("score")
            if html.get("tasting_note"):
                review["tasting_note"] = html["tasting_note"]
            if html.get("reviewer"):
                review["reviewer"] = html["reviewer"]
            if html.get("drink_from") and not review.get("drink_from"):
                review["drink_from"] = html["drink_from"]
            if html.get("drink_to") and not review.get("drink_to"):
                review["drink_to"] = html["drink_to"]
        if review.get("score_20") is not None or review.get("tasting_note"):
            reviews.append(review)
    return reviews


# â”€â”€â”€ Main Entry Point â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def search_wine(
    session: requests.Session,
    wine_name: str,
    vintage: str,
    lwin: str = "",
    search_hints: Optional[Dict[str, Any]] = None,
) -> List[Dict]:
    """
    Search JancisRobinson for a wine.
    Returns list of reviews sorted by score desc.
    Each review includes: score_20, tasting_note, reviewer, drink_from/to, review_url
    """
    norm_vintage = _normalize_search_vintage(vintage, wine_name, lwin)
    norm_name = _normalize_search_name(wine_name, norm_vintage)
    _jr_debug(f"search_wine normalized name={norm_name!r} vintage={norm_vintage!r} lwin={lwin!r}")
    hits = jr_msearch(session, norm_name, norm_vintage, "", search_hints=search_hints)
    if not hits:
        print("    -> 0 usable, 0 scored")
        return []

    match_context = _build_match_contexts(norm_name, norm_vintage, search_hints)
    _jr_debug(f"match_context primary={match_context.get('primary_name')!r} query_names={match_context.get('query_names')!r}")
    candidates: List[Dict[str, Any]] = []
    for src in hits:
        candidate = _jr_es_candidate(match_context["query_names"], norm_vintage, "", src)
        if candidate:
            candidates.append(candidate)

    if not candidates:
        _jr_debug("all raw hits were rejected during candidate filtering")
        print("    -> 0 usable, 0 scored")
        return []

    candidates.sort(key=lambda c: float(c.get("rank") or -9999.0), reverse=True)
    _jr_debug(
        f"candidate shortlist={[(round(float(c.get('rank') or 0.0), 2), str(c.get('match_text') or '')[:80]) for c in candidates[:5]]}"
    )
    reviews: List[Dict[str, Any]] = []
    seen_urls: set[str] = set()
    for idx, candidate in enumerate(candidates):
        review = _jr_review_from_es_candidate(session, candidate, fetch_full=(idx == 0))
        if not review:
            continue
        review_url = str(review.get("review_url") or "").strip()
        if review_url and review_url in seen_urls:
            continue
        if review_url:
            seen_urls.add(review_url)
        reviews.append(review)

    reviews.sort(key=lambda x: (str(x.get("date_tasted") or ""), float(x.get("score_20") or -9999.0)), reverse=True)
    _jr_debug(
        f"final reviews={[(r.get('wine_name_jr'), r.get('score_20'), r.get('date_tasted'), r.get('review_url')) for r in reviews[:5]]}"
    )
    print(f"    -> {len(reviews)} usable, {sum(1 for r in reviews if r.get('score_20') is not None)} scored")
    return reviews



# â”€â”€â”€ CLI Test â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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
        print(f"\nâ˜… {r.get('score_20')}/20  |  {r.get('wine_name_jr', wine)}")
        print(f"  Reviewer   : {r.get('reviewer') or 'â€”'}")
        print(f"  Drink      : {r.get('drink_from','?')} â€“ {r.get('drink_to','?')}")
        print(f"  JR LWIN    : {r.get('jr_lwin','â€”')}")
        note = r.get("tasting_note") or ""
        print(f"  Note       : {note[:300]}{'...' if len(note)>300 else ''}")
        print(f"  URL        : {r.get('review_url','')}")
        print("â”€" * 70)

