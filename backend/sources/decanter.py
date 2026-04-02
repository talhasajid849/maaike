"""
sources/decanter.py
===================
Decanter scraper — v5 (engineered rewrite).

Key improvements over v4
------------------------
FIX-A  Vintage embedded in Product_Name is stripped *before* building any
        search query so we never send "Chateau X 2010 2010" to Decanter.
        Almost every wine in the dataset has the vintage in the name string.

FIX-B  Appellation normalisation:
        "Saint-Emilion Grand Cru"  → "St-Emilion"
        "Pessac-Leognan"           → kept as-is (Decanter uses the full form)
        "Moulis en Medoc"          → "Moulis-en-Médoc"
        Decanter uses "St-" for appellation names but "Saint" in château names.

FIX-C  Second-label parent lookup table (_SECOND_LABEL_PARENTS).
        201 second-label wines found in the dataset; their parent château names
        are stored so the scraper can generate "Château Parent, Sub-label,
        Appellation" queries matching Decanter's title structure.

FIX-D  "Rouge Cru Classé" / "Blanc Cru Classé" stripped as a unit (already
        existed but now also strips "Cru Classé" after a colour word that
        immediately precedes the appellation).

FIX-E  Colour in brand name (e.g. "Le Petit Smith Haut Lafitte Rouge") is
        preserved in the query — it is part of the wine's Decanter identity,
        not a search filter token.

FIX-F  _name_matches now uses a *symmetric* token-overlap check:
        both forward (query→hit) and reverse (hit→query) ratios must be
        reasonable, and a hard minimum of 2 shared tokens is required when
        either party is very short.

FIX-G  _build_search_queries generates a tiered query list:
        1. Exact cleaned name + vintage (highest specificity)
        2. Cleaned name without classification junk
        3. Producer + appellation only
        4. Parent château + sub-label + appellation  (for second labels)
        5. Brand-only fallback
        6. Saint/St alias variants of all of the above
        Queries are deduplicated; duplicated vintage in the name is removed.

FIX-H  Appellation "Grand Cru" qualifier on Saint-Emilion is stripped from
        search queries — Decanter uses "St-Emilion", not "St-Emilion Grand Cru".

Previous fixes retained
-----------------------
FIX 1  Bottle-size suffixes stripped
FIX 2  Colour words only trigger colour filter when standalone segment
FIX 3  "Premier Cru" not stripped from château names
FIX 4  Second-label brand-only query fallback
FIX 5  Direct URL bypasses name matching
FIX 6  Reverse-ratio threshold 0.45 → 0.35
FIX 7  "Rouge/Blanc Cru Classé" stripped as unit
FIX 8  Colour in first segment not treated as filter
FIX 9  Standalone colour qualifier segment dropped from query
FIX 10 "X de Y" parent-château query extraction
FIX 11 "Brand, Château X, App" embedded château extraction
FIX 12 Sub-label producer-token subset check
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
import unicodedata
from datetime import datetime
from pathlib import Path
from urllib.parse import quote, unquote, urljoin, urlparse

from bs4 import BeautifulSoup

try:
    from curl_cffi import requests
    _CFFI = True
except ImportError:
    import requests
    _CFFI = False

logger = logging.getLogger("maaike.dc")

COOKIE_FILE = "cookies/decanter.json"
BASE_URL = "https://www.decanter.com"
SEARCH_PATH = "/wine-reviews/search/term/{query}/page/{page}/"
TMP_DIR = Path("tmp")
_OPENAI_RATE_LIMIT_UNTIL = 0.0
_SEARCH_RETRY_SLEEP_SEC = 1.0
_DETAIL_RETRY_SLEEP_SEC = 1.0
_CANDIDATE_FETCH_SLEEP_CAP_SEC = 0.15
_OPENAI_RETRY_SLEEP_CAP_SEC = 0.25
_SEARCH_CACHE_ATTR = "_maaike_dc_search_cache"
_DETAIL_CACHE_ATTR = "_maaike_dc_detail_cache"

DETAIL_URL_RE = re.compile(
    r"https?://(?:www\.)?decanter\.com/wine-reviews/(?!search/)[^\s#]+",
    re.I,
)
SEARCH_URL_RE = re.compile(
    r"https?://(?:www\.)?decanter\.com/wine-reviews/search/[^\s#]+",
    re.I,
)
YEAR_RE = re.compile(r"\b(?:19|20)\d{2}\b")
DATE_RE = re.compile(r"\b\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}\b")

# ---------------------------------------------------------------------------
# FIX-A: Bottle-size and vintage stripping
# ---------------------------------------------------------------------------
_BOTTLE_SIZE_RE = re.compile(
    r"\s*\(\s*(?:"
    r"(?:dbl\.?\s*)?magnum"
    r"|double\s+magnum"
    r"|imperial"
    r"|jeroboam"
    r"|rehoboam"
    r"|methuselah"
    r"|salmanazar"
    r"|balthazar"
    r"|nebuchadnezzar"
    r"|demi"
    r"|half\s+bottle"
    r"|half"
    r"|(?:\d+(?:\.\d+)?\s*[Ll])"
    r")\s*\)",
    re.I,
)

# FIX-A: strip vintage that is embedded directly in the wine name string
# (almost every wine in the dataset has it: "Chateau X, Appellation 2021")
_TRAILING_VINTAGE_RE = re.compile(r"\s+(?:19|20)\d{2}\s*$")
_VINTAGE_RE = re.compile(r"\b(?:19|20)\d{2}\b")


def _strip_bottle_size(name: str) -> str:
    return _clean_text(_BOTTLE_SIZE_RE.sub("", name))


def _strip_embedded_vintage(name: str) -> str:
    """Remove vintage that appears at the end of (or inside) the name string."""
    return _clean_text(_TRAILING_VINTAGE_RE.sub("", name))


# ---------------------------------------------------------------------------
# FIX-B: Appellation normalisation
# ---------------------------------------------------------------------------
# Decanter uses "St-Émilion" for the appellation but "Saint" in château names.
# The rules below normalise appellation tokens only (they appear after a comma).

_APPELLATION_NORMS: list[tuple[re.Pattern, str]] = [
    # "Saint-Emilion Grand Cru" → "St-Emilion"  (FIX-H: drop Grand Cru suffix)
    (re.compile(r"\bSaint-Emilion\s+Grand\s+Cru\b", re.I), "St-Emilion"),
    (re.compile(r"\bSt[.\-]?Emilion\s+Grand\s+Cru\b", re.I), "St-Emilion"),
    (re.compile(r"\bSaint[- ]Emilion\b", re.I), "St-Emilion"),
    (re.compile(r"\bSt[.\-]?Emilion\b", re.I), "St-Emilion"),
    (re.compile(r"\bSaint[- ]Estephe\b", re.I), "St-Estèphe"),
    (re.compile(r"\bSt[.\-]?Estephe\b", re.I), "St-Estèphe"),
    (re.compile(r"\bSaint[- ]Julien\b", re.I), "St-Julien"),
    (re.compile(r"\bSt[.\-]?Julien\b", re.I), "St-Julien"),
    (re.compile(r"\bMoulis\s+en\s+Medoc\b", re.I), "Moulis-en-Médoc"),
    (re.compile(r"\bHaut[- ]Medoc\b", re.I), "Haut-Médoc"),
    (re.compile(r"\bPessac[- ]Leognan\b", re.I), "Pessac-Léognan"),
    (re.compile(r"\bPauillac\b", re.I), "Pauillac"),
    (re.compile(r"\bMargaux\b", re.I), "Margaux"),
    (re.compile(r"\bPomerol\b", re.I), "Pomerol"),
    (re.compile(r"\bSauternes\b", re.I), "Sauternes"),
    (re.compile(r"\bMedoc\b", re.I), "Médoc"),
]


def _normalise_appellation(text: str) -> str:
    """Apply Decanter appellation normalisation to a single segment."""
    for pattern, replacement in _APPELLATION_NORMS:
        text = pattern.sub(replacement, text)
    return text


# ---------------------------------------------------------------------------
# FIX-C: Second-label parent lookup
# ---------------------------------------------------------------------------
# Maps second-label brand name (lowercased, accent-stripped) → parent château
# search token. Used to generate "Château Parent, Brand, Appellation" queries.

_SECOND_LABEL_PARENTS: dict[str, str] = {
    # key: normalised brand name  value: Decanter-style parent name
    "blason d issan":                "Château d'Issan",
    "brio de cantenac brown":        "Château Cantenac Brown",
    "caillou blanc":                 "Château Talbot",
    "carmes de rieussec":            "Château Rieussec",
    "carruades de lafite":           "Château Lafite Rothschild",
    "chapelle d ausone":             "Château Ausone",
    "chapelle de potensac":          "Château Potensac",
    "chevalier de lascombes":        "Château Lascombes",
    "clos du marquis":               "Château Léoville Las Cases",
    "comte de dauzac":               "Château Dauzac",
    "confidences de prieure lichine":"Château Prieuré-Lichine",
    "connetable talbot":             "Château Talbot",
    "croix canon":                   "Canon",
    "duluc de branaire ducru":       "Château Branaire-Ducru",
    "echo de lynch bages":           "Château Lynch-Bages",
    "fleur de pedesclaux":           "Château Pédesclaux",
    "fugue de nenin":                "Château Nénin",
    "g d estournel":                 "Château Cos d'Estournel",
    "haut bailly ii":                "Château Haut-Bailly",
    "la parde de haut bailly":       "Château Haut-Bailly",
    "la chapelle de bages":          "Château Haut-Bages Libéral",
    "la chapelle de la mission haut brion": "Château La Mission Haut-Brion",
    "la clarte de haut brion":       "Château Haut-Brion",
    "la closerie de fourtet":        "Château Canon-La-Gaffelière",
    "la dame de montrose":           "Château Montrose",
    "la demoiselle de sociando mallet": "Château Sociando-Mallet",
    "la gravette de certan":         "Vieux Château Certan",
    "la mondotte":                   "Château La Mondotte",
    "la petite eglise":              "Château L'Église-Clinet",
    "la reserve de leoville barton": "Château Léoville-Barton",
    "lacoste borie":                 "Château Grand-Puy-Lacoste",
    "le benjamin de beauregard":     "Château Beauregard",
    "le clarence de haut brion":     "Château Haut-Brion",
    "le clementin de pape clement rouge": "Château Pape Clément",
    "le clementin de pape clement":  "Château Pape Clément",
    "le comte de malartic rouge":    "Château Malartic-Lagravière",
    "le comte de malartic":          "Château Malartic-Lagravière",
    "le dauphin d olivier rouge":    "Château Olivier",
    "le dauphin d olivier blanc":    "Château Olivier",
    "le dauphin d olivier":          "Château Olivier",
    "le haut medoc de giscours":     "Château Giscours",
    "le marquis de calon segur":     "Château Calon Ségur",
    "le merle de peby faugeres":     "Château Péby Faugères",
    "le pauillac de chateau latour": "Château Latour",
    "le petit lion du marquis de las cases": "Château Léoville Las Cases",
    "le petit mouton de mouton rothschild": "Château Mouton Rothschild",
    "le petit smith haut lafitte rouge": "Château Smith Haut Lafitte",
    "le petit smith haut lafitte":   "Château Smith Haut Lafitte",
    "le plus de la fleur de bouard": "Château La Fleur de Bouard",
    "le reflet de laffitte carcasset": "Château Laffitte-Carcasset",
    "le seuil de mazeyres":          "Château Mazeyres",
    "les allees de cantemerle":      "Château Cantemerle",
    "les angelots de villemaurine":  "Château Villemaurine",
    "les eclats de branas grand poujeaux": "Château Branas Grand Poujeaux",
    "les fiefs de lagrange":         "Château Lagrange",
    "esperance de trotanoy":         "Château Trotanoy",
    "les forts de latour":           "Château Latour",
    "les griffons de pichon baron":  "Château Pichon Baron",
    "les hauts de larrivet haut brion blanc": "Château Larrivet Haut-Brion",
    "les hauts de larrivet haut brion": "Château Larrivet Haut-Brion",
    "les hauts du tertre":           "Château du Tertre",
    "les pelerins de lafon rochet":  "Château Lafon-Rochet",
    "les pensees de la tour carnet": "Château La Tour Carnet",
    "les perrieres":                 "Château Lafleur",
    "les tourelles de longueville":  "Château Pichon Baron",
    "les tours de laroque":          "Château Laroque",
    "lions de suduiraut":            "Château Suduiraut",
    "madame de beaucaillou":         "Château Ducru-Beaucaillou",
    "margaux de brane":              "Château Brane-Cantenac",
    "baron de brane":                "Château Brane-Cantenac",
    "moulin de duhart":              "Château Duhart-Milon",
    "n 2 de maucaillou":             "Château Maucaillou",
    "pagodes de cos":                "Château Cos d'Estournel",
    "pagus de lagrange":             "Château Lagrange",
    "pastourelle de clerc milon":    "Château Clerc Milon",
    "pavillon rouge du chateau margaux": "Château Margaux",
    "pensees de lafleur":            "Château Lafleur",
    "pichon comtesse reserve":       "Château Pichon Comtesse de Lalande",
    "prelude a grand puy ducasse":   "Château Grand-Puy-Ducasse",
    "sarget de gruaud larose":       "Château Gruaud Larose",
    "seigneurs d aiguilhe":          "Château d'Aiguilhe",
    "symphonie de haut peyraguey":   "Château Haut-Peyraguey",
    "verso":                         "Château Haut-Batailley",
    "9 de marquis de terme":         "Château Marquis de Terme",
    "alter ego":                     "Château Palmer",
    "aile d argent":                 "Château Mouton Rothschild",
    "l esprit de chevalier rouge":   "Domaine de Chevalier",
    "l esprit de chevalier":         "Domaine de Chevalier",
    "l abeille de fieuzal rouge":    "Château de Fieuzal",
    "l abeille de fieuzal":          "Château de Fieuzal",
    "l extravagant de doisy daene":  "Château Doisy-Daëne",
    # Added from v19 analysis — second labels missing from dict
    "lassegue":                      "Château Lassègue",
    "lynsolence":                    "Château Lynsolence",
    "bellevue mondotte":             "Château Bellevue Mondotte",
    "blanc de lynch bages":          "Château Lynch-Bages",
    "clarendelle rouge":             "Château Haut-Brion",
    "clarendelle":                   "Château Haut-Brion",
    "cos d estournel":               "Château Cos d'Estournel",
    "croix de beausejour":           "Château Beauséjour",
    "l if":                          "L'If",
    "la chenade":                    "Château La Chenade",
    "la mauriane":                   "Château La Mauriane",
    "le blanc d aiguilhe":           "Château d'Aiguilhe",
    "le dome":                       "Le Dôme",
    "le kid d arsac":                "Château d'Arsac",
    "le pin":                        "Le Pin",
    "les arums de lagrange":         "Château Lagrange",
    "les asteries":                  "Les Astéries",
    # Note: 'les champs libres' removed — it IS its own brand on Decanter
    # (page: "Les Champs Libres, Guinaudeau, 2021"). The raw query finds it fine.
    "pavillon blanc du chateau margaux": "Château Margaux",
    "pavillon blanc":                "Château Margaux",
    "petit corbin despagne":         "Château Grand Corbin-Despagne",
    "petit soutard":                 "Château Soutard",
    "petrus":                        "Pétrus",
    "r rieussec":                    "Château Rieussec",
    "s de siran":                    "Château Siran",
    "s de suduiraut":                "Château Suduiraut",
    "saintayme":                     "Château Saintayme",
    "stella solare":                 "Château Croix de Labrie",
    "hubert de bouard":              "Hubert de Bouard",
    "les menuts":                    "Château La Grâce Dieu",
}


def _lookup_parent(brand: str) -> str | None:
    """Return the Decanter-style parent château name for a known second label.
    Returns None if the parent would be the same as the brand (self-referential),
    since that would generate useless duplicate queries like 'X, X, appellation'.
    """
    raw = _clean_text(_normalize_text(brand)).lower()
    key = re.sub(r"[^a-z0-9 ]+", " ", raw)
    key = re.sub(r"\s+", " ", key).strip()
    # Direct hit
    if key in _SECOND_LABEL_PARENTS:
        parent = _SECOND_LABEL_PARENTS[key]
        # Guard: skip if parent normalises to the same key as brand
        parent_key = re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]+", " ",
                            _clean_text(_normalize_text(parent)).lower())).strip()
        if parent_key == key:
            return None
        return parent
    # Partial match: key starts with a known brand
    for known_key, parent in _SECOND_LABEL_PARENTS.items():
        if key.startswith(known_key) or known_key.startswith(key):
            parent_key = re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]+", " ",
                                _clean_text(_normalize_text(parent)).lower())).strip()
            if parent_key == key:
                continue
            return parent
    return None


# ---------------------------------------------------------------------------
# Token sets
# ---------------------------------------------------------------------------
_STOP_WORDS = frozenset({
    "a", "an", "and", "the", "wine", "wines", "de", "du", "des", "le", "la",
    "les", "d", "en", "sur", "et", "review", "reviews",
})
_GENERIC_NAME_TOKENS = frozenset({
    "cru", "classe", "class", "grand", "premier", "rouge", "blanc",
    "exceptionnel", "bourgeois", "superieur", "superieure",
    "1er", "1ere", "1st", "2eme", "2e", "2nd", "3eme", "3e", "3rd",
    "4eme", "4e", "4th", "5eme", "5e", "5th",
    "red", "white", "rose", "sparkling",
})
_LIEU_DIT_WORDS = frozenset({
    "grand", "premier", "cru", "classe", "class", "vignes", "vigne", "vieilles",
    "old", "vine", "vineyard", "reserve", "riserva", "selection", "special",
    "blanc", "rouge", "rose", "red", "white", "sparkling",
})
_COLOUR_MAP = {
    "red": "red", "white": "white", "rose": "rose", "sparkling": "sparkling",
}
_STANDALONE_COLOUR_RE = re.compile(
    r"^\s*(?:rouge|blanc|red|white|rosé?|sparkling)\s*$", re.I
)


# ---------------------------------------------------------------------------
# Optional OpenAI fallback
# ---------------------------------------------------------------------------

def _openai_fallback_enabled(search_hints: dict | None = None) -> bool:
    hints = search_hints or {}
    raw = hints.get("enable_openai_fallback")
    if raw is None:
        raw = os.environ.get("DECANTER_OPENAI_FALLBACK")
    if raw is None or str(raw).strip() == "":
        return bool(str(hints.get("openai_api_key") or os.environ.get("OPENAI_API_KEY") or "").strip())
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}

def _openai_normalize_name(name: str, vintage: int | None, api_key: str) -> str | None:
    global _OPENAI_RATE_LIMIT_UNTIL
    if time.time() < _OPENAI_RATE_LIMIT_UNTIL:
        return None
    try:
        import urllib.request
        prompt = (
            "You are an expert on Bordeaux wine. "
            "Rewrite the following wine name into the canonical format used by "
            "Decanter magazine wine reviews. "
            "Rules:\n"
            "1. Add correct French accents (Château, Médoc, Pomerol, etc.)\n"
            "2. Remove bottle-size annotations: Magnum, Imperial, Demi, Jeroboam, etc.\n"
            "3. Remove classification rank from the producer segment "
            "(e.g. '5ème Cru Classé', 'Premier Grand Cru Classé B') — "
            "Decanter places these after the appellation or omits them\n"
            "4. For second-label wines, prepend the parent château name: "
            "'Château Parent, Sub-label, Appellation'\n"
            "5. Abbreviate Saint- to St- in APPELLATION names only "
            "(St-Émilion, St-Julien, St-Estèphe) but keep 'Saint' in château names\n"
            "6. If Decanter files the wine under an extended subtitle or second-wine "
            "name, prefer that exact Decanter review title even when the input is shorter.\n"
            "7. Return ONLY the normalised name, nothing else.\n\n"
            f"Wine: {name}\n"
            f"Vintage: {vintage or 'unknown'}\n"
            "Normalised Decanter name:"
        )
        payload = json.dumps({
            "model": "gpt-4o-mini",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 80,
            "temperature": 0,
        }).encode()
        req = urllib.request.Request(
            "https://api.openai.com/v1/chat/completions",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        normalised = data["choices"][0]["message"]["content"].strip().strip('"\'')
        if normalised and normalised != name:
            logger.info("DC OpenAI normalised %r -> %r", name, normalised)
            return normalised
        return None
    except Exception as exc:
        if "429" in str(exc):
            _OPENAI_RATE_LIMIT_UNTIL = time.time() + 600.0
            logger.warning("DC OpenAI rate-limited; disabling for 10 minutes")
        logger.warning("DC OpenAI normalisation failed for %r: %s", name, exc)
        return None


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------

def load_session(cookie_file: str = COOKIE_FILE):
    path = Path(cookie_file)
    if not path.exists():
        raise FileNotFoundError(f"Cookie file not found: {cookie_file}")
    cookies = json.loads(path.read_text(encoding="utf-8"))
    session = requests.Session(impersonate="chrome124") if _CFFI else requests.Session()
    for c in cookies:
        session.cookies.set(
            c["name"], c["value"],
            domain=c.get("domain", ".decanter.com"),
            path=c.get("path", "/"),
        )
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": f"{BASE_URL}/wine-reviews/search/",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    })
    setattr(session, _SEARCH_CACHE_ATTR, {})
    setattr(session, _DETAIL_CACHE_ATTR, {})
    return session


def _get_session_cache(session, attr_name: str) -> dict:
    cache = getattr(session, attr_name, None)
    if isinstance(cache, dict):
        return cache
    cache = {}
    setattr(session, attr_name, cache)
    return cache


def check_session(session) -> dict:
    cookie_names = {c.name for c in session.cookies}
    return {
        "source": "decanter",
        "has_oauth_cookie": "wine_api_oauth_tokens" in cookie_names,
        "cookie_count": len(cookie_names),
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def search_wine(
    session,
    name: str,
    vintage: int | None = None,
    lwin: str | None = None,
    sleep_sec: float = 3.0,
    search_hints: dict | None = None,
) -> list[dict]:
    del lwin
    sleep_sec = max(0.0, min(float(sleep_sec or 0.0), 360.0))
    # FIX 1 + FIX-A: strip bottle-size and embedded vintage immediately
    name = _strip_bottle_size(name)
    name = _strip_embedded_vintage(name)

    matched_reviews: list[dict] = []
    candidate_reviews: list[dict] = []
    expected_name = _preferred_query_name(name, vintage, search_hints)

    for entry in _direct_detail_urls(name, vintage, search_hints):
        url = entry["url"]
        review = _fetch_review_from_url(session, url)
        if review and _matches_query(
            expected_name, vintage, review,
            direct_url=url, allow_missing_vintage=bool(entry.get("allow_missing_vintage")),
        ):
            logger.info("DC direct detail accepted: %s", url)
            matched_reviews.append(review)
        elif review:
            logger.info(
                "DC direct detail rejected: query=%r url=%r hit=%r",
                expected_name, url, review.get("wine_name_src"),
            )

    if matched_reviews:
        return [_pick_latest_review(matched_reviews)]

    all_candidates = _search_result_candidates(
        session, expected_name, vintage, search_hints=search_hints
    )
    logger.debug(
        "DC top candidates for %r vintage=%s: %s",
        expected_name, vintage,
        [(c.get("title","")[:50], c.get("vintage"), f"score={c.get('rank_score',0):.1f}")
         for c in all_candidates[:8]],
    )
    candidate_pause = min(_CANDIDATE_FETCH_SLEEP_CAP_SEC, sleep_sec * 0.05)
    for candidate in _candidate_review_pool(all_candidates, vintage):
        if candidate_pause > 0:
            time.sleep(candidate_pause)
        review = _fetch_review_from_url(session, candidate.get("url") or "")
        if review and _matches_query(expected_name, vintage, review):
            logger.info("DC candidate accepted: %s", candidate.get("url"))
            candidate_reviews.append(review)
        elif review:
            logger.info(
                "DC candidate rejected: query=%r url=%r hit=%r",
                expected_name, candidate.get("url"), review.get("wine_name_src"),
            )

    if candidate_reviews:
        return [_pick_latest_review(candidate_reviews)]

    # OpenAI fallback
    hints = search_hints or {}
    api_key = str(hints.get("openai_api_key") or os.environ.get("OPENAI_API_KEY") or "").strip()
    if _openai_fallback_enabled(search_hints) and api_key:
        normalised = _openai_normalize_name(name, vintage, api_key)
        if normalised and normalised != expected_name:
            logger.info("DC retrying with OpenAI-normalised name: %r", normalised)
            openai_pause = min(_OPENAI_RETRY_SLEEP_CAP_SEC, sleep_sec * 0.08)
            if openai_pause > 0:
                time.sleep(openai_pause)
            for candidate in _candidate_review_pool(
                _search_result_candidates(session, normalised, vintage, search_hints=None),
                vintage,
                max_candidates=16,
            ):
                if candidate_pause > 0:
                    time.sleep(candidate_pause)
                review = _fetch_review_from_url(session, candidate.get("url") or "")
                if review and _matches_query(normalised, vintage, review):
                    logger.info("DC OpenAI candidate accepted: %s", candidate.get("url"))
                    candidate_reviews.append(review)
                elif review:
                    logger.info(
                        "DC OpenAI candidate rejected: query=%r url=%r hit=%r",
                        normalised, candidate.get("url"), review.get("wine_name_src"),
                    )
            if candidate_reviews:
                return [_pick_latest_review(candidate_reviews)]

    return []


def diagnose_no_result(
    session,
    name: str,
    vintage: int | None = None,
    lwin: str | None = None,
    search_hints: dict | None = None,
) -> str | None:
    del lwin
    name = _strip_bottle_size(name)
    name = _strip_embedded_vintage(name)
    expected_name = _preferred_query_name(name, vintage, search_hints)
    candidates = _search_result_candidates(session, expected_name, vintage, search_hints=search_hints)
    direct_urls = _direct_detail_urls(name, vintage, search_hints)

    if not candidates and not direct_urls:
        return "Decanter search returned no candidate review pages for that wine query."

    for entry in direct_urls:
        url = entry["url"]
        review = _fetch_review_from_url(session, url)
        if not review:
            continue
        hit_name = review.get("wine_name_src") or ""
        hit_vintage = review.get("vintage_src")
        if vintage is not None and hit_vintage is not None and int(hit_vintage) != int(vintage):
            return (
                f"Decanter URL resolved to {hit_name or 'a different wine'} "
                f"({hit_vintage}), but your search is using vintage {vintage}."
            )
        if hit_name and expected_name and not _name_matches(expected_name, hit_name):
            return (
                f"Decanter URL resolved to '{hit_name}', which does not match "
                "the wine name you entered."
            )
        if review.get("score_native") is None and not review.get("note"):
            return "Decanter returned a page, but Maaike could not parse a usable tasting note from it."

    if candidates:
        top = candidates[0]
        return (
            "Decanter returned candidates, but none passed the exact-match guards. "
            f"Top candidate was '{top.get('title') or top.get('url')}'."
        )
    return None


# ---------------------------------------------------------------------------
# Search plumbing
# ---------------------------------------------------------------------------

def _search_result_candidates(
    session,
    name: str,
    vintage: int | None,
    search_hints: dict | None = None,
) -> list[dict]:
    ranked: list[dict] = []
    position = 0
    max_ranked = 48 if _extract_year(vintage) is not None else 24

    for manual_url in _manual_search_urls(name, vintage, search_hints):
        html = _fetch_search_page(session, manual_url)
        page_cands = _parse_search_candidates(html)
        logger.debug("DC manual-url %r → %d candidates", manual_url, len(page_cands))
        for candidate in page_cands:
            position += 1
            candidate["rank_score"] = _search_candidate_rank(name, vintage, candidate, 0, position)
            ranked.append(candidate)

    if ranked:
        ranked = _dedupe_search_candidates(ranked)
        result = _sort_candidates(ranked, vintage)
        logger.debug("DC manual-url total: %d candidates, top=%r",
                     len(result), result[0].get("title") if result else None)
        return result

    queries = _build_search_queries(name, vintage, search_hints)
    logger.debug("DC query=%r vintage=%s → %d query variants: %s",
                 name, vintage, len(queries),
                 [q[:60] for q in queries[:4]])

    for query_index, query in enumerate(queries, start=1):
        query_new_count = 0
        for page in (1, 2, 3, 4):
            url = _build_search_url(query, page)
            html = _fetch_search_page(session, url)
            page_candidates = _parse_search_candidates(html)
            logger.debug("DC   q%d/%d page%d: url=%r → %d results",
                         query_index, len(queries), page, url, len(page_candidates))
            if not page_candidates:
                break
            for candidate in page_candidates:
                position += 1
                candidate["rank_score"] = _search_candidate_rank(
                    name, vintage, candidate, query_index, position
                )
                ranked.append(candidate)
                query_new_count += 1
            if len(ranked) >= max_ranked:
                break
        if query_new_count:
            logger.debug("DC   q%d added %d candidates (total=%d)",
                         query_index, query_new_count, len(ranked))
        if len(ranked) >= max_ranked:
            break

    ranked = _dedupe_search_candidates(ranked)
    return _sort_candidates(ranked, vintage)


def _fetch_search_page(session, url: str) -> str:
    """Fetch a Decanter search page. Retries once after a short sleep on timeout."""
    if not url:
        return ""
    cache = _get_session_cache(session, _SEARCH_CACHE_ATTR)
    cached = cache.get(url)
    if cached is not None:
        return cached
    for attempt in range(2):
        try:
            response = session.get(url, timeout=10, allow_redirects=True)
        except Exception as exc:
            if attempt == 0 and "timed out" in str(exc).lower():
                # One retry after a short pause — Decanter CDN sometimes needs a moment
                logger.debug("DC search timeout on attempt 1, retrying: %r", url)
                time.sleep(_SEARCH_RETRY_SLEEP_SEC)
                continue
            logger.warning("DC search request failed %r: %s", url, exc)
            return ""
        if not response.ok:
            logger.warning("DC search HTTP %s for %r", response.status_code, url)
            return ""
        html = response.text or ""
        cache[url] = html
        return html
    return ""


def _parse_search_candidates(html: str) -> list[dict]:
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    candidates: list[dict] = []
    seen: set[str] = set()

    for anchor in soup.select('a[href^="/wine-reviews/"]'):
        href = str(anchor.get("href") or "").strip()
        if not href or "/wine-reviews/search/" in href:
            continue
        card = (
            anchor.select_one('div[class*="WineGeneral_wines__box"]')
            or anchor.select_one('div[class*="WineRow_container"]')
        )
        if not card:
            continue
        if not anchor.select_one("h5"):
            continue

        url = urljoin(BASE_URL, href)
        if url in seen:
            continue
        seen.add(url)

        producer = _clean_text(anchor.select_one("h5").get_text(" ", strip=True))
        brand_node = anchor.select_one("h6")
        brand = _clean_text(brand_node.get_text(" ", strip=True) if brand_node else "")
        region_container = anchor.select_one('[class*="region-container"]')
        location_parts: list[str] = []
        if region_container:
            for node in region_container.select("div"):
                text = _clean_text(node.get_text(" ", strip=True))
                if text and text not in location_parts and _extract_year(text) is None:
                    location_parts.append(text)
        else:
            region_node = anchor.select_one('[class*="region"]')
            text = _clean_text(region_node.get_text(" ", strip=True) if region_node else "")
            if text:
                location_parts.append(text)
        location = ", ".join(location_parts)
        vintage_node = anchor.select_one('[class*="vintage"]')
        vintage = _extract_year(vintage_node.get_text(" ", strip=True) if vintage_node else "")
        score_node = anchor.select_one('[class*="score__"]')
        score = _parse_score(score_node.get_text(" ", strip=True) if score_node else "")
        tasting_node = anchor.select_one('[class*="tasting"] span')
        date_text = _clean_text(tasting_node.get_text(" ", strip=True) if tasting_node else "")

        title = producer
        if brand:
            title = f"{title}, {brand}" if title else brand
        tail = " ".join(part for part in [location, str(vintage or "")] if part)
        if tail:
            title = f"{title}, {tail}" if title else tail

        candidates.append({
            "url": url, "title": title, "producer": producer, "brand": brand,
            "location": location, "date_text": date_text, "score": score, "vintage": vintage,
        })
    return candidates


def _fetch_review_from_url(session, url: str) -> dict | None:
    html, final_url = _fetch_detail_page(session, url)
    if not html:
        return None
    return _parse_detail_page(html, final_url or url)


def _fetch_detail_page(session, url: str) -> tuple[str, str]:
    """Fetch a Decanter wine detail page. Retries once on timeout."""
    cache = _get_session_cache(session, _DETAIL_CACHE_ATTR)
    cached = cache.get(url)
    if cached is not None:
        return cached
    for attempt in range(2):
        try:
            response = session.get(url, timeout=10, allow_redirects=True)
        except Exception as exc:
            if attempt == 0 and "timed out" in str(exc).lower():
                logger.debug("DC detail timeout on attempt 1, retrying: %r", url)
                time.sleep(_DETAIL_RETRY_SLEEP_SEC)
                continue
            logger.warning("DC detail request failed %r: %s", url, exc)
            return "", ""
        if not response.ok:
            logger.warning("DC detail HTTP %s for %r", response.status_code, url)
            return "", ""
        if "/404" in response.url:
            return "", ""
        result = (response.text or "", response.url)
        cache[url] = result
        if response.url and response.url != url:
            cache[response.url] = result
        return result
    return "", ""


def _parse_detail_page(html: str, url: str) -> dict | None:
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    title_node = soup.select_one('h1[class*="WineInfo_wine-title"]') or soup.find("h1")
    title = _clean_text(title_node.get_text(" ", strip=True) if title_node else "")
    if not title:
        return None

    score_node = soup.select_one('div[class*="detail__tabs__score"] span')
    score = _parse_score(score_node.get_text(" ", strip=True) if score_node else "")

    note_node = soup.select_one('div[class*="detail__tabs__review"] > div')
    note = _clean_text(note_node.get_text(" ", strip=True) if note_node else "")

    tasted_by_node = soup.select_one('div[class*="detail__tabs__tastedBy"]')
    tasted_by_text = _clean_text(tasted_by_node.get_text(" ", strip=True) if tasted_by_node else "")
    reviewer_node = tasted_by_node.select_one('span[class*="author"]') if tasted_by_node else None
    reviewer = _clean_text(reviewer_node.get_text(" ", strip=True) if reviewer_node else "")
    if not reviewer and tasted_by_text:
        match = re.search(r"Tasted by:\s*(.+?)(?:\(|$)", tasted_by_text)
        reviewer = _clean_text(match.group(1)) if match else "Decanter"
    reviewer = reviewer or "Decanter"

    date_match = DATE_RE.search(tasted_by_text)
    date_tasted = date_match.group(0) if date_match else None

    window_node = soup.select_one('div[class*="detail__tabs__window"]')
    drink_from, drink_to = _parse_drinking_window(
        _clean_text(window_node.get_text(" ", strip=True) if window_node else "")
    )

    next_data = _parse_next_data_review(soup)
    if score is None:
        score = next_data.get("score")
    if not note:
        note = _clean_text(next_data.get("note") or "")
    if (not reviewer or reviewer == "Decanter") and next_data.get("reviewer"):
        reviewer = _clean_text(next_data.get("reviewer") or "") or reviewer
    if not date_tasted:
        date_tasted = next_data.get("date_tasted") or date_tasted
    if drink_from is None:
        drink_from = next_data.get("drink_from")
    if drink_to is None:
        drink_to = next_data.get("drink_to")

    facts = _parse_fact_rows(soup)
    wine_colour = _parse_colour(facts.get("colour") or "")
    vintage = (
        _extract_year(facts.get("vintage") or "")
        or _extract_year(next_data.get("vintage") or "")
        or _extract_year(title)
    )
    producer = _clean_text(facts.get("producer") or "")
    appellation = _clean_text(facts.get("appellation") or "")
    region = _clean_text(facts.get("region") or "")
    country = _clean_text(facts.get("country") or "")
    brand = _clean_text(facts.get("wine") or facts.get("wine name") or "")
    location_parts = [part for part in (appellation, region, country) if part]
    location = ", ".join(dict.fromkeys(location_parts))

    review = {
        "score_native": score,
        "note": note,
        "reviewer": reviewer,
        "drink_from": drink_from,
        "drink_to": drink_to,
        "date_tasted": date_tasted,
        "vintage_src": vintage,
        "review_url": _canonical_url(url),
        "colour": wine_colour,
        "wine_name_src": title,
        "producer": producer,
        "brand": brand,
        "appellation": appellation,
        "region": region,
        "country": country,
        "location": location,
        "score_label": f"DC {int(score)}" if score is not None else "DC",
    }

    if review["score_native"] is None and not review["note"]:
        return None
    return review


def _parse_next_data_review(soup: BeautifulSoup) -> dict[str, object]:
    node = soup.find("script", id="__NEXT_DATA__")
    if not node or not node.string:
        return {}
    try:
        data = json.loads(node.string)
        wine = (
            data.get("props", {})
            .get("pageProps", {})
            .get("wine", {})
        )
        tasting = wine.get("primary_tasting") or {}
        scores = tasting.get("scores") or []
        first_score = scores[0] if isinstance(scores, list) and scores else {}
        score = tasting.get("rounded_score") or tasting.get("average_score") or first_score.get("score")
        note = tasting.get("consolidated_review") or first_score.get("review") or ""
        reviewer = (
            (first_score.get("judge") or {}).get("name")
            or (tasting.get("judge") or {}).get("name")
            or ""
        )
        tasting_date = (tasting.get("tasting") or {}).get("start_date") or tasting.get("published_at") or ""
        tasting_date = str(tasting_date).split("T", 1)[0] if tasting_date else None
        return {
            "score": _parse_score(score),
            "note": _clean_text(note),
            "reviewer": _clean_text(reviewer),
            "date_tasted": tasting_date,
            "drink_from": tasting.get("drink_from"),
            "drink_to": tasting.get("drink_to"),
            "vintage": (
                _extract_year(wine.get("vintage"))
                or _extract_year(wine.get("year"))
                or _extract_year(tasting.get("vintage"))
                or _extract_year(tasting.get("year"))
                or _extract_year(first_score.get("vintage"))
            ),
        }
    except Exception as exc:
        logger.debug("DC __NEXT_DATA__ parse failed for detail page: %s", exc)
        return {}


def _parse_fact_rows(soup: BeautifulSoup) -> dict[str, str]:
    facts: dict[str, str] = {}
    for row in soup.select('div[class*="WineInfo_wineInfo__item__"]'):
        type_node = row.select_one('div[class*="type__"]')
        value_node = row.select_one('div[class*="value__"]')
        if not type_node or not value_node:
            continue
        key = _clean_text(type_node.get_text(" ", strip=True)).lower()
        value = _clean_text(value_node.get_text(" ", strip=True))
        if key:
            facts[key] = value
    return facts


def _parse_drinking_window(text: str) -> tuple[int | None, int | None]:
    years = [int(y) for y in re.findall(r"\b(?:19|20)\d{2}\b", text or "")]
    if len(years) >= 2:
        return years[0], years[1]
    if len(years) == 1:
        return years[0], None
    return None, None


# ---------------------------------------------------------------------------
# Ranking
# ---------------------------------------------------------------------------

def _search_candidate_rank(
    query_name: str,
    query_vintage: int | None,
    candidate: dict,
    query_index: int,
    position: int,
) -> float:
    title = str(candidate.get("title") or "")
    query_norm = _clean_text(_normalize_text(query_name)).lower()
    title_norm = _clean_text(_normalize_text(title)).lower()
    query_tokens = _name_tokens(query_name)
    title_tokens = _name_tokens(title)
    producer_tokens = _producer_tokens(query_name)
    tail_tokens = _query_tail_tokens(query_name)
    lieu_tokens = _query_lieu_dit_tokens(query_name)
    candidate_tokens = _candidate_identity_tokens(candidate)

    score = 0.0

    if title_norm and query_norm:
        if title_norm == query_norm:
            score += 12.0
        elif title_norm.startswith(query_norm):
            score += 8.0
        elif query_norm in title_norm:
            score += 4.0

    if query_tokens and title_tokens:
        overlap = len(query_tokens & title_tokens)
        score += 8.0 * (overlap / len(query_tokens))
        reverse = overlap / len(title_tokens)
        if reverse < 0.35:
            score -= 2.0

    if producer_tokens and title_tokens:
        producer_overlap = len(producer_tokens & title_tokens) / max(len(producer_tokens), 1)
        score += 5.0 * producer_overlap
        if producer_overlap < 0.5:
            score -= 12.0

    if tail_tokens and title_tokens:
        tail_overlap = len(tail_tokens & title_tokens) / max(len(tail_tokens), 1)
        score += 6.0 * tail_overlap
        if tail_overlap == 0:
            score -= 8.0

    if lieu_tokens and candidate_tokens:
        lieu_overlap_count = len(lieu_tokens & candidate_tokens)
        lieu_overlap = lieu_overlap_count / max(len(lieu_tokens), 1)
        score += 18.0 * lieu_overlap
        if lieu_overlap_count == 0:
            # Root-cause guard: producer-only matches should not outrank
            # vineyard-specific hits for Burgundy/Barolo-style queries.
            score -= 26.0
        elif lieu_overlap < 0.5:
            score -= 7.0
        elif lieu_overlap >= 0.9:
            score += 4.0

    query_tier = _cru_tier_signature(query_name)
    hit_tier = _cru_tier_signature(title)
    if query_tier and hit_tier:
        if query_tier == hit_tier:
            score += 3.0
        else:
            score -= 10.0

    if _name_matches(query_name, title):
        score += 5.0

    if query_vintage is not None:
        if _vintage_matches(query_vintage, candidate.get("vintage"), title):
            score += 4.0
        else:
            score -= 7.0

    score += max(0.0, 2.0 - (position - 1) * 0.15)
    score += max(0.0, 1.0 - (query_index - 1) * 0.2)
    score += _parse_sortable_date(candidate.get("date_text"))[0] * 0.001
    score += float(candidate.get("score") or 0.0) * 0.01
    return score


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------

def _matches_query(
    query_name: str,
    query_vintage: int | None,
    review: dict,
    direct_url: str | None = None,
    allow_missing_vintage: bool = False,
) -> bool:
    query_name = _strip_bottle_size(_clean_text(query_name))
    hit_name = str(review.get("wine_name_src") or "")
    hit_identity = _review_identity_text(review)
    hit_url  = str(review.get("review_url") or "")
    direct_match = bool(direct_url or _extract_direct_url(query_name))

    # ── Name check ──────────────────────────────────────────────────────────
    name_ok, name_reason = _name_matches_with_reason(query_name, hit_identity or hit_name)
    if not name_ok:
        logger.debug(
            "DC reject[name/%s]: query=%r hit=%r url=%s",
            name_reason, query_name, hit_identity or hit_name, hit_url,
        )
        return False

    if name_reason == "ok-producer-subset-bypass":
        subset_ok, subset_reason = _subset_bypass_consistent(query_name, review)
        if not subset_ok:
            logger.debug(
                "DC reject[subset-bypass/%s]: query=%r hit=%r url=%s",
                subset_reason, query_name, hit_identity or hit_name, hit_url,
            )
            return False

    identity_ok, identity_reason = _identity_consistent(query_name, review)
    if not identity_ok:
        logger.debug(
            "DC reject[identity/%s]: query=%r hit=%r url=%s",
            identity_reason, query_name, hit_identity or hit_name, hit_url,
        )
        return False

    query_class = _classification_signature(query_name)
    hit_class = _classification_signature(hit_identity or hit_name)
    if query_class and hit_class and query_class != hit_class:
        logger.debug(
            "DC reject[classification]: query=%r query_class=%s hit=%r hit_class=%s url=%s",
            query_name, query_class, hit_identity or hit_name, hit_class, hit_url,
        )
        return False

    # ── Colour check ────────────────────────────────────────────────────────
    query_colour = _extract_colour_from_name_segments(query_name)
    hit_colour = (
        _extract_colour_from_text(str(review.get("colour") or ""))
        or _extract_colour_from_text(hit_identity or hit_name)
    )
    if query_colour and hit_colour and query_colour != hit_colour:
        logger.debug(
            "DC reject[colour]: query=%r query_colour=%s hit=%r hit_colour=%s url=%s",
            query_name, query_colour, hit_name, hit_colour, hit_url,
        )
        return False

    # ── Vintage check ────────────────────────────────────────────────────────
    vintage_ok = _vintage_matches(
        query_vintage, review.get("vintage_src"), hit_identity or hit_name,
        hit_url, allow_missing=(allow_missing_vintage or direct_match),
    )
    if not vintage_ok:
        logger.debug(
            "DC reject[%s-vintage]: query=%r vintage=%s hit=%r hit_vintage=%s url=%s",
            "direct" if direct_match else "search",
            query_name, query_vintage, hit_identity or hit_name,
            review.get("vintage_src"), hit_url,
        )
    return vintage_ok


def _review_identity_text(review: dict) -> str:
    """
    Build a richer Decanter identity string from the parsed detail page.

    Decanter titles sometimes omit the appellation and only show a broad
    location like "Bordeaux, France", while the facts panel still contains the
    exact appellation. Matching against the merged identity avoids rejecting
    obviously-correct results such as producer + appellation + exact vintage.
    """
    parts: list[str] = []
    for key in ("wine_name_src", "producer", "brand", "appellation", "region", "country"):
        value = _clean_text(review.get(key))
        if value and value not in parts:
            parts.append(value)
    return ", ".join(parts)


def _extract_colour_from_name_segments(name: str) -> str | None:
    parts = [p.strip() for p in str(name or "").split(",") if p.strip()]
    if len(parts) < 2:
        return None
    for part in parts:
        low = _normalize_text(part).lower().strip()
        cleaned = _normalize_text(_strip_descriptor_phrases(part)).lower().strip()
        if _STANDALONE_COLOUR_RE.match(part) or not cleaned:
            if "rouge" in low or "red" in low:
                return "red"
            if "blanc" in low or "white" in low:
                return "white"
            if "rose" in low:
                return "rose"
            if "sparkling" in low:
                return "sparkling"
    return None


def _strict_identity_tokens(text: str) -> set[str]:
    cleaned = _strip_descriptor_phrases(_normalize_text(text).lower())
    cleaned = re.sub(r"\b(?:19|20)\d{2}\b", " ", cleaned)
    tokens = [
        _normalize_token(tok)
        for tok in re.findall(r"[A-Za-z0-9]+", cleaned)
    ]
    ignore = _STOP_WORDS | {
        "cru", "classe", "class", "1er", "1ere", "1st", "2eme", "2e", "2nd",
        "3eme", "3e", "3rd", "4eme", "4e", "4th", "5eme", "5e", "5th",
    }
    return {tok for tok in tokens if len(tok) > 1 and tok not in ignore}


def _hit_primary_identity_tokens(review: dict) -> set[str]:
    producer = _clean_text(review.get("producer"))
    brand = _hit_brand_candidate(review)
    if producer or brand:
        return _strict_identity_tokens(" ".join(part for part in (producer, brand) if part))
    hit_name = str(review.get("wine_name_src") or "")
    parts = [p.strip() for p in hit_name.split(",") if p.strip()]
    return _strict_identity_tokens(" ".join(parts[:2]))


def _hit_brand_candidate(review: dict) -> str:
    brand = _clean_text(review.get("brand"))
    if brand:
        return brand
    hit_name = str(review.get("wine_name_src") or "")
    parts = [p.strip() for p in hit_name.split(",") if p.strip()]
    if len(parts) < 2:
        return ""
    second = _clean_text(parts[1])
    comparators = {
        _clean_text(review.get("appellation")).lower(),
        _clean_text(review.get("region")).lower(),
        _clean_text(review.get("country")).lower(),
    }
    second_low = second.lower()
    if second and second_low not in comparators and not _part_is_generic_descriptor(second):
        return second
    return ""


def _identity_consistent(query_name: str, review: dict) -> tuple[bool, str]:
    query_first = _clean_text(str(query_name or "").split(",")[0])
    if not query_first:
        return True, "identity-empty-query"

    query_primary = _strict_identity_tokens(query_first)
    hit_primary = _hit_primary_identity_tokens(review)
    if not query_primary or not hit_primary:
        return True, "identity-insufficient"

    query_is_named_estate = bool(re.match(
        r"(?i)^(?:ch[aâ]teau|domaine|clos|vieux\s+ch[aâ]teau)\b",
        query_first,
    ))
    query_is_known_second_label = _lookup_parent(query_first) is not None
    missing = query_primary - hit_primary
    extra = hit_primary - query_primary - {"saint"}

    if missing:
        return False, f"missing-primary={sorted(missing)}"
    if query_is_named_estate and not query_is_known_second_label and extra:
        hit_brand = _hit_brand_candidate(review)
        hit_brand_parent = _lookup_parent(hit_brand) if hit_brand else None
        if hit_brand_parent and _strict_identity_tokens(hit_brand_parent) == query_primary:
            return True, "ok-known-brand-alias"
        return False, f"extra-primary={sorted(extra)}"
    if not query_is_named_estate and not query_is_known_second_label and len(query_primary) <= 2 and extra:
        return False, f"unexpected-extra={sorted(extra)}"
    return True, "ok"


def _classification_signature(text: str) -> str:
    low = _normalize_text(text).lower()
    if not low:
        return ""
    m = re.search(r"\b(?:premier|1er|1ere|1st)\s+grand\s+cru\s+classe\s+([ab])\b", low)
    if m:
        return f"pgcc-{m.group(1)}"
    m = re.search(r"\b(?:1er|1ere|1st|2eme|2e|2nd|3eme|3e|3rd|4eme|4e|4th|5eme|5e|5th)\s+cru\s+classe\b", low)
    if m:
        return m.group(0).replace(" ", "")
    m = re.search(r"\bgrand\s+cru\s+classe\b", low)
    if m:
        return "gcc"
    return ""


def _subset_bypass_consistent(query_name: str, review: dict) -> tuple[bool, str]:
    """Allow the producer-subset shortcut only when Decanter provides no specific appellation."""
    query_tail = _query_tail_tokens(query_name)
    if not query_tail:
        return True, "no-tail"

    appellation = _clean_text(review.get("appellation"))
    if appellation:
        app_tokens = _name_tokens(appellation)
        if not app_tokens:
            return False, "appellation-unparseable"
        overlap = len(query_tail & app_tokens)
        if overlap == 0:
            return False, f"appellation-drift qt={sorted(query_tail)} app={sorted(app_tokens)}"
        return True, "appellation-overlap"

    return True, "no-appellation"


def _name_matches(query_name: str, hit_name: str) -> bool:
    ok, _ = _name_matches_with_reason(query_name, hit_name)
    return ok


def _name_matches_with_reason(query_name: str, hit_name: str) -> tuple[bool, str]:
    """
    Same logic as _name_matches but also returns a human-readable reason string
    so _matches_query can log exactly WHY a candidate was rejected.

    Key guards:
    1. forward ≥ 0.75 AND reverse ≥ 0.35  (basic overlap)
    2. Extra-word guard (named-château queries only): if hit producer has tokens
       the query doesn't (e.g. 'despagne'), reject as different château.
       Skipped for second-label brands (no Château/Domaine prefix) because
       Decanter prepends the parent name to the hit title.
    """
    _NOISE = frozenset({"bordeaux", "france", "medoc", "gironde"})

    query_tokens = _name_tokens(query_name)
    hit_tokens = _name_tokens(hit_name)

    if not query_tokens:
        return True, "ok-empty-query"
    if not hit_tokens:
        return False, "empty-hit"

    semantic_ok, semantic_reason = _semantic_name_match(query_name, hit_name)
    if semantic_ok:
        return True, semantic_reason

    overlap = len(query_tokens & hit_tokens)
    if overlap == 0:
        return False, f"no-overlap qt={sorted(query_tokens)} ht={sorted(hit_tokens)}"

    if len(query_tokens) >= 3 and overlap < 2:
        return False, f"min2-fail overlap={overlap} qt={sorted(query_tokens)} ht={sorted(hit_tokens)}"

    forward = overlap / len(query_tokens)
    reverse = overlap / len(hit_tokens)

    if forward >= 0.75 and reverse >= 0.35:
        query_first_part = str(query_name or "").split(",")[0].strip()
        query_is_named_chateau = bool(re.match(
            r"(?i)^(?:ch[aâ]teau|domaine|clos|vieux\s+ch[aâ]teau)\b",
            query_first_part,
        ))
        if query_is_named_chateau:
            q_prod = _producer_tokens(query_name)
            h_prod = _name_tokens(str(hit_name or "").split(",")[0])
            prod_extras = h_prod - q_prod - _NOISE
            if prod_extras and len(q_prod) >= 2:
                non_chateau_extras = prod_extras - {"chateau", "domaine", "clos"}
                if non_chateau_extras:
                    return False, (
                        f"extra-prod-word qprod={sorted(q_prod)} "
                        f"hprod={sorted(h_prod)} extras={sorted(non_chateau_extras)}"
                    )
        return True, "ok"

    producer_tokens = _producer_tokens(query_name)
    if producer_tokens:
        producer_ratio = len(producer_tokens & hit_tokens) / len(producer_tokens)
        if producer_ratio < 0.6 and not producer_tokens.issubset(hit_tokens):
            return False, (
                f"producer-ratio={producer_ratio:.2f} prod={sorted(producer_tokens)} "
                f"ht={sorted(hit_tokens)}"
            )

    tail_tokens = _query_tail_tokens(query_name)
    if tail_tokens:
        tail_ratio = len(tail_tokens & hit_tokens) / len(tail_tokens)
        if tail_ratio < 0.5:
            # Bypass when producer is a perfect subset of hit tokens AND forward >= 0.5.
            # Decanter sometimes shows only "Bordeaux, France" instead of the specific
            # appellation, so the tail tokens score 0. A perfect producer match with
            # forward >= 0.5 is sufficient evidence of the same wine.
            if producer_tokens and producer_tokens.issubset(hit_tokens) and forward >= 0.5:
                return True, "ok-producer-subset-bypass"
            return False, (
                f"tail-ratio={tail_ratio:.2f} tail={sorted(tail_tokens)} "
                f"ht={sorted(hit_tokens)}"
            )

    sim = _similarity(query_name, hit_name)
    if sim >= 0.82 and forward >= 0.5:
        return True, "ok-similarity"
    return False, (
        f"similarity-fail fwd={forward:.2f} rev={reverse:.2f} sim={sim:.2f} "
        f"qt={sorted(query_tokens)} ht={sorted(hit_tokens)}"
    )


def _semantic_name_match(query_name: str, hit_name: str) -> tuple[bool, str]:
    """
    Deterministic fallback for accent/sub-label differences.

    This treats normalised forms like Chateau/Château as equivalent and accepts
    extra Decanter subtitle words when the core wine identity still matches:
    producer tokens must all match, and the query tail/appellation tokens must
    also be represented in the hit.
    """
    query_tokens = _name_tokens(query_name)
    hit_tokens = _name_tokens(hit_name)
    if not query_tokens or not hit_tokens:
        return False, "semantic-empty"

    producer_tokens = _producer_tokens(query_name)
    tail_tokens = _query_tail_tokens(query_name)

    if producer_tokens and not producer_tokens.issubset(hit_tokens):
        return False, "semantic-producer-miss"
    if tail_tokens and not tail_tokens.issubset(hit_tokens):
        return False, "semantic-tail-miss"

    overlap = len(query_tokens & hit_tokens)
    if producer_tokens and overlap >= max(2, len(query_tokens) - 1):
        return True, "ok-semantic-identity"
    return False, "semantic-insufficient"


def _vintage_matches(
    query_vintage,
    hit_vintage,
    hit_name: str,
    hit_url: str | None = None,
    allow_missing: bool = False,
) -> bool:
    query_year = _extract_year(query_vintage)
    if query_year is None:
        return True
    hit_year = _extract_year(hit_vintage)
    if hit_year is None:
        hit_year = _extract_year(hit_name)
    if hit_year is None and hit_url:
        hit_year = _extract_year(hit_url)
    if hit_year is None:
        return allow_missing
    return hit_year == query_year


# ---------------------------------------------------------------------------
# Query builder — tiered, deduplicated, vintage-aware
# ---------------------------------------------------------------------------
# ROOT CAUSE FIX (v6):
# Decanter's search engine is a KEYWORD engine. 71% of manual finds used the
# FULL raw product name ("Grand Cru Classé", "5ème", "Saint-Emilion" intact).
# Our old code stripped those words BEFORE searching → engine couldn't match.
# Fix: send the raw name first, simplified variants as fallback layers.

def _build_search_queries(
    name: str,
    vintage: int | None = None,
    search_hints: dict | None = None,
) -> list[str]:
    hinted_name = _hint_wine_name(search_hints)
    base = _strip_bottle_size(_clean_text(hinted_name or name))
    if not base:
        return _hint_search_keywords(name, vintage, search_hints)

    # Strip bottle-size and embedded vintage — vintage appended cleanly below
    base_no_vintage = _strip_embedded_vintage(base)
    vintage_text = str(vintage) if vintage else ""

    queries: list[str] = []

    # Layer 0: hinted search keyword URLs
    queries.extend(_hint_search_keywords(name, vintage, search_hints))

    # Layer 0b: FIX-B — for known second-label wines, try the parent-based query FIRST
    # Decanter indexes second-label wines under the parent château name first:
    # e.g. "Château Nénin, Fugue de Nénin, Pomerol" — so searching just
    # "Fugue de Nenin, Pomerol" finds unrelated wines. The parent query finds the right page.
    parts_early = [p.strip() for p in base_no_vintage.split(",") if p.strip()]
    brand_early = parts_early[0] if parts_early else ""
    parent_early = _lookup_parent(brand_early)
    if parent_early:
        appellation_early = parts_early[-1] if len(parts_early) > 1 else ""
        appellation_early_norm = _normalise_appellation(appellation_early)
        # Parent + brand + appellation + vintage — Decanter's exact title structure
        if vintage_text:
            queries.append(f"{parent_early}, {brand_early}, {appellation_early_norm} {vintage_text}")
            queries.append(f"{parent_early} {brand_early} {vintage_text}")
        queries.append(f"{parent_early}, {brand_early}, {appellation_early_norm}")
        queries.append(f"{parent_early} {brand_early}")

    # Layer 1: RAW name — the human's strategy ─────────────────────────────
    # Send the full name as-is (only bottle stripped, vintage appended once).
    # Decanter's keyword engine indexes "Grand Cru Classé", "5ème Cru Classé",
    # "Saint-Emilion" etc. — those words must be IN the search term to match.
    # Do NOT apply Saint→St here; the engine indexes "Saint" not "St".
    raw_query = base_no_vintage
    if vintage_text and not _vintage_RE_search(raw_query, vintage_text):
        queries.append(f"{raw_query} {vintage_text}")
    else:
        queries.append(raw_query)

    # Saint ↔ St swap of the raw query as immediate variant
    saint_swap = _saint_alias_variant(raw_query)
    if saint_swap:
        q = f"{saint_swap} {vintage_text}" if vintage_text and not _vintage_RE_search(saint_swap, vintage_text) else saint_swap
        queries.append(q)

    # Layer 2: simplified name (classification stripped) + vintage
    simplified = _simplify_query_name(base_no_vintage)
    if simplified and simplified != base_no_vintage:
        for variant in _query_name_variants(simplified):
            if vintage_text and not _vintage_RE_search(variant, vintage_text):
                queries.append(f"{variant} {vintage_text}")
            queries.append(variant)

    # Layer 3: producer + normalised appellation only (no classification, no colour)
    producer_app = _producer_appellation_query(base_no_vintage)
    if producer_app:
        if vintage_text:
            queries.append(f"{producer_app} {vintage_text}")
        queries.append(producer_app)

    # Layer 4: second-label parent lookup (remaining variants not added in Layer 0b)
    parts = [p.strip() for p in base_no_vintage.split(",") if p.strip()]
    brand = parts[0] if parts else ""
    parent = _lookup_parent(brand)
    if parent:
        # parent-only query as final fallback
        queries.append(parent)

    # Layer 5: FIX 10/11 — "X de Y" parent extraction and embedded château
    for extra in _extra_parent_queries(base_no_vintage):
        if vintage_text:
            queries.append(f"{extra} {vintage_text}")
        queries.append(extra)

    # Layer 6: brand-only fallback (FIX 4)
    if brand and not re.match(r"(?i)^(?:ch[aâ]teau|domaine|clos|château)\b", brand):
        if vintage_text:
            queries.append(f"{brand} {vintage_text}")
        queries.append(brand)

    return _dedupe([q for q in queries if _clean_text(q)])


def _vintage_RE_search(text: str, vintage: str) -> bool:
    return bool(re.search(rf"\b{re.escape(vintage)}\b", text))


def _producer_appellation_query(name: str) -> str:
    """
    Return "Producer, Appellation" with classification junk and colour segments
    stripped and the appellation normalised to Decanter format.
    """
    parts = [_clean_text(p) for p in name.split(",") if _clean_text(p)]
    if not parts:
        return ""
    producer = _strip_descriptor_phrases(parts[0])
    producer = _clean_text(producer)
    if not producer:
        return ""
    if len(parts) < 2:
        return producer

    # Find the appellation — last part that is not a generic descriptor
    appellation = ""
    for part in reversed(parts[1:]):
        cleaned = _strip_descriptor_phrases(part)
        cleaned = _clean_text(cleaned)
        # Skip standalone colour qualifiers
        if _STANDALONE_COLOUR_RE.match(cleaned):
            continue
        if cleaned and not _part_is_generic_descriptor(cleaned):
            appellation = _normalise_appellation(cleaned)
            break

    return f"{producer}, {appellation}" if appellation else producer


def _extra_parent_queries(name: str) -> list[str]:
    """FIX 10 + 11: generate additional queries from 'X de Y' and embedded château."""
    extras: list[str] = []
    parts = [p.strip() for p in name.split(",")]
    first_part = parts[0]

    # FIX 10: "X de/du/des Y" — extract parent token after "de"
    de_match = re.search(r"\bde\s+(?:la\s+|le\s+|les\s+)?(.+)$", first_part, re.I)
    if de_match:
        parent_token = _clean_text(de_match.group(1))
        if parent_token and len(parent_token) > 3:
            extras.append(parent_token)
            if not re.match(r"(?i)^ch[aâ]teau\b", parent_token):
                extras.append(f"Chateau {parent_token}")

    # FIX 11: "Brand, Château X, Appellation" — extract embedded château
    for part in parts[1:]:
        if re.match(r"(?i)^ch[aâ]teau\b", part):
            ch = _clean_text(part)
            if ch:
                extras.append(ch)
                extras.append(f"{ch}, {parts[0]}")
            break

    return _dedupe(extras)


def _manual_search_urls(name: str, vintage: int | None, search_hints: dict | None) -> list[str]:
    hints = search_hints or {}
    candidates: list[str] = []
    for key in ("decanter_url", "dc_url", "decanter_search_url", "review_url"):
        value = str(hints.get(key) or "").strip()
        for part in _split_hint_values(value):
            url = _extract_search_url(part)
            if url and _search_url_hint_plausible(name, vintage, url):
                candidates.append(url)
    direct_from_name = _extract_search_url(name)
    if direct_from_name:
        candidates.append(direct_from_name)
    return _dedupe(candidates)


def _direct_detail_urls(name: str, vintage: int | None, search_hints: dict | None) -> list[dict]:
    hints = search_hints or {}
    candidates: list[dict] = []
    for key in ("decanter_url", "dc_url", "decanter_review_url", "review_url"):
        value = str(hints.get(key) or "").strip()
        for part in _split_hint_values(value):
            url = _extract_direct_url(part)
            if url and _direct_url_hint_plausible(name, vintage, url):
                candidates.append({"url": url, "allow_missing_vintage": False, "source": "hint"})
    direct_from_name = _extract_direct_url(name)
    if direct_from_name:
        candidates.append({"url": direct_from_name, "allow_missing_vintage": True, "source": "name"})

    deduped: list[dict] = []
    seen: set[str] = set()
    for item in candidates:
        url = _clean_text(item.get("url"))
        if not url or url in seen:
            continue
        seen.add(url)
        deduped.append(item)
    return deduped


def _hint_wine_name(search_hints: dict | None) -> str:
    hints = search_hints or {}
    for key in ("decanter_wine_name", "dc_wine_name", "wine_on_decanter"):
        value = _clean_text(hints.get(key))
        if value:
            return value
    return ""


def _preferred_query_name(name: str, vintage: int | None, search_hints: dict | None) -> str:
    hinted = _hint_wine_name(search_hints)
    if hinted and _hint_name_plausible(name, vintage, hinted):
        return hinted
    return _clean_text(name)


def _hint_search_keywords(name: str, vintage: int | None, search_hints: dict | None) -> list[str]:
    queries: list[str] = []
    for url in _manual_search_urls(name, vintage, search_hints):
        keyword = _search_keyword_from_url(url)
        if keyword:
            queries.append(keyword)
            queries.append(_normalize_text(keyword))
    return _dedupe(queries)


def _hint_name_plausible(query_name: str, query_vintage: int | None, hinted_name: str) -> bool:
    query = _clean_text(query_name)
    hint = _clean_text(hinted_name)
    if not hint:
        return False

    query_year = _extract_year(query_vintage)
    hint_year = _extract_year(hint)
    if query_year is not None and hint_year is not None and hint_year != query_year:
        return False

    if _name_matches(query, hint):
        return True
    semantic_ok, _ = _semantic_name_match(query, hint)
    return semantic_ok


def _hint_url_slug_text(url: str) -> str:
    parsed = urlparse(str(url or "").strip())
    parts = [unquote(part) for part in parsed.path.split("/") if part]
    if not parts:
        return ""
    slug = parts[-1]
    slug = re.sub(r"-\d{3,}$", "", slug)
    slug = slug.replace("-", " ")
    return _clean_text(slug)


def _url_hint_identity_plausible(query_name: str, query_vintage: int | None, url: str) -> bool:
    slug_text = _hint_url_slug_text(url)
    if not slug_text:
        return False

    query_year = _extract_year(query_vintage)
    url_year = _extract_year(url)
    if query_year is not None:
        if url_year is None:
            return False
        if url_year != query_year:
            return False

    if _name_matches(query_name, slug_text):
        return True
    semantic_ok, _ = _semantic_name_match(query_name, slug_text)
    return semantic_ok


def _search_url_hint_plausible(query_name: str, query_vintage: int | None, url: str) -> bool:
    keyword = _search_keyword_from_url(url)
    if not keyword:
        return False

    query_year = _extract_year(query_vintage)
    keyword_year = _extract_year(keyword)
    if query_year is not None and keyword_year is not None and keyword_year != query_year:
        return False

    if _hint_name_plausible(query_name, query_vintage, keyword):
        return True

    query_tokens = _name_tokens(query_name)
    keyword_tokens = _name_tokens(keyword)
    producer_tokens = _producer_tokens(query_name)
    if producer_tokens and not (producer_tokens & keyword_tokens):
        return False
    return bool(query_tokens & keyword_tokens)


def _direct_url_hint_plausible(query_name: str, query_vintage: int | None, url: str) -> bool:
    return _url_hint_identity_plausible(query_name, query_vintage, url)


def _search_keyword_from_url(url: str) -> str:
    parsed = urlparse(str(url or "").strip())
    parts = [unquote(part) for part in parsed.path.split("/") if part]
    if "term" in parts:
        idx = parts.index("term")
        if idx + 1 < len(parts):
            return _clean_text(parts[idx + 1].replace("-", " "))
    return ""


def _build_search_url(query: str, page: int = 1) -> str:
    # Decanter's own UI generates hyphen-slug URLs like:
    #   /search/term/chateau-trotanoy%2C-pomerol-2010/page/1/
    # Spaces become hyphens, commas are encoded as %2C.
    # Using %20 for spaces instead of hyphens may return different results.
    slug = query.lower().replace(" ", "-")
    # Encode only characters that MUST be encoded (commas, quotes, etc)
    # but preserve hyphens and alphanumerics
    slug = quote(slug, safe="-,.'")
    # Encode commas explicitly as %2C (Decanter expects this)
    slug = slug.replace(",", "%2C")
    return f"{BASE_URL}/wine-reviews/search/term/{slug}/page/{max(1, int(page))}/"


def _extract_direct_url(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    match = DETAIL_URL_RE.search(text)
    return match.group(0) if match else ""


def _extract_search_url(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    match = SEARCH_URL_RE.search(text)
    return match.group(0) if match else ""


def _extract_year(value) -> int | None:
    if value is None:
        return None
    match = YEAR_RE.search(str(value))
    return int(match.group(0)) if match else None


def _parse_score(raw) -> float | None:
    match = re.search(r"\b(\d{2,3})\b", str(raw or ""))
    if not match:
        return None
    value = float(match.group(1))
    return value if 50 <= value <= 100 else None


def _parse_colour(raw: str) -> str | None:
    text = _normalize_text(raw).lower()
    for key, value in _COLOUR_MAP.items():
        if key in text:
            return value
    return None


def _extract_colour_from_text(text: str) -> str | None:
    if not text:
        return None
    low = _normalize_text(text).lower()
    if "rose" in low:
        return "rose"
    if "white" in low or "blanc" in low:
        return "white"
    if "red" in low or "rouge" in low:
        return "red"
    if "sparkling" in low:
        return "sparkling"
    return None


def _parse_sortable_date(raw: str | None) -> tuple[int, int, int]:
    text = _clean_text(raw)
    if not text:
        return (0, 0, 0)
    for fmt in ("%d %b %Y", "%d %B %Y", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(text, fmt)
            return (dt.year, dt.month, dt.day)
        except ValueError:
            continue
    year = _extract_year(text) or 0
    return (year, 0, 0)


def _pick_latest_review(reviews: list[dict]) -> dict:
    return max(
        reviews,
        key=lambda r: (
            _parse_sortable_date(r.get("date_tasted")),
            float(r.get("score_native") or 0.0),
            _clean_text(r.get("review_url")),
        ),
    )


def _canonical_url(url: str) -> str:
    parsed = urlparse(str(url or ""))
    if not parsed.scheme:
        return str(url or "").strip()
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


# ---------------------------------------------------------------------------
# Token helpers
# ---------------------------------------------------------------------------

def _name_tokens(value: str) -> set[str]:
    text = _normalize_text(value).lower()
    text = re.sub(r"\b(?:19|20)\d{2}\b", " ", text)
    text = re.sub(r"\bst[.]?\b", " saint ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return {
        _normalize_token(tok)
        for tok in text.split()
        if len(tok) > 1
        and tok not in _STOP_WORDS
        and _normalize_token(tok) not in _GENERIC_NAME_TOKENS
    }


def _producer_tokens(value: str) -> set[str]:
    return _name_tokens(str(value or "").split(",")[0])


def _query_tail_tokens(value: str) -> set[str]:
    parts = [p.strip() for p in str(value or "").split(",") if p.strip()]
    if len(parts) <= 1:
        return set()
    tail = " ".join(parts[1:])
    return _name_tokens(tail)


def _query_lieu_dit_tokens(value: str) -> set[str]:
    parts = [p.strip() for p in str(value or "").split(",") if p.strip()]
    if len(parts) < 2:
        return set()
    tail_tokens = _name_tokens(" ".join(parts[1:]))
    if not tail_tokens:
        return set()
    producer_tokens = _producer_tokens(value)
    return {
        tok for tok in tail_tokens
        if tok not in producer_tokens
        and tok not in _LIEU_DIT_WORDS
    }


def _candidate_identity_tokens(candidate: dict) -> set[str]:
    chunks = [
        _clean_text(candidate.get("title")),
        _clean_text(candidate.get("producer")),
        _clean_text(candidate.get("brand")),
    ]
    url_text = _clean_text(candidate.get("url"))
    if url_text:
        chunks.append(url_text.replace("-", " ").replace("/", " "))
    return _name_tokens(" ".join(part for part in chunks if part))


def _cru_tier_signature(value: str) -> str:
    low = _normalize_text(value).lower()
    if not low:
        return ""
    has_grand = bool(re.search(r"\bgrand\s+cru\b", low))
    has_premier = bool(re.search(r"\b(?:premier|1er|1ere|1st)\s+cru\b", low))
    if has_grand and not has_premier:
        return "grand-cru"
    if has_premier:
        return "premier-cru"
    return ""


def _normalize_text(value) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    return "".join(ch for ch in text if not unicodedata.combining(ch))


def _clean_text(value) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _normalize_token(token: str) -> str:
    tok = str(token or "").strip(". ").lower()
    if tok in {"st", "saint", "sainte"}:
        return "saint"
    return tok


# ---------------------------------------------------------------------------
# Name simplification (classification stripping)
# ---------------------------------------------------------------------------

def _query_name_variants(name: str) -> list[str]:
    base = _clean_text(name)
    if not base:
        return []

    variants: list[str] = [base, re.sub(r"\s*,\s*", " ", base)]
    simplified = _simplify_query_name(base)
    if simplified and simplified != base:
        variants.extend([simplified, re.sub(r"\s*,\s*", " ", simplified)])

    producer, tail = _split_name_parts(simplified or base)
    if producer:
        variants.append(producer)
        if tail:
            variants.append(f"{producer}, {tail}")
            variants.append(f"{producer} {tail}")

    # FIX 4: brand-only fallback
    if not re.match(r"(?i)^(?:ch[aâ]teau|domaine|clos|château)\b", base):
        brand_only = _clean_text(base.split(",")[0])
        if brand_only and brand_only != base:
            variants.append(brand_only)

    out: list[str] = []
    for variant in variants:
        cleaned = _clean_text(variant)
        if not cleaned:
            continue
        out.append(cleaned)
        saint_variant = _saint_alias_variant(cleaned)
        if saint_variant and saint_variant != cleaned:
            out.append(saint_variant)
    return _dedupe(out)


def _split_name_parts(name: str) -> tuple[str, str]:
    parts = [_clean_text(p) for p in str(name or "").split(",") if _clean_text(p)]
    if not parts:
        return "", ""
    return parts[0], ", ".join(parts[1:])


def _simplify_query_name(name: str) -> str:
    """
    Strip classification noise (Cru Classé, Grand Cru, etc.) from each segment.
    Colour qualifier segments (standalone 'Rouge'/'Blanc') are dropped.
    Appellation segments are kept AS-IS for the simplified query — Saint→St
    substitution happens in _name_matches (for comparison), NOT in search queries,
    because Decanter's search engine indexes "Saint" not "St".
    """
    parts = [_clean_text(p) for p in str(name or "").split(",") if _clean_text(p)]
    if not parts:
        return ""

    cleaned_parts: list[str] = []
    for idx, part in enumerate(parts):
        cleaned = _strip_descriptor_phrases(part)
        cleaned = _clean_text(cleaned)
        if not cleaned or _part_is_generic_descriptor(cleaned):
            continue
        if idx > 0:
            cleaned = re.sub(r"\bgrand\s+cru\b\s*$", "", cleaned, flags=re.I)
            cleaned = _clean_text(cleaned)
            # FIX 9: drop standalone colour qualifier segments
            if re.match(r"(?i)^(?:rouge|blanc|red|white|rosé?)$", cleaned):
                continue
            # NOTE: do NOT call _normalise_appellation here — Saint→St in queries
            # breaks Decanter's keyword engine which indexes "Saint" not "St".
            # _normalise_appellation is only used in _name_matches for comparison.
        if cleaned:
            cleaned_parts.append(cleaned)
    return ", ".join(cleaned_parts)


def _strip_descriptor_phrases(text: str) -> str:
    value = str(text or "")
    patterns = [
        r"\b(?:premier|1er|1ere|1st)\s+grand\s+cru\s+classe\s+[ab]\b",
        r"\b(?:premier|1er|1ere|1st)\s+grand\s+cru\s+classe\b",
        r"\bgrand\s+cru\s+classe\b",
        r"\b(?:rouge|blanc)\s+cru\s+classe\b",    # FIX 7
        r"\b(?:1er|1ere|1st|2eme|2e|2nd|3eme|3e|3rd|4eme|4e|4th|5eme|5e|5th)\s+cru\s+classe\b",
        r"\bcru\s+classe\b",
        r"\b(?:1er|1ere|1st|2eme|2e|2nd|3eme|3e|3rd|4eme|4e|4th|5eme|5e|5th)\b",
        # FIX-H: "Grand Cru" appellation suffix in Saint-Emilion context
        r"\bgrand\s+cru\b(?!\s+classe)",
    ]
    for pattern in patterns:
        value = re.sub(pattern, " ", value, flags=re.I)
    # FIX 3: only strip bare "premier" when NOT followed by "(grand )cru"
    # This catches "Premier Cru Classé" → stripped correctly, leaving no dangling "Premier"
    value = re.sub(r"\bpremier\b(?!\s+(?:grand\s+)?cru)", " ", value, flags=re.I)
    # Clean up double commas left by stripping middle segments
    value = re.sub(r",\s*,", ",", value)
    return re.sub(r"\s+", " ", value).strip(" ,")


def _part_is_generic_descriptor(text: str) -> bool:
    tokens = [
        _normalize_token(tok)
        for tok in re.findall(r"[A-Za-z0-9]+", _normalize_text(text).lower())
    ]
    meaningful = [tok for tok in tokens if tok not in _STOP_WORDS]
    return bool(meaningful) and all(tok in _GENERIC_NAME_TOKENS for tok in meaningful)


def _saint_alias_variant(text: str) -> str:
    if re.search(r"\bsaint\b", text, flags=re.I):
        return re.sub(r"\bsaint\b", "St", text, flags=re.I)
    if re.search(r"\bst\.?(?=\s)", text, flags=re.I):
        return re.sub(r"\bst\.?(?=\s)", "Saint", text, flags=re.I)
    return ""


def _similarity(a: str, b: str) -> float:
    from difflib import SequenceMatcher
    return SequenceMatcher(
        None,
        _clean_text(_normalize_text(a)).lower(),
        _clean_text(_normalize_text(b)).lower(),
    ).ratio()


# ---------------------------------------------------------------------------
# Candidate sorting / deduplication
# ---------------------------------------------------------------------------

def _sort_candidates(candidates: list[dict], query_vintage: int | None) -> list[dict]:
    return sorted(
        candidates,
        key=lambda c: (
            _candidate_vintage_bucket(query_vintage, c),
            -float(c.get("rank_score") or 0.0),
        ),
    )


def _candidate_review_pool(
    candidates: list[dict],
    query_vintage: int | None,
    max_candidates: int | None = None,
) -> list[dict]:
    """
    Select the ranked candidates whose detail pages we will actually fetch.

    Decanter can return the correct wine a bit deeper in the ranked set even when
    early candidates are same-appellation neighbors. For vintage searches we scan
    a wider pool before giving up, while still preferring exact-vintage hits.
    """
    sorted_candidates = _sort_candidates(candidates, query_vintage)
    if not sorted_candidates:
        return []

    if max_candidates is None:
        max_candidates = 12 if _extract_year(query_vintage) is not None else 8

    return sorted_candidates[: max(1, int(max_candidates))]


def _candidate_vintage_bucket(query_vintage: int | None, candidate: dict) -> int:
    query_year = _extract_year(query_vintage)
    if query_year is None:
        return 0
    hit_year = _extract_year(candidate.get("vintage"))
    if hit_year is None:
        hit_year = _extract_year(candidate.get("title"))
    if hit_year is None:
        hit_year = _extract_year(candidate.get("url"))
    if hit_year == query_year:
        return 0
    if hit_year is None:
        return 1
    return 2


def _dedupe_search_candidates(candidates: list[dict]) -> list[dict]:
    best_by_url: dict[str, dict] = {}
    for candidate in candidates:
        url = _clean_text(candidate.get("url"))
        if not url:
            continue
        current = best_by_url.get(url)
        if current is None or float(candidate.get("rank_score") or 0.0) > float(
            current.get("rank_score") or 0.0
        ):
            best_by_url[url] = candidate
    return list(best_by_url.values())


def _split_hint_values(value: str) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    return [part.strip() for part in re.split(r"[\n\r,;]+", text) if part.strip()]


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        key = value.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def _debug_write_tmp(filename: str, text: str) -> None:
    try:
        TMP_DIR.mkdir(parents=True, exist_ok=True)
        (TMP_DIR / filename).write_text(text, encoding="utf-8", errors="ignore")
    except Exception:
        pass
