"""
sources/jamessuckling.py
========================
James Suckling scraper.

This source keeps search resolution separate from detail parsing:
  - search helpers only resolve candidate tasting-note URLs/IDs
  - detail helpers fetch and parse the tasting-note page

Search strategy:
  - trust explicit tasting-note URLs or IDs when supplied
  - accept manual /search-result?keyword=... URLs from search_hints
  - use the site's own GraphQL SearchWineRatings query for ranked candidates
  - fall back to parsing search-result HTML only if the GraphQL path fails
"""

from __future__ import annotations

import difflib
import json
import logging
import re
import time
import unicodedata
from pathlib import Path
from urllib.parse import parse_qs, quote, unquote, urljoin, urlparse

from bs4 import BeautifulSoup

try:
    from curl_cffi import requests
    _CFFI = True
except ImportError:
    import requests
    _CFFI = False

logger = logging.getLogger("maaike.js")

COOKIE_FILE = "cookies/jamessuckling.json"
BASE_URL = "https://www.jamessuckling.com"
GRAPHQL_URL = f"{BASE_URL}/graphql"
TMP_DIR = Path("tmp")
DETAIL_URL_RE = re.compile(
    r"https?://(?:www\.)?jamessuckling\.com/tasting-notes/\d+[^\s]*", re.I
)
SEARCH_RESULT_URL_RE = re.compile(
    r"https?://(?:www\.)?jamessuckling\.com/search-result\?[^\s#]*\bkeyword=[^\s#]+", re.I
)
YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")

SEARCH_WINE_RATINGS_QUERY = """
query SearchWineRatings(
  $keyword: String!,
  $offset: Int!,
  $limit: Int!,
  $scoreFrom: Int!,
  $scoreTo: Int!,
  $priceFrom: Int!,
  $priceTo: Int!,
  $country: String!,
  $region: String!,
  $vintage: String!,
  $color: String!,
  $orderBy: AdvancedSearchOrderBy!,
  $order: AdvancedSearchOrder!,
  $uln: String,
  $isExpandSearch: Boolean,
  $isFilter: Boolean
) {
  search(
    keyword: $keyword,
    offset: $offset,
    order: $order,
    orderBy: $orderBy,
    limit: $limit,
    type: TASTING_NOTE,
    scoreFrom: $scoreFrom,
    scoreTo: $scoreTo,
    priceFrom: $priceFrom,
    priceTo: $priceTo,
    country: $country,
    region: $region,
    vintage: $vintage,
    color: $color,
    uln: $uln,
    isExpandSearch: $isExpandSearch,
    isFilter: $isFilter
  ) {
    results {
      country
      excerpt
      checkPriceUrl
      score
      region
      postedDate
      searchResultUrl
      title
      vintage
      color
      averagePrice {
        currency
        price
      }
      wineGalaxyUrl
    }
    count
    suggestions
    isExpandSearch
  }
}
""".strip()

# ---------------------------------------------------------------------------
# Colour detection
# ---------------------------------------------------------------------------
# Maps raw colour words found in a product name or URL slug to a canonical form.
#   "rouge" / "red"           → "rouge"
#   "blanc" / "white"         → "blanc"
#   "rose" / "rosé" / "rosado"→ "rose"
#
# If the query declares rouge and the page is blanc (or vice versa) the
# candidate is rejected — this fixes the 13 colour-mismatch errors in v13.

_COLOUR_CANON: dict[str, str] = {
    "rouge": "rouge", "red": "rouge",
    "blanc": "blanc", "white": "blanc", "blanche": "blanc",
    "rose": "rose", "rosé": "rose", "rosado": "rose",
}

# Appellation / style fragments that contain a colour-like word but are NOT
# a wine-colour signal — exempted from colour detection.
_COLOUR_EXEMPT_FRAGMENTS = frozenset({
    "bordeaux-blanc",    # AOC name, not wine colour
    "blanc-de-blancs",   # Champagne style
    "blanc-de-noirs",
    "gran-blanc",
})


def _extract_colour_from_text(text: str) -> str | None:
    """
    Return the canonical colour ('rouge'|'blanc'|'rose') if unambiguously
    present in *text* (a wine name or URL slug), else None.
    Exempt appellation fragments are masked first.
    """
    t = _normalize_text(text).lower()
    for frag in _COLOUR_EXEMPT_FRAGMENTS:
        t = t.replace(frag, " ")
    for word, canon in _COLOUR_CANON.items():
        if re.search(rf"\b{re.escape(word)}\b", t):
            return canon
    return None


def _colours_conflict(colour_a: str | None, colour_b: str | None) -> bool:
    """True when both colours are known and they disagree."""
    return colour_a is not None and colour_b is not None and colour_a != colour_b


# ---------------------------------------------------------------------------
# Conflict pairs
# ---------------------------------------------------------------------------
# Each tuple (frag_a, frag_b) means: a slug containing frag_a and a slug
# containing frag_b represent DIFFERENT wines — they must never match each
# other, even when they share many name tokens.
#
# Covers four scenarios:
#   1. Second wine queried → parent château URL returned (and vice versa)
#   2. Two different second wines / cuvées of the same parent confused together
#   3. Unrelated châteaux sharing a name fragment (Gazin / Lafleur-Gazin)
#   4. Same-producer cuvées that are different products (Baron/Margaux de Brane)

_CONFLICT_PAIRS: list[tuple[str, str]] = [
    # ── second wine ↔ parent ──────────────────────────────────────────────
    ("pagodes-de-cos",               "cos-d-estournel"),
    ("les-pagodes-de-cos",           "cos-d-estournel"),
    ("haut-bailly-ii",               "haut-bailly"),
    ("la-parde-de-haut-bailly",      "haut-bailly"),
    # Two historical second-wine labels of the same château — not interchangeable:
    ("haut-bailly-ii",               "la-parde-de-haut-bailly"),
    ("duluc-de-branaire",            "chateau-branaire-ducru"),
    ("verso",                        "haut-batailley"),
    ("le-pauillac-de-chateau-latour","chateau-latour"),
    ("les-forts-de-latour",          "chateau-latour"),
    ("carruades-de-lafite",          "lafite-rothschild"),
    ("pavillon-rouge",               "chateau-margaux"),
    ("le-clarence-de-haut-brion",    "chateau-haut-brion"),
    ("la-chapelle-de-la-mission",    "chateau-la-mission-haut-brion"),
    ("alter-ego",                    "chateau-palmer"),
    ("echo-de-lynch-bages",          "chateau-lynch-bages"),
    ("blanc-de-lynch-bages",         "chateau-lynch-bages"),
    # Two distinct cuvées of Lynch-Bages:
    ("blanc-de-lynch-bages",         "echo-de-lynch-bages"),
    ("brio-de-cantenac-brown",       "chateau-cantenac-brown"),
    ("les-allees-de-cantemerle",     "chateau-cantemerle"),
    ("les-hauts-de-larrivet",        "chateau-larrivet-haut-brion"),
    ("la-chapelle-de-bages",         "haut-bages-liberal"),
    ("le-seuil-de-mazeyres",         "chateau-mazeyres"),
    ("fugue-de-nenin",               "chateau-nenin"),
    ("la-petite-eglise",             "chateau-l-eglise-clinet"),
    ("les-pensees-de-la-tour-carnet","chateau-la-tour-carnet"),
    ("9-de-marquis-de-terme",        "chateau-marquis-de-terme"),
    ("comte-de-dauzac",              "chateau-dauzac"),
    ("n2-de-maucaillou",             "chateau-maucaillou"),
    ("madame-de-beaucaillou",        "chateau-ducru-beaucaillou"),
    ("la-dame-de-montrose",          "chateau-montrose"),
    ("les-griffons-de-pichon-baron", "pichon-baron"),
    ("lacoste-borie",                "grand-puy-lacoste"),
    ("moulin-de-duhart",             "duhart-milon"),
    ("les-fiefs-de-lagrange",        "chateau-lagrange"),
    ("chapelle-de-potensac",         "chateau-potensac"),
    ("clos-du-marquis",              "chateau-leoville-las-cases"),
    ("fleur-de-pedesclaux",          "chateau-pedesclaux"),
    ("pastourelle-de-clerc-milon",   "chateau-clerc-milon"),
    ("le-petit-mouton",              "mouton-rothschild"),
    ("le-petit-cheval",              "chateau-cheval-blanc"),
    ("pensees-de-lafleur",           "chateau-lafleur"),
    ("benjamin-de-beauregard",       "chateau-beauregard"),
    ("reserve-de-la-comtesse",       "pichon-lalande"),
    ("baron-de-brane",               "chateau-brane-cantenac"),
    ("margaux-de-brane",             "chateau-brane-cantenac"),
    # Two distinct cuvées — not interchangeable:
    ("baron-de-brane",               "margaux-de-brane"),
    ("confidences-de-prieure-lichine","chateau-prieure-lichine"),
    ("la-demoiselle-de-sociando",    "chateau-sociando-mallet"),
    ("prelude-a-grand-puy-ducasse",  "chateau-grand-puy-ducasse"),
    ("lions-de-suduiraut",           "chateau-suduiraut"),
    ("carmes-de-rieussec",           "chateau-rieussec"),
    ("g-d-estournel",                "cos-d-estournel"),
    ("goulee-by-cos",                "cos-d-estournel"),
    ("le-dauphin-d-olivier",         "chateau-olivier"),
    ("l-esprit-de-chevalier",        "domaine-de-chevalier"),
    ("le-clementin-de-pape-clement", "chateau-pape-clement"),
    ("le-comte-de-malartic",         "chateau-malartic-lagraviere"),
    ("clos-plince",                  "chateau-plince"),
    ("les-hauts-du-tertre",          "chateau-du-tertre"),
    ("blason-d-issan",               "chateau-d-issan"),
    ("haut-medoc-giscours",          "chateau-giscours"),
    ("le-haut-medoc-de-giscours",    "chateau-giscours"),
    ("les-tours-de-laroque",         "chateau-laroque"),
    ("les-angelots-de-villemaurine", "chateau-villemaurine"),
    ("le-merle-de-peby-faugeres",    "chateau-peby-faugeres"),
    ("croix-de-beausejour",          "chateau-beausejour"),
    ("les-pelerins-de-lafon-rochet", "chateau-lafon-rochet"),
    ("le-marquis-de-calon-segur",    "chateau-calon-segur"),
    ("oratoire-de-chasse-spleen",    "chateau-chasse-spleen"),
    ("connetable-talbot",            "chateau-talbot"),
    ("caillou-blanc",                "chateau-talbot"),
    ("le-petit-smith-haut-lafitte",  "chateau-smith-haut-lafitte"),
    ("seigneurs-d-aiguilhe",         "chateau-d-aiguilhe"),
    ("le-blanc-d-aiguilhe",          "chateau-d-aiguilhe"),
    ("stella-solare",                "chateau-croix-de-labrie"),
    ("symphonie-de-haut-peyraguey",  "chateau-haut-peyraguey"),
    ("l-abeille-de-fieuzal",         "chateau-de-fieuzal"),

    # ── unrelated châteaux sharing a name fragment ────────────────────────
    # Pomerol "Clinet" family — all distinct properties:
    ("chateau-clinet",               "chateau-l-eglise-clinet"),
    ("chateau-clinet",               "chateau-feytit-clinet"),
    ("chateau-l-eglise-clinet",      "chateau-feytit-clinet"),
    # Lafleur family in Pomerol:
    ("chateau-lafleur",              "chateau-lafleur-gazin"),
    ("chateau-lafleur",              "chateau-la-fleur-petrus"),
    ("chateau-lafleur-gazin",        "chateau-la-fleur-petrus"),
    # Gazin is completely separate from Lafleur-Gazin:
    ("chateau-gazin",                "chateau-lafleur-gazin"),
    # Moulin variants in Moulis / Médoc:
    ("chateau-moulin-a-vent",        "chateau-moulin-de-st-vincent"),
    ("chateau-moulin-a-vent",        "chateau-moulin-riche"),
    # Ormes de Pez vs Château de Pez:
    ("chateau-ormes-de-pez",         "chateau-de-pez"),
    # Beausejour variants (Saint-Émilion vs Pomerol):
    ("chateau-petit-beausejour",     "chateau-beausejour"),
    # Feytit-Clinet vs Feytit-Guillot:
    ("chateau-feytit-clinet",        "chateau-feytit-guillot"),
    # Haut-Mayne vs Haut-Cantiroy (Graves):
    ("chateau-haut-mayne",           "chateau-haut-cantiroy"),
    # Tayet vs Argadens (Bordeaux Supérieur):
    ("chateau-tayet",                "chateau-argadens"),
    # Parenchère vs Cazenove:
    ("chateau-de-parenchere",        "chateau-de-cazenove"),
    # Vieux Chevrol vs La Fleur Chevrol:
    ("chateau-vieux-chevrol",        "chateau-la-fleur-chevrol"),
    # Branon vs Haut-Lagrange:
    ("chateau-branon",               "chateau-haut-lagrange"),
    # Poujeaux vs its name-alike neighbours:
    ("chateau-poujeaux",             "chateau-granins-grand-poujeaux"),
    ("chateau-poujeaux",             "chateau-branas-grand-poujeaux"),
    # Haut-Ballet vs Haut-Francarney:
    ("chateau-haut-ballet",          "chateau-haut-francarney"),
    # Domaine Allary Haut-Brion is a separate producer, not Château Haut-Brion:
    ("domaine-allary-haut-brion",    "chateau-haut-brion"),
    # Meyney vs Cos d'Estournel (same commune, very different wines):
    ("chateau-meyney",               "chateau-cos-d-estournel"),
]

# Appellation abbreviations used in JS URL slugs (saint → st, etc.)
_APPELLATION_SUBS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\bsaint\b", re.I), "st"),
    (re.compile(r"\bsainte\b", re.I), "ste"),
]

# Generic stop-tokens excluded from name-token sets
_NAME_STOP_WORDS = frozenset({
    "a", "an", "and", "the", "wine", "vin", "de", "du", "des", "le", "la",
    "les", "d", "en", "sur", "et",
})


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------

def load_session(cookie_file: str = COOKIE_FILE):
    path = Path(cookie_file)
    if not path.exists():
        raise FileNotFoundError(f"Cookie file not found: {cookie_file}")

    cookies = json.loads(path.read_text(encoding="utf-8"))
    session = requests.Session(impersonate="chrome110") if _CFFI else requests.Session()

    for c in cookies:
        session.cookies.set(
            c["name"],
            c["value"],
            domain=c.get("domain", "www.jamessuckling.com"),
            path=c.get("path", "/"),
        )

    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": f"{BASE_URL}/",
    })
    return session


def check_session(session) -> dict:
    cookie_names = {c.name for c in session.cookies}
    return {
        "source": "jamessuckling",
        "has_session_cookie": "__Secure-next-auth.session-token" in cookie_names,
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
    sleep_sec = max(1.0, min(float(sleep_sec or 1.0), 360.0))
    matched_reviews: list[dict] = []
    candidate_reviews: list[dict] = []

    for url in _direct_detail_urls(name, search_hints):
        review = _fetch_review_from_url(session, url)
        if review and _matches_query(name, vintage, review):
            logger.info("JS direct detail accepted: %s", url)
            matched_reviews.append(review)
        elif review:
            logger.info(
                "JS direct detail rejected by guards: query=%r url=%r hit=%r",
                name, url, review.get("wine_name_src"),
            )

    if matched_reviews:
        return [_pick_latest_review(matched_reviews)]

    for candidate in _search_result_candidates(
        session, name, vintage, search_hints=search_hints
    )[:5]:
        time.sleep(sleep_sec * 0.2)
        url = candidate.get("url") or ""
        review = _fetch_review_from_url(session, url)
        if review and _matches_query(name, vintage, review):
            logger.info("JS search-result candidate accepted: %s", url)
            candidate_reviews.append(review)
        elif review:
            logger.info(
                "JS search-result candidate rejected by guards: query=%r url=%r hit=%r",
                name, url, review.get("wine_name_src"),
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
    candidates = _direct_detail_urls(name, search_hints)
    search_candidates = _search_result_candidates(
        session, name, vintage, search_hints=search_hints
    )
    if not candidates and not search_candidates:
        return (
            "James Suckling search returned no tasting-note matches for that wine query."
        )

    for url in candidates:
        html = _fetch_tasting_note_page(session, url)
        if not html:
            continue

        payload = _extract_payload(html)
        if not payload:
            return (
                "James Suckling returned a page, but Maaike could not parse "
                "a tasting-note payload from it."
            )

        page_name = str(payload.get("appellationName") or "").strip()
        page_vintage = _extract_year(payload.get("vintageYear")) or _extract_year(page_name)
        score = _parse_score(payload.get("score"))
        note = str(payload.get("note") or "").strip()
        note_is_truncated = note.endswith("...")

        if vintage is not None and page_vintage is not None and page_vintage != vintage:
            return (
                f"JS URL/ID points to {page_name or 'a different wine'} "
                f"({page_vintage}), but your test search is using vintage {vintage}."
            )

        if score is None and (not note or note_is_truncated):
            return (
                "James Suckling accepted the URL/ID, but the page only returned "
                "teaser or locked content. Your cookie file is present, but this "
                "session does not have full access to the tasting note."
            )

        if page_name and name and not _name_matches(name, page_name):
            return (
                f"JS URL/ID resolved to '{page_name}', which does not match "
                "the wine name you entered."
            )

    return None


# ---------------------------------------------------------------------------
# Internal search helpers
# ---------------------------------------------------------------------------

def _search_candidates(
    name: str, vintage: int | None, search_hints: dict | None
) -> list[str]:
    hints = search_hints or {}
    candidates: list[str] = []

    for key in ("jamessuckling_url", "js_review_url", "js_url", "review_url"):
        value = str(hints.get(key) or "").strip()
        for part in _split_hint_values(value):
            if "jamessuckling.com/tasting-notes/" in part:
                candidates.append(part)

    for key in ("jamessuckling_id", "js_tasting_note_id", "tasting_note_id"):
        raw = str(hints.get(key) or "").strip()
        for part in _split_hint_values(raw):
            if part.isdigit():
                candidates.append(f"{BASE_URL}/tasting-notes/{part}")

    direct_from_name = _extract_direct_url(name)
    if direct_from_name:
        candidates.append(direct_from_name)

    return _dedupe(candidates)


def _search_result_candidates(
    session,
    name: str,
    vintage: int | None = None,
    search_hints: dict | None = None,
) -> list[dict]:
    search_targets = _build_search_targets(name, vintage, search_hints)
    ordered_candidates: list[dict] = []

    for target_index, target in enumerate(search_targets, start=1):
        query = target.get("query") or ""
        search_url = target.get("url") or ""
        query_candidates = 0

        parsed_candidates = _fetch_search_result_graphql(session, query, vintage)
        if not parsed_candidates:
            html = _fetch_search_result_page(session, search_url)
            if not html:
                continue
            parsed_candidates = _parse_search_result_candidates(html)

        for position, candidate in enumerate(parsed_candidates, start=1):
            title = candidate.get("title") or ""
            date_text = candidate.get("date_text") or ""
            full_url = candidate.get("url") or ""
            ordered_candidates.append({
                **candidate,
                "query": query,
                "search_url": search_url,
                "query_index": target_index,
                "position": position,
                "rank_score": _search_candidate_rank(
                    name, vintage, candidate, target_index, position
                ),
            })
            query_candidates += 1
            logger.info(
                "JS search-result card #%s query=%r title=%r date=%r url=%s",
                position, query, title, date_text, full_url,
            )

        logger.info(
            "JS search-result query=%r yielded %s matched card(s)", query, query_candidates
        )
        if query_candidates:
            break

    candidates = _dedupe_search_candidates(ordered_candidates)
    candidates.sort(
        key=lambda c: (
            float(c.get("rank_score") or 0.0),
            -int(c.get("query_index") or 0),
            -int(c.get("position") or 0),
            _parse_sortable_date(c.get("date_text")),
            _clean_text(c.get("url")),
        ),
        reverse=True,
    )

    if candidates:
        logger.info(
            "JS search-result selected %s candidate URL(s) for query=%r",
            len(candidates), name,
        )
    else:
        logger.info("JS search-result found no candidate URLs for query=%r", name)
    return candidates


def _search_result_urls(
    session,
    name: str,
    vintage: int | None = None,
    search_hints: dict | None = None,
) -> list[str]:
    return [
        c.get("url")
        for c in _search_result_candidates(
            session, name, vintage, search_hints=search_hints
        )
        if c.get("url")
    ]


def _fetch_search_result_page(session, url: str) -> str:
    try:
        response = session.get(url, timeout=20, allow_redirects=True)
    except Exception as exc:
        logger.warning("JS search request failed %r: %s", url, exc)
        return ""
    if not response.ok:
        logger.warning("JS search HTTP %s for %r", response.status_code, url)
        return ""
    _debug_write_tmp("js_search_result.html", response.text)
    return response.text


def _fetch_search_result_graphql(
    session,
    query: str,
    vintage: int | None = None,
    limit: int = 10,
) -> list[dict]:
    payload = {
        "query": SEARCH_WINE_RATINGS_QUERY,
        "variables": {
            "keyword": _clean_text(query),
            "offset": 0,
            "limit": max(1, int(limit)),
            "scoreFrom": 65,
            "scoreTo": 100,
            "priceFrom": 0,
            "priceTo": 1050,
            "country": "",
            "region": "",
            "vintage": "",
            "color": "",
            "orderBy": "BEST_MATCH",
            "order": "DESC",
            "uln": "Y",
            "isExpandSearch": False,
            "isFilter": False,
        },
    }

    try:
        response = session.post(GRAPHQL_URL, json=payload, timeout=20)
    except Exception as exc:
        logger.warning("JS graphql search failed %r: %s", query, exc)
        return []
    if not response.ok:
        logger.warning("JS graphql HTTP %s for %r", response.status_code, query)
        return []

    try:
        data = response.json()
    except ValueError as exc:
        logger.warning("JS graphql JSON parse failed for %r: %s", query, exc)
        return []

    if data.get("errors"):
        logger.warning(
            "JS graphql returned errors for %r: %s", query, data.get("errors")
        )
        return []

    search = (data.get("data") or {}).get("search") or {}
    results = search.get("results") or []
    candidates: list[dict] = []
    for result in results:
        url = urljoin(BASE_URL, _clean_text(result.get("searchResultUrl")))
        if not url:
            continue
        candidates.append({
            "url": url,
            "title": _clean_text(result.get("title")),
            "date_text": _clean_text(result.get("postedDate")),
            "vintage": (
                _extract_year(result.get("vintage"))
                or _extract_year(result.get("title"))
            ),
            "excerpt": _clean_text(result.get("excerpt")),
        })
    return candidates


def _direct_detail_urls(name: str, search_hints: dict | None) -> list[str]:
    return _search_candidates(name, None, search_hints)


def _build_search_targets(
    name: str,
    vintage: int | None = None,
    search_hints: dict | None = None,
) -> list[dict]:
    targets: list[dict] = []

    for url in _manual_search_result_urls(name, search_hints):
        targets.append({
            "url": url,
            "query": _search_keyword_from_url(url) or _clean_text(name),
        })

    for query in _build_search_queries(name, vintage, search_hints=search_hints):
        targets.append({
            "url": f"{BASE_URL}/search-result?keyword={quote(query)}",
            "query": query,
        })

    deduped: list[dict] = []
    seen_urls: set[str] = set()
    for target in targets:
        url = _clean_text(target.get("url"))
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        deduped.append({"url": url, "query": _clean_text(target.get("query"))})
    return deduped


def _parse_search_result_candidates(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    candidates: list[dict] = []
    for anchor in soup.select('a[href^="/tasting-notes/"]'):
        href = _clean_text(anchor.get("href"))
        if not href:
            continue
        title_node = anchor.select_one("p.text-lg") or anchor.select_one("p")
        title = _clean_text(
            title_node.get_text(" ", strip=True) if title_node else ""
        )
        date_node = anchor.select_one("div.text-gray-400")
        date_text = _clean_text(
            date_node.get_text(" ", strip=True) if date_node else ""
        )
        candidates.append({
            "url": urljoin(BASE_URL, href),
            "title": title,
            "date_text": date_text,
            "vintage": _extract_year(title),
        })
    return candidates


def _search_candidate_rank(
    query_name: str,
    query_vintage: int | None,
    candidate: dict,
    query_index: int,
    position: int,
) -> float:
    title = str(candidate.get("title") or "")
    url = str(candidate.get("url") or "")
    query_norm = _clean_text(_normalize_text(query_name)).lower()
    title_norm = _clean_text(_normalize_text(title)).lower()
    query_tokens = _name_tokens(query_name)
    title_tokens = _name_tokens(title)
    producer_tokens = _producer_tokens(query_name)

    score = 0.0

    # Exact / prefix title match
    if title_norm and query_norm:
        if title_norm == query_norm:
            score += 12.0
        elif title_norm.startswith(query_norm):
            score += 8.0

    # Bidirectional token overlap
    colour_words = frozenset(_COLOUR_CANON)
    qt = query_tokens - colour_words
    tt = title_tokens - colour_words
    if qt and tt:
        overlap = len(qt & tt)
        forward_ratio = overlap / len(qt)
        reverse_ratio = overlap / len(tt)
        score += 6.0 * forward_ratio
        if reverse_ratio < 0.5:
            score -= 2.0

    if producer_tokens and title_tokens:
        score += 4.0 * (
            len((producer_tokens - colour_words) & title_tokens)
            / max(len(producer_tokens - colour_words), 1)
        )

    if _name_matches(query_name, title):
        score += 6.0

    # Conflict pair — heavy penalty before any page fetch
    query_slug = _slugify(query_name)
    hit_slug = _extract_url_slug(url)
    if _slugs_conflict(query_slug, hit_slug):
        score -= 15.0
        logger.debug(
            "JS rank penalty: conflict pair query=%r hit_url=%r", query_name, url
        )

    # Colour mismatch between query name and candidate title / URL slug
    query_colour = _extract_colour_from_text(query_name)
    hit_colour = (
        _extract_colour_from_text(title)
        or _extract_colour_from_text(hit_slug)
    )
    if _colours_conflict(query_colour, hit_colour):
        score -= 10.0
        logger.debug(
            "JS rank penalty: colour mismatch query_colour=%r hit_colour=%r url=%r",
            query_colour, hit_colour, url,
        )

    if query_vintage is not None:
        if _vintage_matches(query_vintage, candidate.get("vintage"), title):
            score += 5.0
        else:
            score -= 6.0

    score += max(0.0, 2.5 - (position - 1) * 0.25)
    score += max(0.0, 1.5 - (query_index - 1) * 0.25)
    score += _parse_sortable_date(candidate.get("date_text"))[0] * 0.001
    return score


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


# ---------------------------------------------------------------------------
# Detail page fetching / parsing
# ---------------------------------------------------------------------------

def _fetch_review_from_url(session, url: str) -> dict | None:
    html = _fetch_tasting_note_page(session, url)
    if not html:
        return None
    return _parse_tasting_note_page(html, url)


def _fetch_tasting_note_page(session, url: str) -> str:
    try:
        response = session.get(url, timeout=20, allow_redirects=True)
    except Exception as exc:
        logger.warning("JS detail request failed %r: %s", url, exc)
        return ""
    if not response.ok:
        logger.warning("JS detail HTTP %s for %r", response.status_code, url)
        return ""
    if "/404" in response.url:
        logger.info("JS detail not found for %r", url)
        return ""
    return response.text


def _parse_tasting_note_page(html: str, url: str) -> dict | None:
    payload = _extract_payload(html)
    if not payload:
        return None

    name = str(payload.get("appellationName") or "").strip()
    score = _parse_score(payload.get("score"))
    note = str(payload.get("note") or "").strip()
    vintage = _extract_year(payload.get("vintageYear")) or _extract_year(name)

    if note.endswith("..."):
        note = ""

    # Canonical colour from the API payload — most reliable source
    colour_raw = _clean_text(payload.get("color") or "").lower()
    colour_canon = _COLOUR_CANON.get(colour_raw) or _extract_colour_from_text(name)

    review = {
        "score_native": score,
        "note": note,
        "reviewer": _clean_text(payload.get("tasterName")) or "James Suckling",
        "drink_from": _extract_drink_from(note),
        "drink_to": None,
        "date_tasted": _normalize_date(payload.get("tastingDate")),
        "vintage_src": vintage,
        "review_url": _canonical_detail_url(url, payload),
        "colour": colour_canon,
        "colour_canon": colour_canon,   # 'rouge'|'blanc'|'rose'|None
        "wine_name_src": name,
        "score_label": f"JS {int(score)}" if score is not None else "JS",
    }

    if review["score_native"] is None and not review["note"]:
        return None

    return review


def _extract_payload(html: str) -> dict | None:
    normalized = str(html or "").replace('\\"', '"')
    match = re.search(r'"data":(\{.*?\}),"session":', normalized, re.S)
    if not match:
        return None
    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError as exc:
        logger.warning("JS payload JSON parse failed: %s", exc)
        return None


def _parse_score(raw) -> float | None:
    text = str(raw or "").strip()
    if not text:
        return None
    match = re.search(r"\b(\d{2,3})\b", text)
    if not match:
        return None
    value = float(match.group(1))
    return value if 50 <= value <= 100 else None


def _extract_drink_from(note: str) -> int | None:
    match = re.search(r"\bTry from\s+((?:19|20)\d{2})\b", note or "", re.I)
    return int(match.group(1)) if match else None


def _normalize_date(raw) -> str | None:
    text = _clean_text(raw)
    return text or None


def _parse_sortable_date(raw: str | None) -> tuple[int, int, int]:
    text = _clean_text(raw)
    if not text:
        return (0, 0, 0)
    text = re.sub(r"^[A-Za-z]+,\s*", "", text)
    for fmt in (
        "%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%d %b %Y",
        "%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y",
    ):
        try:
            from datetime import datetime
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


def _canonical_detail_url(url: str, payload: dict) -> str:
    tasting_note_id = payload.get("tastingNoteId")
    name = str(payload.get("appellationName") or "").strip()
    if tasting_note_id:
        slug = _slugify(name)
        if slug:
            return f"{BASE_URL}/tasting-notes/{tasting_note_id}/{slug}"
        return f"{BASE_URL}/tasting-notes/{tasting_note_id}"
    return url


def _extract_url_slug(url: str) -> str:
    """Extract the wine-name slug from a JS tasting-note URL."""
    try:
        parsed = urlparse(str(url or ""))
        parts = [p for p in parsed.path.split("/") if p]
        if len(parts) >= 3 and parts[0] == "tasting-notes":
            return parts[2]
    except Exception:
        pass
    return ""


def _slug_tokens(slug: str) -> set[str]:
    return {tok for tok in re.split(r"[-_]+", str(slug or "")) if tok}


# ---------------------------------------------------------------------------
# Core matching — the final acceptance gate
# ---------------------------------------------------------------------------

def _matches_query(query_name: str, query_vintage: int | None, review: dict) -> bool:
    """
    Return True only when *review* is a genuine match for *query_name* /
    *query_vintage*.  Gates applied in order:

      1. Direct URL/ID supplied → validate vintage only.
      2. Wine name broad match.
      3. Known conflict-pair check (second wines, siblings, unrelated same-fragment).
      4. Colour check — rouge ≠ blanc is a hard rejection.
      5. Slug token overlap ≥ 60 % (colour tokens excluded, handled by gate 4).
      6. Vintage match.
    """
    hit_name = str(review.get("wine_name_src") or "")
    direct_url = _extract_direct_url(query_name)

    # Gate 1 — explicit URL/ID: trust caller, only verify vintage
    if direct_url:
        return _vintage_matches(query_vintage, review.get("vintage_src"), hit_name)

    # Gate 2 — name
    if not _name_matches(query_name, hit_name):
        return False

    # Gate 3 — conflict pairs
    query_slug = _slugify(query_name)
    hit_slug = _extract_url_slug(review.get("review_url") or "")
    if query_slug and hit_slug and _slugs_conflict(query_slug, hit_slug):
        logger.info(
            "JS matches_query rejected conflict pair: query=%r hit_url=%r",
            query_name, review.get("review_url"),
        )
        return False

    # Gate 4 — colour mismatch
    #   Priority: API payload colour_canon > URL slug colour > hit name colour
    query_colour = _extract_colour_from_text(query_name)
    hit_colour = (
        review.get("colour_canon")
        or _extract_colour_from_text(hit_slug)
        or _extract_colour_from_text(hit_name)
    )
    if _colours_conflict(query_colour, hit_colour):
        logger.info(
            "JS matches_query rejected colour mismatch: "
            "query_colour=%r hit_colour=%r query=%r hit_url=%r",
            query_colour, hit_colour, query_name, review.get("review_url"),
        )
        return False

    # Gate 5 — slug token overlap (colour tokens already handled above)
    if query_slug and hit_slug:
        colour_words = frozenset(_COLOUR_CANON)
        q_toks = _slug_tokens(query_slug) - colour_words
        h_toks = _slug_tokens(hit_slug) - colour_words
        if q_toks and h_toks:
            overlap = len(q_toks & h_toks)
            forward = overlap / len(q_toks)
            if forward < 0.6:
                logger.info(
                    "JS matches_query rejected low slug overlap (%.0f%%): "
                    "query=%r hit_url=%r",
                    forward * 100, query_name, review.get("review_url"),
                )
                return False
            reverse = overlap / len(h_toks)
            if reverse < 0.35 and len(h_toks) > len(q_toks) + 2:
                logger.info(
                    "JS matches_query rejected oversized hit slug: "
                    "query=%r hit_url=%r",
                    query_name, review.get("review_url"),
                )
                return False

    # Gate 6 — vintage
    return _vintage_matches(query_vintage, review.get("vintage_src"), hit_name)


def _name_matches(query_name: str, hit_name: str) -> bool:
    """
    Decide whether *hit_name* is the same wine as *query_name*.

    Colour tokens are stripped here — they are validated separately in
    _matches_query (Gate 4), so "Malartic Rouge" and "Malartic Blanc" are
    not rejected at the name level, only at the colour gate.

    Bidirectional token ratios prevent partial-name confusion:
    "Lafleur" cannot match "Lafleur-Gazin" because the reverse ratio is too low.
    """
    colour_words = frozenset(_COLOUR_CANON)
    query_tokens = _name_tokens(query_name) - colour_words
    hit_tokens = _name_tokens(hit_name) - colour_words

    if not query_tokens:
        return True
    if not hit_tokens:
        return False

    overlap = len(query_tokens & hit_tokens)
    if overlap == 0:
        return False

    forward_ratio = overlap / len(query_tokens)
    reverse_ratio = overlap / len(hit_tokens)

    if forward_ratio >= 0.75 and reverse_ratio >= 0.50:
        return True

    # Conflict pair check at name level
    q_slug = _slugify(query_name)
    h_slug = _slugify(hit_name)
    if _slugs_conflict(q_slug, h_slug):
        return False

    # Fuzzy fallback for accent / minor spelling differences
    query_norm = _clean_text(_normalize_text(query_name)).lower()
    hit_norm = _clean_text(_normalize_text(hit_name)).lower()
    if difflib.SequenceMatcher(None, query_norm, hit_norm).ratio() >= 0.80:
        return True

    # Relax when all producer tokens match
    producer_tokens = _producer_tokens(query_name) - colour_words
    if not producer_tokens:
        return forward_ratio >= 0.55

    producer_ratio = len(producer_tokens & hit_tokens) / len(producer_tokens)
    return forward_ratio >= 0.50 and producer_ratio >= 0.80


def _vintage_matches(query_vintage, hit_vintage, hit_name: str) -> bool:
    query_year = _extract_year(query_vintage)
    if query_year is None:
        return True
    hit_year = _extract_year(hit_vintage)
    if hit_year is None:
        hit_year = _extract_year(hit_name)
    if hit_year is None:
        return True
    return hit_year == query_year


# ---------------------------------------------------------------------------
# Conflict-pair detection
# ---------------------------------------------------------------------------

def _slugs_conflict(slug_a: str, slug_b: str) -> bool:
    """
    Return True when slug_a and slug_b match a known conflict pair,
    meaning they represent different wines despite sharing name tokens.
    Both orderings of every pair are checked.
    """
    if not slug_a or not slug_b:
        return False
    a = slug_a.lower()
    b = slug_b.lower()
    for frag_x, frag_y in _CONFLICT_PAIRS:
        if (frag_x in a and frag_y in b) or (frag_y in a and frag_x in b):
            return True
    return False


# ---------------------------------------------------------------------------
# URL / text utilities
# ---------------------------------------------------------------------------

def _extract_direct_url(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    match = DETAIL_URL_RE.search(text)
    if match:
        return match.group(0)
    if text.startswith("/tasting-notes/"):
        return f"{BASE_URL}{text}"
    return ""


def _extract_search_result_url(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    match = SEARCH_RESULT_URL_RE.search(text)
    if match:
        return match.group(0)
    if text.startswith("/search-result?") and "keyword=" in text:
        return f"{BASE_URL}{text}"
    return ""


def _manual_search_result_urls(name: str, search_hints: dict | None) -> list[str]:
    hints = search_hints or {}
    candidates: list[str] = []
    for key in ("jamessuckling_url", "js_search_url", "js_url", "review_url"):
        value = str(hints.get(key) or "").strip()
        for part in _split_hint_values(value):
            url = _extract_search_result_url(part)
            if url:
                candidates.append(url)
    direct_from_name = _extract_search_result_url(name)
    if direct_from_name:
        candidates.append(direct_from_name)
    return _dedupe(candidates)


def _search_keyword_from_url(url: str) -> str:
    parsed = urlparse(str(url or "").strip())
    if not parsed.query:
        return ""
    keyword_values = parse_qs(parsed.query).get("keyword") or []
    if not keyword_values:
        return ""
    return _clean_text(unquote(keyword_values[0]))


def _extract_year(value) -> int | None:
    if value is None:
        return None
    match = YEAR_RE.search(str(value))
    return int(match.group(0)) if match else None


def _slugify(value: str) -> str:
    text = _normalize_text(value).lower()
    for pattern, replacement in _APPELLATION_SUBS:
        text = pattern.sub(replacement, text)
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return text


def _normalize_text(value) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    return "".join(ch for ch in text if not unicodedata.combining(ch))


def _clean_text(value) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _split_hint_values(value: str) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    return [part.strip() for part in re.split(r"[\s,;]+", text) if part.strip()]


def _debug_write_tmp(filename: str, text: str) -> None:
    try:
        TMP_DIR.mkdir(parents=True, exist_ok=True)
        (TMP_DIR / filename).write_text(text, encoding="utf-8", errors="ignore")
    except Exception:
        pass


def _build_search_queries(
    name: str,
    vintage: int | None = None,
    search_hints: dict | None = None,
) -> list[str]:
    base = _clean_text(name)
    if not base:
        return _hint_search_keywords(name, search_hints)

    vintage_text = str(vintage) if vintage else ""
    variants: list[str] = []

    exact_with_vintage = (
        f"{base} {vintage_text}".strip()
        if vintage_text and vintage_text not in base
        else base
    )
    variants.append(exact_with_vintage)
    variants.append(base)

    no_commas = re.sub(r"\s*,\s*", " ", base)
    variants.append(no_commas)

    producer = str(base.split(",")[0]).strip()
    variants.append(producer)
    if vintage_text:
        variants.append(
            f"{no_commas} {vintage_text}" if vintage_text not in no_commas else no_commas
        )
        if producer and vintage_text not in producer:
            variants.append(f"{producer} {vintage_text}")

    ascii_variants = [_normalize_text(v) for v in variants]
    hinted = _hint_search_keywords(name, search_hints)
    return _dedupe([v for v in [*hinted, *variants, *ascii_variants] if _clean_text(v)])


def _hint_search_keywords(name: str, search_hints: dict | None) -> list[str]:
    queries: list[str] = []
    for url in _manual_search_result_urls(name, search_hints):
        keyword = _search_keyword_from_url(url)
        if keyword:
            queries.append(keyword)
            queries.append(_normalize_text(keyword))
    return _dedupe([q for q in queries if _clean_text(q)])


def _name_tokens(value: str) -> set[str]:
    text = _normalize_text(value).lower()
    text = re.sub(r"\b(19|20)\d{2}\b", " ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return {
        tok for tok in text.split()
        if len(tok) > 1 and tok not in _NAME_STOP_WORDS
    }


def _producer_tokens(value: str) -> set[str]:
    head = str(value or "").split(",")[0]
    return _name_tokens(head)


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