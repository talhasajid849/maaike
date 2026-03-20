"""
test_matching.py
================
Run from the backend/ directory:

    python test_matching.py              # logic tests only (no network)
    python test_matching.py live         # + live RP search (needs cookies)
    python test_matching.py live jr      # + live JR search (needs cookies)

Tests:
  1. Name-match logic — RP  (_name_matches)
  2. Name-match logic — JR  (_name_matches_jr)
  3. Query builder  — RP    (_build_rp_queries)
  4. Live RP search  (optional, needs cookies/robertparker.json)
  5. Live JR search  (optional, needs cookies/jancisrobinson.json or real_cookies.json)
"""
import io
import os
import sys
import maaike_phase1 as jr_mod
from openpyxl import Workbook

# ── Import from both scrapers ──────────────────────────────────────────────────
from sources.robertparker import _name_matches, _name_tokens, _build_rp_queries
from maaike_phase1 import (
    _build_search_queries,
    _candidate_prerank_score,
    _name_matches_jr,
    _name_tokens as _jr_tokens,
    _clean_name_variants,
    _parse_query_structured,
    _jr_query_identity,
    _jr_plausible_match,
    _jr_candidate_passes,
    _strict_match_structured,
)
from services import xlsx_service as xlsx_mod

PASS = "  ✓"
FAIL = "  ✗"

def check(label: str, result: bool, expected: bool):
    ok = result == expected
    mark = PASS if ok else FAIL
    detail = "" if ok else f"  ← expected {expected}, got {result}"
    print(f"{mark}  {label}{detail}")
    return ok


# ─── 1. RP name matching ──────────────────────────────────────────────────────
print("\n══ 1. RP name matching (_name_matches) ══════════════════════════════")

CASES_RP = [
    # (query,                                      hit_name,                                      expected)
    ("Charles Noellat, Nuits-Saint-Georges, Blanc", "Charles Noellat Nuits-Saint-Georges Blanc",    True),
    ("Charles Noellat, Nuits-Saint-Georges, Blanc", "Charles Noellat Richebourg",                   False),
    ("Charles Noellat, Nuits-Saint-Georges, Blanc", "Domaine de la Romanée-Conti Romanée-Conti",    False),
    ("Château Margaux, 2015",                        "Château Margaux",                             True),
    ("Château Margaux, 2015",                        "Château Palmer",                              False),
    ("Giacomo Conterno, Barolo, Monfortino Riserva", "Giacomo Conterno Barolo Monfortino Riserva",  True),
    ("Giacomo Conterno, Barolo, Monfortino Riserva", "Giacomo Conterno Barolo Francia",             False),
    ("Pétrus",                                       "Pétrus",                                      True),
    ("Pétrus",                                       "Le Pin",                                      False),
    ("Domaine Leroy, Chambolle-Musigny, Les Charmes","Leroy Chambolle-Musigny Les Charmes",         True),
    ("Domaine Leroy, Chambolle-Musigny, Les Charmes","Leroy Gevrey-Chambertin",                     False),
]

rp_ok = sum(check(f"query={q!r:50s} | hit={h!r:45s}", _name_matches(q, h), exp)
            for q, h, exp in CASES_RP)
print(f"\n  {rp_ok}/{len(CASES_RP)} passed")


# ─── 2. JR name matching ──────────────────────────────────────────────────────
print("\n══ 2. JR name matching (_name_matches_jr) ═══════════════════════════")

CASES_JR = [
    ("Charles Noellat, Nuits-Saint-Georges, Blanc 2006", "Charles Noellat, Nuits-Saint-Georges Blanc", True),
    ("Charles Noellat, Nuits-Saint-Georges, Blanc 2006", "Charles Noellat, Richebourg",               False),
    ("Château Latour 2010",                              "Château Latour",                            True),
    ("Château Latour 2010",                              "Château Léoville-Las Cases",                False),
    ("Domaine Leflaive, Puligny-Montrachet, Pucelles",   "Leflaive Puligny-Montrachet Pucelles",      True),
    ("Domaine Leflaive, Puligny-Montrachet, Pucelles",   "Leflaive Bâtard-Montrachet",                False),
]

CASES_JR += [
    ("Chateau Tour de Pez, Les Hauts de Pez, Saint-Estephe", "L'Ame du Domaine",                      False),
    ("Chateau Tour de Pez, Les Hauts de Pez, Saint-Estephe", "Tour de Pez Les Hauts de Pez",          True),
    ("Azienda Agricola Salvioni La Cerbaiola, Brunello di Montalcino", "Dry Amber Reserve",           False),
    ("Azienda Agricola Salvioni La Cerbaiola, Brunello di Montalcino", "Salvioni Brunello",           True),
    ("Azienda Agricola Salvioni La Cerbaiola, Brunello di Montalcino", "Salvioni La Cerbaiola",       True),
    ("Domaine d'Eugenie, Vosne-Romanee Premier Cru, Aux Brulees", "Aux Brulees",                      True),
    ("Chateau La Fleur de Bouard, Lalande de Pomerol", "La Fleur de Bouard",                          True),
    ("Chateau La Fleur de Bouard, Lalande de Pomerol", "Random Lalande de Pomerol",                   False),
]

jr_ok = sum(check(f"query={q!r:55s} | hit={h!r:45s}", _name_matches_jr(q, h), exp)
            for q, h, exp in CASES_JR)
print(f"\n  {jr_ok}/{len(CASES_JR)} passed")

print("\n══ 2b. JR variant generation (_clean_name_variants) ═════════════════════")
_v = _clean_name_variants("Chateau La Fleur de Bouard, Lalande de Pomerol")
has_bad = any("Ateau " in x for x in _v)
print(f"  {'✓' if not has_bad else '✗'}  no broken 'Ateau ...' variant")
check("JR emits Ch abbreviation", "Ch La Fleur De Bouard, Lalande De Pomerol" in _v, True)

_v2 = _clean_name_variants("Domaine d'Eugenie, Vosne-Romanee Premier Cru, Aux Brulees")
check("JR keeps apostrophe-aware producer form", "D'Eugenie Aux Brulees" in _v2, True)
check("JR emits decontracted producer form", "Eugenie Aux Brulees" in _v2, True)

_v3 = _clean_name_variants("Azienda Agricola Salvioni La Cerbaiola, Brunello di Montalcino")
check("JR emits bare left-side identity for 2-part names", "Salvioni La Cerbaiola" in _v3, True)

_v4 = _clean_name_variants("Georges Lignier Et Fils, Gevrey-Chambertin")
check("JR trims producer suffixes before region", "Georges Lignier" in _v4, True)

print("\n══ 2c. JR query identity (_jr_query_identity) ═══════════════════════════")
identity = _jr_query_identity("Domaine Leflaive, Puligny-Montrachet, Pucelles 2020", "2020")
check("producer parsed", identity.get("producer") == "Domaine Leflaive", True)
check("specific parsed", identity.get("specific") == "Pucelles", True)
check("vintage parsed", identity.get("vintage") == "2020", True)

print("\n══ 2d. JR structured query + variants ════════════════════════════════════")
structured = _parse_query_structured("Domaine Leflaive, Puligny-Montrachet, Pucelles 2020", "2020")
check("structured producer", structured.get("producer") == "Domaine Leflaive", True)
check("structured wine", structured.get("wine") == "Puligny-Montrachet", True)
check("structured appellation", structured.get("appellation") == "Pucelles", True)
structured_two_part = _parse_query_structured("Chateau La Fleur de Bouard, Lalande de Pomerol", "2020")
check("2-part region parsed as producer", structured_two_part.get("producer") == "Chateau La Fleur de Bouard", True)
check("2-part region parsed as appellation", structured_two_part.get("appellation") == "Lalande de Pomerol", True)
queries = _build_search_queries("Domaine Leflaive, Puligny-Montrachet, Pucelles", "2020", "LWIN12345672020", {
    "jr_search_url": "https://www.jancisrobinson.com/tastings?search-full=%22Leflaive%20Pucelles%202020%22"
})
check("query builder keeps URL hint first", queries[0] == "Leflaive Pucelles 2020", True)
check("query builder does not add LWIN as text query", all(q != "1234567" for q in queries), True)
queries_manual_style = _build_search_queries("Domaine Leflaive, Puligny-Montrachet, Pucelles", "2020", "")
check("manual-style exact full query is prioritized", queries_manual_style[0] == "Domaine Leflaive, Puligny-Montrachet, Pucelles 2020", True)
queries_two_part = _build_search_queries("Chateau La Fleur de Bouard, Lalande de Pomerol", "2020", "")
check("2-part region query builder prioritizes exact full query", queries_two_part[0] == "Chateau La Fleur de Bouard, Lalande de Pomerol 2020", True)
check("2-part region query builder adds producer-only vintage query", "Chateau La Fleur de Bouard 2020" in queries_two_part, True)
queries_with_hints = _build_search_queries(
    "Chateau La Fleur de Bouard, Lalande de Pomerol",
    "2020",
    "",
    {
        "jr_search_url": "https://www.jancisrobinson.com/tastings?search-full=%22Ch%20La%20Fleur%20de%20Bo%C3%BCard%202020%22",
        "jr_producer": "Ch La Fleur de Bouard",
        "jr_appellation": "Lalande-de-Pomerol",
    },
)
check("manual JR URL stays first", queries_with_hints[0] == "Ch La Fleur de Boüard 2020", True)
check("manual JR producer/appellation query is prioritized", "Ch La Fleur de Bouard, Lalande-de-Pomerol" in queries_with_hints[:3], True)
check("manual JR hints keep query count compact", len(queries_with_hints) <= 3, True)

print("\nâ•â• 2d2. XLSX manual JR hint parsing â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•")
wb = Workbook()
ws = wb.active
ws.append(["Publisher", "LWIN11", "Product_Name", "Vintage", "url on the jr", "wine on the jr", "Producer on jr", "appellation on jr ", "url on the rp"])
ws.append(["Jancis Robinson", "Optional if Name set", "Optional if LWIN set", "YYYY or NV", "", "", "", "", ""])
ws.append([
    "Jancis Robinson",
    "10098312020",
    "Chateau La Fleur de Bouard, Lalande de Pomerol 2020 (Magnum)",
    "2020",
    "https://www.jancisrobinson.com/tastings?search-full=%22Ch%20La%20Fleur%20de%20Bo%C3%BCard%202020%22",
    "",
    "Ch La Fleur de Boüard",
    "Lalande-de-Pomerol",
    "https://www.robertparker.com/wines/123456/test-slug",
])
buf = io.BytesIO()
wb.save(buf)
parsed_rows = xlsx_mod.parse_xlsx(buf.getvalue())
check("xlsx parser keeps one data row", len(parsed_rows) == 1, True)
check("xlsx parser strips trailing pack/year from name", parsed_rows[0].get("name") == "Chateau La Fleur de Bouard, Lalande de Pomerol", True)
check("xlsx parser stores JR producer hint", parsed_rows[0].get("search_hints", {}).get("jr_producer") == "Ch La Fleur de Boüard", True)
check("xlsx parser stores JR appellation hint", parsed_rows[0].get("search_hints", {}).get("jr_appellation") == "Lalande-de-Pomerol", True)
check("xlsx parser stores RP URL hint", parsed_rows[0].get("search_hints", {}).get("rp_search_url") == "https://www.robertparker.com/wines/123456/test-slug", True)

print("\n══ 2e. JR plausible-match fallback (_jr_plausible_match) ════════════════")
check(
    "JR allows strong abbreviated producer/title matches",
    _jr_plausible_match(
        "Azienda Agricola Salvioni La Cerbaiola, Brunello di Montalcino",
        "Salvioni Brunello",
        {"vintage": ["2020"]},
        "2020",
        "",
    ),
    True,
)
check(
    "JR still rejects unrelated weak matches",
    _jr_plausible_match(
        "Azienda Agricola Salvioni La Cerbaiola, Brunello di Montalcino",
        "Dry Amber Reserve",
        {"vintage": ["2020"]},
        "2020",
        "",
    ),
    False,
)

print("\n══ 2f. JR strict structured match ═══════════════════════════════════════")
strict_candidate = {
    "producer": "Domaine Leflaive",
    "wine_name": "Puligny-Montrachet",
    "appellation": "Pucelles",
    "vintage": "2020",
    "match_text": "Domaine Leflaive Puligny-Montrachet Pucelles",
}
check("strict structured match accepts exact identity", _strict_match_structured(structured, strict_candidate), True)

print("\nâ•â• 2f2. JR benchmark candidate ranking â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•")
benchmark_query = _parse_query_structured("Chateau La Fleur de Bouard, Lalande de Pomerol", "2020")
benchmark_good = {
    "producer": "Ch La Fleur de Bouard",
    "wine_name": "",
    "title": "",
    "appellation": "Lalande-de-Pomerol",
    "vintage": "2020",
    "match_text": "Ch La Fleur de Bouard Lalande-de-Pomerol",
    "rank": 70.0,
    "date_tasted": "08 Jun 2023",
}
benchmark_bad = {
    "producer": "",
    "wine_name": "",
    "title": "",
    "appellation": "Lalande-de-Pomerol",
    "vintage": "2020",
    "match_text": "Random Lalande-de-Pomerol",
    "rank": 70.0,
    "date_tasted": "08 Jun 2023",
}
check(
    "benchmark scorer prefers same producer over region-only match",
    _candidate_prerank_score(benchmark_query, benchmark_good) > _candidate_prerank_score(benchmark_query, benchmark_bad),
    True,
)
check(
    "benchmark gate rejects region-only false positive",
    _jr_candidate_passes([benchmark_query], benchmark_bad),
    False,
)
check(
    "benchmark gate accepts producer/appellation match",
    _jr_candidate_passes([benchmark_query], benchmark_good),
    True,
)

print("\n══ 2g. JR candidate selection (_jr_select_candidate) ════════════════════")
_orig_pick = jr_mod._jr_llm_pick_candidate
_orig_verify = jr_mod._jr_llm_verify_exact_match
_orig_strict = jr_mod.JR_STRICT_MODE
try:
    c1 = {"match_text": "Producer A Wine A", "rank": 80.0}
    c2 = {"match_text": "Producer A Wine B", "rank": 76.0}
    jr_mod.JR_STRICT_MODE = True
    jr_mod._jr_llm_pick_candidate = lambda *args, **kwargs: c2
    jr_mod._jr_llm_verify_exact_match = lambda *args, **kwargs: True
    picked = jr_mod._jr_select_candidate("Producer A, Region, Wine B", "2020", [c1, c2])
    check("LLM can resolve ambiguous JR top results", picked is c2, True)
finally:
    jr_mod._jr_llm_pick_candidate = _orig_pick
    jr_mod._jr_llm_verify_exact_match = _orig_verify
    jr_mod.JR_STRICT_MODE = _orig_strict

print("\n══ 2h. JR search aggregation (jr_msearch) ═══════════════════════════════")
_orig_do = jr_mod._do_msearch
try:
    calls = []
    hit_a = {"url": ["/a"], "title": ["A"], "producer": ["Prod"], "wine_name": ["Wine A"], "appellation": ["App"], "lwin": ["1111111"], "vintage": ["2020"]}

    def fake_do(_session, payload):
        calls.append(payload)
        return [hit_a] if len(calls) == 1 else []

    jr_mod._do_msearch = fake_do
    hits = jr_mod.jr_msearch(object(), "Producer, Region, Wine", "2020", "")
    check("JR keeps first relevant compact ES hit set", len(hits) == 1, True)
finally:
    jr_mod._do_msearch = _orig_do

print("\n══ 2i. JR search_wine prefers structured ES search ═══════════════════════")
_orig_jr_msearch = jr_mod.jr_msearch
_orig_es_candidate = jr_mod._jr_es_candidate
_orig_review_from_candidate = jr_mod._jr_review_from_es_candidate
try:
    jr_mod.jr_msearch = lambda *args, **kwargs: [{"title": ["Used"]}]
    jr_mod._jr_es_candidate = lambda *args, **kwargs: {"rank": 77.0, "review_url": "https://example.test/review", "src": {}}
    jr_mod._jr_review_from_es_candidate = lambda *args, **kwargs: {"score_20": 16, "wine_name_jr": "ES Pick", "review_url": "https://example.test/review"}
    hits = jr_mod.search_wine(object(), "Producer, Region, Wine", "2020", "")
    check("JR uses ES result", len(hits) == 1 and hits[0].get("wine_name_jr") == "ES Pick", True)
finally:
    jr_mod.jr_msearch = _orig_jr_msearch
    jr_mod._jr_es_candidate = _orig_es_candidate
    jr_mod._jr_review_from_es_candidate = _orig_review_from_candidate

print("\n══ 2j. JR search_wine falls back to manual search ════════════════════════")
_orig_jr_msearch = jr_mod.jr_msearch
try:
    jr_mod.jr_msearch = lambda *args, **kwargs: []
    hits = jr_mod.search_wine(object(), "Producer, Region, Wine", "2020", "")
    check("JR returns empty when ES has no result", len(hits) == 0, True)
finally:
    jr_mod.jr_msearch = _orig_jr_msearch


print("\n══ 2k. XLSX runner passes JR search hints ═════════════════")
_orig_get_session = None
_orig_jr_search = None
try:
    import services.session_service as session_service
    _orig_get_session = session_service.get_session
    _orig_jr_search = jr_mod.search_wine
    captured = {}

    def fake_get_session(_source_key):
        return object()

    def fake_search(session, name, vintage, lwin="", search_hints=None):
        captured["search_hints"] = dict(search_hints or {})
        return [{"score_20": 16.0, "date_tasted": "08 Jun 2023", "review_url": "https://example.test/review"}]

    session_service.get_session = fake_get_session
    jr_mod.search_wine = fake_search

    job_id = xlsx_mod.create_job(
        buf.getvalue(),
        [{
            "row_idx": 3,
            "name": "Chateau La Fleur de Bouard, Lalande de Pomerol",
            "vintage": "2020",
            "lwin": "10098312020",
            "search_hints": {
                "jr_search_url": "https://www.jancisrobinson.com/tastings?search-full=%22Ch%20La%20Fleur%20de%20Bo%C3%BCard%202020%22",
                "jr_producer": "Ch La Fleur de Bo\u00fcard",
                "jr_appellation": "Lalande-de-Pomerol",
                "rp_search_url": "https://www.robertparker.com/wines/123456/test-slug",
            },
        }],
    )
    xlsx_mod.run_job(job_id, source_key="jancisrobinson", sleep_sec=0)
    check("xlsx runner forwards JR producer hint", captured.get("search_hints", {}).get("jr_producer") == "Ch La Fleur de Bo\u00fcard", True)
    check("xlsx runner forwards JR URL hint", "search-full" in (captured.get("search_hints", {}).get("jr_search_url") or ""), True)
    check("xlsx runner forwards RP URL hint", captured.get("search_hints", {}).get("rp_search_url") == "https://www.robertparker.com/wines/123456/test-slug", True)
finally:
    if _orig_get_session is not None:
        session_service.get_session = _orig_get_session
    if _orig_jr_search is not None:
        jr_mod.search_wine = _orig_jr_search

# ─── 3. RP query builder ──────────────────────────────────────────────────────
print("\n══ 3. RP query builder (_build_rp_queries) ══════════════════════════")

WINES = [
    ("Charles Noellat, Nuits-Saint-Georges, Blanc", 2006),
    ("Château Margaux",                              2015),
    ("Giacomo Conterno, Barolo, Monfortino Riserva", 2016),
    ("Domaine Leroy, Chambolle-Musigny, Les Charmes",2019),
    ("Pétrus",                                       2012),
    ("Penfolds Grange",                              2018),
]

for name, vintage in WINES:
    qs = _build_rp_queries(name, vintage)
    print(f"\n  Wine   : {name} ({vintage})")
    for i, q in enumerate(qs):
        print(f"  Query {i+1}: {q!r}")

print("\nâ•â• Optional manual benchmark workbook â•â•")
manual_workbook = r"C:\Users\RTS84\Downloads\ruepinard-attachments (1)\Jancis Robinson for the research the data 1-200.xlsx"
if os.path.exists(manual_workbook):
    with open(manual_workbook, "rb") as fh:
        benchmark_rows = xlsx_mod.parse_xlsx(fh.read())
    check("manual benchmark workbook keeps 198 wine rows", len(benchmark_rows) == 198, True)
    first_row_queries = _build_search_queries(benchmark_rows[0]["name"], benchmark_rows[0]["vintage"], benchmark_rows[0]["lwin"])
    check("manual benchmark first row starts with exact full search", first_row_queries[0] == "Chateau La Fleur de Bouard, Lalande de Pomerol 2020", True)
    check("manual benchmark query list stays compact", len(first_row_queries) <= 12, True)
else:
    print("  - skipped (manual benchmark workbook not present)")


# ─── 4. Live RP search ────────────────────────────────────────────────────────
if "live" in sys.argv:
    print("\n══ 4. Live RP search ════════════════════════════════════════════════")
    try:
        from sources.robertparker import load_session, search_wine as rp_search

        session_rp = load_session("cookies/robertparker.json")
        tests = [
            ("Charles Noellat, Nuits-Saint-Georges, Blanc", 2006, None),
            ("Château Margaux",                              2015, None),
            ("Giacomo Conterno, Barolo, Monfortino Riserva", 2016, None),
        ]
        for name, vintage, lwin in tests:
            print(f"\n  Searching: {name} {vintage}")
            results = rp_search(session_rp, name, vintage, lwin, sleep_sec=1.5)
            if results:
                r = results[0]
                print(f"  {PASS}  Found: {r.get('wine_name_src')} | "
                      f"score={r.get('score_native')}/100 | "
                      f"reviewer={r.get('reviewer')}")
            else:
                print(f"  {FAIL}  Not found")
    except Exception as e:
        print(f"  {FAIL}  {type(e).__name__}: {e}")


# ─── 5. Live JR search ────────────────────────────────────────────────────────
if "live" in sys.argv and "jr" in sys.argv:
    print("\n══ 5. Live JR search ════════════════════════════════════════════════")
    try:
        from maaike_phase1 import load_session as jr_load, search_wine as jr_search

        session_jr = jr_load("real_cookies.json")
        tests = [
            ("Charles Noellat, Nuits-Saint-Georges, Blanc", "2006", "LWIN285595020060600750"),
            ("Château Margaux",                              "2015", ""),
        ]
        for name, vintage, lwin in tests:
            print(f"\n  Searching: {name} {vintage}")
            results = jr_search(session_jr, name, vintage, lwin)
            if results:
                r = results[0]
                print(f"  {PASS}  Found: {r.get('wine_name_jr')} | "
                      f"score={r.get('score_20')}/20 | "
                      f"reviewer={r.get('reviewer')}")
            else:
                print(f"  {FAIL}  Not found")
    except Exception as e:
        print(f"  {FAIL}  {type(e).__name__}: {e}")


print("\n")
