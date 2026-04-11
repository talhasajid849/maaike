"""
services/xlsx_service.py
========================
XLSX review enrichment service.

Flow:
  1. User uploads JR-format XLSX template
  2. parse_xlsx()  → extracts wine rows (name, vintage, lwin, row_idx)
  3. create_job()  → stores job in memory
  4. run_job()     → background thread: search JR for each wine
  5. fill_xlsx()   → generates filled XLSX bytes
  6. User downloads the result

Template format (from JR export template):
  Row 1:  Headers — Publisher, LWIN11, Product_Name, Vintage,
                     Critic_Name, Score, Drink_From, Drink_To, Review_Date, Review
  Row 2:  Instructions/examples (skipped)
  Row 3+: Wine data rows
"""
from __future__ import annotations

import io
import re
import threading
import time
import uuid
from datetime import datetime
from typing import Dict, List, Optional

from services.normalize_service import normalize_review, reset_note_tracking

try:
    from openpyxl import load_workbook
    from openpyxl.styles import PatternFill
    OPENPYXL_OK = True
except ImportError:
    OPENPYXL_OK = False


# ─── Column aliases ───────────────────────────────────────────────────────────
# Maps internal field names → list of substrings to look for in headers

_ALIASES: Dict[str, List[str]] = {
    "publisher":   ["publisher", "source"],
    "lwin":        ["lwin"],
    "name":        ["product_name", "wine_name", "name"],
    "vintage":     ["vintage"],
    "jr_search_url": ["jr_search_url", "url_on_the_jr", "jr_url"],
    "jr_wine_name": ["jr_wine_name", "wine_on_the_jr"],
    "jr_producer": ["jr_producer", "producer_on_jr"],
    "jr_appellation": ["jr_appellation", "appellation_on_jr"],
    "rp_search_url": ["rp_search_url", "url_on_the_rp", "rp_url"],
    "js_search_url": ["js_search_url", "jamessuckling_url", "url_on_the_js", "url_on_the_james_suckling", "js_url"],
    "js_tasting_note_id": ["js_tasting_note_id", "jamessuckling_id", "tasting_note_id_on_js", "js_id"],
    "dc_wine_name": ["dc_wine_name", "decanter_wine_name", "name_on_decanter"],
    "dc_search_url": ["dc_search_url", "decanter_search_url", "url_on_decanter_search_page"],
    "dc_review_url": ["dc_review_url", "decanter_review_url", "url_on_decanter_sperate_wine_page", "url_on_decanter_separate_wine_page"],
    "critic":      ["critic_name", "critic", "reviewer", "taster"],
    "score":       ["score"],
    "drink_from":  ["drink_fro", "drink_from", "from"],
    "drink_to":    ["drink_to"],
    "review_date": ["review_d", "review_date", "date_tasted", "date"],
    "review":      ["review", "tasting_note", "note"],
    "source_url":  ["source_url", "review_url", "url"],
}


def _col_idx(headers: List[str], field: str) -> Optional[int]:
    """Find column index matching a field by alias."""
    for alias in _ALIASES.get(field, [field]):
        for i, h in enumerate(headers):
            h_norm = h.lower().replace(" ", "_").replace("-", "_")
            if alias in h_norm:
                return i
    return None


# ─── Parse ────────────────────────────────────────────────────────────────────

def parse_xlsx(file_bytes: bytes) -> List[Dict]:
    """
    Parse JR-format XLSX template and return list of wine dicts.

    Each dict: { row_idx (1-based), name, vintage, lwin, lwin7, prefilled, ...optional source hints }
    """
    if not OPENPYXL_OK:
        raise RuntimeError("openpyxl not installed — run: pip install openpyxl")

    wb = load_workbook(filename=io.BytesIO(file_bytes), read_only=True, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    wb.close()

    if not rows:
        return []

    # Find header row (first row containing "product_name" or "lwin")
    hdr_idx = 0
    for i, row in enumerate(rows[:5]):
        row_low = [str(c or "").strip().lower().replace(" ", "_") for c in row]
        if any("product_name" in c or "lwin" in c for c in row_low):
            hdr_idx = i
            break

    raw_headers = [str(c or "").strip() for c in rows[hdr_idx]]
    lwin_col    = _col_idx(raw_headers, "lwin")
    name_col    = _col_idx(raw_headers, "name")
    vintage_col = _col_idx(raw_headers, "vintage")
    jr_search_url_col = _col_idx(raw_headers, "jr_search_url")
    jr_wine_name_col = _col_idx(raw_headers, "jr_wine_name")
    jr_producer_col = _col_idx(raw_headers, "jr_producer")
    jr_appellation_col = _col_idx(raw_headers, "jr_appellation")
    rp_search_url_col = _col_idx(raw_headers, "rp_search_url")
    js_search_url_col = _col_idx(raw_headers, "js_search_url")
    js_tasting_note_id_col = _col_idx(raw_headers, "js_tasting_note_id")
    dc_wine_name_col = _col_idx(raw_headers, "dc_wine_name")
    dc_search_url_col = _col_idx(raw_headers, "dc_search_url")
    dc_review_url_col = _col_idx(raw_headers, "dc_review_url")
    critic_col = _col_idx(raw_headers, "critic")
    score_col = _col_idx(raw_headers, "score")
    drink_from_col = _col_idx(raw_headers, "drink_from")
    drink_to_col = _col_idx(raw_headers, "drink_to")
    review_date_col = _col_idx(raw_headers, "review_date")
    review_col = _col_idx(raw_headers, "review")
    source_url_col = _col_idx(raw_headers, "source_url")

    def cell(row, col):
        if col is None or col >= len(row) or row[col] is None:
            return ""
        return str(row[col]).strip()

    def is_instruction_row(row_vals: List[str]) -> bool:
        joined = " | ".join(v.lower() for v in row_vals if v).strip()
        if not joined:
            return False
        markers = ("optional if", "e.g.", "yyyy", "text")
        return any(m in joined for m in markers)

    wines = []
    # Data starts after header; instruction rows are skipped dynamically.
    for offset, row in enumerate(rows[hdr_idx + 1:]):
        row_idx_1b = hdr_idx + 1 + offset + 1   # 1-based for openpyxl

        name_raw = cell(row, name_col)
        lwin_raw    = cell(row, lwin_col)
        vintage_raw = cell(row, vintage_col)
        jr_search_url = cell(row, jr_search_url_col)
        jr_wine_name = cell(row, jr_wine_name_col)
        jr_producer = cell(row, jr_producer_col)
        jr_appellation = cell(row, jr_appellation_col)
        rp_search_url = cell(row, rp_search_url_col)
        js_search_url = cell(row, js_search_url_col)
        js_tasting_note_id = cell(row, js_tasting_note_id_col)
        dc_wine_name = cell(row, dc_wine_name_col)
        dc_search_url = cell(row, dc_search_url_col)
        dc_review_url = cell(row, dc_review_url_col)
        row_vals = [cell(row, i) for i in range(len(raw_headers))]

        if is_instruction_row(row_vals):
            continue
        if not name_raw and not lwin_raw:
            continue
        if not name_raw or "optional" in name_raw.lower():
            continue

        # Clean name: strip trailing year and pack-size notes like "(Magnum)"
        name_clean = re.sub(r"\s*\(.*?\)\s*$", "", name_raw).strip()
        name_clean = re.sub(r",?\s*\b(19|20)\d{2}\b\s*$", "", name_clean).strip()

        # Determine vintage: column value → LWIN chars 7-11
        vintage = ""
        if vintage_raw and vintage_raw.upper() not in ("NV", "N/V", ""):
            m = re.search(r"\b(19|20)\d{2}\b", vintage_raw)
            if m:
                vintage = m.group(0)
        if not vintage and len(lwin_raw) >= 11:
            lv = lwin_raw[7:11]
            if lv.isdigit() and int(lv) > 1900:
                vintage = lv

        lwin7 = lwin_raw[:7]  if len(lwin_raw) >= 7  else lwin_raw
        lwin  = lwin_raw[:11] if len(lwin_raw) >= 11 else lwin_raw

        wine = {
            "row_idx": row_idx_1b,
            "name":    name_clean,
            "raw_name": name_raw,
            "vintage": vintage,
            "lwin":    lwin,
            "lwin7":   lwin7,
            "prefilled": any((
                cell(row, critic_col),
                cell(row, score_col),
                cell(row, drink_from_col),
                cell(row, drink_to_col),
                cell(row, review_date_col),
                cell(row, review_col),
                cell(row, source_url_col),
            )),
        }
        if (
            jr_search_url or jr_wine_name or jr_producer or jr_appellation
            or rp_search_url or js_search_url or js_tasting_note_id
            or dc_wine_name or dc_search_url or dc_review_url
        ):
            wine["search_hints"] = {
                "jr_search_url": jr_search_url,
                "jr_wine_name": jr_wine_name,
                "jr_producer": jr_producer,
                "jr_appellation": jr_appellation,
                "rp_search_url": rp_search_url,
                "jamessuckling_url": js_search_url,
                "js_tasting_note_id": js_tasting_note_id,
                "decanter_wine_name": dc_wine_name,
                "decanter_search_url": dc_search_url,
                "decanter_review_url": dc_review_url,
            }
        wines.append(wine)

    return wines


def detect_source_from_template(file_bytes: bytes) -> Optional[str]:
    """
    Infer source from Publisher column values in the uploaded template.
    Returns source key or None when unknown.
    """
    if not OPENPYXL_OK:
        return None

    wb = load_workbook(filename=io.BytesIO(file_bytes), read_only=True, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    wb.close()

    if not rows:
        return None

    hdr_idx = 0
    for i, row in enumerate(rows[:5]):
        row_low = [str(c or "").strip().lower().replace(" ", "_") for c in row]
        if any("publisher" in c or "product_name" in c or "lwin" in c for c in row_low):
            hdr_idx = i
            break

    raw_headers = [str(c or "").strip() for c in rows[hdr_idx]]
    pub_col = _col_idx(raw_headers, "publisher")
    if pub_col is None:
        return None

    for row in rows[hdr_idx + 1: hdr_idx + 8]:
        if pub_col >= len(row):
            continue
        pub = str(row[pub_col] or "").strip().lower()
        if not pub:
            continue
        if "robert parker" in pub or "wine advocate" in pub:
            return "robertparker"
        if "jancis" in pub:
            return "jancisrobinson"
        if "james suckling" in pub:
            return "jamessuckling"

    return None


# ─── Fill ─────────────────────────────────────────────────────────────────────

def fill_xlsx(template_bytes: bytes, results: List[Dict]) -> bytes:
    """
    Fill review data into the template XLSX and return the bytes.

    Each result dict: { row_idx, found, critic_name, score_20,
                        drink_from, drink_to, date_tasted, note }
    Found rows highlighted green, not-found rows highlighted dark red.
    """
    if not OPENPYXL_OK:
        raise RuntimeError("openpyxl not installed")

    wb = load_workbook(filename=io.BytesIO(template_bytes))
    ws = wb.active

    # Find header row to locate writable columns
    hdr_row_num = 1
    for row in ws.iter_rows(min_row=1, max_row=5):
        vals = [str(c.value or "").strip().lower() for c in row]
        if any("product" in v or "lwin" in v for v in vals):
            hdr_row_num = row[0].row
            break

    col_letter: Dict[str, str] = {}
    claimed: set = set()
    for cell in ws[hdr_row_num]:
        v = str(cell.value or "").strip()
        if not v or cell.column_letter in claimed:
            continue
        v_norm = v.lower().replace(" ", "_").replace("-", "_")
        for field, aliases in _ALIASES.items():
            if field not in col_letter and any(a in v_norm for a in aliases):
                col_letter[field] = cell.column_letter
                claimed.add(cell.column_letter)
                break  # one field per column — prevents "review" matching "review_date" header

    if "place" not in col_letter:
        header_cells = [cell for cell in ws[hdr_row_num] if str(cell.value or "").strip()]
        next_col_idx = max((cell.col_idx for cell in header_cells), default=0) + 1
        place_cell = ws.cell(row=hdr_row_num, column=next_col_idx or 1)
        place_cell.value = "Place"
        col_letter["place"] = place_cell.column_letter

    found_fill    = PatternFill("solid", fgColor="1a5c2e")   # visible green
    notfound_fill = PatternFill("solid", fgColor="7a1e1e")   # visible red

    WRITE_FIELDS = ["critic", "score", "drink_from", "drink_to", "review_date", "review", "source_url"]

    for res in results:
        r = res["row_idx"]
        if res.get("found"):
            if col_letter.get("critic"):      ws[f"{col_letter['critic']}{r}"]      = res.get("critic_name") or ""
            if col_letter.get("score"):       ws[f"{col_letter['score']}{r}"]       = res.get("score_20")
            if col_letter.get("drink_from"):  ws[f"{col_letter['drink_from']}{r}"]  = res.get("drink_from") or ""
            if col_letter.get("drink_to"):    ws[f"{col_letter['drink_to']}{r}"]    = res.get("drink_to") or ""
            if col_letter.get("review_date"): ws[f"{col_letter['review_date']}{r}"] = res.get("date_tasted") or ""
            if col_letter.get("review"):      ws[f"{col_letter['review']}{r}"]      = res.get("note") or ""
            if col_letter.get("source_url"):  ws[f"{col_letter['source_url']}{r}"]  = res.get("source_url") or ""
            fill = found_fill
        else:
            fill = notfound_fill

        for field in WRITE_FIELDS:
            cl = col_letter.get(field)
            if cl:
                ws[f"{cl}{r}"].fill = fill

    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()


# ─── Job management ───────────────────────────────────────────────────────────

_jobs: Dict[str, dict] = {}
_lock = threading.Lock()


def _format_progress_label(search_name: str, vintage: str) -> str:
    text = str(search_name or "").strip()
    year = str(vintage or "").strip()
    if not year:
        return text
    if re.search(rf"\b{re.escape(year)}\b", text):
        return text
    return f"{text} {year}".strip()


def create_job(
    template_bytes: bytes,
    wines: List[Dict],
    source_key: str = "jancisrobinson",
    sleep_sec: float = 2.5,
    start_item: int = 1,
) -> str:
    total = len(wines)
    start_index = max(0, min(total, int(start_item or 1) - 1))
    initial_found = sum(1 for w in wines[:start_index] if w.get("prefilled"))
    job_id = str(uuid.uuid4())
    with _lock:
        _jobs[job_id] = {
            "status":         "pending",
            "total":          total,
            "done":           start_index,
            "found":          initial_found,
            "stop_requested": False,
            "auto_stopped":   False,
            "source":         source_key,
            "sleep_sec":      float(sleep_sec),
            "start_item":     start_index + 1,
            "start_index":    start_index,
            "initial_found":  initial_found,
            "wines":          wines,
            "template_bytes": template_bytes,
            "results":        [],
            "output_bytes":   None,
            "error":          None,
            "log":            [],
        }
    return job_id


def get_job(job_id: str) -> Optional[Dict]:
    with _lock:
        j = _jobs.get(job_id)
        if not j:
            return None
        return {
            "status": j["status"],
            "total":  j["total"],
            "done":   j["done"],
            "found":  j["found"],
            "start_item": int(j.get("start_item") or 1),
            "stop_requested": bool(j.get("stop_requested")),
            "auto_stopped": bool(j.get("auto_stopped")),
            "pct":    round(j["done"] / j["total"] * 100, 1) if j["total"] else 0,
            "ready":  j["output_bytes"] is not None,
            "error":  j["error"],
            "source": j.get("source"),
            "sleep_sec": j.get("sleep_sec"),
            "log":    j["log"][-60:],
        }


def get_job_output(job_id: str) -> Optional[bytes]:
    with _lock:
        j = _jobs.get(job_id)
        return j["output_bytes"] if j else None


def request_stop(job_id: str) -> bool:
    with _lock:
        j = _jobs.get(job_id)
        if not j:
            return False
        j["stop_requested"] = True
        if j["status"] == "pending":
            j["status"] = "stopped"
            j["output_bytes"] = fill_xlsx(j["template_bytes"], j["results"])
            j["done"] = int(j.get("start_index") or 0) + len(j["results"])
            j["found"] = int(j.get("initial_found") or 0) + sum(1 for r in j["results"] if r.get("found"))
        return True


def resume_job(job_id: str) -> tuple[bool, str]:
    with _lock:
        j = _jobs.get(job_id)
        if not j:
            return False, "not_found"
        if j["status"] in ("running", "pending"):
            return False, "already_running"
        if int(j.get("done") or 0) >= int(j.get("total") or 0):
            return False, "already_done"

        j["status"] = "pending"
        j["stop_requested"] = False
        j["auto_stopped"] = False
        j["error"] = None
        j["output_bytes"] = None
        return True, "ok"


# ─── Background runner ────────────────────────────────────────────────────────

def _parse_date(dt_str: str) -> Optional[datetime]:
    """Parse review date string into datetime for sorting."""
    for fmt in ("%d %b %Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(dt_str.strip(), fmt)
        except (ValueError, AttributeError):
            pass
    return None


def _is_network_error(exc: Exception) -> bool:
    """Best-effort detector for transient connectivity errors from source scrapers."""
    if isinstance(exc, (ConnectionError, TimeoutError, OSError)):
        return True
    name = type(exc).__name__.lower()
    text = str(exc).lower()
    markers = (
        "connectionerror",
        "newconnectionerror",
        "maxretryerror",
        "proxyerror",
        "name or service not known",
        "temporary failure in name resolution",
        "failed to establish a new connection",
        "read timed out",
        "connect timeout",
        "network is unreachable",
        "dns",
    )
    return any(m in name or m in text for m in markers)


def _is_access_blocked_error(exc: Exception) -> bool:
    text = str(exc).lower()
    markers = (
        "blocked by cloudflare",
        "cf_clearance",
        "cloudflare challenge",
        "access blocked",
    )
    return any(marker in text for marker in markers)


def _result_note_group_key(result: Dict) -> str:
    note = str(result.get("note") or "").strip()
    if len(note) < 40:
        return ""
    reviewer = str(result.get("critic_name") or "").strip().lower()
    date_tasted = str(result.get("date_tasted") or "").strip()
    if not reviewer and not date_tasted:
        return ""
    fp = note[:120].lower().replace(" ", "").replace(",", "")
    return f"{fp}|{date_tasted}|{reviewer}"


def _strip_duplicate_result_notes(results: List[Dict]) -> int:
    groups: Dict[str, List[int]] = {}
    for idx, result in enumerate(results):
        if not result.get("found"):
            continue
        key = _result_note_group_key(result)
        if not key:
            continue
        groups.setdefault(key, []).append(idx)

    cleared = 0
    for indexes in groups.values():
        wine_keys = {
            (
                str(results[i].get("_lwin") or "").strip(),
                str(results[i].get("_wine_name") or "").strip().lower(),
                str(results[i].get("_vintage") or "").strip(),
            )
            for i in indexes
        }
        if len(indexes) < 2 or len(wine_keys) < 2:
            continue
        for i in indexes:
            note = str(results[i].get("note") or "").strip()
            if note:
                results[i]["note"] = ""
                cleared += 1
    return cleared


def _save_found_results_to_db(results: List[Dict], source_key: str, job_id: str) -> int:
    from models.wine_model import upsert_review_wine

    saved = 0
    for result in results:
        if not result.get("found"):
            continue
        upsert_review_wine(
            result.get("_lwin") or "",
            {
                "name": result.get("_wine_name") or "",
                "vintage": result.get("_vintage") or None,
            },
            {
                "source": source_key,
                "score_20": result.get("_score_20_db"),
                "score_100": result.get("_score_100_db"),
                "reviewer": result.get("critic_name") or None,
                "note": result.get("note") or "",
                "date_tasted": result.get("date_tasted") or None,
                "drink_from": result.get("drink_from") or None,
                "drink_to": result.get("drink_to") or None,
                "review_url": result.get("source_url") or None,
            },
            upload_batch=f"xlsx_{job_id[:8]}",
        )
        saved += 1
    return saved


def run_job(job_id: str, source_key: str = "jancisrobinson", sleep_sec: float = 2.5):
    """
    Background thread: search the selected source for each wine and fill results.
    """
    from services.session_service import get_session

    with _lock:
        j = _jobs.get(job_id)
        if not j:
            return
        if j.get("status") == "running":
            return
        wines = list(j["wines"])
        template_bytes = j["template_bytes"]
        source_key = source_key or (j.get("source") or "jancisrobinson")
        sleep_sec = float(sleep_sec or j.get("sleep_sec") or 2.5)
        results: List[Dict] = list(j.get("results") or [])
        start_index = int(j.get("start_index") or 0)
        initial_found = int(j.get("initial_found") or 0)
        j["status"] = "running"
        j["source"] = source_key
        j["sleep_sec"] = float(sleep_sec)
        j["error"] = None
        j["stop_requested"] = False
        j["auto_stopped"] = False
        j["output_bytes"] = None
        j["done"] = start_index + len(results)
        j["found"] = initial_found + sum(1 for r in results if r.get("found"))

    source_label = source_key
    try:
        from config.sources import SOURCES
        source_label = SOURCES.get(source_key, {}).get("short") or source_key
    except Exception:
        pass

    def log(msg: str):
        with _lock:
            if job_id in _jobs:
                _jobs[job_id]["log"].append(msg)

    session = get_session(source_key)
    if not session:
        with _lock:
            _jobs[job_id]["status"] = "error"
            _jobs[job_id]["error"] = f"No session for '{source_key}' - upload cookies in Settings first"
        return

    processed = start_index + len(results)
    saved = 0
    stopped = False
    auto_stopped = False
    consecutive_net_errors = 0
    reset_note_tracking()

    if processed > 0:
        log(f"Resuming from row {processed + 1}/{len(wines)}...")

    for idx, wine in enumerate(wines[processed:], start=processed + 1):
        with _lock:
            if job_id not in _jobs:
                return
            if _jobs[job_id].get("stop_requested"):
                stopped = True
        if stopped:
            log("Stop requested. Finishing current progress for download...")
            break

        name = wine["name"]
        search_name = wine.get("raw_name") if source_key == "decanter" and wine.get("raw_name") else name
        vintage = wine["vintage"]
        lwin = wine["lwin"]

        log(f"[{idx}/{len(wines)}] {_format_progress_label(search_name, vintage or 'NV')}")

        try:
            if source_key == "jancisrobinson":
                from maaike_phase1 import search_wine
                reviews = search_wine(session, search_name, vintage, lwin, wine.get("search_hints"))
            else:
                from services.enrich_service import _search_source
                reviews = _search_source(source_key, session, search_name, vintage, lwin, sleep_sec, wine.get("search_hints"))

            if reviews:
                consecutive_net_errors = 0
                normalized_reviews = [
                    normalize_review(
                        source_key,
                        review,
                        wine_name=name,
                        flag_duplicate_notes=False,
                    )
                    for review in reviews
                ]
                reviews_dated = [(r, _parse_date(r.get("date_tasted") or "")) for r in normalized_reviews]
                reviews_dated.sort(key=lambda x: x[1] or datetime.min, reverse=True)
                rev = reviews_dated[0][0]

                reviewer = rev.get("reviewer") or rev.get("critic_name") or ""
                note = rev.get("note") or rev.get("tasting_note") or ""
                date_tasted = rev.get("date_tasted") or rev.get("review_date") or ""

                score_20 = rev.get("score_20")
                score_100 = rev.get("score_100")
                if score_100 is None:
                    score_100 = rev.get("score_native")
                if score_100 is None:
                    score_100 = rev.get("score")
                out_score = score_20 if score_20 is not None else score_100

                results.append({
                    "row_idx": wine["row_idx"],
                    "found": True,
                    "critic_name": reviewer,
                    "score_20": out_score,
                    "drink_from": rev.get("drink_from") or "",
                    "drink_to": rev.get("drink_to") or "",
                    "date_tasted": date_tasted,
                    "note": note,
                    "source_url": rev.get("review_url") or rev.get("source_url") or "",
                    "_wine_name": name,
                    "_vintage": vintage,
                    "_lwin": lwin,
                    "_score_20_db": score_20,
                    "_score_100_db": score_100,
                })
                scale = "/20" if score_20 is not None else "/100"
                score_log = out_score if out_score is not None else "?"
                log(f"  + score {score_log}{scale} | {date_tasted or '-'}")
            else:
                consecutive_net_errors = 0
                results.append({"row_idx": wine["row_idx"], "found": False})
                log(f"  - not found on {source_label}")

        except Exception as e:
            results.append({"row_idx": wine["row_idx"], "found": False})
            log(f"  x error: {type(e).__name__}: {e}")
            if _is_access_blocked_error(e):
                auto_stopped = True
                stopped = True
                log("Auto-stop: JR access is blocked by Cloudflare. Upload fresh browser cookies including cf_clearance, then Resume.")
            elif _is_network_error(e):
                consecutive_net_errors += 1
                log(f"  - network error streak: {consecutive_net_errors}")
                if consecutive_net_errors >= 3:
                    auto_stopped = True
                    stopped = True
                    log("Auto-stop: network unavailable. Resume when internet is back.")
            else:
                consecutive_net_errors = 0

        with _lock:
            if job_id in _jobs:
                _jobs[job_id]["done"] = start_index + len(results)
                _jobs[job_id]["found"] = initial_found + sum(1 for r in results if r.get("found"))
                _jobs[job_id]["results"] = list(results)
                if _jobs[job_id].get("stop_requested"):
                    stopped = True
        if stopped:
            if auto_stopped:
                log("Generating partial XLSX after auto-stop...")
            else:
                log("Stop requested. Generating partial XLSX...")
            break

        time.sleep(sleep_sec)

    duplicate_notes_cleared = _strip_duplicate_result_notes(results)
    if duplicate_notes_cleared:
        log(f"Cleared {duplicate_notes_cleared} duplicate session note(s) from XLSX results.")

    try:
        saved = _save_found_results_to_db(results, source_key, job_id)
    except Exception as db_err:
        log(f"  - DB save error: {type(db_err).__name__}: {db_err}")

    log("Generating filled XLSX...")
    try:
        output_bytes = fill_xlsx(template_bytes, results)
    except Exception as e:
        with _lock:
            _jobs[job_id]["status"] = "error"
            _jobs[job_id]["error"] = f"Failed to generate XLSX: {e}"
        return

    total = len(wines)
    found = initial_found + sum(1 for r in results if r.get("found"))
    pct = round(found / total * 100, 1) if total else 0
    log(f"Saved to DB: {saved}/{found} found review(s)")
    if stopped:
        if auto_stopped:
            log(f"Auto-stopped - processed {start_index + len(results)}/{total}, found {found}. Click Download or Resume.")
        else:
            log(f"Stopped - processed {start_index + len(results)}/{total}, found {found}. Click Download.")
    else:
        log(f"Done - {found}/{total} found ({pct}%). Click Download.")

    with _lock:
        _jobs[job_id]["status"] = "stopped" if stopped else "done"
        _jobs[job_id]["output_bytes"] = output_bytes
        _jobs[job_id]["done"] = start_index + len(results)
        _jobs[job_id]["found"] = found
        _jobs[job_id]["results"] = list(results)
        _jobs[job_id]["auto_stopped"] = auto_stopped
