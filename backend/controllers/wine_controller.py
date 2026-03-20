"""
controllers/wine_controller.py
================================
HTTP request/response handling for wine endpoints.
Think of this like your Express controller functions —
reads req, calls service/model, sends res.

No business logic here — just parse → call → respond.
"""
from __future__ import annotations

import csv
import io
import re
import sqlite3
from datetime import datetime

from flask import jsonify, request, Response

from config.sources import SOURCES
from models.wine_model import (
    find_wines, find_wine_by_id, create_wine, update_wine,
    delete_wine, find_reviews_for_wine, get_stats,
    get_filter_options, get_wines_for_export, upsert_review_wine,
)
from services.enrich_service import enrich_one


ALLOWED_SORTS = {
    "name","vintage","maaike_score_20","maaike_reviewer","maaike_drink_from",
    "maaike_drink_to","region","colour","price_eur","maaike_review_count",
    "maaike_note_length","created_at","lwin11","maaike_date_tasted","maaike_best_source"
}

SOURCE_LABELS = {
    "jancisrobinson": "Jancis Robinson",
    "robertparker":   "Robert Parker Wine Advocate",
    "jamessuckling":  "James Suckling",
    "decanter":       "Decanter",
}


# ─── Wine list ────────────────────────────────────────────────────────────────

def list_wines():
    page     = int(request.args.get("page", 1))
    per_page = min(int(request.args.get("per_page", 50)), 200)
    sort     = request.args.get("sort", "maaike_score_20")
    sort_dir = "DESC" if request.args.get("dir", "desc").lower() == "desc" else "ASC"

    if sort not in ALLOWED_SORTS:
        sort = "maaike_score_20"

    result = find_wines(
        filters=request.args.to_dict(),
        sort=sort, direction=sort_dir,
        page=page, per_page=per_page,
    )
    return jsonify(result)


# ─── Single wine ──────────────────────────────────────────────────────────────

def get_wine(wine_id: int):
    wine = find_wine_by_id(wine_id)
    if not wine:
        return jsonify({"error": "Not found"}), 404
    return jsonify(wine)


def patch_wine(wine_id: int):
    data = request.get_json(silent=True) or {}
    updated = update_wine(wine_id, data)
    if updated == 0:
        return jsonify({"error": "Nothing to update or wine not found"}), 400
    return jsonify({"ok": True})


def remove_wine(wine_id: int):
    delete_wine(wine_id)
    return jsonify({"ok": True})


# ─── Add wine ─────────────────────────────────────────────────────────────────

def add_wine():
    import threading
    data    = request.get_json(silent=True) or {}
    name    = (data.get("name") or "").strip()
    vintage = (data.get("vintage") or "").strip()
    lwin    = (data.get("lwin") or "").strip()

    if not name:
        return jsonify({"error": "Wine name is required"}), 400

    lwin11 = _parse_lwin11(lwin)

    try:
        wine_id = create_wine({
            "name": name, "vintage": vintage or None,
            "lwin": lwin or None, "lwin11": lwin11 or None,
            "price_eur": data.get("price_eur"),
            "price_usd": data.get("price_usd"),
            "region": data.get("region"),
            "country": data.get("country"),
            "colour": data.get("colour"),
            "stock": data.get("stock"),
            "added_manually": 1,
        })
    except sqlite3.IntegrityError:
        return jsonify({"error": "Duplicate wine"}), 409

    if data.get("auto_enrich", True):
        from services.enrich_service import state as enrich_state
        if not enrich_state["running"]:
            threading.Thread(
                target=enrich_one,
                args=(wine_id, name, vintage, lwin),
                daemon=True,
            ).start()

    return jsonify({"ok": True, "wine_id": wine_id, "status": "pending"})


# ─── Wine reviews ─────────────────────────────────────────────────────────────

def get_wine_reviews(wine_id: int):
    source = request.args.get("source", "")
    reviews = find_reviews_for_wine(wine_id, source)
    return jsonify({"reviews": reviews, "count": len(reviews)})


# ─── Re-enrich single wine ────────────────────────────────────────────────────

def trigger_enrich(wine_id: int):
    """Re-search reviews for a single wine (called from the WineModal button).
    Clears existing reviews first so stale/wrong data never persists."""
    import threading
    wine = find_wine_by_id(wine_id)
    if not wine:
        return jsonify({"error": "Not found"}), 404

    def _run():
        enrich_one(wine["id"], wine["name"],
                   wine.get("vintage") or "",
                   wine.get("lwin") or "",
                   clear_first=True)

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"ok": True})


# ─── Stats & filters ──────────────────────────────────────────────────────────

def stats():
    from services.enrich_service import state as enrich_state
    data = get_stats()
    data["enrichment_running"] = enrich_state["running"]
    return jsonify(data)


def filter_options():
    source = request.args.get("source", "")
    return jsonify(get_filter_options(source=source))


# ─── Sources ──────────────────────────────────────────────────────────────────

def list_sources():
    return jsonify({"sources": SOURCES})


# ─── CSV upload ───────────────────────────────────────────────────────────────

def upload_csv():
    """
    Rue Pinard CSV upload.

    LWIN18 structure:  LWIN + wine(7) + vintage(4) + qty(2) + size(5)
    Example:           LWIN 1269071 2016 06 00750
                            LWIN7   VTG  QT SIZE

    The same wine+vintage can appear many times with different pack sizes
    (1x75cl, 3x75cl, 6x75cl, magnums, etc.).

    Strategy:
      1. Parse every row into a normalised dict
      2. Group by LWIN11 (= LWIN7 + vintage) — one DB row per wine+vintage
      3. Within each group, pick the CANONICAL row:
           - Prefer 1x75cl (qty=01, size=00750)
           - Fall back to smallest qty, then smallest pack price
      4. Store the LWIN18 of the canonical (75cl single bottle) row
      5. Upsert: INSERT if new LWIN11, UPDATE price/stock/url if changed
    """
    import sqlite3 as sl
    from collections import defaultdict

    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400
    f = request.files["file"]
    if not f.filename.lower().endswith(".csv"):
        return jsonify({"error": "Only CSV files accepted"}), 400

    content = f.read().decode("utf-8-sig", errors="replace")
    reader  = csv.DictReader(io.StringIO(content))

    # ── Column name normalisation ──────────────────────────────────────────────
    def norm(s): return re.sub(r"[^a-z0-9]", "_", s.strip().lower())

    NAME_COLS   = ["wine_name","wine","name","product","product_name","description"]
    VINTAGE_COLS= ["vintage","year","millesime"]
    LWIN_COLS   = ["lwin","lwin_code","lwin18","lwin11","lwin7"]
    SIZE_COLS   = ["unit_size","size","format","unit-size","unit_size"]
    PRICE_COLS  = ["price","price_eur","price_euro","eur","selling_price_eur"]
    PRICE_USD   = ["price_usd","usd"]
    STOCK_COLS  = ["stock","stock_level","qty","quantity","available","stock_level"]
    URL_COLS    = ["url","link","product_url","supplier_url"]
    REGION_COLS = ["region","appellation","area"]
    COLOUR_COLS = ["colour","color","type","wine_type"]

    def find(headers, cands):
        for c in cands:
            if c in headers: return c
        return None

    # ── Step 1: parse all rows ─────────────────────────────────────────────────
    raw_rows = list(reader)
    if not raw_rows:
        return jsonify({"error": "Empty CSV"}), 400

    # Build header lookup once
    sample_headers = {norm(k): k for k in raw_rows[0].keys()}

    def get_col(row, cands):
        col = find(sample_headers, cands)
        return row.get(sample_headers[col], "").strip() if col else ""

    def parse_lwin18(lwin_str):
        """Return (lwin7, vintage, qty_str, size_str, lwin11, lwin18_digits)"""
        digits = re.sub(r"[^0-9]", "", lwin_str.upper().replace("LWIN", ""))
        if len(digits) < 11:
            return None
        lwin7   = digits[:7]
        vintage = digits[7:11] if len(digits) >= 11 else ""
        qty     = digits[11:13] if len(digits) >= 13 else "01"
        size    = digits[13:18] if len(digits) >= 18 else "00750"
        lwin11  = digits[:11]
        return {"lwin7": lwin7, "vintage_lwin": vintage, "qty": qty,
                "size": size, "lwin11": lwin11, "lwin18": digits}

    def parse_price(price_str):
        """Parse '€1.165,00' or '1165.00' → float or None"""
        s = re.sub(r"[€$£\s]", "", price_str)
        # European format: 1.165,00 → 1165.00
        if re.search(r"\d\.\d{3},", s):
            s = s.replace(".", "").replace(",", ".")
        elif "," in s and "." not in s:
            s = s.replace(",", ".")
        elif "," in s:
            s = s.replace(",", "")
        try: return float(s)
        except: return None

    def row_score(r):
        """
        Lower = more preferred.
        Prefer: standard 75cl, single bottle, lowest price.
        """
        size = r.get("size", "00750")
        qty  = r.get("qty", "01")
        is_standard = 1 if size == "00750" else 2    # 75cl preferred
        is_single   = 1 if qty  == "01"   else 2    # single bottle preferred
        price       = r.get("price_num") or 9999
        # Normalise price to per-bottle equivalent
        qty_num = max(1, int(qty)) if qty.isdigit() else 1
        per_bottle = price / qty_num
        return (is_standard, is_single, per_bottle)

    # ── Step 2: parse & group by LWIN11 ───────────────────────────────────────
    groups = defaultdict(list)
    parse_errors = 0

    for row in raw_rows:
        try:
            lwin_raw = get_col(row, LWIN_COLS)
            parsed   = parse_lwin18(lwin_raw)
            if not parsed:
                parse_errors += 1
                continue

            name = get_col(row, NAME_COLS)
            if not name:
                continue

            # Vintage from CSV column takes priority, fall back to LWIN
            vintage_raw = get_col(row, VINTAGE_COLS)
            m = re.search(r"\b(19\d{2}|20[0-3]\d)\b", vintage_raw)
            vintage = m.group(1) if m else parsed["vintage_lwin"]

            price_str = get_col(row, PRICE_COLS)
            price_num = parse_price(price_str)

            groups[parsed["lwin11"]].append({
                **parsed,
                "name":         name,
                "vintage":      vintage,
                "price_raw":    price_str,
                "price_num":    price_num,
                "unit_size":    get_col(row, SIZE_COLS),
                "price_usd":    get_col(row, PRICE_USD) or None,
                "stock":        get_col(row, STOCK_COLS) or None,
                "supplier_url": get_col(row, URL_COLS)   or None,
                "region":       get_col(row, REGION_COLS) or None,
                "colour":       get_col(row, COLOUR_COLS) or None,
            })
        except Exception:
            parse_errors += 1

    # ── Step 3 & 4: pick canonical row per LWIN11, upsert ─────────────────────
    inserted = updated = skipped = errors = 0

    for lwin11, candidates in groups.items():
        try:
            # Pick the best row (1x75cl preferred)
            canonical = min(candidates, key=row_score)

            # Build clean price string from the canonical row
            price_eur = canonical["price_raw"] or None

            wine_data = {
                "name":         canonical["name"],
                "vintage":      canonical["vintage"] or None,
                "lwin":         "LWIN" + canonical["lwin18"],
                "lwin11":       lwin11,
                "unit_size":    canonical["unit_size"] or None,
                "price_eur":    price_eur,
                "price_usd":    canonical["price_usd"],
                "stock":        canonical["stock"],
                "supplier_url": canonical["supplier_url"],
                "region":       canonical["region"],
                "colour":       canonical["colour"],
            }

            try:
                create_wine(wine_data)
                inserted += 1
            except sl.IntegrityError:
                # Already exists — update price/stock/url in case they changed
                from models.wine_model import update_wine_supply
                update_wine_supply(lwin11, {
                    "price_eur":    price_eur,
                    "price_usd":    canonical["price_usd"],
                    "stock":        canonical["stock"],
                    "supplier_url": canonical["supplier_url"],
                })
                updated += 1

        except Exception:
            errors += 1

    total_csv_rows   = len(raw_rows)
    total_lwin11     = len(groups)
    pack_size_dupes  = total_csv_rows - total_lwin11

    return jsonify({
        "ok":       True,
        "inserted": inserted,
        "updated":  updated,       # existing wines refreshed with new price/stock
        "errors":   errors + parse_errors,
        "total_csv_rows":  total_csv_rows,
        "total_wines":     total_lwin11,
        "pack_size_dupes": pack_size_dupes,  # rows skipped because different pack size, same wine
        # legacy fields so frontend still works
        "dupes":    updated,
        "total":    total_csv_rows,
    })


# ─── Review CSV upload (maaike export format) ────────────────────────────────

def upload_reviews_csv():
    """
    Upload a review CSV in the standard MAAIKE export format:
    Publisher, LWIN, Product_Name, Vintage, Critic_Name,
    Score_20, Score_100, Drink_From, Drink_To, Review_Date, Review

    Wines are merged into the main table by full LWIN.
    Each row creates/updates one wine + one review.
    data_origin is set to 'uploaded_review'.
    enrichment_status:
      - 'downloaded' if has score + non-empty note
      - 'found'      if has score but no note
    """
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400
    f = request.files["file"]
    if not f.filename.lower().endswith(".csv"):
        return jsonify({"error": "Only CSV files accepted"}), 400

    from datetime import datetime as _dt
    upload_batch = f.filename + "_" + _dt.now().strftime("%Y%m%d_%H%M%S")

    content = f.read().decode("utf-8-sig", errors="replace")
    reader  = csv.DictReader(io.StringIO(content))

    # Normalise headers
    def nh(s): return re.sub(r"[^a-z0-9_]", "_", s.strip().lower()).strip("_")

    inserted = updated = errors = 0

    for row in reader:
        try:
            row_norm = {nh(k): (v or "").strip() for k, v in row.items()}

            lwin_full = row_norm.get("lwin", "")
            if not lwin_full:
                errors += 1
                continue

            # Normalise LWIN: ensure it starts with LWIN
            if not lwin_full.upper().startswith("LWIN"):
                lwin_full = "LWIN" + re.sub(r"[^0-9]", "", lwin_full)

            # Publisher → source key
            publisher = row_norm.get("publisher", "")
            source = _publisher_to_source(publisher)

            # Scores
            def safe_float(s):
                try: return float(s) if s else None
                except: return None

            score_20  = safe_float(row_norm.get("score_20"))
            score_100 = safe_float(row_norm.get("score_100"))
            drink_from = safe_float(row_norm.get("drink_from"))
            drink_to   = safe_float(row_norm.get("drink_to"))

            wine_data = {
                "name":    row_norm.get("product_name") or row_norm.get("name", ""),
                "vintage": row_norm.get("vintage") or None,
            }
            review_data = {
                "source":     source,
                "score_20":   score_20,
                "score_100":  score_100,
                "reviewer":   row_norm.get("critic_name") or row_norm.get("reviewer", ""),
                "note":       row_norm.get("review") or row_norm.get("note", ""),
                "date_tasted":row_norm.get("review_date") or row_norm.get("date_tasted", ""),
                "drink_from": int(drink_from) if drink_from else None,
                "drink_to":   int(drink_to)   if drink_to   else None,
            }

            result = upsert_review_wine(lwin_full, wine_data, review_data, upload_batch)
            if result["action"] == "inserted":
                inserted += 1
            else:
                updated += 1

        except Exception as e:
            errors += 1

    return jsonify({
        "ok":       True,
        "inserted": inserted,
        "updated":  updated,
        "errors":   errors,
        "total":    inserted + updated + errors,
        "upload_batch": upload_batch,
    })


def _publisher_to_source(publisher: str) -> str:
    """Map human-readable publisher name to source key."""
    p = publisher.strip().lower()
    if "jancis" in p:    return "jancisrobinson"
    if "parker" in p or "rpwa" in p or "wine advocate" in p: return "robertparker"
    if "suckling" in p:  return "jamessuckling"
    if "decanter" in p:  return "decanter"
    # Try to build a key from the name
    return re.sub(r"[^a-z0-9]", "", p) or "unknown"


# ─── CSV download ─────────────────────────────────────────────────────────────

def download_csv():
    """
    Export in the canonical MAAIKE format:
    Publisher, LWIN, Product_Name, Vintage, Critic_Name,
    Score_20, Score_100, Drink_From, Drink_To, Review_Date, Review

    One row per wine. LWIN is the full 22-char LWIN (e.g. LWIN101390620060600750).
    Only wines with a score AND note (enrichment_status=downloaded) by default,
    unless status filter overrides this.
    """
    rows = get_wines_for_export(request.args.to_dict())
    out  = io.StringIO()
    w    = csv.writer(out)
    # Exact header matching the CSV format
    w.writerow(["Publisher","LWIN","Product_Name","Vintage",
                "Critic_Name","Score_20","Score_100",
                "Drink_From","Drink_To","Review_Date","Review"])

    seen_lwin = set()   # deduplicate by full LWIN

    for r in rows:
        lwin_full = r["lwin"] or ""
        if lwin_full and lwin_full in seen_lwin:
            continue
        if lwin_full:
            seen_lwin.add(lwin_full)

        w.writerow([
            SOURCE_LABELS.get(r["source"] or "", r["source"] or ""),
            lwin_full,
            r["name"] or "",
            r["vintage"] or "NV",
            r["reviewer"] or "",
            r["score_20"]  if r["score_20"]  is not None else "",
            r["score_100"] if r["score_100"] is not None else "",
            r["drink_from"] if r["drink_from"] and r["drink_from"] != 1900 else "",
            r["drink_to"]   if r["drink_to"]   and r["drink_to"]   != 1900 else "",
            r["date_tasted"] or "",
            (r["note"] or "").strip(),
        ])

    out.seek(0)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Response(out.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename=maaike_{ts}.csv"})


# ─── Admin ────────────────────────────────────────────────────────────────────

def reset_not_found():
    from models.wine_model import reset_not_found as _reset
    n = _reset()
    return jsonify({"ok": True, "reset": n})


def reset_found():
    """Reset all 'found' wines (score but no note) back to pending for re-enrichment."""
    from models.wine_model import reset_found as _reset
    n = _reset()
    return jsonify({"ok": True, "reset": n})


# ─── Helper ───────────────────────────────────────────────────────────────────

def _parse_lwin11(lwin: str) -> str:
    if not lwin:
        return ""
    digits = re.sub(r"[^0-9]", "", lwin.upper().replace("LWIN", ""))
    return digits[:11] if len(digits) >= 11 else ""


def wipe_all_wines():
    """DELETE all wines and reviews — clean slate before re-import."""
    from models.wine_model import wipe_all_wines as _wipe
    count = _wipe()
    return jsonify({"ok": True, "deleted": count})


def fix_notes():
    """Strip duplicate session-level notes from the DB (JR batch tasting bug fix)."""
    from models.wine_model import fix_duplicate_notes
    result = fix_duplicate_notes()
    return jsonify({"ok": True, **result})