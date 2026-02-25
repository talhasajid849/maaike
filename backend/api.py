#!/usr/bin/env python3
"""
MAAIKE API — Multi-source wine review intelligence
Sources: JancisRobinson (live), + extendable to more
"""
from __future__ import annotations

import csv
import io
import json
import os
import re
import sqlite3
import threading
import time
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Any, Dict, List

from flask import Flask, Response, jsonify, redirect, request, send_from_directory
from flask_socketio import SocketIO

BASE_DIR    = Path(__file__).parent
DATA_DIR    = BASE_DIR / "data"
DB_PATH     = DATA_DIR / "maaike.db"
COOKIE_PATH = BASE_DIR / "real_cookies.json"
STATIC_DIR  = BASE_DIR.parent / "frontend"

DATA_DIR.mkdir(parents=True, exist_ok=True)

API_KEY    = os.environ.get("MAAIKE_API_KEY", "rue-pinard-2025")
SECRET_KEY = os.environ.get("FLASK_SECRET", "maaike-secret-2025")

app = Flask(__name__, static_folder=str(STATIC_DIR))
app.config["SECRET_KEY"] = SECRET_KEY
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading",
                    logger=False, engineio_logger=False)

enrich_state = {
    "running": False, "thread": None, "stop_flag": False,
    "total": 0, "done": 0, "found": 0, "errors": 0,
}

# ─── Sources Registry ─────────────────────────────────────────────────────────
# Add new review sources here. Each entry is a dict with metadata.
SOURCES = {
    "jancisrobinson": {
        "name":    "Jancis Robinson",
        "url":     "https://www.jancisrobinson.com",
        "enabled": True,
        "icon":    "🍷",
        "color":   "#00bfa5",
        "needs_cookies": True,
    },
    # Future sources — add here when ready:
    # "decanter": {
    #     "name": "Decanter", "url": "https://www.decanter.com",
    #     "enabled": False, "icon": "📰", "color": "#388bfd", "needs_cookies": False,
    # },
    # "wine_spectator": {
    #     "name": "Wine Spectator", "url": "https://www.winespectator.com",
    #     "enabled": False, "icon": "🏆", "color": "#d29922", "needs_cookies": False,
    # },
}

# ─── DB ───────────────────────────────────────────────────────────────────────

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS wines (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                name                TEXT NOT NULL,
                vintage             TEXT,
                unit_size           TEXT,
                price_eur           TEXT,
                price_usd           TEXT,
                stock               TEXT,
                supplier_url        TEXT,
                region              TEXT,
                country             TEXT,
                colour              TEXT,
                appellation         TEXT,
                lwin                TEXT,
                lwin7               TEXT,
                -- Best review summary (for fast table display)
                maaike_score        REAL,
                maaike_score_20     REAL,
                maaike_reviewer     TEXT,
                maaike_short_quote  TEXT,
                maaike_note_length  INTEGER,
                maaike_drink_from   INTEGER,
                maaike_drink_to     INTEGER,
                maaike_review_url   TEXT,
                maaike_date_tasted  TEXT,
                maaike_colour       TEXT,
                maaike_review_count INTEGER DEFAULT 0,
                maaike_jr_lwin      TEXT,
                enrichment_status   TEXT DEFAULT 'pending',
                added_manually      INTEGER DEFAULT 0,
                created_at          TEXT DEFAULT (datetime('now')),
                updated_at          TEXT DEFAULT (datetime('now')),
                UNIQUE(name, vintage)
            );

            -- All individual reviews per wine per source
            CREATE TABLE IF NOT EXISTS reviews (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                wine_id       INTEGER NOT NULL REFERENCES wines(id) ON DELETE CASCADE,
                source        TEXT NOT NULL DEFAULT 'jancisrobinson',
                score_20      REAL,
                score_100     REAL,
                reviewer      TEXT,
                note          TEXT,
                note_length   INTEGER,
                drink_from    INTEGER,
                drink_to      INTEGER,
                date_tasted   TEXT,
                review_url    TEXT,
                colour        TEXT,
                wine_name_src TEXT,
                jr_lwin       TEXT,
                created_at    TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_wines_status    ON wines(enrichment_status);
            CREATE INDEX IF NOT EXISTS idx_wines_lwin      ON wines(lwin7);
            CREATE INDEX IF NOT EXISTS idx_wines_score     ON wines(maaike_score_20);
            CREATE INDEX IF NOT EXISTS idx_reviews_wine    ON reviews(wine_id);
            CREATE INDEX IF NOT EXISTS idx_reviews_source  ON reviews(source);
            CREATE INDEX IF NOT EXISTS idx_reviews_date    ON reviews(date_tasted);
            CREATE INDEX IF NOT EXISTS idx_reviews_score   ON reviews(score_20);
            CREATE INDEX IF NOT EXISTS idx_reviews_reviewer ON reviews(reviewer);
        """)

        # Migrate existing DBs
        existing = {r[1] for r in conn.execute("PRAGMA table_info(wines)").fetchall()}
        for col, sql in [
            ("lwin",               "ALTER TABLE wines ADD COLUMN lwin TEXT"),
            ("lwin7",              "ALTER TABLE wines ADD COLUMN lwin7 TEXT"),
            ("maaike_note_length", "ALTER TABLE wines ADD COLUMN maaike_note_length INTEGER"),
            ("maaike_review_count","ALTER TABLE wines ADD COLUMN maaike_review_count INTEGER DEFAULT 0"),
            ("maaike_jr_lwin",     "ALTER TABLE wines ADD COLUMN maaike_jr_lwin TEXT"),
            ("added_manually",     "ALTER TABLE wines ADD COLUMN added_manually INTEGER DEFAULT 0"),
        ]:
            if col not in existing:
                conn.execute(sql)


init_db()

# ─── Auth ─────────────────────────────────────────────────────────────────────

def require_api_key(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        key = request.headers.get("X-API-Key")
        if not key: key = request.args.get("api_key")
        if not key and request.is_json:
            try: key = (request.get_json(silent=True) or {}).get("api_key")
            except: pass
        if not key: key = request.form.get("api_key")
        if key != API_KEY:
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated


# ─── Routes ───────────────────────────────────────────────────────────────────

@app.route("/")
def root(): return redirect("/dashboard")

@app.route("/signin")
def signin(): return send_from_directory(str(STATIC_DIR), "signin.html")

@app.route("/dashboard")
def dashboard(): return send_from_directory(str(STATIC_DIR), "dashboard.html")

@app.route("/<path:filename>")
def static_files(filename): return send_from_directory(str(STATIC_DIR), filename)


# ─── Auth API ─────────────────────────────────────────────────────────────────

@app.route("/api/auth", methods=["POST"])
def auth():
    data = request.get_json(silent=True) or {}
    if data.get("api_key") == API_KEY:
        return jsonify({"ok": True})
    return jsonify({"ok": False, "message": "Invalid API key"}), 401


# ─── Sources ──────────────────────────────────────────────────────────────────

@app.route("/api/sources")
@require_api_key
def get_sources():
    return jsonify({"sources": SOURCES})


# ─── Stats ────────────────────────────────────────────────────────────────────

@app.route("/api/stats")
@require_api_key
def stats():
    with get_db() as conn:
        total      = conn.execute("SELECT COUNT(*) FROM wines").fetchone()[0]
        found      = conn.execute("SELECT COUNT(*) FROM wines WHERE enrichment_status='found'").fetchone()[0]
        pending    = conn.execute("SELECT COUNT(*) FROM wines WHERE enrichment_status='pending'").fetchone()[0]
        not_found  = conn.execute("SELECT COUNT(*) FROM wines WHERE enrichment_status='not_found'").fetchone()[0]
        manual     = conn.execute("SELECT COUNT(*) FROM wines WHERE added_manually=1").fetchone()[0]
        avg_row    = conn.execute("SELECT AVG(maaike_score_20) FROM wines WHERE maaike_score_20 IS NOT NULL").fetchone()
        avg_score  = round(avg_row[0], 2) if avg_row[0] else None
        coverage   = round(found / total * 100, 1) if total else 0
        with_note  = conn.execute("SELECT COUNT(*) FROM wines WHERE maaike_short_quote IS NOT NULL AND maaike_short_quote!=''").fetchone()[0]
        with_lwin  = conn.execute("SELECT COUNT(*) FROM wines WHERE lwin IS NOT NULL AND lwin!=''").fetchone()[0]
        total_reviews = conn.execute("SELECT COUNT(*) FROM reviews").fetchone()[0]

        reviewers = conn.execute("""
            SELECT reviewer, COUNT(*) as cnt FROM reviews
            WHERE reviewer IS NOT NULL AND reviewer!=''
            GROUP BY reviewer ORDER BY cnt DESC LIMIT 8
        """).fetchall()

        dist = conn.execute("""
            SELECT CASE
                WHEN score_20 < 15 THEN '<15' WHEN score_20 < 16 THEN '15-16'
                WHEN score_20 < 17 THEN '16-17' WHEN score_20 < 18 THEN '17-18'
                WHEN score_20 < 19 THEN '18-19' ELSE '19-20' END as band,
                COUNT(*) as cnt
            FROM reviews WHERE score_20 IS NOT NULL
            GROUP BY band ORDER BY band
        """).fetchall()

        by_source = conn.execute("""
            SELECT source, COUNT(*) as cnt FROM reviews GROUP BY source ORDER BY cnt DESC
        """).fetchall()

    return jsonify({
        "total": total, "found": found, "pending": pending, "not_found": not_found,
        "manual": manual, "coverage": coverage, "avg_score": avg_score,
        "with_note": with_note, "with_lwin": with_lwin, "total_reviews": total_reviews,
        "reviewers":  [{"name": r["reviewer"], "count": r["cnt"]} for r in reviewers],
        "score_dist": [{"band": d["band"], "count": d["cnt"]} for d in dist],
        "by_source":  [{"source": s["source"], "count": s["cnt"]} for s in by_source],
        "enrichment_running": enrich_state["running"],
    })


# ─── Wines List ───────────────────────────────────────────────────────────────

@app.route("/api/wines")
@require_api_key
def wines():
    page     = int(request.args.get("page", 1))
    per_page = min(int(request.args.get("per_page", 50)), 200)
    offset   = (page - 1) * per_page

    search       = request.args.get("search", "").strip()
    status       = request.args.get("status", "")
    region       = request.args.get("region", "")
    colour       = request.args.get("colour", "")
    vintage      = request.args.get("vintage", "")
    reviewer     = request.args.get("reviewer", "")
    min_score    = request.args.get("min_score", "")
    max_score    = request.args.get("max_score", "")
    lwin_search  = request.args.get("lwin", "").strip()
    min_reviews  = request.args.get("min_reviews", "")
    has_note     = request.args.get("has_note", "")
    min_note_len = request.args.get("min_note_len", "")
    date_from    = request.args.get("date_from", "").strip()
    date_to      = request.args.get("date_to", "").strip()
    review_year  = request.args.get("review_year", "").strip()
    source_filter = request.args.get("source", "")

    sort_by  = request.args.get("sort", "maaike_score_20")
    sort_dir = "DESC" if request.args.get("dir", "desc").lower() == "desc" else "ASC"

    allowed_sorts = {"name","vintage","maaike_score_20","maaike_reviewer","maaike_drink_from",
                     "maaike_drink_to","region","colour","price_eur","maaike_review_count",
                     "maaike_note_length","created_at","lwin7","maaike_date_tasted"}
    if sort_by not in allowed_sorts:
        sort_by = "maaike_score_20"

    conds: List[str] = []
    params: List[Any] = []

    if search:
        conds.append("(w.name LIKE ? OR w.appellation LIKE ? OR w.maaike_reviewer LIKE ? OR w.lwin LIKE ? OR w.lwin7 LIKE ?)")
        s = f"%{search}%"; params += [s, s, s, s, s]
    if lwin_search:
        lc = re.sub(r"[^0-9]", "", lwin_search)
        if lc:
            conds.append("(w.lwin LIKE ? OR w.lwin7 LIKE ? OR w.maaike_jr_lwin LIKE ?)")
            params += [f"%{lc}%", f"%{lc}%", f"%{lc}%"]
    if status:
        conds.append("w.enrichment_status = ?"); params.append(status)
    if region:
        conds.append("(w.region LIKE ? OR w.appellation LIKE ?)"); params += [f"%{region}%", f"%{region}%"]
    if colour:
        conds.append("(w.colour LIKE ? OR w.maaike_colour LIKE ?)"); params += [f"%{colour}%", f"%{colour}%"]
    if vintage:
        conds.append("w.vintage = ?"); params.append(vintage)
    if reviewer:
        conds.append("w.maaike_reviewer LIKE ?"); params.append(f"%{reviewer}%")
    if min_score:
        conds.append("w.maaike_score_20 >= ?"); params.append(float(min_score))
    if max_score:
        conds.append("w.maaike_score_20 <= ?"); params.append(float(max_score))
    if has_note == "1":
        conds.append("w.maaike_short_quote IS NOT NULL AND w.maaike_short_quote != ''")
    if min_reviews:
        conds.append("w.maaike_review_count >= ?"); params.append(int(min_reviews))
    if min_note_len:
        conds.append("w.maaike_note_length >= ?"); params.append(int(min_note_len))
    if review_year:
        conds.append("w.maaike_date_tasted LIKE ?"); params.append(f"% {review_year}")
    if date_from:
        conds.append("w.maaike_date_tasted >= ?"); params.append(date_from)
    if date_to:
        conds.append("w.maaike_date_tasted <= ?"); params.append(date_to)
    if source_filter:
        # Filter wines that have at least one review from this source
        conds.append("EXISTS (SELECT 1 FROM reviews r WHERE r.wine_id=w.id AND r.source=?)")
        params.append(source_filter)

    where = ("WHERE " + " AND ".join(conds)) if conds else ""

    with get_db() as conn:
        total = conn.execute(f"SELECT COUNT(*) FROM wines w {where}", params).fetchone()[0]
        rows  = conn.execute(f"""
            SELECT w.id, w.name, w.vintage, w.unit_size, w.price_eur, w.price_usd, w.stock,
                   w.supplier_url, w.region, w.country, w.colour, w.appellation,
                   w.lwin, w.lwin7,
                   w.maaike_score, w.maaike_score_20, w.maaike_reviewer, w.maaike_short_quote,
                   w.maaike_note_length, w.maaike_drink_from, w.maaike_drink_to,
                   w.maaike_review_url, w.maaike_date_tasted, w.maaike_colour,
                   w.maaike_review_count, w.maaike_jr_lwin, w.enrichment_status,
                   w.added_manually, w.created_at, w.updated_at
            FROM wines w {where}
            ORDER BY
                CASE WHEN w.maaike_score_20 IS NULL THEN 1 ELSE 0 END,
                w.{sort_by} {sort_dir}
            LIMIT ? OFFSET ?
        """, params + [per_page, offset]).fetchall()

    return jsonify({
        "wines": [dict(r) for r in rows],
        "total": total, "page": page, "per_page": per_page,
        "pages": (total + per_page - 1) // per_page,
    })


# ─── Wine Detail ──────────────────────────────────────────────────────────────

@app.route("/api/wines/<int:wine_id>")
@require_api_key
def wine_detail(wine_id: int):
    with get_db() as conn:
        row = conn.execute("SELECT * FROM wines WHERE id=?", (wine_id,)).fetchone()
    if not row:
        return jsonify({"error": "Not found"}), 404
    return jsonify(dict(row))


@app.route("/api/wines/<int:wine_id>", methods=["PATCH"])
@require_api_key
def wine_update(wine_id: int):
    data    = request.get_json(silent=True) or {}
    allowed = {"region","country","colour","appellation","price_eur","price_usd","stock","lwin","lwin7","name","vintage"}
    fields  = {k: v for k, v in data.items() if k in allowed}
    if not fields:
        return jsonify({"error": "No valid fields"}), 400
    set_clause = ", ".join(f"{k}=?" for k in fields)
    with get_db() as conn:
        conn.execute(f"UPDATE wines SET {set_clause}, updated_at=datetime('now') WHERE id=?",
                     list(fields.values()) + [wine_id])
    return jsonify({"ok": True})


@app.route("/api/wines/<int:wine_id>", methods=["DELETE"])
@require_api_key
def wine_delete(wine_id: int):
    with get_db() as conn:
        conn.execute("DELETE FROM wines WHERE id=?", (wine_id,))
    return jsonify({"ok": True})


# ─── All Reviews for a Wine ───────────────────────────────────────────────────

@app.route("/api/wines/<int:wine_id>/reviews")
@require_api_key
def wine_reviews(wine_id: int):
    source = request.args.get("source", "")
    with get_db() as conn:
        q = "SELECT * FROM reviews WHERE wine_id=?"
        p = [wine_id]
        if source:
            q += " AND source=?"; p.append(source)
        q += " ORDER BY date_tasted DESC, score_20 DESC"
        rows = conn.execute(q, p).fetchall()
    return jsonify({
        "reviews": [dict(r) for r in rows],
        "count":   len(rows),
    })


# ─── Add Single Wine Manually ─────────────────────────────────────────────────

@app.route("/api/wines/add", methods=["POST"])
@require_api_key
def add_wine():
    data    = request.get_json(silent=True) or {}
    name    = (data.get("name") or "").strip()
    vintage = (data.get("vintage") or "").strip()
    lwin    = (data.get("lwin") or "").strip()

    if not name:
        return jsonify({"error": "Wine name is required"}), 400

    # Parse LWIN7
    lwin7 = ""
    if lwin:
        raw = lwin.upper().replace("LWIN","")
        lwin7 = raw[:7] if len(raw) >= 7 else raw

    with get_db() as conn:
        try:
            cur = conn.execute("""
                INSERT INTO wines (name, vintage, lwin, lwin7, price_eur, price_usd,
                                   region, country, colour, stock, added_manually)
                VALUES (?,?,?,?,?,?,?,?,?,?,1)
            """, (name, vintage or None, lwin or None, lwin7 or None,
                  data.get("price_eur"), data.get("price_usd"),
                  data.get("region"), data.get("country"),
                  data.get("colour"), data.get("stock")))
            wine_id = cur.lastrowid
            conn.commit()
        except sqlite3.IntegrityError:
            # Already exists — return existing
            row = conn.execute(
                "SELECT id, enrichment_status FROM wines WHERE name=? AND (vintage=? OR (vintage IS NULL AND ? IS NULL))",
                (name, vintage or None, vintage or None)
            ).fetchone()
            if row:
                return jsonify({"ok": True, "wine_id": row["id"], "existed": True,
                                "status": row["enrichment_status"]})
            return jsonify({"error": "Duplicate wine"}), 409

    # Optionally trigger immediate enrichment
    auto_enrich = data.get("auto_enrich", True)
    if auto_enrich and not enrich_state["running"]:
        def _enrich_one():
            try:
                from maaike_phase1 import load_session, search_wine
                session = load_session(str(COOKIE_PATH))
                results = search_wine(session, name, vintage, lwin)
                if results:
                    best = max(results, key=lambda x: x.get("score_20") or 0)
                    note = best.get("tasting_note") or ""
                    with get_db() as c:
                        # Save best to wines table
                        c.execute("""
                            UPDATE wines SET
                                maaike_score=?, maaike_score_20=?, maaike_reviewer=?,
                                maaike_short_quote=?, maaike_note_length=?,
                                maaike_drink_from=?, maaike_drink_to=?,
                                maaike_review_url=?, maaike_date_tasted=?, maaike_colour=?,
                                maaike_review_count=?, maaike_jr_lwin=?,
                                enrichment_status='found', updated_at=datetime('now')
                            WHERE id=?
                        """, (best.get("score"), best.get("score_20"), best.get("reviewer"),
                              note, len(note) if note else 0,
                              best.get("drink_from"), best.get("drink_to"),
                              best.get("review_url"), best.get("date_tasted"),
                              best.get("colour"), len(results), best.get("jr_lwin"), wine_id))
                        # Save ALL reviews to reviews table
                        for r in results:
                            n = r.get("tasting_note") or ""
                            c.execute("""
                                INSERT INTO reviews
                                    (wine_id, source, score_20, reviewer, note, note_length,
                                     drink_from, drink_to, date_tasted, review_url, colour,
                                     wine_name_src, jr_lwin)
                                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                            """, (wine_id, "jancisrobinson",
                                  r.get("score_20"), r.get("reviewer"), n, len(n) if n else 0,
                                  r.get("drink_from"), r.get("drink_to"),
                                  r.get("date_tasted"), r.get("review_url"),
                                  r.get("colour"), r.get("wine_name_jr"), r.get("jr_lwin")))
                        c.commit()
                else:
                    with get_db() as c:
                        c.execute("UPDATE wines SET enrichment_status='not_found', maaike_review_count=0 WHERE id=?",
                                  (wine_id,))
            except Exception as e:
                print(f"[auto-enrich] {e}")
        threading.Thread(target=_enrich_one, daemon=True).start()

    return jsonify({"ok": True, "wine_id": wine_id, "existed": False,
                    "status": "pending", "auto_enrich": auto_enrich})


# ─── Re-enrich a single wine ──────────────────────────────────────────────────

@app.route("/api/wines/<int:wine_id>/enrich", methods=["POST"])
@require_api_key
def enrich_single(wine_id: int):
    with get_db() as conn:
        w = conn.execute("SELECT * FROM wines WHERE id=?", (wine_id,)).fetchone()
    if not w:
        return jsonify({"error": "Not found"}), 404

    def _run():
        try:
            from maaike_phase1 import load_session, search_wine
            session = load_session(str(COOKIE_PATH))
            results = search_wine(session, w["name"], w["vintage"] or "", w["lwin"] or "")
            with get_db() as c:
                # Clear old reviews for this wine from all sources
                c.execute("DELETE FROM reviews WHERE wine_id=? AND source='jancisrobinson'", (wine_id,))
                if results:
                    best = max(results, key=lambda x: x.get("score_20") or 0)
                    note = best.get("tasting_note") or ""
                    c.execute("""
                        UPDATE wines SET
                            maaike_score=?, maaike_score_20=?, maaike_reviewer=?,
                            maaike_short_quote=?, maaike_note_length=?,
                            maaike_drink_from=?, maaike_drink_to=?,
                            maaike_review_url=?, maaike_date_tasted=?, maaike_colour=?,
                            maaike_review_count=?, maaike_jr_lwin=?,
                            enrichment_status='found', updated_at=datetime('now')
                        WHERE id=?
                    """, (best.get("score"), best.get("score_20"), best.get("reviewer"),
                          note, len(note) if note else 0,
                          best.get("drink_from"), best.get("drink_to"),
                          best.get("review_url"), best.get("date_tasted"),
                          best.get("colour"), len(results), best.get("jr_lwin"), wine_id))
                    for r in results:
                        n = r.get("tasting_note") or ""
                        c.execute("""
                            INSERT INTO reviews
                                (wine_id, source, score_20, reviewer, note, note_length,
                                 drink_from, drink_to, date_tasted, review_url, colour,
                                 wine_name_src, jr_lwin)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """, (wine_id, "jancisrobinson",
                              r.get("score_20"), r.get("reviewer"), n, len(n) if n else 0,
                              r.get("drink_from"), r.get("drink_to"),
                              r.get("date_tasted"), r.get("review_url"),
                              r.get("colour"), r.get("wine_name_jr"), r.get("jr_lwin")))
                    c.commit()
                    socketio.emit("wine_enriched", {"wine_id": wine_id, "found": True,
                                                     "count": len(results),
                                                     "score": best.get("score_20")})
                else:
                    c.execute("UPDATE wines SET enrichment_status='not_found', updated_at=datetime('now') WHERE id=?",
                              (wine_id,))
                    c.commit()
                    socketio.emit("wine_enriched", {"wine_id": wine_id, "found": False})
        except Exception as e:
            socketio.emit("wine_enriched", {"wine_id": wine_id, "error": str(e)})

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"ok": True, "message": "Enrichment started for wine ID " + str(wine_id)})


# ─── Filter Options ───────────────────────────────────────────────────────────

@app.route("/api/filter-options")
@require_api_key
def filter_options():
    with get_db() as conn:
        regions   = [r[0] for r in conn.execute("SELECT DISTINCT region FROM wines WHERE region IS NOT NULL AND region!='' ORDER BY region").fetchall()]
        colours   = [r[0] for r in conn.execute("SELECT DISTINCT colour FROM wines WHERE colour IS NOT NULL AND colour!='' ORDER BY colour").fetchall()]
        vintages  = [r[0] for r in conn.execute("SELECT DISTINCT vintage FROM wines WHERE vintage IS NOT NULL AND vintage!='' ORDER BY vintage DESC").fetchall()]
        reviewers = [r[0] for r in conn.execute("SELECT DISTINCT reviewer FROM reviews WHERE reviewer IS NOT NULL AND reviewer!='' ORDER BY reviewer").fetchall()]
        sources   = [r[0] for r in conn.execute("SELECT DISTINCT source FROM reviews ORDER BY source").fetchall()]
    return jsonify({
        "regions": regions, "colours": colours,
        "vintages": vintages, "reviewers": reviewers, "sources": sources,
    })


# ─── CSV Upload ───────────────────────────────────────────────────────────────

def _lwin7(lwin: str) -> str:
    if not lwin: return ""
    raw = lwin.upper().strip()
    digits = raw[4:] if raw.startswith("LWIN") else raw
    return digits[:7] if len(digits) >= 7 else ""


@app.route("/api/upload", methods=["POST"])
@require_api_key
def upload_csv():
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400
    f = request.files["file"]
    if not f.filename.lower().endswith(".csv"):
        return jsonify({"error": "Only CSV files accepted"}), 400

    content = f.read().decode("utf-8-sig", errors="replace")
    reader  = csv.DictReader(io.StringIO(content))

    def norm(s): return re.sub(r"[^a-z0-9]", "_", s.strip().lower())

    NAME_COLS    = ["wine_name","wine","name","product","product_name","description"]
    VINTAGE_COLS = ["vintage","year","millesime"]
    LWIN_COLS    = ["lwin","lwin_code","lwin18","lwin11","lwin7"]
    SIZE_COLS    = ["unit_size","size","unit_size","format","unit-size"]
    PRICE_COLS   = ["price","price_eur","price_euro","eur","selling_price_eur"]
    PRICE_USD    = ["price_usd","usd"]
    STOCK_COLS   = ["stock","stock_level","qty","quantity","available"]
    URL_COLS     = ["url","link","product_url","supplier_url"]
    REGION_COLS  = ["region","appellation","area"]
    COLOUR_COLS  = ["colour","color","type","wine_type"]

    def find(headers, candidates):
        for c in candidates:
            if c in headers: return c
        return None

    inserted = dupes = errors = 0

    for raw_row in reader:
        try:
            headers = {norm(k): k for k in raw_row.keys()}
            def get(cands):
                col = find(headers, cands)
                return raw_row.get(headers[col], "").strip() if col else ""

            name    = get(NAME_COLS)
            if not name: continue

            vintage = get(VINTAGE_COLS)
            m = re.search(r"\b(19\d{2}|20[0-3]\d)\b", vintage)
            vintage_clean = m.group(1) if m else ""

            lwin       = get(LWIN_COLS).strip()
            lwin7_val  = _lwin7(lwin)

            try:
                with get_db() as conn:
                    conn.execute("""
                        INSERT INTO wines (name, vintage, lwin, lwin7, unit_size, price_eur,
                                          price_usd, stock, supplier_url, region, colour)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """, (name, vintage_clean or None, lwin or None, lwin7_val or None,
                          get(SIZE_COLS) or None, get(PRICE_COLS) or None,
                          get(PRICE_USD) or None, get(STOCK_COLS) or None,
                          get(URL_COLS) or None, get(REGION_COLS) or None,
                          get(COLOUR_COLS) or None))
                inserted += 1
            except sqlite3.IntegrityError:
                if lwin:
                    with get_db() as conn:
                        conn.execute("""
                            UPDATE wines SET lwin=?, lwin7=?, updated_at=datetime('now')
                            WHERE name=? AND vintage=? AND (lwin IS NULL OR lwin='')
                        """, (lwin, lwin7_val, name, vintage_clean or None))
                dupes += 1
            except Exception:
                errors += 1
        except Exception:
            errors += 1

    return jsonify({"ok": True, "inserted": inserted, "dupes": dupes,
                    "errors": errors, "total": inserted + dupes + errors})


# ─── CSV Download ─────────────────────────────────────────────────────────────

@app.route("/api/download")
@require_api_key
def download_csv():
    status = request.args.get("status", "")
    only   = request.args.get("only", "")
    conds, params = [], []
    if status: conds.append("enrichment_status=?"); params.append(status)
    if only == "found": conds.append("maaike_score_20 IS NOT NULL")
    where = ("WHERE " + " AND ".join(conds)) if conds else ""

    with get_db() as conn:
        rows = conn.execute(f"""
            SELECT name, vintage, lwin, lwin7, region, colour, price_eur, price_usd, stock,
                   maaike_score_20, maaike_reviewer, maaike_drink_from, maaike_drink_to,
                   maaike_date_tasted, maaike_review_count, maaike_note_length,
                   maaike_short_quote, maaike_review_url, enrichment_status
            FROM wines {where} ORDER BY maaike_score_20 DESC
        """, params).fetchall()

    out = io.StringIO()
    w   = csv.writer(out)
    w.writerow(["Wine Name","Vintage","LWIN","LWIN7","Region","Colour",
                "Price EUR","Price USD","Stock","Score /20","Reviewer",
                "Drink From","Drink To","Date Tasted","Review Count","Note Length",
                "Tasting Note","Jancis URL","Status"])
    for r in rows: w.writerow(list(r))
    out.seek(0)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Response(out.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename=maaike_{ts}.csv"})


# ─── Cookies ──────────────────────────────────────────────────────────────────

@app.route("/api/cookies", methods=["POST"])
@require_api_key
def upload_cookies():
    if "file" not in request.files:
        return jsonify({"error": "No file"}), 400
    try:
        data = json.loads(request.files["file"].read().decode("utf-8"))
        with open(str(COOKIE_PATH), "w") as fp:
            json.dump(data, fp, indent=2)
        return jsonify({"ok": True, "cookies": len(data)})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/cookies/status")
@require_api_key
def cookies_status():
    if not COOKIE_PATH.exists():
        return jsonify({"ok": False, "message": "real_cookies.json not found"})
    try:
        import base64
        with open(str(COOKIE_PATH)) as f:
            cookies = json.load(f)
        jr = next((c for c in cookies if c.get("name") == "jrAccessRole"), None)
        if not jr:
            return jsonify({"ok": False, "message": "jrAccessRole cookie missing"})
        tok     = jr.get("value", "")
        payload = json.loads(base64.urlsafe_b64decode(tok.split(".")[1] + "==").decode())
        days    = (payload.get("exp", 0) - int(datetime.utcnow().timestamp())) // 86400
        has_sess = any(c.get("name", "").upper().startswith(("SESS", "SSESS")) for c in cookies)
        return jsonify({"ok": True, "days_remaining": days,
                        "is_member": payload.get("isMember", False),
                        "tasting_access": payload.get("canAccessTastingNotes", False),
                        "has_session": has_sess, "cookie_count": len(cookies)})
    except Exception as e:
        return jsonify({"ok": False, "message": str(e)})


# ─── Enrichment Engine ────────────────────────────────────────────────────────

def _emit_log(msg, level="info"):
    socketio.emit("enrich_log", {"msg": msg, "level": level,
                                  "ts": datetime.now().strftime("%H:%M:%S")})

def _emit_progress():
    s = enrich_state
    pct = round(s["done"] / s["total"] * 100, 1) if s["total"] else 0
    socketio.emit("enrich_progress", {
        "total": s["total"], "done": s["done"], "found": s["found"],
        "errors": s["errors"], "pct": pct, "running": s["running"],
    })


def _save_all_reviews(conn, wine_id, results, source="jancisrobinson"):
    """Save all review results to the reviews table + update wines summary."""
    if not results:
        return
    best = max(results, key=lambda x: x.get("score_20") or 0)
    note = best.get("tasting_note") or ""

    conn.execute("""
        UPDATE wines SET
            maaike_score=?, maaike_score_20=?, maaike_reviewer=?,
            maaike_short_quote=?, maaike_note_length=?,
            maaike_drink_from=?, maaike_drink_to=?,
            maaike_review_url=?, maaike_date_tasted=?, maaike_colour=?,
            maaike_review_count=?, maaike_jr_lwin=?,
            enrichment_status='found', updated_at=datetime('now')
        WHERE id=?
    """, (best.get("score"), best.get("score_20"), best.get("reviewer"),
          note, len(note) if note else 0,
          best.get("drink_from"), best.get("drink_to"),
          best.get("review_url"), best.get("date_tasted"),
          best.get("colour"), len(results), best.get("jr_lwin"), wine_id))

    # Remove old reviews from this source then re-insert all
    conn.execute("DELETE FROM reviews WHERE wine_id=? AND source=?", (wine_id, source))
    for r in results:
        n = r.get("tasting_note") or ""
        conn.execute("""
            INSERT INTO reviews
                (wine_id, source, score_20, reviewer, note, note_length,
                 drink_from, drink_to, date_tasted, review_url, colour,
                 wine_name_src, jr_lwin)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (wine_id, source,
              r.get("score_20"), r.get("reviewer"), n, len(n) if n else 0,
              r.get("drink_from"), r.get("drink_to"),
              r.get("date_tasted"), r.get("review_url"),
              r.get("colour"), r.get("wine_name_jr"), r.get("jr_lwin")))
    conn.commit()


def _run_enrichment(limit, sleep_sec, only_pending):
    global enrich_state
    try:
        from maaike_phase1 import load_session, search_wine
    except ImportError as e:
        _emit_log(f"[ERROR] Import failed: {e}", "error")
        enrich_state["running"] = False; _emit_progress(); return

    try:
        session = load_session(str(COOKIE_PATH))
        _emit_log("Session loaded ✓", "success")
    except Exception as e:
        _emit_log(f"[ERROR] Session: {e}", "error")
        enrich_state["running"] = False; _emit_progress(); return

    with get_db() as conn:
        cond = "WHERE enrichment_status='pending'" if only_pending \
               else "WHERE enrichment_status IN ('pending','not_found')"
        lim  = f"LIMIT {limit}" if limit else ""
        rows = conn.execute(
            f"SELECT id, name, vintage, lwin, lwin7 FROM wines {cond} ORDER BY RANDOM() {lim}"
        ).fetchall()

    wines = [{"id":r["id"],"name":r["name"],"vintage":r["vintage"] or "",
              "lwin":r["lwin"] or "","lwin7":r["lwin7"] or ""} for r in rows]

    enrich_state.update({"total": len(wines), "done": 0, "found": 0, "errors": 0})
    _emit_log(f"Starting {len(wines)} wines…", "info")
    _emit_progress()

    conn = get_db()
    for i, w in enumerate(wines):
        if enrich_state["stop_flag"]:
            _emit_log("⏹ Stopped.", "warn"); break

        lwin_str = f" [LWIN7:{w['lwin7']}]" if w["lwin7"] else ""
        _emit_log(f"[{i+1}/{len(wines)}] {w['name']} ({w['vintage'] or 'NV'}){lwin_str}", "info")

        try:
            results = search_wine(session, w["name"], w["vintage"], w["lwin"])
            if results:
                _save_all_reviews(conn, w["id"], results)
                enrich_state["found"] += 1
                best = max(results, key=lambda x: x.get("score_20") or 0)
                s = best.get("score_20")
                _emit_log(
                    f"  ✓ {s}/20 | {best.get('reviewer','?')} | {len(results)} review(s) | {len(best.get('tasting_note') or '')}ch",
                    "success"
                )
            else:
                conn.execute(
                    "UPDATE wines SET enrichment_status='not_found', maaike_review_count=0, updated_at=datetime('now') WHERE id=?",
                    (w["id"],))
                conn.commit()
                _emit_log("  ✗ not found", "warn")
        except Exception as e:
            enrich_state["errors"] += 1
            _emit_log(f"  [ERROR] {type(e).__name__}: {e}", "error")

        enrich_state["done"] += 1
        _emit_progress()
        time.sleep(sleep_sec)

    conn.close()
    enrich_state.update({"running": False, "stop_flag": False})
    hr = enrich_state["found"] / enrich_state["total"] * 100 if enrich_state["total"] else 0
    _emit_log(f"Done — {enrich_state['found']}/{enrich_state['total']} found ({hr:.1f}%)", "success")
    _emit_progress()


@app.route("/api/enrich/start", methods=["POST"])
@require_api_key
def enrich_start():
    if enrich_state["running"]:
        return jsonify({"error": "Already running"}), 409
    data = request.get_json(silent=True) or {}
    enrich_state.update({"running": True, "stop_flag": False})
    t = threading.Thread(
        target=_run_enrichment,
        args=(int(data.get("limit",0)), float(data.get("sleep",1.2)), bool(data.get("only_pending",True))),
        daemon=True)
    enrich_state["thread"] = t
    t.start()
    return jsonify({"ok": True})


@app.route("/api/enrich/stop", methods=["POST"])
@require_api_key
def enrich_stop():
    if not enrich_state["running"]:
        return jsonify({"error": "Not running"}), 400
    enrich_state["stop_flag"] = True
    return jsonify({"ok": True})


@app.route("/api/enrich/status")
@require_api_key
def enrich_status_route():
    s = enrich_state
    pct = round(s["done"] / s["total"] * 100, 1) if s["total"] else 0
    return jsonify({"running":s["running"],"total":s["total"],"done":s["done"],
                    "found":s["found"],"errors":s["errors"],"pct":pct})


@app.route("/api/admin/reset-not-found", methods=["POST"])
@require_api_key
def reset_not_found():
    with get_db() as conn:
        n = conn.execute(
            "UPDATE wines SET enrichment_status='pending', updated_at=datetime('now') WHERE enrichment_status='not_found'"
        ).rowcount
    return jsonify({"ok": True, "reset": n})


@socketio.on("connect")
def on_connect(): _emit_progress()


@app.route("/health")
def health(): return jsonify({"status":"ok","time":datetime.now().isoformat()})


if __name__ == "__main__":
    print("="*60)
    print("  MAAIKE  — Wine Review Intelligence")
    print(f"  DB:      {DB_PATH}")
    print(f"  API Key: {API_KEY}")
    print("="*60)
    socketio.run(app, host="0.0.0.0", port=5000, debug=False, allow_unsafe_werkzeug=True)