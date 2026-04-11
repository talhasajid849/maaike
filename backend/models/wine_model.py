"""
models/wine_model.py
====================
All SQL for the wines + reviews tables.
Think of this as your Express Mongoose model or Sequelize model —
pure data access, zero business logic.
"""
from __future__ import annotations

import re
import sqlite3
from typing import Any

from config.database import get_db


# ─── Schema init ──────────────────────────────────────────────────────────────

def init_schema():
    """Create tables and run migrations. Called once at startup."""
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
                lwin11              TEXT,
                -- Best review summary (fast display, denormalized)
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
                maaike_best_source  TEXT,
                enrichment_status   TEXT DEFAULT 'pending',
                -- 'downloaded' = has score AND note text
                -- 'found'      = has score but no note yet
                -- 'pending'    = not yet searched
                -- 'not_found'  = searched but no match
                data_origin         TEXT DEFAULT 'inventory',
                upload_batch        TEXT,
                added_manually      INTEGER DEFAULT 0,
                created_at          TEXT DEFAULT (datetime('now')),
                updated_at          TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS reviews (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                wine_id       INTEGER NOT NULL REFERENCES wines(id) ON DELETE CASCADE,
                source        TEXT NOT NULL,
                score_native  REAL,
                score_20      REAL,
                score_100     REAL,
                score_label   TEXT,
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
            CREATE INDEX IF NOT EXISTS idx_wines_lwin11    ON wines(lwin11);
            CREATE INDEX IF NOT EXISTS idx_wines_score     ON wines(maaike_score_20);
            CREATE INDEX IF NOT EXISTS idx_reviews_wine    ON reviews(wine_id);
            CREATE INDEX IF NOT EXISTS idx_reviews_source  ON reviews(source);
            CREATE INDEX IF NOT EXISTS idx_reviews_score   ON reviews(score_20);
            CREATE INDEX IF NOT EXISTS idx_reviews_reviewer ON reviews(reviewer);
        """)

        # Safe migrations — add columns to existing DBs without breaking them
        _migrate_add_columns(conn, "wines", [
            ("lwin",               "TEXT"),
            ("lwin11",             "TEXT"),
            ("maaike_note_length", "INTEGER"),
            ("maaike_review_count","INTEGER DEFAULT 0"),
            ("maaike_jr_lwin",     "TEXT"),
            ("maaike_best_source", "TEXT"),
            ("added_manually",     "INTEGER DEFAULT 0"),
            ("data_origin",        "TEXT DEFAULT 'inventory'"),
            ("upload_batch",       "TEXT"),
        ])
        _migrate_add_columns(conn, "reviews", [
            ("score_native", "REAL"),
            ("score_100",    "REAL"),
            ("score_label",  "TEXT"),
        ])

        try:
            conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS ux_wines_lwin11
                ON wines(lwin11)
                WHERE lwin11 IS NOT NULL AND trim(lwin11) != ''
            """)
        except Exception:
            pass

        # Data migration: fix stale enrichment_status.
        # A wine marked 'found' (score only, no note) that actually has a real note
        # stored in the reviews table should be 'downloaded'. This can happen when the
        # scraper re-runs and stores notes without calling _refresh_wine_best correctly.
        conn.execute("""
            UPDATE wines
            SET enrichment_status = 'downloaded'
            WHERE enrichment_status = 'found'
              AND EXISTS (
                  SELECT 1 FROM reviews r
                  WHERE r.wine_id = wines.id
                    AND r.note IS NOT NULL
                    AND trim(r.note) != ''
                    AND length(trim(r.note)) > 20
                    AND lower(r.note) NOT LIKE 'become a member%'
                    AND lower(r.note) NOT LIKE 'subscribe to%'
                    AND lower(r.note) NOT LIKE 'sign in to%'
                    AND lower(r.note) NOT LIKE 'log in to%'
              )
        """)
        conn.commit()


def _migrate_add_columns(conn, table: str, columns: list[tuple]):
    existing = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    for col, typedef in columns:
        if col not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typedef}")


# ─── Wines queries ────────────────────────────────────────────────────────────

def find_wines(filters: dict, sort: str = "maaike_score_20",
               direction: str = "DESC", page: int = 1, per_page: int = 50):
    """
    Paginated wine list with filters.
    When `source` is in filters, LEFT JOINs the best review for that source and
    returns src_* columns (src_score_20, src_reviewer, src_note, etc.).
    Inventory wines are ALWAYS returned regardless of whether they have a review
    for the selected source.

    When filters['export'] == '1', the correlated subquery picks the MOST RECENT
    review (date DESC) instead of the highest-quality one — so the CSV export
    always reflects the latest assessment.

    Returns { wines: [...], total, page, per_page, pages }
    """
    source    = filters.get("source", "")
    is_export = filters.get("export") == "1"
    where, params = _build_filters(filters, use_src_review=bool(source))
    offset = (page - 1) * per_page

    if source:
        # LEFT JOIN the best review for the selected source.
        # Normal mode  : note quality first → highest score → most recent date
        # Export mode  : most recent date first → note quality → highest score
        # Wines with no review for this source are still returned (LEFT JOIN).
        # Dates are stored as "DD Mon YYYY" text (e.g. "10 Feb 2026").
        # Lexicographic sort gives wrong results, so we build a YYYYMMDD string.
        _date_sort = """(
                    substr(r2.date_tasted,8,4) ||
                    CASE substr(r2.date_tasted,4,3)
                        WHEN 'Jan' THEN '01' WHEN 'Feb' THEN '02' WHEN 'Mar' THEN '03'
                        WHEN 'Apr' THEN '04' WHEN 'May' THEN '05' WHEN 'Jun' THEN '06'
                        WHEN 'Jul' THEN '07' WHEN 'Aug' THEN '08' WHEN 'Sep' THEN '09'
                        WHEN 'Oct' THEN '10' WHEN 'Nov' THEN '11' WHEN 'Dec' THEN '12'
                        ELSE '00' END ||
                    substr(r2.date_tasted,1,2)
                )"""
        _note_quality = """CASE
                    WHEN r2.note IS NOT NULL
                     AND trim(r2.note) != ''
                     AND trim(r2.note) NOT LIKE 'Become a member%'
                     AND trim(r2.note) NOT LIKE 'Subscribe to%'
                     AND length(trim(r2.note)) > 20
                    THEN 0 ELSE 1
                END"""
        if is_export:
            # Export: most recent review first (latest real-world assessment),
            # then prefer reviews with a note, then highest score as tiebreak.
            review_order = f"""
                {_date_sort} DESC,
                {_note_quality},
                r2.score_20 DESC NULLS LAST"""
        else:
            # Display: best note quality first → highest score → most recent date.
            review_order = f"""
                {_note_quality},
                r2.score_20 DESC NULLS LAST,
                {_date_sort} DESC"""
        src_join = f"""LEFT JOIN reviews r ON r.id = (
            SELECT r2.id FROM reviews r2
            WHERE r2.wine_id = w.id AND r2.source = ?
            ORDER BY{review_order}
            LIMIT 1
        )"""
        # Drink window fallback: the best review (highest score + real note) may not
        # carry a drink window (e.g. Tamlyn Currin never adds df/dt on old vintages).
        # COALESCE falls back to the best drink_from/drink_to from any other review
        # for the same wine+source that does have one.
        src_cols = """,
            r.score_20    AS src_score_20,
            r.score_100   AS src_score_100,
            r.reviewer    AS src_reviewer,
            r.note        AS src_note,
            COALESCE(r.drink_from, (
                SELECT r3.drink_from FROM reviews r3
                WHERE r3.wine_id = w.id AND r3.source = ?
                  AND r3.drink_from IS NOT NULL
                ORDER BY r3.score_20 DESC NULLS LAST
                LIMIT 1
            )) AS src_drink_from,
            COALESCE(r.drink_to, (
                SELECT r3.drink_to FROM reviews r3
                WHERE r3.wine_id = w.id AND r3.source = ?
                  AND r3.drink_to IS NOT NULL
                ORDER BY r3.score_20 DESC NULLS LAST
                LIMIT 1
            )) AS src_drink_to,
            r.date_tasted AS src_date_tasted,
            r.source      AS src_source"""
        # src_cols_params: 2 × source for the 2 COALESCE subqueries above.
        # These appear in the SELECT clause, BEFORE the FROM/JOIN clause,
        # so they must be the FIRST params in the main SELECT query.
        src_cols_params = [source, source]
        join_params = [source]
        # When a source tab is active, wines that have a review for that source
        # (r.source IS NOT NULL) sort first. Wines with no review for that source
        # (pending/not_found) sink to the bottom so reviewed wines always appear first.
        order_clause = (
            f"ORDER BY\n"
            f"    CASE WHEN r.source IS NULL THEN 1 ELSE 0 END,\n"
            f"    CASE WHEN w.maaike_score_20 IS NULL THEN 1 ELSE 0 END,\n"
            f"    w.{sort} {direction}"
        )
    else:
        src_join = ""
        src_cols = ""
        src_cols_params = []
        join_params = []
        order_clause = (
            f"ORDER BY\n"
            f"    CASE WHEN w.maaike_score_20 IS NULL THEN 1 ELSE 0 END,\n"
            f"    w.{sort} {direction}"
        )

    with get_db() as conn:
        # Always include the src_join in the count query so that any WHERE
        # conditions referencing r.* (reviewer, score, date, has_note filters)
        # resolve correctly. join_params is [] when source is empty.
        total = conn.execute(
            f"SELECT COUNT(*) FROM wines w {src_join} {where}",
            join_params + params,
        ).fetchone()[0]
        rows = conn.execute(f"""
            SELECT w.*{src_cols}
            FROM wines w
            {src_join}
            {where}
            {order_clause}
            LIMIT ? OFFSET ?
        """, src_cols_params + join_params + params + [per_page, offset]).fetchall()

    return {
        "wines": [dict(r) for r in rows],
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": (total + per_page - 1) // per_page,
    }


def find_wine_by_id(wine_id: int) -> dict | None:
    with get_db() as conn:
        row = conn.execute("SELECT * FROM wines WHERE id=?", (wine_id,)).fetchone()
    return dict(row) if row else None


def create_wine(data: dict) -> int:
    """Insert wine, return new id. Raises sqlite3.IntegrityError on duplicate."""
    with get_db() as conn:
        cur = conn.execute("""
            INSERT INTO wines
                (name, vintage, lwin, lwin11, price_eur, price_usd,
                 region, country, colour, stock, added_manually)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            data.get("name"), data.get("vintage") or None,
            data.get("lwin") or None, data.get("lwin11") or None,
            data.get("price_eur"), data.get("price_usd"),
            data.get("region"), data.get("country"),
            data.get("colour"), data.get("stock"),
            int(data.get("added_manually", 0)),
        ))
        return cur.lastrowid


def update_wine_supply(lwin11: str, fields: dict):
    """Update price/stock/url for an existing wine matched by LWIN11."""
    allowed = {"price_eur", "price_usd", "stock", "supplier_url"}
    fields = {k: v for k, v in fields.items() if k in allowed and v is not None}
    if not fields:
        return 0
    set_clause = ", ".join(f"{k}=?" for k in fields)
    with get_db() as conn:
        cur = conn.execute(
            f"UPDATE wines SET {set_clause}, updated_at=datetime('now') WHERE lwin11=?",
            list(fields.values()) + [lwin11]
        )
    return cur.rowcount


def update_wine(wine_id: int, fields: dict):
    allowed = {"region","country","colour","appellation","price_eur",
               "price_usd","stock","lwin","lwin11","name","vintage"}
    fields = {k: v for k, v in fields.items() if k in allowed}
    if not fields:
        return 0
    set_clause = ", ".join(f"{k}=?" for k in fields)
    with get_db() as conn:
        cur = conn.execute(
            f"UPDATE wines SET {set_clause}, updated_at=datetime('now') WHERE id=?",
            list(fields.values()) + [wine_id]
        )
    return cur.rowcount


def delete_wine(wine_id: int):
    with get_db() as conn:
        conn.execute("DELETE FROM wines WHERE id=?", (wine_id,))


def clear_wine_source_reviews(wine_id: int, source: str | None = None):
    """
    Delete reviews for a wine from one specific source (or all sources if source=None).
    Used by Re-search Reviews to start clean before re-fetching.
    """
    with get_db() as conn:
        if source:
            conn.execute("DELETE FROM reviews WHERE wine_id=? AND source=?", (wine_id, source))
        else:
            conn.execute("DELETE FROM reviews WHERE wine_id=?", (wine_id,))
        conn.commit()


def mark_not_found(wine_id: int, source: str | None = None):
    """
    Mark a wine as not_found and clear its reviews for that source.
    If source is given, only removes reviews from that source.
    """
    with get_db() as conn:
        if source:
            conn.execute("DELETE FROM reviews WHERE wine_id=? AND source=?", (wine_id, source))
        conn.execute("""
            UPDATE wines SET
                enrichment_status='not_found',
                maaike_review_count=0,
                updated_at=datetime('now')
            WHERE id=?
        """, (wine_id,))
        conn.commit()


def reset_not_found() -> int:
    with get_db() as conn:
        cur = conn.execute("""
            UPDATE wines SET enrichment_status='pending', updated_at=datetime('now')
            WHERE enrichment_status='not_found'
        """)
    return cur.rowcount


def reset_found() -> int:
    """
    Reset wines that have a score but no real tasting note back to 'pending'
    so they get re-enriched on the next enrichment run.

    Three-step operation so one button click catches everything:
      1. Clear paywall stubs still stored in reviews (e.g. "Become a member…")
      2. Downgrade 'downloaded' → 'found' for wines where ANY review has a
         score but no real note (covers: all-empty wines, partial fetches
         where one source got notes and another didn't, paywall stubs)
      3. Reset all 'found' (score present, note absent) → 'pending'

    Returns total wines reset to pending.
    """
    with get_db() as conn:
        # Step 1 — clear paywall stubs stored in reviews
        conn.execute("""
            UPDATE reviews SET note = '', note_length = 0
            WHERE note IS NOT NULL AND length(trim(note)) > 0
              AND (
                  lower(trim(note)) LIKE 'become a member%'
                  OR lower(trim(note)) LIKE 'subscribe to%'
                  OR lower(trim(note)) LIKE 'sign in to%'
                  OR lower(trim(note)) LIKE 'log in to%'
                  OR lower(trim(note)) LIKE 'members only%'
                  OR length(trim(note)) < 25
              )
        """)

        # Step 2 — downgrade 'downloaded' wines that have ANY review with a score
        # but no real note (catches both fully-empty wines AND wines where only
        # some sources are missing notes, e.g. JR has score+date but no text
        # while RP has a full note).
        conn.execute("""
            UPDATE wines
            SET enrichment_status = 'found', updated_at = datetime('now')
            WHERE enrichment_status = 'downloaded'
              AND EXISTS (
                  SELECT 1 FROM reviews r
                  WHERE r.wine_id = wines.id
                    AND r.score_20 IS NOT NULL
                    AND (
                        r.note IS NULL
                        OR trim(r.note) = ''
                        OR length(trim(r.note)) <= 20
                    )
              )
        """)

        # Step 3 — reset all 'found' (score but no note) back to pending
        cur = conn.execute("""
            UPDATE wines
            SET enrichment_status = 'pending', updated_at = datetime('now')
            WHERE enrichment_status = 'found'
        """)
        conn.commit()
    return cur.rowcount


def get_pending_wines(limit: int = -1, include_not_found: bool = False,
                      refetch_found: bool = False,
                      start_from_id: int = 0, end_at_id: int = 0) -> list[dict]:
    """
    Return wines pending enrichment, ordered by id ASC (sequential processing).

    include_not_found: also include wines marked 'not_found' (scope='all')
    refetch_found:     fetch only 'found' wines (score stored, note missing)
                       so the enricher can re-fetch the tasting note text.
                       When True, include_not_found is ignored.
    start_from_id:     skip wines with id < this value (resume from a specific point)
    end_at_id:         stop at wines with id <= this value (process a specific range)
    limit:             cap the number of wines returned (0 = no cap)
    """
    if refetch_found:
        statuses = ["found"]
    else:
        statuses = ["pending"] + (["not_found"] if include_not_found else [])
    placeholders = ",".join("?" * len(statuses))
    params: list = list(statuses)

    range_clauses = []
    if start_from_id > 0:
        range_clauses.append("id >= ?")
        params.append(start_from_id)
    if end_at_id > 0:
        range_clauses.append("id <= ?")
        params.append(end_at_id)

    range_sql = (" AND " + " AND ".join(range_clauses)) if range_clauses else ""
    lim = f"LIMIT {limit}" if limit > 0 else ""

    with get_db() as conn:
        rows = conn.execute(f"""
            SELECT id, name, vintage, lwin, lwin11
            FROM wines
            WHERE enrichment_status IN ({placeholders}){range_sql}
            ORDER BY id ASC {lim}
        """, params).fetchall()
    return [dict(r) for r in rows]


def count_pending(include_not_found: bool = False) -> int:
    statuses = ["pending"] + (["not_found"] if include_not_found else [])
    placeholders = ",".join("?" * len(statuses))
    with get_db() as conn:
        return conn.execute(
            f"SELECT COUNT(*) FROM wines WHERE enrichment_status IN ({placeholders})", statuses
        ).fetchone()[0]


# ─── Review queries ───────────────────────────────────────────────────────────

def find_reviews_for_wine(wine_id: int, source: str = "") -> list[dict]:
    with get_db() as conn:
        q = "SELECT * FROM reviews WHERE wine_id=?"
        p = [wine_id]
        if source:
            q += " AND source=?"; p.append(source)
        q += " ORDER BY score_20 DESC, date_tasted DESC"
        rows = conn.execute(q, p).fetchall()
    return [dict(r) for r in rows]


def upsert_reviews(wine_id: int, source: str, reviews: list[dict]):
    """
    Replace all reviews for (wine_id, source) and update the wine's best score.
    This is the core write operation — called by the enrichment service.
    """
    if not reviews:
        return
    with get_db() as conn:
        conn.execute("DELETE FROM reviews WHERE wine_id=? AND source=?", (wine_id, source))
        for r in reviews:
            note = r.get("note") or ""
            conn.execute("""
                INSERT INTO reviews
                    (wine_id, source, score_native, score_20, score_100, score_label,
                     reviewer, note, note_length,
                     drink_from, drink_to, date_tasted, review_url,
                     colour, wine_name_src, jr_lwin)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                wine_id, source,
                r.get("score_native"), r.get("score_20"), r.get("score_100"),
                r.get("score_label"),
                r.get("reviewer"),
                note, len(note),
                r.get("drink_from"), r.get("drink_to"),
                r.get("date_tasted"), r.get("review_url"),
                r.get("colour"), r.get("wine_name_src"), r.get("jr_lwin"),
            ))
        conn.commit()
        _refresh_wine_best(conn, wine_id)


def _refresh_wine_best(conn, wine_id: int):
    """Denormalize the best review back to the wines row."""
    best = conn.execute("""
        SELECT * FROM reviews
        WHERE wine_id=? AND score_20 IS NOT NULL
        ORDER BY score_20 DESC LIMIT 1
    """, (wine_id,)).fetchone()

    if not best:
        return

    note = best["note"] or ""
    has_real_note = (
        len(note.strip()) > 20 and
        "become a member" not in note.lower() and
        "subscribe" not in note.lower()
    )
    # 'downloaded' = has score AND a real note text
    # 'found'      = has score but note is empty/paywalled
    new_status = "downloaded" if has_real_note else "found"

    conn.execute("""
        UPDATE wines SET
            maaike_score        = ?,
            maaike_score_20     = ?,
            maaike_reviewer     = ?,
            maaike_short_quote  = ?,
            maaike_note_length  = ?,
            maaike_drink_from   = ?,
            maaike_drink_to     = ?,
            maaike_review_url   = ?,
            maaike_date_tasted  = ?,
            maaike_colour       = ?,
            maaike_best_source  = ?,
            enrichment_status   = ?,
            updated_at          = datetime('now')
        WHERE id = ?
    """, (
        best["score_100"], best["score_20"],
        best["reviewer"], note, len(note),
        best["drink_from"], best["drink_to"],
        best["review_url"], best["date_tasted"],
        best["colour"], best["source"],
        new_status,
        wine_id,
    ))

    count = conn.execute(
        "SELECT COUNT(*) FROM reviews WHERE wine_id=?", (wine_id,)
    ).fetchone()[0]
    conn.execute("UPDATE wines SET maaike_review_count=? WHERE id=?", (count, wine_id))
    conn.commit()


# ─── Stats query ──────────────────────────────────────────────────────────────

def get_stats() -> dict:
    with get_db() as conn:
        total      = conn.execute("SELECT COUNT(*) FROM wines").fetchone()[0]
        found      = conn.execute("SELECT COUNT(*) FROM wines WHERE enrichment_status IN ('found','downloaded')").fetchone()[0]
        downloaded = conn.execute("SELECT COUNT(*) FROM wines WHERE enrichment_status='downloaded'").fetchone()[0]
        pending    = conn.execute("SELECT COUNT(*) FROM wines WHERE enrichment_status='pending'").fetchone()[0]
        not_found  = conn.execute("SELECT COUNT(*) FROM wines WHERE enrichment_status='not_found'").fetchone()[0]
        manual     = conn.execute("SELECT COUNT(*) FROM wines WHERE added_manually=1").fetchone()[0]
        avg_row    = conn.execute("SELECT AVG(maaike_score_20) FROM wines WHERE maaike_score_20 IS NOT NULL").fetchone()
        avg_score  = round(avg_row[0], 2) if avg_row[0] else None
        with_note  = conn.execute("SELECT COUNT(*) FROM wines WHERE maaike_short_quote IS NOT NULL AND maaike_short_quote!=''").fetchone()[0]
        with_lwin  = conn.execute("SELECT COUNT(*) FROM wines WHERE lwin IS NOT NULL AND lwin!=''").fetchone()[0]
        total_reviews = conn.execute("SELECT COUNT(*) FROM reviews").fetchone()[0]

        reviewers = conn.execute("""
            SELECT reviewer, COUNT(*) as cnt FROM reviews
            WHERE reviewer IS NOT NULL AND reviewer!=''
            GROUP BY reviewer ORDER BY cnt DESC LIMIT 8
        """).fetchall()

        dist = conn.execute("""
            SELECT
                CASE
                    WHEN score_20 < 15 THEN '<15'
                    WHEN score_20 < 16 THEN '15-16'
                    WHEN score_20 < 17 THEN '16-17'
                    WHEN score_20 < 18 THEN '17-18'
                    WHEN score_20 < 19 THEN '18-19'
                    ELSE '19-20'
                END as band,
                COUNT(*) as cnt
            FROM reviews WHERE score_20 IS NOT NULL
            GROUP BY band ORDER BY band
        """).fetchall()

        by_source = conn.execute("""
            SELECT source, COUNT(*) as cnt FROM reviews
            GROUP BY source ORDER BY cnt DESC
        """).fetchall()

    return {
        "total": total, "found": found, "downloaded": downloaded,
        "pending": pending, "not_found": not_found, "manual": manual,
        "coverage": round(found / total * 100, 1) if total else 0,
        "avg_score": avg_score,
        "with_note": with_note, "with_lwin": with_lwin,
        "total_reviews": total_reviews,
        "reviewers":  [{"name": r["reviewer"], "count": r["cnt"]} for r in reviewers],
        "score_dist": [{"band": d["band"], "count": d["cnt"]} for d in dist],
        "by_source":  [{"source": s["source"], "count": s["cnt"]} for s in by_source],
    }


def get_filter_options(source: str = "") -> dict:
    with get_db() as conn:
        def col(q, params=()):
            return [r[0] for r in conn.execute(q, params).fetchall()]
        if source:
            reviewers = col(
                "SELECT DISTINCT reviewer FROM reviews WHERE reviewer IS NOT NULL AND reviewer!='' AND source=? ORDER BY reviewer",
                (source,)
            )
        else:
            reviewers = col("SELECT DISTINCT reviewer FROM reviews WHERE reviewer IS NOT NULL AND reviewer!='' ORDER BY reviewer")
        return {
            "regions":   col("SELECT DISTINCT region FROM wines WHERE region IS NOT NULL AND region!='' ORDER BY region"),
            "colours":   col("SELECT DISTINCT colour FROM wines WHERE colour IS NOT NULL AND colour!='' ORDER BY colour"),
            "vintages":  col("SELECT DISTINCT vintage FROM wines WHERE vintage IS NOT NULL AND vintage!='' ORDER BY vintage DESC"),
            "reviewers": reviewers,
            "sources":   col("SELECT DISTINCT source FROM reviews ORDER BY source"),
        }


# ─── Download query ───────────────────────────────────────────────────────────

def get_wines_for_export(filters: dict) -> list:
    """
    Export one row per wine — the LATEST review that has a real score.

    Rules:
      1. One row per wine — wines table is the driver.
      2. Must have a score (score_20 IS NOT NULL) to be included.
      3. Pick the LATEST review by date_tasted as the canonical review.
      4. Note: use the note from that review. If blank, note column is empty
         (do NOT fall back to wrong notes from other reviews).
      5. Filter out paywall non-notes ("Become a member...").
    """
    where, params = _build_filters(filters)

    # Default: only export wines that have a real score
    # Both 'found' (score only) and 'downloaded' (score + note) are exported
    if not filters.get("status"):
        extra = """w.enrichment_status IN ('found','downloaded')
                   AND EXISTS (
                       SELECT 1 FROM reviews rx
                       WHERE rx.wine_id = w.id AND rx.score_20 IS NOT NULL
                   )"""
        where = f"WHERE {extra}" if not where else f"{where} AND {extra}"

    with get_db() as conn:
        return conn.execute(f"""
            SELECT
                w.name,
                w.vintage,
                w.lwin,
                r.source         AS maaike_best_source,
                r.reviewer,
                r.score_20,
                r.score_100,
                r.source,
                r.drink_from,
                r.drink_to,
                r.date_tasted,
                -- Clean note: use this review's note if it's real,
                -- otherwise leave empty (don't pull from other reviews — those may be wrong)
                CASE
                    WHEN r.note IS NOT NULL
                     AND trim(r.note) != ''
                     AND trim(r.note) NOT LIKE 'Become a member%'
                     AND trim(r.note) NOT LIKE 'Subscribe to%'
                     AND length(trim(r.note)) > 20
                    THEN trim(r.note)
                    ELSE ''
                END AS note
            FROM wines w
            LEFT JOIN reviews r ON r.id = (
                -- Latest review that actually has a score
                SELECT r2.id FROM reviews r2
                WHERE r2.wine_id = w.id
                  AND r2.score_20 IS NOT NULL
                ORDER BY
                    -- Prefer reviews with a real non-paywall note
                    CASE
                        WHEN r2.note IS NOT NULL
                         AND trim(r2.note) != ''
                         AND trim(r2.note) NOT LIKE 'Become a member%'
                         AND length(trim(r2.note)) > 20
                        THEN 0 ELSE 1
                    END,
                    -- Then latest date
                    r2.date_tasted DESC,
                    -- Highest score as tiebreak
                    r2.score_20 DESC
                LIMIT 1
            )
            {where}
            ORDER BY r.score_20 DESC NULLS LAST
        """, params).fetchall()


# ─── Filter builder (shared) ─────────────────────────────────────────────────

def _build_filters(args: dict, use_src_review: bool = False) -> tuple[str, list]:
    """
    Build WHERE clause from filter dict.
    Same shape as Express req.query — dict of string params.

    When use_src_review=True (source is active), the caller has LEFT JOIN'd the
    reviews table as `r`. Review-specific filters (reviewer, score, date, has_note)
    then use `r.*` columns instead of the denormalised `w.maaike_*` columns.
    The source EXISTS check is also skipped — the LEFT JOIN already scopes to that
    source and inventory wines must always remain visible.
    """
    conds: list[str] = []
    params: list[Any] = []

    if s := args.get("search", "").strip():
        reviewer_col = "r.reviewer" if use_src_review else "w.maaike_reviewer"
        conds.append(f"(w.name LIKE ? OR w.appellation LIKE ? OR {reviewer_col} LIKE ? OR w.lwin LIKE ?)")
        like = f"%{s}%"; params += [like, like, like, like]
    if v := re.sub(r"[^0-9]", "", args.get("lwin", "")):
        conds.append("(w.lwin LIKE ? OR w.lwin11 LIKE ?)"); params += [f"%{v}%", f"%{v}%"]
    if s := args.get("status", ""):
        # 'found' in the filter means either found or downloaded (both have score)
        if s == "found":
            conds.append("w.enrichment_status IN ('found','downloaded')")
        else:
            conds.append("w.enrichment_status = ?"); params.append(s)
    if s := args.get("region", ""):
        conds.append("(w.region LIKE ? OR w.appellation LIKE ?)"); params += [f"%{s}%", f"%{s}%"]
    if s := args.get("colour", ""):
        conds.append("(w.colour LIKE ? OR w.maaike_colour LIKE ?)"); params += [f"%{s}%", f"%{s}%"]
    if s := args.get("vintage", ""):
        conds.append("w.vintage = ?"); params.append(s)

    # Review-specific filters: use r.* when source JOIN is active, else w.maaike_*
    if s := args.get("reviewer", ""):
        col = "r.reviewer" if use_src_review else "w.maaike_reviewer"
        conds.append(f"{col} LIKE ?"); params.append(f"%{s}%")
    if s := args.get("min_score", ""):
        col = "r.score_20" if use_src_review else "w.maaike_score_20"
        conds.append(f"{col} >= ?"); params.append(float(s))
    if s := args.get("max_score", ""):
        col = "r.score_20" if use_src_review else "w.maaike_score_20"
        conds.append(f"{col} <= ?"); params.append(float(s))
    if args.get("has_note") == "1":
        if use_src_review:
            conds.append("r.note IS NOT NULL AND r.note != ''")
        else:
            conds.append("w.maaike_short_quote IS NOT NULL AND w.maaike_short_quote != ''")
    if s := args.get("min_note_len", ""):
        conds.append("w.maaike_note_length >= ?"); params.append(int(s))
    if s := args.get("date_from", ""):
        col = "r.date_tasted" if use_src_review else "w.maaike_date_tasted"
        conds.append(f"{col} >= ?"); params.append(s)
    if s := args.get("date_to", ""):
        col = "r.date_tasted" if use_src_review else "w.maaike_date_tasted"
        conds.append(f"{col} <= ?"); params.append(s)

    # Source filter: only used when NOT doing a source LEFT JOIN (generic queries).
    # When use_src_review=True the JOIN already scopes to the source — adding an
    # EXISTS check here would hide inventory wines that have no review yet.
    if not use_src_review:
        if s := args.get("source", ""):
            conds.append("EXISTS (SELECT 1 FROM reviews r2 WHERE r2.wine_id=w.id AND r2.source=?)")
            params.append(s)

    if s := args.get("data_origin", ""):
        conds.append("w.data_origin = ?"); params.append(s)
    if s := args.get("reviewer_name", ""):
        conds.append("EXISTS (SELECT 1 FROM reviews r2 WHERE r2.wine_id=w.id AND r2.reviewer LIKE ?)")
        params.append(f"%{s}%")

    # ID range — used by the export range selector in WineList
    if s := args.get("id_from", ""):
        try: conds.append("w.id >= ?"); params.append(int(s))
        except ValueError: pass
    if s := args.get("id_to", ""):
        try: conds.append("w.id <= ?"); params.append(int(s))
        except ValueError: pass

    return ("WHERE " + " AND ".join(conds)) if conds else "", params


def upsert_review_wine(lwin_full: str, wine_data: dict, review_data: dict, upload_batch: str = "") -> dict:
    """
    Upsert a wine+review row from an uploaded review CSV (e.g. maaike export).
    The full LWIN (LWIN101390620060600750) is used as the unique key.
    Returns {"action": "inserted"|"updated", "wine_id": int}
    """
    lwin_digits = re.sub(r"[^0-9]", "", lwin_full.upper().replace("LWIN", ""))
    lwin11 = lwin_digits[:11] if len(lwin_digits) >= 11 else lwin_digits

    source   = review_data.get("source", "jancisrobinson")
    note     = (review_data.get("note") or "").strip()
    score_20 = review_data.get("score_20")

    has_real_note = (
        len(note) > 20 and
        "become a member" not in note.lower() and
        "subscribe" not in note.lower()
    )
    status = "downloaded" if (score_20 is not None and has_real_note) else (
             "found"      if score_20 is not None else "pending")

    with get_db() as conn:
        existing = conn.execute(
            "SELECT id FROM wines WHERE lwin=? LIMIT 1", (lwin_full,)
        ).fetchone()
        if not existing and lwin11:
            existing = conn.execute(
                "SELECT id FROM wines WHERE lwin11=? LIMIT 1", (lwin11,)
            ).fetchone()

        if existing:
            wine_id = existing["id"]
            conn.execute("""
                UPDATE wines SET
                    lwin         = COALESCE(?, lwin),
                    lwin11       = COALESCE(?, lwin11),
                    upload_batch = COALESCE(?, upload_batch),
                    updated_at   = datetime('now')
                WHERE id = ?
            """, (lwin_full or None, lwin11 or None, upload_batch or None, wine_id))
            action = "updated"
        else:
            cur = conn.execute("""
                INSERT INTO wines
                    (name, vintage, lwin, lwin11, region, colour,
                     enrichment_status, data_origin, upload_batch,
                     maaike_score_20, maaike_reviewer, maaike_date_tasted,
                     maaike_drink_from, maaike_drink_to,
                     maaike_short_quote, maaike_note_length, maaike_best_source)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                wine_data.get("name", ""),
                wine_data.get("vintage") or None,
                lwin_full or None, lwin11 or None,
                wine_data.get("region") or None,
                wine_data.get("colour") or None,
                status, "uploaded_review", upload_batch or None,
                score_20,
                review_data.get("reviewer") or None,
                review_data.get("date_tasted") or None,
                review_data.get("drink_from") or None,
                review_data.get("drink_to") or None,
                note if has_real_note else None,
                len(note) if has_real_note else 0,
                source,
            ))
            wine_id = cur.lastrowid
            action = "inserted"

        conn.execute(
            "DELETE FROM reviews WHERE wine_id=? AND source=?", (wine_id, source)
        )
        conn.execute("""
            INSERT INTO reviews
                (wine_id, source, score_20, score_100, reviewer,
                 note, note_length, drink_from, drink_to, date_tasted, review_url, jr_lwin)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            wine_id, source,
            score_20,
            review_data.get("score_100") or None,
            review_data.get("reviewer") or None,
            note if has_real_note else "",
            len(note) if has_real_note else 0,
            review_data.get("drink_from") or None,
            review_data.get("drink_to") or None,
            review_data.get("date_tasted") or None,
            review_data.get("review_url") or None,
            lwin_full,
        ))

        _refresh_wine_best(conn, wine_id)
        conn.commit()

    return {"action": action, "wine_id": wine_id}

def wipe_all_wines() -> int:
    """Delete ALL wines and reviews. Returns count of deleted wines."""
    with get_db() as conn:
        count = conn.execute("SELECT COUNT(*) FROM wines").fetchone()[0]
        conn.execute("DELETE FROM reviews")
        conn.execute("DELETE FROM wines")
        conn.execute("DELETE FROM sqlite_sequence WHERE name IN ('wines','reviews')")
        conn.commit()
    return count


def fix_duplicate_notes() -> dict:
    """
    Find and strip notes that appear identically on multiple wines
    reviewed on the same date by the same critic.

    WHY this happens:
      JancisRobinson publishes batch tasting articles (e.g. "12 Feb 2026 blind
      tasting"). The scraper fetches the right score per wine but grabs the
      article-level note (or first wine's note) for every wine in the session.
      Result: 3-7 different wines all share the exact same note text.

    FIX:
      When the same note appears on 2+ DIFFERENT wines from the same date+critic,
      ALL of those notes are cleared (score is kept — scores are correct).
      A note that truly belongs to just one wine is left untouched.

    Returns counts of what was fixed.
    """
    with get_db() as conn:
        # Step 0: clear paywall notes already stored in DB
        paywall_cleared = conn.execute("""
            UPDATE reviews
            SET note = '', note_length = 0
            WHERE note IS NOT NULL
              AND (
                  lower(trim(note)) LIKE 'become a member%'
                  OR lower(trim(note)) LIKE 'subscribe to%'
                  OR lower(trim(note)) LIKE 'sign in to%'
                  OR lower(trim(note)) LIKE 'log in to%'
                  OR lower(trim(note)) LIKE 'members only%'
                  OR length(trim(note)) < 25
              )
        """).rowcount

        # Step 1: Find groups: same note fingerprint + same date + same reviewer on different wines
        dupes = conn.execute("""
            SELECT
                substr(trim(note), 1, 200) AS note_fp,
                date_tasted,
                reviewer,
                COUNT(DISTINCT wine_id)    AS wine_count,
                GROUP_CONCAT(id)           AS review_ids,
                GROUP_CONCAT(wine_id)      AS wine_ids
            FROM reviews
            WHERE note IS NOT NULL
              AND trim(note) != ''
              AND length(trim(note)) > 60
            GROUP BY note_fp, date_tasted, reviewer
            HAVING wine_count > 1
        """).fetchall()

        nulled = 0
        groups_fixed = 0

        for row in dupes:
            review_ids = [int(x) for x in row["review_ids"].split(",")]
            wine_ids   = [int(x) for x in row["wine_ids"].split(",")]

            # NULL the note on every review in this group —
            # we can't know which wine (if any) it actually belongs to.
            # Score is preserved; only the note text is cleared.
            for rid in review_ids:
                conn.execute(
                    "UPDATE reviews SET note = '', note_length = 0 WHERE id = ?",
                    (rid,)
                )
                nulled += 1

            # Re-sync the denormalized best-note on the wines table
            for wid in set(wine_ids):
                _refresh_wine_best(conn, wid)

            groups_fixed += 1

        # After clearing bad notes: wines that now have no score at all
        # should be reset to pending so they get re-enriched
        reset_count = conn.execute("""
            UPDATE wines
            SET enrichment_status = 'pending',
                maaike_score       = NULL,
                maaike_score_20    = NULL,
                maaike_reviewer    = NULL,
                maaike_short_quote = NULL,
                maaike_note_length = 0,
                updated_at         = datetime('now')
            WHERE enrichment_status = 'found'
              AND NOT EXISTS (
                  SELECT 1 FROM reviews r
                  WHERE r.wine_id = wines.id
                    AND r.score_20 IS NOT NULL
              )
        """).rowcount
        conn.commit()

        return {
            "groups_fixed":      groups_fixed,
            "notes_nulled":      nulled,
            "paywall_cleared":   paywall_cleared,
            "reset_to_pending":  reset_count,
            "message": (
                f"Cleared {nulled} session-duplicate notes across {groups_fixed} groups. "
                f"Removed {paywall_cleared} paywall stubs. "
                f"{reset_count} wines reset to pending for re-enrichment."
            )
        }
