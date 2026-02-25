#!/usr/bin/env python3
"""
MAAIKE Enrich — CLI enrichment runner
Usage:
    python enrich.py --limit 100 --sleep 1.2
    python enrich.py --all --sleep 0.8
    python enrich.py --count          # show pending count
    python enrich.py --retry          # retry not_found wines
"""
from __future__ import annotations

import argparse
import signal
import sqlite3
import sys
import time
from pathlib import Path
from typing import Dict, List

BASE_DIR    = Path(__file__).parent
DATA_DIR    = BASE_DIR / "data"
DB_PATH     = DATA_DIR / "maaike.db"
COOKIE_PATH = BASE_DIR / "real_cookies.json"

# ─── Ctrl+C ───────────────────────────────────────────────────────────────────

STOP = False

def _sigint(*_):
    global STOP
    STOP = True
    print("\n  [Ctrl+C] Stop requested — finishing current wine…")

signal.signal(signal.SIGINT, _sigint)

# ─── DB ───────────────────────────────────────────────────────────────────────

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def get_pending(limit: int = 0, retry_not_found: bool = False) -> List[Dict]:
    statuses = ["pending"]
    if retry_not_found:
        statuses.append("not_found")

    placeholders = ",".join("?" * len(statuses))

    conn = get_db()
    cur  = conn.cursor()

    cur.execute(f"SELECT COUNT(*) FROM wines WHERE enrichment_status IN ({placeholders})", statuses)
    total = cur.fetchone()[0]

    if limit == 0:
        conn.close()
        return [{"__count__": total}]

    lim_clause = f"LIMIT {limit}" if limit > 0 else ""
    cur.execute(f"""
        SELECT id, name, vintage
        FROM wines
        WHERE enrichment_status IN ({placeholders})
        ORDER BY RANDOM()
        {lim_clause}
    """, statuses)

    rows = [{"id": r["id"], "name": r["name"], "vintage": r["vintage"] or ""} for r in cur.fetchall()]
    conn.close()
    return rows


def save_review(conn: sqlite3.Connection, wine_id: int, r: Dict):
    conn.execute("""
        UPDATE wines SET
            maaike_score        = ?,
            maaike_score_20     = ?,
            maaike_reviewer     = ?,
            maaike_short_quote  = ?,
            maaike_drink_from   = ?,
            maaike_drink_to     = ?,
            maaike_review_url   = ?,
            maaike_date_tasted  = ?,
            maaike_colour       = ?,
            enrichment_status   = 'found',
            updated_at          = datetime('now')
        WHERE id = ?
    """, (
        r.get("score"),
        r.get("score_20"),
        r.get("reviewer"),
        r.get("tasting_note"),
        r.get("drink_from"),
        r.get("drink_to"),
        r.get("review_url"),
        r.get("date_tasted"),
        r.get("colour"),
        wine_id,
    ))
    conn.commit()


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="MAAIKE — Enrich wines with JancisRobinson.com scores"
    )
    parser.add_argument("--limit", type=int, default=-1, metavar="N",
                        help="Number of wines to process (default: all pending)")
    parser.add_argument("--all", action="store_true",
                        help="Process all pending wines (same as --limit 0)")
    parser.add_argument("--count", action="store_true",
                        help="Print pending count and exit")
    parser.add_argument("--retry", action="store_true",
                        help="Also retry 'not_found' wines")
    parser.add_argument("--sleep", type=float, default=1.2, metavar="SEC",
                        help="Seconds between requests (default: 1.2)")
    parser.add_argument("--cookies", default=str(COOKIE_PATH), metavar="PATH",
                        help="Path to real_cookies.json")
    args = parser.parse_args()

    # ── Count only ──
    if args.count:
        rows = get_pending(limit=0, retry_not_found=args.retry)
        print(f"Pending wines: {rows[0]['__count__']}")
        return

    # ── Import scraper ──
    try:
        sys.path.insert(0, str(BASE_DIR))
        from maaike_phase1 import load_session, search_wine
    except ImportError as e:
        print(f"[ERROR] Cannot import maaike_phase1: {e}")
        sys.exit(1)

    # ── Session ──
    print(f"\nLoading session from {args.cookies}…")
    try:
        session = load_session(args.cookies)
    except SystemExit as e:
        print(f"[ERROR] {e}")
        sys.exit(1)

    # ── Fetch pending ──
    limit = 0 if args.all else (args.limit if args.limit >= 0 else 0)
    wines = get_pending(limit=limit if limit > 0 else -1, retry_not_found=args.retry)

    # handle count sentinel
    if wines and "__count__" in wines[0]:
        print("Use --all or --limit N to start enrichment.")
        sys.exit(0)

    if not wines:
        print("Nothing to enrich. All done!")
        return

    conn  = get_db()
    found = not_found = errors = 0
    t0    = time.time()

    print(f"\n{'═'*64}")
    print(f"  Enriching {len(wines)} wines | sleep={args.sleep}s")
    print(f"{'═'*64}\n")

    for i, w in enumerate(wines, 1):
        if STOP:
            print("\n[STOPPED]")
            break

        eta_secs = (time.time() - t0) / i * (len(wines) - i) if i > 1 else 0
        eta_str  = f"ETA ~{int(eta_secs//60)}m{int(eta_secs%60)}s" if eta_secs > 0 else ""
        pct = i / len(wines) * 100

        print(f"[{i:>4}/{len(wines)}] {pct:4.0f}%  {eta_str}")
        print(f"  Wine   : {w['name']} ({w['vintage'] or 'NV'})")

        try:
            results = search_wine(session, w["name"], w["vintage"])

            if results:
                best = max(results, key=lambda x: x.get("score_20") or 0)
                save_review(conn, w["id"], best)
                found += 1
                s = best.get("score_20")
                print(f"  ✓ {s}/20 — {best.get('reviewer','?')} — {best.get('drink_from','?')}–{best.get('drink_to','?')}")
                note = best.get("tasting_note") or ""
                if note:
                    print(f"  ↳ {note[:120]}{'…' if len(note) > 120 else ''}")
            else:
                conn.execute(
                    "UPDATE wines SET enrichment_status='not_found', updated_at=datetime('now') WHERE id=?",
                    (w["id"],)
                )
                conn.commit()
                not_found += 1
                print("  ✗ not found")

        except KeyboardInterrupt:
            STOP = True
            break
        except Exception as e:
            errors += 1
            print(f"  [ERROR] {type(e).__name__}: {e}")

        print("─" * 64)
        time.sleep(args.sleep)

    conn.close()
    elapsed = time.time() - t0
    processed = found + not_found + errors
    hr = found / processed * 100 if processed else 0

    print(f"\n{'═'*64}")
    print(f"  Found      : {found:>5}")
    print(f"  Not found  : {not_found:>5}")
    print(f"  Errors     : {errors:>5}")
    print(f"  Hit rate   : {hr:>5.1f}%")
    print(f"  Time       : {elapsed/60:.1f} min")
    print(f"{'═'*64}")


if __name__ == "__main__":
    main()