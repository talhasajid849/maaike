#!/usr/bin/env python3
"""
enrich.py  — CLI enrichment runner
====================================
Thin shell — all logic lives in services/enrich_service.py
This file just parses args and calls the service.

Usage:
  python enrich.py --all --sleep 3.0
  python enrich.py --limit 50
  python enrich.py --count
  python enrich.py --retry
  python enrich.py --source rp --limit 20    # RP only
"""
import argparse
import signal
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from models.wine_model import init_schema, count_pending
from services.enrich_service import enrich_one, get_state
from services.session_service import get_session
from models.wine_model import get_pending_wines, mark_not_found
from config.sources import SOURCES

STOP = False

def _sigint(*_):
    global STOP
    STOP = True
    print("\n  [Ctrl+C] finishing current wine…")

signal.signal(signal.SIGINT, _sigint)


def main():
    parser = argparse.ArgumentParser(description="MAAIKE CLI enricher")
    parser.add_argument("--limit",  type=int, default=-1)
    parser.add_argument("--all",    action="store_true")
    parser.add_argument("--count",  action="store_true")
    parser.add_argument("--retry",  action="store_true", help="Include not_found wines")
    parser.add_argument("--sleep",  type=float, default=3.0)
    parser.add_argument("--source", default="", help="jancisrobinson | robertparker | jr | rp")
    args = parser.parse_args()

    init_schema()

    if args.count:
        print(f"Pending: {count_pending(args.retry)}")
        return

    # Resolve which sources to use
    src_map = {"jr": "jancisrobinson", "rp": "robertparker",
               "js": "jamessuckling",  "dc": "decanter"}
    filter_source = src_map.get(args.source.lower(), args.source.lower())

    # Validate + load sessions upfront
    active = {}
    for key, cfg in SOURCES.items():
        if not cfg.get("enabled"):
            continue
        if filter_source and key != filter_source:
            continue
        session = get_session(key)
        if session:
            active[key] = session
            print(f"[{cfg['short']}] ✓ session loaded")
        else:
            print(f"[{cfg['short']}] ✗ no session — check cookies/{key}.json")

    if not active:
        print("[ERROR] No active sessions. Exiting.")
        sys.exit(1)

    limit = -1 if args.all else args.limit
    wines = get_pending_wines(limit=limit, include_not_found=args.retry)

    if not wines:
        print("Nothing to enrich.")
        return

    found = not_found = errors = 0
    t0 = time.time()

    print(f"\n{'═'*60}")
    print(f"  {len(wines)} wines | sources: {', '.join(active)} | sleep={args.sleep}s")
    print(f"{'═'*60}\n")

    for i, w in enumerate(wines, 1):
        if STOP:
            break

        pct = i / len(wines) * 100
        print(f"[{i:>4}/{len(wines)}] {pct:4.0f}%  {w['name']} ({w.get('vintage') or 'NV'})")

        try:
            ok = enrich_one(
                w["id"], w["name"],
                w.get("vintage") or "",
                w.get("lwin") or "",
                args.sleep,
            )
            if ok:
                found += 1
                print(f"  ✓ found")
            else:
                not_found += 1
                print(f"  ✗ not found")
        except Exception as e:
            errors += 1
            print(f"  [ERROR] {e}")

        print("─" * 60)
        time.sleep(args.sleep)

    elapsed = time.time() - t0
    processed = found + not_found + errors
    print(f"\n{'═'*60}")
    print(f"  Found     : {found}")
    print(f"  Not found : {not_found}")
    print(f"  Errors    : {errors}")
    print(f"  Hit rate  : {found/processed*100:.1f}%" if processed else "  Hit rate  : —")
    print(f"  Time      : {elapsed/60:.1f} min")
    print(f"{'═'*60}")


if __name__ == "__main__":
    main()