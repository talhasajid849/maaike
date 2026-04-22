"""
services/enrich_service.py
===========================
Enrichment engine — now with per-source state and error visibility.

Changes:
  - Each source gets its own state dict (running, done, found, errors)
  - Errors from scrapers are surfaced to the UI log (not just logger)
  - Session check at batch start — warns immediately if a source has no cookies
  - start_batch() accepts optional source= param to run a single source only
"""
from __future__ import annotations

import importlib
import logging
import threading
import time
from typing import Callable

from config.sources import SOURCES
from models.job_state_model import load_enrich_snapshot, save_enrich_snapshot
from models.wine_model import (
    get_pending_wines, mark_not_found,
    upsert_reviews, count_pending, clear_wine_source_reviews,
)
from services.cookie_service import get_all_statuses
from services.normalize_service import normalize_reviews, reset_note_tracking
from services.session_service import get_session

logger = logging.getLogger("maaike.enrich")

# ─── Per-source state ─────────────────────────────────────────────────────────
# Global batch state (overall progress)
state = {
    "running":    False,
    "stop_flag":  False,
    "total":      0,
    "done":       0,
    "found":      0,
    "errors":     0,
    "last_id":    0,      # ID of the last wine processed (for resume)
    "source":     None,   # None = all sources, else key of single source
    "scope":      "pending",  # 'pending' | 'all' | 'found'
    "thread":     None,
    "source_stats": {},   # { source_key: { found, errors, skipped, no_session } }
}
_recovery_attempted = False


def _snapshot_payload(extra: dict | None = None) -> dict:
    payload = {
        "running": bool(state.get("running")),
        "stop_flag": bool(state.get("stop_flag")),
        "total": int(state.get("total") or 0),
        "done": int(state.get("done") or 0),
        "found": int(state.get("found") or 0),
        "errors": int(state.get("errors") or 0),
        "last_id": int(state.get("last_id") or 0),
        "source": state.get("source"),
        "scope": state.get("scope") or "pending",
        "source_stats": state.get("source_stats") or {},
    }
    if extra:
        payload.update(extra)
    return payload


def _save_snapshot(extra: dict | None = None) -> None:
    try:
        save_enrich_snapshot(_snapshot_payload(extra))
    except Exception:
        pass


def _recover_if_needed() -> None:
    global _recovery_attempted
    if _recovery_attempted:
        return
    _recovery_attempted = True
    snapshot = load_enrich_snapshot()
    if not snapshot:
        return
    if not snapshot.get("running"):
        state.update({
            "running": False,
            "stop_flag": False,
            "total": int(snapshot.get("total") or 0),
            "done": int(snapshot.get("done") or 0),
            "found": int(snapshot.get("found") or 0),
            "errors": int(snapshot.get("errors") or 0),
            "last_id": int(snapshot.get("last_id") or 0),
            "source": snapshot.get("source"),
            "scope": snapshot.get("scope") or "pending",
            "source_stats": snapshot.get("source_stats") or {},
        })
        return

    resume_from = int(snapshot.get("resume_from_id") or 0)
    saved_last = int(snapshot.get("last_id") or 0)
    start_from = max(resume_from, saved_last + 1 if saved_last > 0 else 0)
    start_batch(
        limit=int(snapshot.get("limit") or 0),
        sleep_sec=float(snapshot.get("sleep_sec") or 3.0),
        scope=snapshot.get("scope") or "pending",
        source_filter=snapshot.get("source") or None,
        start_from_id=start_from,
        end_at_id=int(snapshot.get("end_at_id") or 0),
    )


# ─── Public API ───────────────────────────────────────────────────────────────

def enrich_one(wine_id: int, name: str, vintage: str, lwin: str,
               sleep_sec: float = 3.0,
               source_filter: str | None = None,
               on_log: Callable | None = None,
               clear_first: bool = False) -> bool:
    """
    Enrich a single wine from all enabled sources (or one specific source).
    Returns True if at least one source found a review.

    clear_first=True: delete existing reviews for each source before searching
    (used by Re-search Reviews so stale/wrong reviews are always removed).
    """
    found_any = False
    sources_to_try = {
        k: v for k, v in SOURCES.items()
        if v.get("enabled") and (source_filter is None or k == source_filter)
    }

    source_items = list(sources_to_try.items())
    for idx, (source_key, cfg) in enumerate(source_items):
        ss = state["source_stats"].setdefault(source_key, {
            "found": 0, "errors": 0, "skipped": 0, "no_session": 0
        })

        session = get_session(source_key)
        if not session:
            msg = f"  [{source_key.upper()}] ⚠ No session — upload cookies in Settings"
            logger.warning(msg)
            _log(on_log, msg, "warn", source_key)
            ss["no_session"] += 1
            continue

        # When re-searching, wipe old reviews first so stale wrong data can't persist
        if clear_first:
            clear_wine_source_reviews(wine_id, source_key)

        try:
            results = _search_source(source_key, session, name, vintage, lwin, sleep_sec)
            if results:
                normalized = normalize_reviews(source_key, results, wine_name=name, wine_vintage=vintage)
                upsert_reviews(wine_id, source_key, normalized)
                found_any = True
                ss["found"] += 1
                _log(on_log, f"  [{source_key.upper()}] ✓ {len(results)} review(s)", "success", source_key)
            else:
                ss["skipped"] += 1
                _log(on_log, f"  [{source_key.upper()}] – not found", "info", source_key)
                # Also clear stale source reviews when search finds nothing
                # (prevents wrong previously-stored reviews from staying visible)
                clear_wine_source_reviews(wine_id, source_key)
        except Exception as e:
            ss["errors"] += 1
            err_msg = f"  [{source_key.upper()}] ✗ ERROR: {type(e).__name__}: {e}"
            logger.error(err_msg)
            _log(on_log, err_msg, "error", source_key)

        if idx < len(source_items) - 1:
            time.sleep(sleep_sec * 0.3)

    if not found_any:
        mark_not_found(wine_id)

    return found_any


def test_search_one_source(source_key: str, name: str, vintage: str = "",
                           lwin: str = "", sleep_sec: float = 1.5,
                           limit: int = 5,
                           search_hints: dict | None = None) -> list[dict]:
    """
    Run a single ad-hoc search against one source without writing to DB.
    Used by UI quality/accuracy checks.
    """
    if source_key not in SOURCES:
        raise ValueError(f"Unknown source: {source_key}")

    session = get_session(source_key)
    if not session:
        raise RuntimeError(f"No session for '{source_key}' - upload cookies first")

    results = _search_source(source_key, session, name, vintage, lwin, sleep_sec, search_hints=search_hints)
    if not results:
        if source_key != "jancisrobinson":
            mod = importlib.import_module(f"sources.{source_key}")
            diagnose = getattr(mod, "diagnose_no_result", None)
            if callable(diagnose):
                vintage_int = int(vintage) if vintage and vintage.isdigit() else None
                message = diagnose(session, name, vintage_int, lwin, search_hints=search_hints)
                if message:
                    raise RuntimeError(message)
        return []

    normalized = normalize_reviews(source_key, results, wine_name=name, wine_vintage=vintage)
    return normalized[:max(1, int(limit))]


def start_batch(limit: int = 0, sleep_sec: float = 3.0,
                scope: str = "pending",
                source_filter: str | None = None,
                start_from_id: int = 0,
                end_at_id: int = 0,
                on_progress: Callable | None = None,
                on_log: Callable | None = None) -> bool:
    """
    Start a background batch enrichment job.
    Returns False if already running.

    scope:
      'pending' — only wines not yet enriched (default)
      'all'     — pending + not_found (retry failed lookups)
      'found'   — wines that have a score but no tasting note (re-fetch notes)

    source_filter: None = all enabled sources, or e.g. "robertparker"

    RESTART BEHAVIOUR:
      When stopped mid-run and re-started, get_pending_wines returns only
      the wines NOT yet processed (still in the target status).
      So the counter correctly shows 1/N where N = remaining wines.
    """
    if state["running"]:
        return False

    limit = int(limit or 0)
    sleep_sec = float(sleep_sec or 3.0)
    start_from_id = int(start_from_id or 0)
    end_at_id = int(end_at_id or 0)

    state.update({
        "running": True, "stop_flag": False, "source": source_filter, "scope": scope,
        "total": 0, "done": 0, "found": 0, "errors": 0, "last_id": 0,
        "source_stats": {},
    })
    _save_snapshot({
        "limit": limit,
        "sleep_sec": sleep_sec,
        "resume_from_id": start_from_id,
        "end_at_id": end_at_id,
    })

    def _run():
        # ── Pre-flight: check which sources have sessions ──────────────────
        active = _active_sources(source_filter)
        _log(on_log, f"━━ Enrichment starting ━━", "info", source_filter)
        scope_label = {
            "pending": "pending only",
            "all":     "pending + not found",
            "found":   "score-only (re-fetch notes)",
        }.get(scope, scope)
        id_range = f" | IDs {start_from_id}–{end_at_id or '∞'}" if start_from_id or end_at_id else ""
        _log(on_log, f"Sources: {len(active)} active | scope: {scope_label} | {limit or 'all'} wines | sleep={sleep_sec}s{id_range}", "info", source_filter)

        for src in active:
            session = get_session(src)
            if session:
                _log(on_log, f"  ✓ [{src.upper()}] session ready", "success", src)
            else:
                _log(on_log, f"  ✗ [{src.upper()}] NO SESSION — upload cookies in Settings first", "error", src)

        no_sessions = [s for s in active if not get_session(s)]
        if no_sessions and len(no_sessions) == len(active):
            _log(on_log, "✗ No sources have valid sessions. Stopping.", "error", source_filter)
            state.update({"running": False})
            _save_snapshot({
                "running": False,
                "limit": limit,
                "sleep_sec": sleep_sec,
                "resume_from_id": start_from_id,
                "end_at_id": end_at_id,
            })
            _emit(on_progress)
            return

        _log(on_log, "", "info", source_filter)

        # ── Main loop ─────────────────────────────────────────────────────
        wines = get_pending_wines(
            limit=limit,
            include_not_found=(scope == "all"),
            refetch_found=(scope == "found"),
            start_from_id=start_from_id,
            end_at_id=end_at_id,
        )
        state["total"] = len(wines)
        reset_note_tracking()
        _save_snapshot({
            "limit": limit,
            "sleep_sec": sleep_sec,
            "resume_from_id": start_from_id,
            "end_at_id": end_at_id,
        })

        if not wines:
            _log(on_log, "✓ No pending wines — nothing to do.", "success", source_filter)
            state.update({"running": False, "stop_flag": False})
            _save_snapshot({
                "running": False,
                "limit": limit,
                "sleep_sec": sleep_sec,
                "resume_from_id": start_from_id,
                "end_at_id": end_at_id,
            })
            _emit(on_progress)
            return

        _log(on_log, f"Processing {len(wines)} wines…", "info", source_filter)
        _emit(on_progress)

        for idx, w in enumerate(wines):
            if state["stop_flag"]:
                remaining = state["total"] - state["done"]
                _log(on_log, f"⏹ Stopped by user. {remaining} wines remaining — re-run to continue.", "warn", source_filter)
                break

            _log(on_log, f"[{state['done']+1}/{len(wines)}] {w['name']} ({w.get('vintage') or 'NV'})", "info", source_filter)

            try:
                found = enrich_one(
                    w["id"], w["name"],
                    w.get("vintage") or "",
                    w.get("lwin") or "",
                    sleep_sec,
                    source_filter=source_filter,
                    on_log=on_log,
                )
                if found:
                    state["found"] += 1
                else:
                    _log(on_log, "  → not found on any source", "warn", source_filter)
            except Exception as e:
                state["errors"] += 1
                _log(on_log, f"  ✗ FATAL: {e}", "error", source_filter)

            state["done"]    += 1
            state["last_id"]  = w["id"]
            _save_snapshot({
                "limit": limit,
                "sleep_sec": sleep_sec,
                "resume_from_id": start_from_id,
                "end_at_id": end_at_id,
            })
            _emit(on_progress)
            if idx < len(wines) - 1:
                time.sleep(sleep_sec)

        # ── Summary ───────────────────────────────────────────────────────
        state.update({"running": False, "stop_flag": False})
        hr = state["found"] / state["total"] * 100 if state["total"] else 0
        _save_snapshot({
            "running": False,
            "limit": limit,
            "sleep_sec": sleep_sec,
            "resume_from_id": start_from_id,
            "end_at_id": end_at_id,
        })
        _log(on_log, "", "info", source_filter)
        _log(on_log, f"━━ Done: {state['found']}/{state['total']} found ({hr:.1f}%) ━━", "success", source_filter)

        for src, ss in state["source_stats"].items():
            _log(on_log,
                f"  [{src.upper()}] found={ss['found']} errors={ss['errors']} "
                f"not_found={ss['skipped']} no_session={ss['no_session']}",
                "success" if ss["errors"] == 0 else "warn",
                src
            )

        _emit(on_progress)

    t = threading.Thread(target=_run, daemon=True)
    state["thread"] = t
    t.start()
    return True


def stop_batch():
    state["stop_flag"] = True
    _save_snapshot({"running": bool(state.get("running")), "stop_flag": True})


def get_state() -> dict:
    _recover_if_needed()
    pct = round(state["done"] / state["total"] * 100, 1) if state["total"] else 0
    return {**state, "pct": pct, "thread": None}


def get_source_status() -> dict:
    """
    Return session + stats status for all sources (enabled OR disabled).
    Always returns at least one entry per source so the UI never gets stuck.
    """
    _recover_if_needed()
    result = {}
    cookie_statuses = get_all_statuses()
    for key, cfg in SOURCES.items():
        enabled = cfg.get("enabled", False)
        # Try to load session — but never block/raise here
        session = None
        if enabled:
            try:
                session = get_session(key)
            except (Exception, SystemExit) as e:
                logger.warning("[%s] session check error: %s", key, e)

        ss = state["source_stats"].get(key, {})
        ck = cookie_statuses.get(key, {}) if cfg.get("needs_cookies") else {}
        session_message = None
        if session is None:
            session_message = ck.get("message") or ("No cookies" if cfg.get("needs_cookies") else None)
        result[key] = {
            "key":         key,
            "name":        cfg.get("name", key),
            "short":       cfg.get("short", key.upper()[:2]),
            "icon":        cfg.get("icon", "🔍"),
            "color":       cfg.get("color", "#8b949e"),
            "enabled":     enabled,
            "has_session": session is not None,
            "session_message": session_message,
            "found":       ss.get("found", 0),
            "errors":      ss.get("errors", 0),
            "skipped":     ss.get("skipped", 0),
            "no_session":  ss.get("no_session", 0),
        }
    return result


# ─── Internals ────────────────────────────────────────────────────────────────

def _search_source(source: str, session, name: str, vintage: str,
                   lwin: str, sleep_sec: float, search_hints: dict | None = None) -> list:
    """Dynamically dispatch to the right scraper."""
    if source == "jancisrobinson":
        from maaike_phase1 import search_wine
        return search_wine(session, name, vintage, lwin, search_hints)

    mod = importlib.import_module(f"sources.{source}")
    vintage_int = int(vintage) if vintage and vintage.isdigit() else None
    cfg = SOURCES[source]
    effective_sleep = max(sleep_sec, cfg.get("sleep_sec", sleep_sec))
    return mod.search_wine(session, name, vintage_int, lwin, effective_sleep, search_hints=search_hints)


def _active_sources(source_filter: str | None = None) -> list:
    if source_filter:
        return [source_filter] if SOURCES.get(source_filter, {}).get("enabled") else []
    return [k for k, v in SOURCES.items() if v.get("enabled")]


def _emit(cb: Callable | None):
    if cb:
        try: cb(get_state())
        except Exception: pass


def _log(cb: Callable | None, msg: str, level: str = "info", source: str | None = None):
    logger.info(msg)
    if cb:
        try: cb(msg, level, source)  # pass source tag so frontend can filter per-engine
        except Exception: pass
