"""
controllers/enrich_controller.py
==================================
HTTP handlers for enrichment endpoints.
"""
from flask import jsonify, request
from services.enrich_service import (
    start_batch, stop_batch, get_state, get_source_status, test_search_one_source
)


def start():
    data          = request.get_json(silent=True) or {}
    limit         = int(data.get("limit", 0))
    sleep_sec     = float(data.get("sleep", 3.0))
    source        = data.get("source") or None    # None = all sources
    start_from_id = int(data.get("start_from_id", 0))
    end_at_id     = int(data.get("end_at_id", 0))

    # 'scope' is the canonical field: 'pending' | 'all' | 'found'
    # Fallback: honour legacy 'only_pending' boolean from older callers
    if "scope" in data:
        scope = data["scope"]
    elif not data.get("only_pending", True):
        scope = "all"
    else:
        scope = "pending"

    from flask import current_app
    from datetime import datetime
    sio = current_app.extensions.get("socketio")

    def on_progress(s):
        if sio: sio.emit("enrich_progress", s)

    def on_log(msg, level="info", source=None):
        if sio: sio.emit("enrich_log", {
            "msg": msg, "level": level,
            "source": source,  # which source this log line belongs to (for per-engine filtering)
            "ts": datetime.now().strftime("%H:%M:%S"),
        })

    started = start_batch(
        limit=limit, sleep_sec=sleep_sec,
        scope=scope,
        source_filter=source,
        start_from_id=start_from_id,
        end_at_id=end_at_id,
        on_progress=on_progress,
        on_log=on_log,
    )
    if not started:
        return jsonify({"error": "Already running"}), 409
    return jsonify({"ok": True})


def stop():
    stop_batch()
    return jsonify({"ok": True})


def status():
    return jsonify(get_state())


def source_status():
    """Per-source session health + run stats."""
    return jsonify(get_source_status())


def test_search():
    """
    POST /api/enrich/test-search
    Body: { source, name?, vintage?, lwin?, sleep?, limit?, search_hints? }
    """
    data = request.get_json(silent=True) or {}
    source = (data.get("source") or "").strip()
    name = (data.get("name") or "").strip()
    vintage = str(data.get("vintage") or "").strip()
    lwin = str(data.get("lwin") or "").strip()
    sleep_sec = float(data.get("sleep", 1.5))
    limit = int(data.get("limit", 5))
    search_hints = data.get("search_hints") if isinstance(data.get("search_hints"), dict) else None

    if not source:
        return jsonify({"ok": False, "error": "source is required"}), 400
    if not name and not search_hints:
        return jsonify({"ok": False, "error": "name is required"}), 400

    try:
        rows = test_search_one_source(
            source_key=source,
            name=name,
            vintage=vintage,
            lwin=lwin,
            sleep_sec=sleep_sec,
            limit=limit,
            search_hints=search_hints,
        )
        return jsonify({"ok": True, "count": len(rows), "results": rows})
    except (ValueError, RuntimeError) as e:
        return jsonify({"ok": False, "error": str(e)})
    except Exception as e:
        return jsonify({"ok": False, "error": f"Search failed: {type(e).__name__}: {e}"}), 500
