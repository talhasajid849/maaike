"""
controllers/xlsx_controller.py
================================
HTTP handlers for XLSX review enrichment endpoints.
"""
import io
import threading
from flask import jsonify, request, send_file


def upload():
    """POST /api/xlsx/upload - parse XLSX, start background enrichment job."""
    from config.sources import SOURCES
    from services.xlsx_service import (
        apply_lwin_filter,
        create_file_upload,
        create_job,
        detect_source_from_template,
        run_job,
    )

    f = request.files.get("file")
    if not f:
        return jsonify({"ok": False, "error": "No file uploaded"}), 400

    filename = f.filename or ""
    if not filename.lower().endswith((".xlsx", ".xlsm")):
        return jsonify({"ok": False, "error": "File must be .xlsx or .xlsm"}), 400

    template_bytes = f.read()

    source = (request.form.get("source") or "").strip().lower()
    if not source:
        source = detect_source_from_template(template_bytes) or "jancisrobinson"
    if source not in SOURCES:
        return jsonify({"ok": False, "error": f"Unsupported source: {source}"}), 400

    sleep_sec = float(request.form.get("sleep_sec", "2.5"))
    start_item_raw = (request.form.get("start_item") or "1").strip()
    try:
        start_item = int(start_item_raw or "1")
    except ValueError:
        return jsonify({"ok": False, "error": "Start item must be a whole number"}), 400
    if start_item < 1:
        return jsonify({"ok": False, "error": "Start item must be 1 or greater"}), 400

    try:
        upload = create_file_upload(filename, template_bytes, source)
        wines, lwin_filter = apply_lwin_filter(upload["wines"], request.form.get("lwin_filter"))
    except RuntimeError as e:
        return jsonify({"ok": False, "error": str(e)}), 500
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": f"Failed to parse XLSX: {e}"}), 400

    job_id = create_job(
        upload["template_bytes"],
        wines,
        source,
        sleep_sec,
        start_item=start_item,
        file_id=upload["file_id"],
        lwin_filter=lwin_filter,
    )
    threading.Thread(target=run_job, args=(job_id, source, sleep_sec), daemon=True).start()

    return jsonify({
        "ok": True,
        "job_id": job_id,
        "file_id": upload["file_id"],
        "total": len(wines),
        "file_total": len(upload["wines"]),
        "source": source,
        "start_item": start_item,
        "lwin_filter": lwin_filter,
    })


def status(job_id: str):
    """GET /api/xlsx/status/<job_id> - poll job progress."""
    from services.xlsx_service import get_job

    j = get_job(job_id)
    if not j:
        return jsonify({"ok": False, "error": "Job not found"}), 404
    return jsonify({"ok": True, **j})


def stop(job_id: str):
    """POST /api/xlsx/stop/<job_id> - request stopping a running job."""
    from services.xlsx_service import request_stop, get_job

    ok = request_stop(job_id)
    if not ok:
        return jsonify({"ok": False, "error": "Job not found"}), 404

    j = get_job(job_id) or {}
    return jsonify({"ok": True, **j})


def resume(job_id: str):
    """POST /api/xlsx/resume/<job_id> - resume a stopped/errored job from last progress."""
    from services.xlsx_service import resume_job, get_job, run_job

    resumed, reason = resume_job(job_id)
    if not resumed:
        code = 404 if reason == "not_found" else 400
        msg = {
            "not_found": "Job not found",
            "already_running": "Job is already running",
            "already_done": "Job is already complete",
        }.get(reason, "Job cannot be resumed")
        return jsonify({"ok": False, "error": msg}), code

    j = get_job(job_id) or {}
    source = j.get("source") or "jancisrobinson"
    sleep_sec = float(j.get("sleep_sec") or 2.5)
    threading.Thread(target=run_job, args=(job_id, source, sleep_sec), daemon=True).start()
    return jsonify({"ok": True, **(get_job(job_id) or {})})


def download(job_id: str):
    """GET /api/xlsx/download/<job_id> - download filled XLSX."""
    from services.xlsx_service import get_job_output

    data = get_job_output(job_id)
    if not data:
        return jsonify({"ok": False, "error": "Not ready yet"}), 404

    return send_file(
        io.BytesIO(data),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name="maaike_reviews.xlsx",
    )


def list_files():
    from services.xlsx_service import list_file_records

    return jsonify({"ok": True, "files": list_file_records()})


def file_detail(file_id: str):
    from services.xlsx_service import get_file_detail

    include_preview = (request.args.get("preview") or "1").strip().lower() not in ("0", "false", "no")
    detail = get_file_detail(file_id, include_preview=include_preview)
    if not detail:
        return jsonify({"ok": False, "error": "File not found"}), 404
    return jsonify({"ok": True, **detail})


def file_download(file_id: str):
    from services.xlsx_service import get_file_detail, get_file_download, get_job_progress_download

    kind = (request.args.get("kind") or "original").strip().lower()
    if kind not in ("original", "output", "progress"):
        return jsonify({"ok": False, "error": "Invalid download kind"}), 400

    if kind == "progress":
        detail = get_file_detail(file_id, include_preview=False)
        job_id = (
            (detail.get("active_job") or {}).get("job_id")
            or detail.get("active_job_id")
            or (detail.get("last_job") or {}).get("job_id")
            or detail.get("last_job_id")
            if detail else None
        )
        payload = get_job_progress_download(job_id) if job_id else None
    else:
        payload = get_file_download(file_id, kind=kind)

    if not payload:
        return jsonify({"ok": False, "error": "File not ready"}), 404

    if payload.get("bytes") is not None:
        return send_file(
            io.BytesIO(payload["bytes"]),
            as_attachment=True,
            download_name=payload["download_name"],
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    return send_file(
        payload["path"],
        as_attachment=True,
        download_name=payload["download_name"],
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


def restart(file_id: str):
    from config.sources import SOURCES
    from services.xlsx_service import restart_file_job, run_job

    data = request.get_json(silent=True) or {}
    source = (data.get("source") or "").strip().lower()
    if not source:
        source = "jancisrobinson"
    if source not in SOURCES:
        return jsonify({"ok": False, "error": f"Unsupported source: {source}"}), 400

    try:
        start_item = int(data.get("start_item") or 1)
    except ValueError:
        return jsonify({"ok": False, "error": "Start item must be a whole number"}), 400
    if start_item < 1:
        return jsonify({"ok": False, "error": "Start item must be 1 or greater"}), 400

    sleep_sec = float(data.get("sleep_sec") or 2.5)
    try:
        payload, reason = restart_file_job(
            file_id,
            source,
            sleep_sec=sleep_sec,
            start_item=start_item,
            lwin_filter_raw=data.get("lwin_filter"),
        )
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    if not payload:
        code = 404 if reason == "not_found" else 409 if reason == "already_running" else 400
        msg = {
            "not_found": "File not found",
            "already_running": "This file already has a running job",
        }.get(reason, "File cannot be restarted")
        return jsonify({"ok": False, "error": msg}), code

    threading.Thread(target=run_job, args=(payload["job_id"], source, sleep_sec), daemon=True).start()
    return jsonify({"ok": True, **payload})


def delete(file_id: str):
    from services.xlsx_service import delete_file

    ok, reason = delete_file(file_id)
    if not ok:
        code = 404 if reason == "not_found" else 409 if reason == "job_running" else 400
        msg = {
            "not_found": "File not found",
            "job_running": "Stop the running job before deleting this file",
        }.get(reason, "File cannot be deleted")
        return jsonify({"ok": False, "error": msg}), code

    return jsonify({"ok": True, "file_id": file_id})

