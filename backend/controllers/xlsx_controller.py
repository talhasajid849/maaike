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
    from services.xlsx_service import parse_xlsx, detect_source_from_template, create_job, run_job

    f = request.files.get("file")
    if not f:
        return jsonify({"ok": False, "error": "No file uploaded"}), 400

    filename = f.filename or ""
    if not filename.lower().endswith((".xlsx", ".xlsm")):
        return jsonify({"ok": False, "error": "File must be .xlsx or .xlsm"}), 400

    template_bytes = f.read()

    try:
        wines = parse_xlsx(template_bytes)
    except RuntimeError as e:
        return jsonify({"ok": False, "error": str(e)}), 500
    except Exception as e:
        return jsonify({"ok": False, "error": f"Failed to parse XLSX: {e}"}), 400

    if not wines:
        return jsonify({"ok": False, "error": "No wine rows found. Check the file format."}), 400

    source = (request.form.get("source") or "").strip().lower()
    if not source:
        source = detect_source_from_template(template_bytes) or "jancisrobinson"
    if source not in SOURCES:
        return jsonify({"ok": False, "error": f"Unsupported source: {source}"}), 400

    sleep_sec = float(request.form.get("sleep_sec", "2.5"))

    job_id = create_job(template_bytes, wines, source, sleep_sec)
    threading.Thread(target=run_job, args=(job_id, source, sleep_sec), daemon=True).start()

    return jsonify({"ok": True, "job_id": job_id, "total": len(wines), "source": source})


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

