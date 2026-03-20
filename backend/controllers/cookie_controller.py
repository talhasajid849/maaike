"""
controllers/cookie_controller.py
==================================
HTTP handlers for cookie management endpoints.
"""
import json
from flask import jsonify, request
from services.cookie_service import get_all_statuses, save_cookies


def get_status():
    return jsonify(get_all_statuses())


def upload():
    source = request.args.get("source", "jancisrobinson")
    if "file" not in request.files:
        return jsonify({"error": "No file"}), 400
    try:
        data   = json.loads(request.files["file"].read().decode("utf-8"))
        result = save_cookies(source, data)
        return jsonify(result)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 400