"""
middleware/auth.py
==================
Like Express middleware:

  app.use((req, res, next) => {
    if (req.headers['x-api-key'] !== API_KEY) return res.status(401).json({error:'Unauthorized'})
    next()
  })
"""
from functools import wraps
from flask import request, jsonify
from config import API_KEY


def require_api_key(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        # Allow CORS preflight without API key.
        if request.method == "OPTIONS":
            return ("", 204)

        key = (
            request.headers.get("X-API-Key")
            or request.args.get("api_key")
            or (request.get_json(silent=True) or {}).get("api_key")
            or request.form.get("api_key")
        )
        if key != API_KEY:
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated
