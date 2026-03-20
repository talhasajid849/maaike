"""
app.py
======
The Express equivalent of app.js / server.js

  const app = express()
  app.use('/api/wines',   wineRoutes)
  app.use('/api/enrich',  enrichRoutes)
  app.use('/api',         miscRoutes)
  app.listen(5000)

Nothing else lives here. No business logic. No SQL.
"""
import os
import logging
from pathlib import Path

from flask import Flask, redirect, send_from_directory
from flask_socketio import SocketIO

from config import API_KEY, SECRET_KEY
from models.wine_model import init_schema
from routes.wine_routes import wine_bp
from routes.enrich_routes import enrich_bp
from routes.misc_routes import misc_bp
from routes.xlsx_routes import xlsx_bp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
)

BASE_DIR   = Path(__file__).parent
STATIC_DIR = BASE_DIR.parent / "frontend"

# ─── App factory ─────────────────────────────────────────────────────────────

app = Flask(__name__, static_folder=str(STATIC_DIR))
app.config["SECRET_KEY"] = SECRET_KEY
app.url_map.strict_slashes = False

socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading",
                    logger=False, engineio_logger=False)

# Store socketio on app so controllers can access it
app.extensions["socketio"] = socketio

# ─── Register blueprints (like app.use() in Express) ─────────────────────────

app.register_blueprint(wine_bp)
app.register_blueprint(enrich_bp)
app.register_blueprint(misc_bp)
app.register_blueprint(xlsx_bp)


@app.after_request
def add_cors_headers(resp):
    # Dev-safe CORS for /api routes (especially for browser preflight/redirect edge cases).
    if getattr(resp, "headers", None) is not None:
        resp.headers["Access-Control-Allow-Origin"] = "http://localhost:5173"
        resp.headers["Vary"] = "Origin"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type, X-API-Key"
        resp.headers["Access-Control-Allow-Methods"] = "GET, POST, PATCH, DELETE, OPTIONS"
    return resp

# ─── Auth endpoint ────────────────────────────────────────────────────────────

@app.post("/api/auth")
def auth():
    from flask import request, jsonify
    data = request.get_json(silent=True) or {}
    if data.get("api_key") == API_KEY:
        return jsonify({"ok": True})
    return jsonify({"ok": False, "message": "Invalid API key"}), 401

# ─── Frontend static files ────────────────────────────────────────────────────

@app.get("/")
def root():        return redirect("/dashboard")

@app.get("/signin")
def signin():      return send_from_directory(str(STATIC_DIR), "signin.html")

@app.get("/dashboard")
def dashboard():   return send_from_directory(str(STATIC_DIR), "dashboard.html")

@app.get("/<path:filename>")
def static_files(filename): return send_from_directory(str(STATIC_DIR), filename)

# ─── Health ───────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    from flask import jsonify
    from datetime import datetime
    return jsonify({"status": "ok", "time": datetime.now().isoformat()})

# ─── WebSocket ────────────────────────────────────────────────────────────────

@socketio.on("connect")
def on_connect():
    from services.enrich_service import get_state
    socketio.emit("enrich_progress", get_state())

# ─── Boot ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_schema()

    from config.sources import SOURCES
    active = [k for k, v in SOURCES.items() if v.get("enabled")]

    print("=" * 56)
    print("  MAAIKE — Wine Review Intelligence")
    print(f"  API Key : {API_KEY}")
    print(f"  Sources : {', '.join(active)}")
    print("=" * 56)

    socketio.run(app, host="0.0.0.0", port=5000,
                 debug=False, allow_unsafe_werkzeug=True)
