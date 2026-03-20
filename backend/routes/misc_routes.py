"""
routes/misc_routes.py
======================
All remaining API endpoints.
"""
from flask import Blueprint
from middleware.auth import require_api_key
from controllers import wine_controller as wctrl
from controllers import cookie_controller as cctrl

misc_bp = Blueprint("misc", __name__, url_prefix="/api")

# Stats & meta
misc_bp.get ("/stats"          )(require_api_key(wctrl.stats))
misc_bp.get ("/sources"        )(require_api_key(wctrl.list_sources))
misc_bp.get ("/filter-options" )(require_api_key(wctrl.filter_options))

# CSV
misc_bp.post("/upload"          )(require_api_key(wctrl.upload_csv))
misc_bp.post("/upload-reviews"  )(require_api_key(wctrl.upload_reviews_csv))
# /download removed — CSV export is now 100% frontend (see WineList.jsx winestoCSV)
# misc_bp.get ("/download"        )(require_api_key(wctrl.download_csv))

# Cookies
misc_bp.get ("/cookies/status")(require_api_key(cctrl.get_status))
misc_bp.post("/cookies"       )(require_api_key(cctrl.upload))

# Admin
misc_bp.post("/admin/reset-not-found")(require_api_key(wctrl.reset_not_found))
misc_bp.post("/admin/reset-found"     )(require_api_key(wctrl.reset_found))
misc_bp.post("/admin/wipe-wines"      )(require_api_key(wctrl.wipe_all_wines))
misc_bp.post("/admin/fix-notes"       )(require_api_key(wctrl.fix_notes))