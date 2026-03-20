"""
routes/enrich_routes.py
========================
  router.post('/start',  enrichController.start)
  router.post('/stop',   enrichController.stop)
  router.get ('/status', enrichController.status)
"""
from flask import Blueprint
from middleware.auth import require_api_key
from controllers import enrich_controller as ctrl

enrich_bp = Blueprint("enrich", __name__, url_prefix="/api/enrich")

enrich_bp.post("/start" )(require_api_key(ctrl.start))
enrich_bp.post("/stop"  )(require_api_key(ctrl.stop))
enrich_bp.post("/test-search")(require_api_key(ctrl.test_search))
enrich_bp.get ("/status"       )(require_api_key(ctrl.status))
enrich_bp.get ("/source-status")(require_api_key(ctrl.source_status))
