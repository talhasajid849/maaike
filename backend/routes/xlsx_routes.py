"""
routes/xlsx_routes.py
======================
  router.post('/upload',          xlsxController.upload)
  router.get ('/status/<job_id>', xlsxController.status)
  router.get ('/download/<job_id>',xlsxController.download)
"""
from flask import Blueprint
from middleware.auth import require_api_key
from controllers import xlsx_controller as ctrl

xlsx_bp = Blueprint("xlsx", __name__, url_prefix="/api/xlsx")

xlsx_bp.post("/upload"              )(require_api_key(ctrl.upload))
xlsx_bp.get ("/files"               )(require_api_key(ctrl.list_files))
xlsx_bp.get ("/files/<file_id>"     )(require_api_key(ctrl.file_detail))
xlsx_bp.get ("/files/<file_id>/download")(require_api_key(ctrl.file_download))
xlsx_bp.post("/files/<file_id>/restart")(require_api_key(ctrl.restart))
xlsx_bp.delete("/files/<file_id>"   )(require_api_key(ctrl.delete))
xlsx_bp.post("/stop/<job_id>"       )(require_api_key(ctrl.stop))
xlsx_bp.post("/resume/<job_id>"     )(require_api_key(ctrl.resume))
xlsx_bp.get ("/status/<job_id>"     )(require_api_key(ctrl.status))
xlsx_bp.get ("/download/<job_id>"   )(require_api_key(ctrl.download))
