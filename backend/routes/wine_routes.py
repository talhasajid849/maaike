"""
routes/wine_routes.py
======================
Express equivalent:

  const router = express.Router()
  router.get('/',           wineController.listWines)
  router.post('/add',       wineController.addWine)
  router.get('/:id',        wineController.getWine)
  router.patch('/:id',      wineController.patchWine)
  router.delete('/:id',     wineController.removeWine)
  router.get('/:id/reviews',wineController.getWineReviews)
  router.post('/:id/enrich',wineController.triggerEnrich)
  module.exports = router
"""
from flask import Blueprint
from middleware.auth import require_api_key
from controllers import wine_controller as ctrl

wine_bp = Blueprint("wines", __name__, url_prefix="/api/wines")

# Accept both "/api/wines" and "/api/wines/" without creating duplicate endpoints.
wine_bp.get  ("/", strict_slashes=False)(require_api_key(ctrl.list_wines))
wine_bp.post ("/add"          )(require_api_key(ctrl.add_wine))
wine_bp.get  ("/<int:wine_id>")(require_api_key(ctrl.get_wine))
wine_bp.patch("/<int:wine_id>")(require_api_key(ctrl.patch_wine))
wine_bp.delete("/<int:wine_id>")(require_api_key(ctrl.remove_wine))

wine_bp.get ("/<int:wine_id>/reviews")(require_api_key(ctrl.get_wine_reviews))
wine_bp.post("/<int:wine_id>/enrich" )(require_api_key(ctrl.trigger_enrich))
