"""
Rutas de productos. Delega en services; no contiene lógica SQL.
"""

from flask import Blueprint, jsonify

bp = Blueprint("products", __name__, url_prefix="/products")


@bp.get("/")
def list_products():
    """Placeholder: listado de productos (a implementar)."""
    return jsonify([])
