"""
Rutas de ventas. Delega en services; no contiene lógica SQL.
"""

from flask import Blueprint, jsonify

bp = Blueprint("sales", __name__, url_prefix="/sales")


@bp.get("/")
def list_sales():
    """Placeholder: listado de ventas (a implementar)."""
    return jsonify([])
