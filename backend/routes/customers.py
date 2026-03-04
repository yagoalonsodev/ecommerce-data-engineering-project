"""
Rutas de clientes. Delega en services; no contiene lógica SQL.
"""

from flask import Blueprint, jsonify

bp = Blueprint("customers", __name__, url_prefix="/customers")


@bp.get("/")
def list_customers():
    """Placeholder: listado de clientes (a implementar)."""
    return jsonify([])
