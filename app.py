# app.py
from __future__ import annotations
from flask import Flask
from pathlib import Path

from routes import bp
from db import init_db, get_db, DB_PATH

app = Flask(__name__, static_folder="static", template_folder="templates")
app.secret_key = "RelatorioTelefonia@Estrela-2025"  # pode trocar depois

# registra rotas
app.register_blueprint(bp)

def _ensure_db():
    """Cria/valida o schema do SQLite."""
    if not DB_PATH.exists():
        init_db()
        return
    # tenta um SELECT simples; se der ruim, recria schema
    try:
        with get_db() as con:
            con.execute("SELECT 1 FROM months LIMIT 1")
    except Exception:
        init_db()

if __name__ == "__main__":
    _ensure_db()
    # Sobe o Flask dev server
    app.run(host="0.0.0.0", port=8000, debug=True)
