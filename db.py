# db.py
from __future__ import annotations
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).parent
DB_PATH  = BASE_DIR / "app.db"

def get_db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def init_db():
    schema = """
    PRAGMA journal_mode=WAL;

    CREATE TABLE IF NOT EXISTS months (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ym TEXT UNIQUE NOT NULL,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS uploads (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      month_id INTEGER,
      filename TEXT NOT NULL,
      uploaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (month_id) REFERENCES months(id)
    );

    -- Tabela principal das métricas (já com volume_total e pct_perda)
    CREATE TABLE IF NOT EXISTS metrics (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      month_id INTEGER NOT NULL,
      store TEXT NOT NULL,
      recebidas INTEGER NOT NULL DEFAULT 0,
      perdidas INTEGER NOT NULL DEFAULT 0,
      volume_total INTEGER NOT NULL DEFAULT 0,
      pct_perda REAL NOT NULL DEFAULT 0.0,
      FOREIGN KEY (month_id) REFERENCES months(id),
      UNIQUE(month_id, store)
    );
    """
    with get_db() as con:
        con.executescript(schema)

def month_id_for(con: sqlite3.Connection, ym: str) -> int:
    """
    Pega o ID do mês (AAAA-MM) ou cria se não existir.
    Usa a conexão aberta recebida (não abre outra).
    """
    row = con.execute("SELECT id FROM months WHERE ym = ?", (ym,)).fetchone()
    if row:
        return row["id"]
    cur = con.execute("INSERT INTO months (ym) VALUES (?)", (ym,))
    con.commit()
    return cur.lastrowid
