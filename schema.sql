PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS months (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ym TEXT UNIQUE NOT NULL,            -- ex: '2025-09'
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- uploads registrados (opcional p/ auditoria)
CREATE TABLE IF NOT EXISTS uploads (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  month_id INTEGER NOT NULL,
  kind TEXT NOT NULL CHECK(kind IN ('recebidas','perdas')),
  filename TEXT NOT NULL,
  uploaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (month_id) REFERENCES months(id)
);

-- métrica consolidada por mês/loja
CREATE TABLE IF NOT EXISTS metrics (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  month_id INTEGER NOT NULL,
  store TEXT NOT NULL,
  recebidas INTEGER NOT NULL DEFAULT 0,
  perdidas INTEGER NOT NULL DEFAULT 0,
  pct_perda REAL NOT NULL DEFAULT 0.0,
  FOREIGN KEY (month_id) REFERENCES months(id),
  UNIQUE(month_id, store)
);
