# Relatório Telefonia — Container

Stack: Flask + Gunicorn + Pandas, rodando em Python 3.11-slim.
Porta padrão: **8000**.

## 0) Pré-requisitos

- Docker 24+ (ou Podman equivalente)
- (Opcional) Docker Compose v2

> **Persistência do banco (SQLite)**  
> Se o seu `db.py` usa um `DB_PATH` em um subdiretório (ex.: `./data/relatorio.db`),
> é mais fácil persistir com volume. Se estiver no root `./relatorio.db`,
> recomendo mover para `./data/relatorio.db` (ou adaptar seu `db.py`) para montar `/app/data`.

## 1) Build

```bash
docker build -t estrela-telefonia:latest .
