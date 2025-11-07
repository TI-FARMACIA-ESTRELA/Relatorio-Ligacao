# -------- Base: Python slim, timezone BR, sem raízes carnívoras --------
FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=America/Sao_Paulo

# libs de sistema mínimas p/ pandas/openpyxl etc.
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential \
      tzdata \
      curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# (opcional) copie só requirements primeiro pra cachear melhor
# Se você já tem requirements.txt, descomente as duas linhas abaixo:
# COPY requirements.txt .
# RUN pip install -r requirements.txt

# Como nem sempre o req.txt tá redondo, instalo o core aqui
# (se já usa requirements.txt, pode REMOVER esse RUN)
RUN pip install --no-cache-dir \
      flask \
      gunicorn \
      pandas \
      openpyxl \
      pyarrow

# Copia código do app
COPY . /app

# cria pastas que o app usa (ajuste se seu projeto usar outras)
RUN mkdir -p /app/uploads /app/static /app/templates

# user não-root
ARG APP_UID=1001
ARG APP_GID=1001
RUN addgroup --gid ${APP_GID} appgroup && \
    adduser --disabled-password --uid ${APP_UID} --gid ${APP_GID} appuser && \
    chown -R appuser:appgroup /app
USER appuser

# Porta do app (usa 1327 no projeto)
EXPOSE 1327

# Healthcheck simples
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD curl -fsS http://127.0.0.1:1327/ >/dev/null || exit 1

# Gunicorn — 2 workers sync (CPU-bound? aumente depois), bind 0.0.0.0:
# 'app:app' -> arquivo app.py com objeto Flask chamado 'app'
ENV GUNICORN_CMD_ARGS="--workers=2 --threads=4 --timeout=60 --bind=0.0.0.0:1327 --log-level=info"
CMD ["gunicorn", "app:app"]
