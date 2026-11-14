# Contexto de construcción = raíz del repo:  docker build -f etl/Dockerfile -t solarbi-etl:1.0.0 .
# Base python:3.12-slim (Debian): pandas/numpy/psycopg traen wheels manylinux -> sin compilar.
# Alpine (musl) obligaría a compilar numpy/pandas: imagen final más grande y build de minutos.
FROM python:3.12-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 SQL_DIR=/app/sql
WORKDIR /app
COPY etl/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && useradd --uid 10001 --no-create-home etl
COPY common/ common/
COPY sql/ sql/
COPY etl/etl.py etl/etl.py
USER 10001
ENTRYPOINT ["python", "-u", "etl/etl.py"]
