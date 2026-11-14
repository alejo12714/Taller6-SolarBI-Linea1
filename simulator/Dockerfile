# docker build -f simulator/Dockerfile -t solarbi-simulator:1.0.0 .
# Misma base y mismo núcleo de reglas que el ETL (las capas de pip se comparten en caché).
FROM python:3.12-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY simulator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && useradd --uid 10002 --no-create-home sim
COPY common/ common/
COPY simulator/simulador.py simulator/simulador.py
USER 10002
ENTRYPOINT ["python", "-u", "simulator/simulador.py"]
