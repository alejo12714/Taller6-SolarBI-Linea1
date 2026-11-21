#!/usr/bin/env bash
# Crea el clúster k3d de SolarBI (INFORME_TECNICO.md §6.1). Es idempotente: si ya existe, no hace nada.
#
#   * 1 nodo servidor, sin Traefik (no se usa Ingress: ahorra RAM)
#   * NodePort 30432 (PostgreSQL) -> localhost:5432   | NodePort 30300 (Grafana) -> localhost:3000
#   * ./data  ->  /data del nodo (Bronze, solo lectura desde el Job ETL vía hostPath)
#
# Variables opcionales:  CLUSTER=solarbi  PG_PORT=5432  GRAFANA_PORT=3000  DATA_DIR=./data
# Si el 5432 del anfitrión está ocupado por otro PostgreSQL:  PG_PORT=5434 make cluster
set -euo pipefail

CLUSTER="${CLUSTER:-solarbi}"
PG_PORT="${PG_PORT:-5432}"
GRAFANA_PORT="${GRAFANA_PORT:-3000}"
RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="${DATA_DIR:-$RAIZ/data}"

for cmd in docker k3d; do
  command -v "$cmd" >/dev/null 2>&1 || { echo "ERROR: falta '$cmd' en el PATH (ver 'Requisitos del equipo anfitrión' del README)." >&2; exit 1; }
done
docker info >/dev/null 2>&1 || { echo "ERROR: el daemon de Docker no responde. ¿Está abierto Docker Desktop?" >&2; exit 1; }

if [ ! -d "$DATA_DIR" ]; then
  echo "ERROR: no existe $DATA_DIR. Descarga y descomprime el Bronze primero (README, sección 1)." >&2
  exit 1
fi
if ! ls "$DATA_DIR"/*.sqlite >/dev/null 2>&1; then
  echo "AVISO: no hay ningún .sqlite en $DATA_DIR; el Job ETL fallará hasta que lo coloques." >&2
fi

if k3d cluster list 2>/dev/null | awk 'NR>1 {print $1}' | grep -qx "$CLUSTER"; then
  echo "El clúster '$CLUSTER' ya existe; no se crea de nuevo."
  k3d cluster start "$CLUSTER" >/dev/null 2>&1 || true   # por si estaba detenido
  exit 0
fi

# Git Bash (Windows): evita que MSYS reescriba rutas y entrega a Docker la ruta con formato C:/...
export MSYS_NO_PATHCONV=1
DATA_DOCKER="$(cd "$DATA_DIR" && (pwd -W 2>/dev/null || pwd))"

echo "Creando clúster k3d '$CLUSTER' (Grafana :$GRAFANA_PORT, PostgreSQL :$PG_PORT, datos: $DATA_DOCKER)"
k3d cluster create "$CLUSTER" \
  --servers 1 --agents 0 \
  --volume "$DATA_DOCKER:/data@server:0" \
  -p "$PG_PORT:30432@server:0" \
  -p "$GRAFANA_PORT:30300@server:0" \
  --k3s-arg "--disable=traefik@server:0" \
  --wait --timeout 180s

kubectl config use-context "k3d-$CLUSTER" >/dev/null
echo "Clúster listo. Contexto actual: $(kubectl config current-context)"
