"""
Prueba de idempotencia del ETL (INFORME_TECNICO.md §4.1).

Qué demuestra: ejecutar el ETL dos veces seguidas sobre la misma fuente deja Gold
exactamente igual (misma huella md5 en audit.etl_run y mismos conteos por tabla).

Cómo funciona:
  1. Extrae documentation/schema.sql del .zip de soporte de OpenCEM (ya versionado).
  2. Genera una SQLite SINTÉTICA con scripts/crear_sqlite_prueba.py (no se usan datos reales).
  3. Crea una base PostgreSQL temporal (solarbi_test_<id>) y corre etl/etl.py dos veces.
  4. Compara huellas y conteos. Al terminar elimina la base temporal.

La prueba NUNCA toca la base 'solarbi' real: crea y borra su propia base.

Requisitos: PostgreSQL 16 accesible y un usuario con permiso CREATEDB (en local, el
superusuario). Variables (todas opcionales, con estos valores por defecto):

    TEST_PGHOST=localhost  TEST_PGPORT=5432  TEST_PGUSER=postgres  TEST_PGPASSWORD=<clave>

Ejecución (con docker compose levantado, o cualquier PostgreSQL local):

    pip install -r etl/requirements.txt -r tests/requirements.txt
    TEST_PGPASSWORD=... pytest tests/test_idempotencia_etl.py -v

Si no hay PostgreSQL disponible, la prueba se omite (skip) en lugar de fallar.
"""
from __future__ import annotations

import os
import subprocess
import sys
import uuid
import zipfile
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

RAIZ = Path(__file__).resolve().parent.parent
ZIP_SOPORTE = RAIZ / "descargas-SolarBI" / "opencem-v1.0.0-support.zip"

PG = {
    "host": os.environ.get("TEST_PGHOST", "localhost"),
    "port": os.environ.get("TEST_PGPORT", "5432"),
    "user": os.environ.get("TEST_PGUSER", "postgres"),
    "password": os.environ.get("TEST_PGPASSWORD", os.environ.get("PGPASSWORD", "")),
}

TABLAS = [
    "silver.lectura_minuto",
    "gold.dim_inversor",
    "gold.fact_telemetria_minuto",
    "gold.fact_calidad_diaria",
    "gold.fact_evento_operativo",
    "gold.fact_contexto",
    "gold.fact_conciliacion_diaria",
]


def _conectar(dbname: str, autocommit: bool = False):
    return psycopg.connect(dbname=dbname, autocommit=autocommit, connect_timeout=5, **PG)


@pytest.fixture(scope="module")
def corridas(tmp_path_factory):
    """Corre el ETL dos veces sobre una base temporal y devuelve lo observado en cada corrida."""
    # --- PostgreSQL disponible?
    try:
        _conectar("postgres").close()
    except psycopg.OperationalError as e:
        pytest.skip(f"PostgreSQL no disponible para la prueba ({e}). Ver TEST_PG* en el docstring.")

    if not ZIP_SOPORTE.exists():
        pytest.skip(f"Falta {ZIP_SOPORTE} (necesario para extraer schema.sql)")

    tmp = tmp_path_factory.mktemp("idem")

    # --- 1. schema.sql oficial desde el .zip de soporte
    with zipfile.ZipFile(ZIP_SOPORTE) as z:
        miembro = next(n for n in z.namelist() if n.endswith("documentation/schema.sql"))
        schema = tmp / "schema.sql"
        schema.write_bytes(z.read(miembro))

    # --- 2. SQLite sintética
    sqlite_prueba = tmp / "prueba.sqlite"
    subprocess.run(
        [sys.executable, "-I", str(RAIZ / "scripts" / "crear_sqlite_prueba.py"), str(schema), str(sqlite_prueba)],
        check=True, capture_output=True, text=True)

    # --- 3. Base temporal (nunca la real)
    dbname = f"solarbi_test_{uuid.uuid4().hex[:8]}"
    with _conectar("postgres", autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{dbname}"')

    entorno = {
        **os.environ,
        "PGHOST": PG["host"], "PGPORT": PG["port"], "PGDATABASE": dbname,
        "PGUSER": PG["user"], "PGPASSWORD": PG["password"],
        "SQLITE_PATH": str(sqlite_prueba),
        "SQL_DIR": str(RAIZ / "sql"),
        "CALCULAR_SHA": "0",                       # la SQLite de prueba no tiene el SHA oficial
        "REPLAY_DESDE": "2026-06-13T00:00:00+08:00",
        "REPLAY_DIAS": "2",
    }

    resultados = []
    try:
        for n in (1, 2):
            p = subprocess.run([sys.executable, str(RAIZ / "etl" / "etl.py")],
                               env=entorno, capture_output=True, text=True, timeout=600)
            assert p.returncode == 0, f"ETL corrida {n} falló:\n{p.stdout}\n{p.stderr}"
            with _conectar(dbname) as con:
                huella, estado = con.execute(
                    "SELECT huella_gold, estado FROM audit.etl_run ORDER BY run_id DESC LIMIT 1").fetchone()
                conteos = {t: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in TABLAS}
            resultados.append({"huella": huella, "estado": estado, "conteos": conteos, "log": p.stderr + p.stdout})
        yield resultados
    finally:
        with _conectar("postgres", autocommit=True) as admin:
            admin.execute(f'DROP DATABASE IF EXISTS "{dbname}" WITH (FORCE)')


def test_ambas_corridas_terminan_ok(corridas):
    assert [c["estado"] for c in corridas] == ["OK", "OK"]


def test_huella_gold_identica_entre_corridas(corridas):
    primera, segunda = corridas
    assert primera["huella"], "la primera corrida no registró huella de Gold"
    assert primera["huella"] == segunda["huella"], "Gold cambió al re-ejecutar el ETL (no es idempotente)"


def test_conteos_por_tabla_no_cambian(corridas):
    primera, segunda = corridas
    assert primera["conteos"] == segunda["conteos"], (
        f"Conteos distintos entre corridas:\n1ª: {primera['conteos']}\n2ª: {segunda['conteos']}")


def test_la_prueba_no_es_vacia(corridas):
    """Evita un falso positivo: igualdad de tablas vacías no demuestra nada."""
    c = corridas[0]["conteos"]
    assert c["silver.lectura_minuto"] > 0
    assert c["gold.fact_telemetria_minuto"] > 0
    assert c["gold.fact_calidad_diaria"] > 0


def test_el_log_reporta_idempotencia(corridas):
    assert "IGUAL a" in corridas[1]["log"], "el log de la 2ª corrida debería declarar la huella IGUAL a la anterior"
