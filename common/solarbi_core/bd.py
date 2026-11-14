"""Conexión a PostgreSQL y utilidades de carga masiva (COPY)."""
from __future__ import annotations

import io
import os

import pandas as pd
import psycopg


def conectar() -> psycopg.Connection:
    # Todas las credenciales llegan por variables de entorno inyectadas desde un Secret.
    return psycopg.connect(
        host=os.environ.get("PGHOST", "postgres"),          # nombre DNS del Service en K8s
        port=int(os.environ.get("PGPORT", "5432")),
        dbname=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        application_name=os.environ.get("APP_NAME", "solarbi"),
    )


def copiar(cur: psycopg.Cursor, tabla: str, df: pd.DataFrame, columnas: list[str]) -> int:
    """COPY FROM STDIN en formato CSV: ~10x más rápido que INSERT fila a fila."""
    if df.empty:
        return 0
    buf = io.StringIO()
    df[columnas].to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)
    with cur.copy(f"COPY {tabla} ({', '.join(columnas)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')") as cp:
        while chunk := buf.read(1 << 20):
            cp.write(chunk)
    return len(df)


def ejecutar_archivo(cur: psycopg.Cursor, ruta: str) -> None:
    with open(ruta, encoding="utf-8") as f:
        cur.execute(f.read())
