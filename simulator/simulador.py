"""
SolarBI · Simulador de streaming (CronJob de Kubernetes, cada minuto).

Estrategia "replay con desplazamiento temporal":
  * Cada ejecución toma el siguiente minuto de lecturas CRUDAS históricas
    (stream.fuente_replay, cargada por el ETL) y las re-emite con marca de tiempo
    del minuto actual, conservando los intervalos reales entre lecturas.
  * Pasa por las MISMAS reglas de calidad del ETL (solarbi_core.reglas) y hace
    upsert en Silver y Gold con origen='simulado' -> Grafana lo ve casi en tiempo real.
  * Puede inyectar anomalías (configurables por ConfigMap, sin reconstruir la imagen)
    para demostrar que las alertas reaccionan.
  * Idempotente: PK (inversor, read_ts) + ON CONFLICT; si el CronJob repite un minuto,
    no se duplica nada.
"""
from __future__ import annotations

import logging
import os
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "common"))
from solarbi_core import bd, reglas  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("simulador")

PROB = float(os.environ.get("ANOMALIA_PROB", "0.05"))
FORZAR = os.environ.get("FORZAR_ANOMALIA", "").strip()     # soc_bajo | trama_nula | sin_datos
CAMPOS = ["battvolt", "battcurr", "battsoc", "pv1volt", "pv1curr", "pv1power", "outvolta", "outw_a",
          "outva_a", "linevolta", "linepowerw_a", "outfreq", "linefreq", "temper1"]

UPSERT_GOLD = """
INSERT INTO gold.fact_telemetria_minuto AS f
      (sk_fecha, sk_tiempo, sk_inversor, ts_minuto_utc, n_lecturas, n_validas, seg_cubiertos,
       seg_cubiertos_valid, e_pv_kwh, e_carga_kwh, e_red_kwh, e_bat_carga_kwh, e_bat_descarga_kwh,
       soc_prom_pct, soc_min_pct, p_pv_max_w, origen)
SELECT to_char(s.ts_minuto AT TIME ZONE 'Asia/Shanghai', 'YYYYMMDD')::int,
       to_char(s.ts_minuto AT TIME ZONE 'Asia/Shanghai', 'HH24MI')::smallint,
       i.sk_inversor, s.ts_minuto, s.n_lecturas, s.n_validas, s.seg_cubiertos, s.seg_cubiertos_valid,
       s.e_pv_kwh, s.e_carga_kwh, s.e_red_kwh, s.e_bat_carga_kwh, s.e_bat_descarga_kwh,
       s.soc_prom_pct, s.soc_min_pct, s.p_pv_max_w, 'simulado'
FROM silver.lectura_minuto s
JOIN gold.dim_inversor i ON i.inversor_id = s.inversor AND i.es_vigente
WHERE s.origen = 'simulado' AND s.ts_minuto >= %(desde)s
ON CONFLICT (sk_inversor, ts_minuto_utc) DO UPDATE SET
   n_lecturas = EXCLUDED.n_lecturas, n_validas = EXCLUDED.n_validas,
   seg_cubiertos = EXCLUDED.seg_cubiertos, seg_cubiertos_valid = EXCLUDED.seg_cubiertos_valid,
   e_pv_kwh = EXCLUDED.e_pv_kwh, e_carga_kwh = EXCLUDED.e_carga_kwh, e_red_kwh = EXCLUDED.e_red_kwh,
   e_bat_carga_kwh = EXCLUDED.e_bat_carga_kwh, e_bat_descarga_kwh = EXCLUDED.e_bat_descarga_kwh,
   soc_prom_pct = EXCLUDED.soc_prom_pct, soc_min_pct = EXCLUDED.soc_min_pct, p_pv_max_w = EXCLUDED.p_pv_max_w
"""


def emitir(cur, inv: int, ahora: datetime) -> int:
    cur.execute("SELECT ultimo_original, ultimo_emitido FROM stream.cursor WHERE inversor = %s", (inv,))
    fila = cur.fetchone()
    if fila is not None and fila[1] >= ahora:
        log.info("inv%s: minuto %s ya emitido (re-ejecución idempotente)", inv, ahora.isoformat())
        return 0
    if fila is None:
        cur.execute("SELECT min(read_ts) FROM stream.fuente_replay WHERE inversor = %s", (inv,))
        inicio = cur.fetchone()[0]
        if inicio is None:
            log.warning("Sin fuente de replay para inversor %s (¿corrió el ETL?)", inv)
            return 0
        orig = inicio - timedelta(microseconds=1)
    else:
        orig = fila[0]
    cur.execute(f"SELECT read_ts, {', '.join(CAMPOS)} FROM stream.fuente_replay "
                "WHERE inversor = %s AND read_ts > %s AND read_ts <= %s + interval '60 seconds' ORDER BY read_ts",
                (inv, orig, orig))
    filas = cur.fetchall()
    if not filas:                                    # fin de la ventana: vuelve a empezar (bucle)
        cur.execute("DELETE FROM stream.cursor WHERE inversor = %s", (inv,))
        return emitir(cur, inv, ahora)
    df = pd.DataFrame(filas, columns=["read_ts_original"] + CAMPOS)
    base = ahora - timedelta(seconds=60)
    df["read_ts"] = base + (df["read_ts_original"] - df["read_ts_original"].iloc[0])
    df["inversor"], df["anomalia_inyectada"] = inv, None

    anomalia = FORZAR or (random.choice(["soc_bajo", "trama_nula", "sin_datos"]) if random.random() < PROB else "")
    if anomalia == "sin_datos":
        log.info("inv%s: anomalía 'sin_datos' -> no se emite el minuto (prueba de alerta de frescura)", inv)
        df = df.iloc[0:0]
    elif anomalia == "soc_bajo":
        df["battsoc"] = 8                            # por debajo de cualquier umbral razonable
        df["anomalia_inyectada"] = anomalia
    elif anomalia == "trama_nula":
        m = df.index % 2 == 0                        # 50 % de tramas vacías -> cae el % de válidas
        df.loc[m, CAMPOS] = None
        df.loc[m, "anomalia_inyectada"] = anomalia
    cur.execute("INSERT INTO stream.cursor VALUES (%s, %s, %s) ON CONFLICT (inversor) DO UPDATE "
                "SET ultimo_original = EXCLUDED.ultimo_original, ultimo_emitido = EXCLUDED.ultimo_emitido",
                (inv, filas[-1][0], ahora))
    if df.empty:
        return 0
    for c in ("battsoc", "pv1power", "outw_a", "outva_a", "linepowerw_a"):
        df[c] = df[c].astype("Int64")
    cur.execute("CREATE TEMP TABLE IF NOT EXISTS tmp_emit (LIKE stream.lectura_cruda) ON COMMIT DELETE ROWS")
    cols = ["inversor", "read_ts"] + CAMPOS + ["read_ts_original", "anomalia_inyectada"]
    bd.copiar(cur, "tmp_emit", df, cols)
    cur.execute(f"INSERT INTO stream.lectura_cruda ({', '.join(cols)}) SELECT {', '.join(cols)} FROM tmp_emit "
                "ON CONFLICT (inversor, read_ts) DO NOTHING")
    log.info("inv%s: %d lecturas emitidas (anomalía=%s)", inv, cur.rowcount, anomalia or "ninguna")
    return cur.rowcount


def procesar(cur, ahora: datetime) -> None:
    """Re-agrega los últimos 3 minutos (el minuto previo gana su último Δt)."""
    desde = ahora - timedelta(minutes=3)
    cur.execute(f"SELECT inversor AS inverter, read_ts, {', '.join(CAMPOS)} FROM stream.lectura_cruda "
                "WHERE read_ts >= %s - interval '2 minutes' ORDER BY inversor, read_ts", (desde,))
    filas = cur.fetchall()
    if not filas:
        return
    df = pd.DataFrame(filas, columns=["inverter", "read_ts"] + CAMPOS)
    for c in ("temper2", "temper3", "temper4"):
        df[c] = None
    df = reglas.preparar(df)
    # Mismo signo de battcurr que decidió el ETL con evidencia física (audit.etl_run).
    cur.execute("SELECT signo_battcurr FROM audit.etl_run WHERE estado = 'OK' ORDER BY run_id DESC LIMIT 1")
    fila = cur.fetchone()
    signo = int(os.environ.get("SIGNO_BATTCURR") or (fila[0] if fila else 1))
    p = reglas.Parametros(dt_max_s=float(os.environ.get("DT_MAX_S", "120")), signo_descarga=signo)
    df = reglas.aplicar_reglas(df, p, conteo=None)
    df = df.sort_values(["inverter", "read_ts"])
    df["dt_s"] = (df.groupby("inverter")["read_ts"].shift(-1) - df["read_ts"]).dt.total_seconds()
    df = df[df["read_ts"] >= pd.Timestamp(desde)]
    m = reglas.finalizar_minuto(reglas.agregar_minuto(reglas.integrar(df, p, None)))
    m = m.rename(columns={"inverter": "inversor"})
    m["origen"] = "simulado"
    for c in ("p_pv_max_w", "p_carga_max_w", "soc_min_pct"):
        m[c] = m[c].round().astype("Int64")
    cur.execute("DELETE FROM silver.lectura_minuto WHERE origen = 'simulado' AND ts_minuto >= %s", (desde,))
    bd.copiar(cur, "silver.lectura_minuto", m, list(m.columns))
    cur.execute(UPSERT_GOLD, {"desde": desde})
    log.info("Gold actualizado: %d minuto-inversor", cur.rowcount)


def main() -> int:
    ahora = (datetime.fromisoformat(os.environ["SIM_AHORA"]) if os.environ.get("SIM_AHORA")   # solo pruebas
             else datetime.now(timezone.utc)).replace(second=0, microsecond=0)
    with bd.conectar() as pg, pg.cursor() as cur:
        cur.execute("SELECT to_regclass('stream.fuente_replay')")
        if cur.fetchone()[0] is None:          # el CronJob arranca antes de que termine el Job ETL
            log.info("El ETL histórico aún no termina; nada que emitir en este minuto.")
            return 0
        for inv in (1, 2):
            emitir(cur, inv, ahora)
        procesar(cur, ahora)
        # Retención: el aterrizaje crudo solo guarda 2 días (Grafana lee Gold).
        cur.execute("DELETE FROM stream.lectura_cruda WHERE read_ts < now() - interval '2 days'")
        pg.commit()
    return 0


if __name__ == "__main__":
    sys.exit(main())
