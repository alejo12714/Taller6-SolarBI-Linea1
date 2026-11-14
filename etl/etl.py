"""
SolarBI · ETL histórico Bronze (SQLite OpenCEM) -> Silver -> Gold (PostgreSQL).

Se ejecuta como Job de Kubernetes: corre una vez, termina y deja evidencia en
audit.etl_run (estado, conteos por regla, huella de Gold) y en el log del pod.

Idempotencia:
  * La fuente se abre en modo inmutable (mode=ro&immutable=1): Bronze nunca cambia.
  * Silver: reemplazo transaccional de la ventana [desde, hasta) -> DELETE + COPY.
  * Gold: upsert por llave natural con guarda IS DISTINCT FROM.
  * Al final se calcula una huella md5 de Gold; dos corridas seguidas deben coincidir.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "common"))
from solarbi_core import bd, reglas  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("etl")

SQLITE_PATH = os.environ.get("SQLITE_PATH", "/data/opencem-v1.0.0.sqlite")
SQL_DIR = Path(os.environ.get("SQL_DIR", Path(__file__).resolve().parent.parent / "sql"))
CHUNK = int(os.environ.get("CHUNK_ROWS", "250000"))
DESDE = os.environ.get("VENTANA_DESDE")            # opcional, ISO UTC (carga parcial)
HASTA = os.environ.get("VENTANA_HASTA")
REPLAY_DESDE = os.environ.get("REPLAY_DESDE", "2026-06-01T00:00:00+08:00")
REPLAY_DIAS = int(os.environ.get("REPLAY_DIAS", "7"))
CALCULAR_SHA = os.environ.get("CALCULAR_SHA", "1") == "1"
SHA_ESPERADO = "5fe1f213a4a6804cdfc75f6173f437772c918f45dc0f127eb7e8936c2c2d9fbb"  # validation_results.json

DDL = ["01_esquemas_y_roles.sql", "02_silver_audit.sql", "03_gold_ddl.sql",
       "04_gold_semillas.sql", "05_vistas_kpi.sql"]


# --------------------------------------------------------------------------- utilidades
def sha256(ruta: str) -> str:
    h = hashlib.sha256()
    with open(ruta, "rb") as f:
        while b := f.read(1 << 24):
            h.update(b)
    return h.hexdigest()


def abrir_bronze() -> sqlite3.Connection:
    # immutable=1: SQLite no crea -wal/-journal ni toma bloqueos -> la capa Bronze no se toca.
    return sqlite3.connect(f"file:{SQLITE_PATH}?mode=ro&immutable=1", uri=True)


def ventana(src: sqlite3.Connection) -> tuple[float, float]:
    lo, hi = src.execute("SELECT min(read_ts), max(read_ts) FROM analog_measurements").fetchone()
    if DESDE:
        lo = max(lo, pd.Timestamp(DESDE).timestamp())
    if HASTA:
        hi = min(hi, pd.Timestamp(HASTA).timestamp())
    # Alinear la ventana al minuto: Silver se indexa por el INICIO del minuto, así que una
    # ventana que empieza a mitad de minuto dejaría ese primer minuto fuera del reemplazo.
    return float(int(lo // 60) * 60), float((int(hi // 60) + 1) * 60)


def detectar_signo(src: sqlite3.Connection, lo: float) -> int:
    """
    El diccionario dice 'battcurr positivo = descarga', pero el pipeline derivado
    de los autores etiqueta 'battery_discharging' con battcurr <= -5 A. Conflicto
    documental -> se resuelve con evidencia física: correlación entre la corriente
    media horaria y el cambio de SoC en esa hora (30 días de muestra).
    """
    forzado = os.environ.get("SIGNO_BATTCURR")
    if forzado in ("1", "-1"):
        return int(forzado)
    df = pd.read_sql_query(
        "SELECT inverter, read_ts, battcurr, battsoc FROM analog_measurements "
        "WHERE read_ts >= ? AND read_ts < ? AND battcurr IS NOT NULL AND battsoc IS NOT NULL",
        src, params=(lo, lo + 30 * 86400))
    if len(df) < 1000:
        return 1
    df["h"] = (df["read_ts"] // 3600).astype(int)
    g = df.groupby(["inverter", "h"]).agg(i=("battcurr", "mean"),
                                           s0=("battsoc", "first"), s1=("battsoc", "last"))
    corr = np.corrcoef(g["i"], g["s1"] - g["s0"])[0, 1]
    signo = -1 if corr > 0 else 1        # corriente positiva sube el SoC => positivo = carga
    log.info("Correlación corriente vs ΔSoC = %.3f -> signo de descarga = %+d", corr, signo)
    return signo


# --------------------------------------------------------------------------- dimensiones
def cargar_dim_inversor(src, cur):
    """
    SCD2 sobre (SoC de alarma, SoC de parada). Ignora tramas nulas.
    battpackrateah NO es atributo de cambio: en el inversor 1 alterna 0 <-> 200 Ah unas
    106 000 veces (lectura intermitente del registro) y en el inversor 2 vale siempre 0.
    Se toma el último valor > 0 como capacidad declarada y el 0 se trata como "no reportado".
    """
    cfg = pd.read_sql_query("""
        WITH s AS (
          SELECT inverter, read_ts, batsoclowalarm AS alarma, batstopsoc AS parada,
                 lag(batsoclowalarm) OVER w AS al0, lag(batstopsoc) OVER w AS pa0
          FROM settings
          WHERE batsoclowalarm IS NOT NULL AND batstopsoc IS NOT NULL
          WINDOW w AS (PARTITION BY inverter ORDER BY read_ts))
        SELECT inverter, read_ts, alarma, parada FROM s
        WHERE al0 IS NULL OR alarma <> al0 OR parada <> pa0
        ORDER BY inverter, read_ts""", src)
    capacidad = dict(src.execute(
        "SELECT inverter, max(battpackrateah) FROM settings WHERE battpackrateah > 0 GROUP BY inverter").fetchall())
    cfg["ah"] = cfg["inverter"].map(capacidad)
    proto = dict(src.execute(
        "SELECT inverter, max(protocolver) FROM specs WHERE protocolver IS NOT NULL GROUP BY inverter").fetchall())
    for inv, grp in cfg.groupby("inverter"):
        grp = grp.reset_index(drop=True)
        for k, r in grp.iterrows():
            desde = "-infinity" if k == 0 else datetime.fromtimestamp(r.read_ts, timezone.utc)
            hasta = ("infinity" if k == len(grp) - 1
                     else datetime.fromtimestamp(grp.loc[k + 1, "read_ts"], timezone.utc))
            attrs = f"{r.ah}|{r.alarma}|{r.parada}|{proto.get(inv)}"
            cur.execute("""
                INSERT INTO gold.dim_inversor (inversor_id, nombre, subsistema, version_protocolo,
                    capacidad_bat_ah, soc_alarma_pct, soc_parada_pct, hash_atributos,
                    vigente_desde, vigente_hasta, es_vigente)
                VALUES (%s, %s, %s, %s, %s, %s, %s, md5(%s), %s, %s, %s)
                ON CONFLICT (inversor_id, vigente_desde) DO UPDATE SET
                    vigente_hasta = EXCLUDED.vigente_hasta, es_vigente = EXCLUDED.es_vigente,
                    capacidad_bat_ah = EXCLUDED.capacidad_bat_ah, soc_alarma_pct = EXCLUDED.soc_alarma_pct,
                    soc_parada_pct = EXCLUDED.soc_parada_pct, hash_atributos = EXCLUDED.hash_atributos""",
                (int(inv), f"Inversor {inv}", f"Subsistema {inv}", proto.get(inv),
                 None if pd.isna(r.ah) else float(r.ah),
                 int(r.alarma), int(r.parada), attrs, desde, hasta, hasta == "infinity"))
    log.info("dim_inversor: %d versiones SCD2", len(cfg))


def cargar_contexto(src, cur):
    """Contexto JSON -> dim_tipo_contexto + fact_contexto, con minimización (Ley 1581)."""
    permitidas = {"source", "source_version", "context_type", "device", "inverter", "task_type",
                  "table_name", "field_group", "source_table"}
    familias = [("workload", "Carga de trabajo"), ("solar", "Solar"), ("calendar", "Calendario"),
                ("weather", "Clima"), ("data_", "Registro"), ("telemetry", "Registro"),
                ("battery", "Telemetría derivada"), ("pv_", "Telemetría derivada"),
                ("grid", "Telemetría derivada"), ("high_ac", "Telemetría derivada")]
    filas, tipos = [], {}
    for cid, rec, ini, fin, value in src.execute(
            'SELECT id, recorded, start, "end", value FROM context ORDER BY id'):
        v = json.loads(value)
        ct = v.get("context_type") or "sin_tipo"
        # Minimización: se conservan llaves de la lista blanca y valores numéricos/booleanos.
        # Texto libre (textual_description, summary, comandos) NO pasa a Gold.
        mini = {k: x for k, x in v.items()
                if k in permitidas or isinstance(x, (int, float, bool))}
        inv = v.get("inverter") if v.get("inverter") in (1, 2) else None
        filas.append((cid, rec, ini, fin, ct, v.get("source", "desconocida"), json.dumps(mini), inv))
        if ct not in tipos:
            fam = next((f for p, f in familias if p in ct), "Otro")
            tipos[ct] = (fam, ("workload" in ct) or ("high_ac" in ct))
    for ct, (fam, carga) in tipos.items():
        cur.execute("""INSERT INTO gold.dim_tipo_contexto (sk_tipo_contexto, context_type, familia, es_carga_electrica)
                       VALUES ((SELECT coalesce(max(sk_tipo_contexto), 0) + 1 FROM gold.dim_tipo_contexto), %s, %s, %s)
                       ON CONFLICT (context_type) DO NOTHING""", (ct, fam, carga))
    cur.execute("CREATE TEMP TABLE tmp_ctx (id bigint, rec double precision, ini double precision, "
                "fin double precision, ct text, fuente text, payload jsonb, inv smallint) ON COMMIT DROP")
    with cur.copy("COPY tmp_ctx FROM STDIN") as cp:
        for f in filas:
            cp.write_row(f)
    cur.execute("""
        INSERT INTO gold.fact_contexto AS x (contexto_id, sk_fecha_inicio, sk_tipo_contexto, sk_inversor,
               registrado_utc, inicio_utc, fin_utc, fuente, payload)
        SELECT t.id, to_char(to_timestamp(t.ini) AT TIME ZONE 'Asia/Shanghai', 'YYYYMMDD')::int,
               tc.sk_tipo_contexto, coalesce(i.sk_inversor, -1),
               to_timestamp(t.rec), to_timestamp(t.ini), to_timestamp(t.fin), t.fuente, t.payload
        FROM tmp_ctx t
        JOIN gold.dim_tipo_contexto tc ON tc.context_type = t.ct
        LEFT JOIN gold.dim_inversor i ON i.inversor_id = t.inv
              AND to_timestamp(t.ini) >= i.vigente_desde AND to_timestamp(t.ini) < i.vigente_hasta
        ON CONFLICT (contexto_id) DO UPDATE SET payload = EXCLUDED.payload,
               sk_tipo_contexto = EXCLUDED.sk_tipo_contexto, sk_inversor = EXCLUDED.sk_inversor
        WHERE (x.payload, x.sk_tipo_contexto, x.sk_inversor)
              IS DISTINCT FROM (EXCLUDED.payload, EXCLUDED.sk_tipo_contexto, EXCLUDED.sk_inversor)""")
    log.info("fact_contexto: %d registros, %d tipos", len(filas), len(tipos))


def cargar_contadores(src, cur):
    df = pd.read_sql_query("""
        SELECT inverter AS inversor,
               date(read_ts + 8*3600, 'unixepoch') AS fecha_local,      -- día local del sitio
               max(pvgeneratenergytoday) AS pv_kwh_contador,
               max(usedenergytoday)      AS uso_kwh_contador,
               count(*) AS n_muestras
        FROM statistics
        WHERE pvgeneratenergytoday IS NOT NULL
        GROUP BY 1, 2""", src)
    cur.execute("TRUNCATE silver.contador_diario")
    bd.copiar(cur, "silver.contador_diario", df, list(df.columns))
    log.info("silver.contador_diario: %d días-inversor", len(df))


def cargar_eventos_fuente(src, cur, lo: float, hi: float):
    """
    Eventos que el propio inversor reporta (no derivados):
      * ALARMA_SISTEMA: status.sysalarmflag con algún elemento != 0. Se agrupan en islas
        (mismo payload, separación <= 5 min). Bits sin decodificar: el mapa es propietario.
      * FALLA_HISTORICA: cada payload distinto de fault_history, anclado a su tiempo
        calibrado *_time (política de los autores: primer régimen de reloj observado).
    """
    alarmas = pd.read_sql_query(
        "SELECT inverter, read_ts, sysalarmflag FROM status WHERE read_ts >= ? AND read_ts < ? "
        "AND sysalarmflag GLOB '*[1-9]*' ORDER BY inverter, read_ts", src, params=(lo, hi))
    eventos = []
    for inv, g in alarmas.groupby("inverter"):
        nueva = (g["sysalarmflag"] != g["sysalarmflag"].shift()) | (g["read_ts"].diff() > 300)
        for _, isla in g.groupby(nueva.cumsum()):
            eventos.append((int(inv), 4, isla["read_ts"].iloc[0], isla["read_ts"].iloc[-1],
                            json.dumps({"origen": "fuente", "flags": json.loads(isla["sysalarmflag"].iloc[0]),
                                        "lecturas": len(isla)})))
    if os.environ.get("EVENTOS_FALLAS", "1") == "1":
        cols = ", ".join(f"faulthistoryrecord{n:02d}, faulthistoryrecord{n:02d}_time" for n in range(32))
        vistos = set()
        for fila in src.execute(f"SELECT inverter, {cols} FROM fault_history WHERE read_ts >= ? AND read_ts < ?",
                                (lo, hi)):
            inv = fila[0]
            for n in range(32):
                payload, t = fila[1 + 2 * n], fila[2 + 2 * n]
                if payload and t and (inv, payload, t) not in vistos:
                    vistos.add((inv, payload, t))
                    eventos.append((int(inv), 5, t, None,
                                    json.dumps({"origen": "fuente", "registro": json.loads(payload)})))
    cur.execute("CREATE TEMP TABLE tmp_ev (inv smallint, tipo smallint, ini double precision, "
                "fin double precision, detalle jsonb) ON COMMIT DROP")
    with cur.copy("COPY tmp_ev FROM STDIN") as cp:
        for e in eventos:
            cp.write_row(e)
    cur.execute("""
        INSERT INTO gold.fact_evento_operativo (sk_fecha, sk_tiempo, sk_inversor, sk_tipo_evento,
               inicio_utc, fin_utc, detalle)
        SELECT to_char(to_timestamp(e.ini) AT TIME ZONE 'Asia/Shanghai', 'YYYYMMDD')::int,
               to_char(to_timestamp(e.ini) AT TIME ZONE 'Asia/Shanghai', 'HH24MI')::smallint,
               i.sk_inversor, e.tipo, to_timestamp(e.ini), to_timestamp(e.fin), e.detalle
        FROM (  -- varias fallas distintas pueden compartir instante: un evento con todos sus registros
              SELECT inv, tipo, ini, max(fin) AS fin,
                     CASE WHEN count(*) = 1 THEN min(detalle::text)::jsonb
                          ELSE jsonb_build_object('origen', 'fuente', 'n_registros', count(*),
                                                  'registros', jsonb_agg(detalle ORDER BY detalle::text)) END AS detalle
              FROM tmp_ev GROUP BY inv, tipo, ini) e
        JOIN gold.dim_fecha df ON df.sk_fecha =
             to_char(to_timestamp(e.ini) AT TIME ZONE 'Asia/Shanghai', 'YYYYMMDD')::int  -- descarta relojes absurdos
        JOIN gold.dim_inversor i ON i.inversor_id = e.inv
         AND to_timestamp(e.ini) >= i.vigente_desde AND to_timestamp(e.ini) < i.vigente_hasta
        ON CONFLICT (sk_inversor, sk_tipo_evento, inicio_utc) DO UPDATE
           SET fin_utc = EXCLUDED.fin_utc, detalle = EXCLUDED.detalle
        WHERE (fact_evento_operativo.fin_utc, fact_evento_operativo.detalle)
              IS DISTINCT FROM (EXCLUDED.fin_utc, EXCLUDED.detalle)""")
    log.info("Eventos de la fuente: %d (alarmas + fallas)", len(eventos))


def cargar_replay(src, cur):
    """Copia una ventana de lecturas CRUDAS (válidas e inválidas) para el simulador."""
    ini = pd.Timestamp(REPLAY_DESDE).timestamp()
    df = pd.read_sql_query(
        f"SELECT {', '.join(c for c in reglas.COLUMNAS_FUENTE if c not in ('temper2','temper3','temper4'))} "
        "FROM analog_measurements WHERE read_ts >= ? AND read_ts < ? ORDER BY read_ts, inverter",
        src, params=(ini, ini + REPLAY_DIAS * 86400))
    df["read_ts"] = pd.to_datetime(df["read_ts"], unit="s", utc=True)
    df = df.rename(columns={"inverter": "inversor"})
    cur.execute("""CREATE TABLE IF NOT EXISTS stream.fuente_replay (LIKE stream.lectura_cruda INCLUDING DEFAULTS);
                   ALTER TABLE stream.fuente_replay DROP COLUMN IF EXISTS read_ts_original,
                                                   DROP COLUMN IF EXISTS anomalia_inyectada;
                   TRUNCATE stream.fuente_replay""")
    for c in ("battsoc", "pv1power", "outw_a", "outva_a", "linepowerw_a"):
        df[c] = df[c].astype("Int64")
    bd.copiar(cur, "stream.fuente_replay", df, list(df.columns))
    log.info("stream.fuente_replay: %d lecturas crudas (%d días)", len(df), REPLAY_DIAS)


# --------------------------------------------------------------------------- hechos
def procesar_telemetria(src, p: reglas.Parametros, lo: float, hi: float):
    conteo = reglas.ConteoRegla()
    rechazos: list = []
    parciales, huecos = [], []
    pendiente = None
    total = 0
    cols = ", ".join(reglas.COLUMNAS_FUENTE)
    # El índice único (read_ts, inverter) de la fuente permite recorrer en orden sin ordenar en memoria.
    cur = src.execute(f"SELECT {cols} FROM analog_measurements WHERE read_ts >= ? AND read_ts < ? "
                      "ORDER BY read_ts, inverter", (lo, hi))
    while True:
        filas = cur.fetchmany(CHUNK)
        ultimo = not filas
        if filas:
            nuevo = reglas.preparar(pd.DataFrame(filas, columns=reglas.COLUMNAS_FUENTE))
            nuevo = reglas.aplicar_reglas(nuevo, p, conteo, rechazos)   # reglas: una sola vez por fila
            total += len(filas)
            comb = nuevo if pendiente is None else pd.concat([pendiente, nuevo], ignore_index=True)
        elif pendiente is not None:
            comb = pendiente
        else:
            break
        comb = comb.sort_values(["inverter", "read_ts"], kind="stable").reset_index(drop=True)
        sig = comb.groupby("inverter")["read_ts"].shift(-1)
        comb["dt_s"] = (sig - comb["read_ts"]).dt.total_seconds()
        if not ultimo:
            es_ult = comb["dt_s"].isna()                 # última lectura por inversor: aún sin Δt
            pendiente = comb.loc[es_ult].drop(columns="dt_s")
            listo = comb.loc[~es_ult]
        else:
            listo, pendiente = comb, None
        g = listo.loc[listo["dt_s"] > p.dt_max_s, ["inverter", "read_ts", "dt_s", "fecha_local"]]
        huecos.append(g)
        integ = reglas.integrar(listo, p, conteo)
        parciales.append(reglas.agregar_minuto(integ))
        log.info("  leídas %s filas", f"{total:,}")
        if ultimo:
            break
    minuto = reglas.finalizar_minuto(reglas.combinar(pd.concat(parciales, ignore_index=True)))
    huecos = pd.concat(huecos, ignore_index=True) if huecos else pd.DataFrame()
    return minuto, conteo, rechazos, huecos, total


def huella_gold(cur) -> str:
    cur.execute("""
      SELECT md5(string_agg(x, '|' ORDER BY x)) FROM (
        SELECT 'T' || sk_inversor || ':' || count(*) || ':' || round(sum(e_pv_kwh), 4) || ':' ||
               round(sum(e_carga_kwh), 4) || ':' || round(sum(e_red_kwh), 4) || ':' || sum(n_validas) AS x
          FROM gold.fact_telemetria_minuto WHERE origen = 'historico' GROUP BY sk_inversor
        UNION ALL SELECT 'Q' || sum(lecturas_evaluadas) || ':' || sum(lecturas_afectadas) FROM gold.fact_calidad_diaria
        UNION ALL SELECT 'E' || count(*) || ':' || coalesce(round(sum(duracion_s)), 0) FROM gold.fact_evento_operativo
        UNION ALL SELECT 'C' || count(*) FROM gold.fact_contexto
        UNION ALL SELECT 'K' || count(*) || ':' || coalesce(round(sum(pv_kwh_integrado), 3), 0) FROM gold.fact_conciliacion_diaria
      ) h""")
    return cur.fetchone()[0]


def main() -> int:
    t0 = time.time()
    if not Path(SQLITE_PATH).exists():
        log.error("No existe la fuente Bronze en %s (montar el volumen de datos).", SQLITE_PATH)
        return 2
    src = abrir_bronze()
    huella_src = sha256(SQLITE_PATH) if CALCULAR_SHA else None
    if huella_src and huella_src != SHA_ESPERADO:
        log.warning("SHA-256 distinto al publicado (%s): ¿archivo de prueba o descarga corrupta?", huella_src[:12])

    with bd.conectar() as pg, pg.cursor() as cur:
        for f in DDL:
            bd.ejecutar_archivo(cur, str(SQL_DIR / f))
        pg.commit()
        lo, hi = ventana(src)
        p = reglas.Parametros(signo_descarga=detectar_signo(src, lo))
        cur.execute("""INSERT INTO audit.etl_run (fuente_sha256, ventana_desde_utc, ventana_hasta_utc, signo_battcurr)
                       VALUES (%s, to_timestamp(%s), to_timestamp(%s), %s) RETURNING run_id""",
                    (huella_src, lo, hi, p.signo_descarga))
        run_id = cur.fetchone()[0]
        pg.commit()
        try:
            cargar_dim_inversor(src, cur)
            minuto, conteo, rechazos, huecos, total = procesar_telemetria(src, p, lo, hi)
            minuto = minuto.rename(columns={"inverter": "inversor"})
            minuto["origen"], minuto["run_id"] = "historico", run_id
            for c in ("p_pv_max_w", "p_carga_max_w", "soc_min_pct"):
                minuto[c] = minuto[c].round().astype("Int64")

            # ---- Silver: reemplazo transaccional de la ventana (idempotente)
            cur.execute("DELETE FROM silver.lectura_minuto WHERE origen = 'historico' "
                        "AND ts_minuto >= to_timestamp(%s) AND ts_minuto < to_timestamp(%s)", (lo, hi))
            bd.copiar(cur, "silver.lectura_minuto", minuto, list(minuto.columns))
            cur.execute("DELETE FROM audit.lectura_rechazada WHERE read_ts >= to_timestamp(%s) "
                        "AND read_ts < to_timestamp(%s)", (lo, hi))
            with cur.copy("COPY audit.lectura_rechazada FROM STDIN") as cp:
                for inv, ts, cod, val in rechazos:
                    cp.write_row((inv, ts, cod, json.dumps(val)))
            cargar_contadores(src, cur)

            # ---- Staging para Gold
            cur.execute("""CREATE TEMP TABLE etl_ctx ON COMMIT DROP AS
                           SELECT %s::bigint AS run_id, to_timestamp(%s) AS desde, to_timestamp(%s) AS hasta""",
                        (run_id, lo, hi))
            cur.execute("CREATE TEMP TABLE tmp_calidad (inversor smallint, fecha_local date, codigo_regla text, "
                        "evaluadas bigint, afectadas bigint) ON COMMIT DROP")
            with cur.copy("COPY tmp_calidad FROM STDIN") as cp:
                for (inv, fecha, cod), (ev, af) in conteo.diario.items():
                    cp.write_row((inv, fecha, cod, ev, af))
            cur.execute("CREATE TEMP TABLE tmp_huecos (inversor smallint, desde_utc timestamptz, "
                        "hasta_utc timestamptz, gap_s double precision, fecha_local date) ON COMMIT DROP")
            if not huecos.empty:
                with cur.copy("COPY tmp_huecos FROM STDIN") as cp:
                    for r in huecos.itertuples(index=False):
                        cp.write_row((int(r.inverter), r.read_ts.to_pydatetime(warn=False),
                                      (r.read_ts + pd.Timedelta(seconds=r.dt_s)).to_pydatetime(warn=False),
                                      float(r.dt_s), r.fecha_local))
            bd.ejecutar_archivo(cur, str(SQL_DIR / "07_carga_gold.sql"))
            cargar_eventos_fuente(src, cur, lo, hi)
            cargar_contexto(src, cur)
            cargar_replay(src, cur)

            for (orden, cod, inv), (antes, afe, despues) in sorted(conteo.global_.items()):
                cur.execute("INSERT INTO audit.conteo_regla VALUES (%s,%s,%s,%s,%s,%s,%s)",
                            (run_id, orden, cod, inv, antes, afe, despues))
            hg = huella_gold(cur)
            cur.execute("""UPDATE audit.etl_run SET estado='OK', finalizado_utc=now(), filas_leidas=%s,
                           huella_gold=%s, detalle=%s WHERE run_id=%s""",
                        (total, hg, json.dumps({"minutos_silver": len(minuto), "rechazadas": len(rechazos),
                                                "huecos_mayor_120s": len(huecos),
                                                "segundos": round(time.time() - t0, 1)}), run_id))
            pg.commit()
        except Exception:
            pg.rollback()
            cur.execute("UPDATE audit.etl_run SET estado='FALLIDO', finalizado_utc=now() WHERE run_id=%s", (run_id,))
            pg.commit()
            log.exception("ETL fallido (run %s); la transacción se revirtió completa.", run_id)
            return 1

        # Evidencia en el log del pod (kubectl logs job/solarbi-etl)
        cur.execute("SELECT run_id, huella_gold FROM audit.etl_run WHERE estado='OK' ORDER BY run_id DESC LIMIT 2")
        corridas = cur.fetchall()
        log.info("RUN %s OK · %s filas · huella Gold %s", run_id, f"{total:,}", hg)
        if len(corridas) == 2:
            log.info("Idempotencia: huella actual %s la corrida anterior (%s)",
                     "IGUAL a" if corridas[0][1] == corridas[1][1] else "DISTINTA de", corridas[1][0])
        cur.execute("SELECT codigo_regla, inversor, filas_antes, filas_afectadas, filas_despues "
                    "FROM audit.conteo_regla WHERE run_id=%s ORDER BY orden, inversor", (run_id,))
        for r in cur.fetchall():
            log.info("  %-24s inv%s  antes=%-9s afectadas=%-8s después=%s", *r)
    return 0


if __name__ == "__main__":
    sys.exit(main())
