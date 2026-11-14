-- =====================================================================
-- SolarBI · 02 · Capa Silver y esquema de auditoría
-- Silver: grano = 1 inversor × 1 minuto UTC con ≥ 1 lectura recibida.
-- =====================================================================

-- Corridas del ETL: evidencia de cada ejecución (requisito K.3 "deja evidencia").
CREATE TABLE IF NOT EXISTS audit.etl_run (
  run_id            bigserial PRIMARY KEY,
  iniciado_utc      timestamptz NOT NULL DEFAULT now(),
  finalizado_utc    timestamptz,
  estado            text NOT NULL DEFAULT 'EN_CURSO'
                    CHECK (estado IN ('EN_CURSO','OK','FALLIDO')),
  fuente_sha256     char(64),            -- huella del .sqlite (Bronze)
  ventana_desde_utc timestamptz,
  ventana_hasta_utc timestamptz,
  signo_battcurr    smallint CHECK (signo_battcurr IN (-1, 1)),  -- +1: positivo = descarga
  filas_leidas      bigint,
  huella_gold       text,                -- md5 de los hechos tras la carga (prueba de idempotencia)
  detalle           jsonb NOT NULL DEFAULT '{}'::jsonb
);

-- Conteos antes/después por regla (requisito E: "conteos antes y después de cada regla").
CREATE TABLE IF NOT EXISTS audit.conteo_regla (
  run_id        bigint  NOT NULL REFERENCES audit.etl_run(run_id) ON DELETE CASCADE,
  orden         smallint NOT NULL,
  codigo_regla  text    NOT NULL,
  inversor      smallint NOT NULL,
  filas_antes   bigint  NOT NULL,
  filas_afectadas bigint NOT NULL,
  filas_despues bigint  NOT NULL,
  PRIMARY KEY (run_id, orden, inversor),
  CHECK (filas_despues = filas_antes - filas_afectadas OR filas_despues = filas_antes)
);

-- Muestras rechazadas (trazabilidad fila a fila de lo que NO entra a Silver).
CREATE TABLE IF NOT EXISTS audit.lectura_rechazada (
  inversor      smallint    NOT NULL,
  read_ts       timestamptz NOT NULL,
  codigo_regla  text        NOT NULL,
  valores       jsonb       NOT NULL,
  PRIMARY KEY (inversor, read_ts, codigo_regla)
);

-- ---------------------------------------------------------------------
-- Silver: telemetría limpia agregada a 1 minuto
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.lectura_minuto (
  inversor            smallint    NOT NULL CHECK (inversor IN (1, 2)),
  ts_minuto           timestamptz NOT NULL,              -- inicio del minuto, UTC
  n_lecturas          integer     NOT NULL CHECK (n_lecturas >= 0),
  n_validas           integer     NOT NULL CHECK (n_validas BETWEEN 0 AND n_lecturas),
  n_sospechosas       integer     NOT NULL DEFAULT 0,
  seg_cubiertos       numeric(6,2) NOT NULL CHECK (seg_cubiertos BETWEEN 0 AND 180),
  seg_cubiertos_valid numeric(6,2) NOT NULL,
  -- Energías integradas (retención de orden cero, Δt ≤ 120 s), kWh
  e_pv_kwh            numeric(12,6) NOT NULL DEFAULT 0,
  e_carga_kwh         numeric(12,6) NOT NULL DEFAULT 0,
  e_red_kwh           numeric(12,6) NOT NULL DEFAULT 0,
  e_bat_carga_kwh     numeric(12,6) NOT NULL DEFAULT 0,
  e_bat_descarga_kwh  numeric(12,6) NOT NULL DEFAULT 0,
  -- Estados (solo lecturas válidas)
  p_pv_max_w          integer,
  p_carga_max_w       integer,
  soc_prom_pct        numeric(5,2) CHECK (soc_prom_pct BETWEEN 0 AND 100),
  soc_min_pct         smallint     CHECK (soc_min_pct  BETWEEN 0 AND 100),
  v_bat_prom          numeric(6,2),
  v_red_prom          numeric(6,2),
  ratio_red_ausente   numeric(4,3) CHECK (ratio_red_ausente BETWEEN 0 AND 1),
  v_salida_prom       numeric(6,2),
  origen              text NOT NULL DEFAULT 'historico' CHECK (origen IN ('historico','simulado')),
  run_id              bigint,
  PRIMARY KEY (inversor, ts_minuto)
);
-- BRIN: ts_minuto crece monótono con la carga; índice diminuto frente a B-tree
-- y suficiente para los filtros por rango de Grafana ($__timeFilter).
CREATE INDEX IF NOT EXISTS ix_silver_minuto_brin
  ON silver.lectura_minuto USING brin (ts_minuto) WITH (pages_per_range = 32);

-- Estadísticas del propio inversor (contadores diarios) para conciliar.
CREATE TABLE IF NOT EXISTS silver.contador_diario (
  inversor        smallint NOT NULL,
  fecha_local     date     NOT NULL,          -- Asia/Shanghai
  pv_kwh_contador numeric(10,3),              -- max(statistics.pvgeneratenergytoday) del día
  uso_kwh_contador numeric(10,3),             -- max(statistics.usedenergytoday)
  n_muestras      integer  NOT NULL,
  PRIMARY KEY (inversor, fecha_local)
);

-- Aterrizaje del simulador (mismo contrato que una lectura cruda de analog_measurements)
CREATE TABLE IF NOT EXISTS stream.lectura_cruda (
  inversor      smallint    NOT NULL,
  read_ts       timestamptz NOT NULL,
  battvolt real, battcurr real, battsoc integer,
  pv1volt real, pv1curr real, pv1power integer,
  outvolta real, outw_a integer, outva_a integer,
  linevolta real, linepowerw_a integer, outfreq real, linefreq real,
  temper1 real,
  read_ts_original timestamptz,                 -- lectura histórica que se re-emite
  anomalia_inyectada text,                      -- NULL = lectura histórica fiel
  PRIMARY KEY (inversor, read_ts)
);
CREATE TABLE IF NOT EXISTS stream.cursor (
  inversor        smallint PRIMARY KEY,
  ultimo_original timestamptz NOT NULL,
  ultimo_emitido  timestamptz NOT NULL
);
