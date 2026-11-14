-- =====================================================================
-- SolarBI · 03 · Capa Gold · Esquema en estrella (constelación de hechos)
-- Convenciones: sk_* = llave sustituta; -1 = miembro "No aplica/Desconocido".
-- Llaves de fecha y hora "inteligentes" (yyyymmdd, hhmm) → deterministas → idempotencia.
-- =====================================================================

-- ---------------------------------------------------------------- DIMENSIONES
CREATE TABLE IF NOT EXISTS gold.dim_fecha (
  sk_fecha        integer PRIMARY KEY,               -- yyyymmdd, fecha local Asia/Shanghai
  fecha           date    NOT NULL UNIQUE,
  anio            smallint NOT NULL,
  semestre        smallint NOT NULL CHECK (semestre IN (1,2)),
  trimestre       smallint NOT NULL CHECK (trimestre BETWEEN 1 AND 4),
  mes             smallint NOT NULL CHECK (mes BETWEEN 1 AND 12),
  nombre_mes      text     NOT NULL,
  anio_mes        char(7)  NOT NULL,                 -- '2026-03' (orden natural)
  semana_iso      smallint NOT NULL,
  dia_mes         smallint NOT NULL,
  dia_semana_iso  smallint NOT NULL CHECK (dia_semana_iso BETWEEN 1 AND 7),
  nombre_dia      text     NOT NULL,
  es_fin_semana   boolean  NOT NULL,
  estacion        text     NOT NULL                  -- hemisferio norte (Shenzhen, 22,7° N)
);

CREATE TABLE IF NOT EXISTS gold.dim_tiempo (
  sk_tiempo       smallint PRIMARY KEY,              -- hhmm local (0..2359)
  hora            smallint NOT NULL CHECK (hora BETWEEN 0 AND 23),
  minuto          smallint NOT NULL CHECK (minuto BETWEEN 0 AND 59),
  hora_texto      char(5)  NOT NULL,                 -- '13:45'
  franja_horaria  text     NOT NULL,                 -- dimensión construida (ver diccionario)
  es_ventana_solar boolean NOT NULL                  -- 06:00–18:59 local
);

-- SCD Tipo 2: la configuración relevante del inversor cambia en el año
-- (umbral de alarma de SoC, capacidad Ah declarada, versión de protocolo).
CREATE TABLE IF NOT EXISTS gold.dim_inversor (
  sk_inversor        serial   PRIMARY KEY,
  inversor_id        smallint NOT NULL,              -- llave natural (1, 2)
  nombre             text     NOT NULL,
  subsistema         text     NOT NULL,
  version_protocolo  integer,                        -- specs.protocolver
  capacidad_bat_ah   numeric(8,1),                   -- settings.battpackrateah
  soc_alarma_pct     smallint,                       -- settings.batsoclowalarm
  soc_parada_pct     smallint,                       -- settings.batstopsoc
  hash_atributos     char(32) NOT NULL,              -- md5 de los atributos SCD2
  vigente_desde      timestamptz NOT NULL,
  vigente_hasta      timestamptz NOT NULL DEFAULT 'infinity',
  es_vigente         boolean  NOT NULL DEFAULT true,
  UNIQUE (inversor_id, vigente_desde),
  CHECK (vigente_hasta > vigente_desde)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_inversor_vigente
  ON gold.dim_inversor (inversor_id) WHERE es_vigente;  -- una sola versión vigente

-- DIMENSIÓN CONSTRUIDA: no existe en la fuente; codifica el catálogo de reglas de calidad.
CREATE TABLE IF NOT EXISTS gold.dim_regla_calidad (
  sk_regla        smallint PRIMARY KEY,
  codigo          text     NOT NULL UNIQUE,          -- R01_FRAME_NULO ...
  nombre          text     NOT NULL,
  dimension_dq    text     NOT NULL CHECK (dimension_dq IN
                   ('Completitud','Validez','Consistencia','Unicidad','Oportunidad','Exactitud')),
  nivel           text     NOT NULL CHECK (nivel IN ('Fila','Campo','Intervalo')),
  accion          text     NOT NULL CHECK (accion IN
                   ('Rechazar fila','Anular campo','Marcar sospechosa','No integrar intervalo','Deduplicar')),
  severidad       smallint NOT NULL CHECK (severidad BETWEEN 1 AND 3),   -- 3 = invalida la lectura
  expresion       text     NOT NULL,                 -- regla legible / pseudo-SQL
  evidencia       text     NOT NULL                  -- por qué existe (dato de la fuente)
);

-- DIMENSIÓN CONSTRUIDA: tipos de evento operativo con su origen y criterio.
CREATE TABLE IF NOT EXISTS gold.dim_tipo_evento (
  sk_tipo_evento  smallint PRIMARY KEY,
  codigo          text NOT NULL UNIQUE,
  categoria       text NOT NULL CHECK (categoria IN ('Red','Bateria','Inversor','Registro')),
  severidad       text NOT NULL CHECK (severidad IN ('Info','Advertencia','Critica')),
  origen          text NOT NULL,                     -- tabla/derivación
  criterio        text NOT NULL
);

CREATE TABLE IF NOT EXISTS gold.dim_tipo_contexto (
  sk_tipo_contexto smallint PRIMARY KEY,
  context_type     text NOT NULL UNIQUE,             -- value->>'context_type'
  familia          text NOT NULL,                    -- Carga de trabajo, Solar, Calendario, Registro, Clima, Otro
  es_carga_electrica boolean NOT NULL                -- ¿explica consumo? (GPU/CPU/aire)
);

CREATE TABLE IF NOT EXISTS gold.dim_tarifa (
  sk_tarifa       smallint PRIMARY KEY,
  descripcion     text    NOT NULL,
  cop_kwh         numeric(10,2) NOT NULL CHECK (cop_kwh > 0),
  vigente_desde   date    NOT NULL,
  vigente_hasta   date    NOT NULL DEFAULT '9999-12-31',
  fuente          text    NOT NULL,
  EXCLUDE USING gist (daterange(vigente_desde, vigente_hasta, '[)') WITH &&)  -- sin traslapes
);

-- -------------------------------------------------------------------- HECHOS
-- P1 · Medición energética.  GRANO: un inversor × un minuto local con ≥ 1 lectura recibida.
CREATE TABLE IF NOT EXISTS gold.fact_telemetria_minuto (
  sk_fecha            integer  NOT NULL REFERENCES gold.dim_fecha(sk_fecha),
  sk_tiempo           smallint NOT NULL REFERENCES gold.dim_tiempo(sk_tiempo),
  sk_inversor         integer  NOT NULL REFERENCES gold.dim_inversor(sk_inversor),
  ts_minuto_utc       timestamptz NOT NULL,          -- degenerada: eje de Grafana
  n_lecturas          integer  NOT NULL,
  n_validas           integer  NOT NULL,
  seg_cubiertos       numeric(6,2) NOT NULL,
  seg_cubiertos_valid numeric(6,2) NOT NULL,
  e_pv_kwh            numeric(12,6) NOT NULL,
  e_carga_kwh         numeric(12,6) NOT NULL,
  e_red_kwh           numeric(12,6) NOT NULL,
  e_bat_carga_kwh     numeric(12,6) NOT NULL,
  e_bat_descarga_kwh  numeric(12,6) NOT NULL,
  soc_prom_pct        numeric(5,2),                  -- semiaditiva: promediar, nunca sumar
  soc_min_pct         smallint,
  p_pv_max_w          integer,
  origen              text NOT NULL,
  PRIMARY KEY (sk_inversor, ts_minuto_utc),
  CHECK (n_validas <= n_lecturas)
);
CREATE INDEX IF NOT EXISTS ix_ftm_fecha_inv  ON gold.fact_telemetria_minuto (sk_fecha, sk_inversor);
CREATE INDEX IF NOT EXISTS ix_ftm_ts_brin    ON gold.fact_telemetria_minuto USING brin (ts_minuto_utc);

-- P2 · Auditoría de calidad.  GRANO: un inversor × un día local × una regla de calidad
--      (snapshot periódico; incluye filas con 0 violaciones → denominador completo).
CREATE TABLE IF NOT EXISTS gold.fact_calidad_diaria (
  sk_fecha          integer  NOT NULL REFERENCES gold.dim_fecha(sk_fecha),
  sk_inversor       integer  NOT NULL REFERENCES gold.dim_inversor(sk_inversor),
  sk_regla          smallint NOT NULL REFERENCES gold.dim_regla_calidad(sk_regla),
  lecturas_evaluadas bigint  NOT NULL CHECK (lecturas_evaluadas >= 0),
  lecturas_afectadas bigint  NOT NULL CHECK (lecturas_afectadas BETWEEN 0 AND lecturas_evaluadas),
  PRIMARY KEY (sk_fecha, sk_inversor, sk_regla)
);

-- P2b · Disponibilidad diaria del registro. GRANO: un inversor × un día local.
CREATE TABLE IF NOT EXISTS gold.fact_disponibilidad_diaria (
  sk_fecha           integer NOT NULL REFERENCES gold.dim_fecha(sk_fecha),
  sk_inversor        integer NOT NULL REFERENCES gold.dim_inversor(sk_inversor),
  lecturas_recibidas bigint  NOT NULL,
  lecturas_validas   bigint  NOT NULL,
  seg_cubiertos      numeric(9,2) NOT NULL,
  seg_cubiertos_valid numeric(9,2) NOT NULL,
  seg_periodo        integer NOT NULL DEFAULT 86400,
  huecos_mayor_10min integer NOT NULL DEFAULT 0,
  PRIMARY KEY (sk_fecha, sk_inversor)
);

-- P3 · Eventos operativos.  GRANO: una ocurrencia de evento (inversor, tipo, instante de inicio).
CREATE TABLE IF NOT EXISTS gold.fact_evento_operativo (
  sk_evento        bigserial PRIMARY KEY,
  sk_fecha         integer  NOT NULL REFERENCES gold.dim_fecha(sk_fecha),
  sk_tiempo        smallint NOT NULL REFERENCES gold.dim_tiempo(sk_tiempo),
  sk_inversor      integer  NOT NULL REFERENCES gold.dim_inversor(sk_inversor),
  sk_tipo_evento   smallint NOT NULL REFERENCES gold.dim_tipo_evento(sk_tipo_evento),
  inicio_utc       timestamptz NOT NULL,
  fin_utc          timestamptz,
  duracion_s       numeric(12,1) GENERATED ALWAYS AS (extract(epoch FROM fin_utc - inicio_utc)) STORED,
  conteo           smallint NOT NULL DEFAULT 1,      -- factless + medida aditiva
  detalle          jsonb    NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE (sk_inversor, sk_tipo_evento, inicio_utc),  -- llave natural → upsert idempotente
  CHECK (fin_utc IS NULL OR fin_utc >= inicio_utc)
);
CREATE INDEX IF NOT EXISTS ix_fev_fecha ON gold.fact_evento_operativo (sk_fecha, sk_tipo_evento);

-- P4 · Contexto operativo (JSON).  GRANO: un registro de context (id fuente).
CREATE TABLE IF NOT EXISTS gold.fact_contexto (
  contexto_id      bigint   PRIMARY KEY,             -- context.id (degenerada, natural)
  sk_fecha_inicio  integer  NOT NULL REFERENCES gold.dim_fecha(sk_fecha),
  sk_tipo_contexto smallint NOT NULL REFERENCES gold.dim_tipo_contexto(sk_tipo_contexto),
  sk_inversor      integer  NOT NULL REFERENCES gold.dim_inversor(sk_inversor),  -- -1 si no aplica
  registrado_utc   timestamptz NOT NULL,
  inicio_utc       timestamptz NOT NULL,
  fin_utc          timestamptz NOT NULL,
  duracion_s       numeric(12,1) GENERATED ALWAYS AS (extract(epoch FROM fin_utc - inicio_utc)) STORED,
  fuente           text     NOT NULL,                -- value->>'source'
  payload          jsonb    NOT NULL,                -- JSON minimizado (Ley 1581: sin texto libre)
  CHECK (fin_utc >= inicio_utc)
);
CREATE INDEX IF NOT EXISTS ix_fctx_payload_gin ON gold.fact_contexto USING gin (payload jsonb_path_ops);
-- Rango temporal para cruzar contexto con telemetría (operador && de rangos)
CREATE INDEX IF NOT EXISTS ix_fctx_rango ON gold.fact_contexto
  USING gist (tstzrange(inicio_utc, fin_utc, '[]'));

-- P5 · Conciliación.  GRANO: un inversor × un día local.
CREATE TABLE IF NOT EXISTS gold.fact_conciliacion_diaria (
  sk_fecha           integer NOT NULL REFERENCES gold.dim_fecha(sk_fecha),
  sk_inversor        integer NOT NULL REFERENCES gold.dim_inversor(sk_inversor),
  pv_kwh_integrado   numeric(10,3) NOT NULL,
  pv_kwh_contador    numeric(10,3),
  carga_kwh_integrado numeric(10,3) NOT NULL,
  uso_kwh_contador   numeric(10,3),
  desv_pv_pct        numeric(8,2) GENERATED ALWAYS AS
     (CASE WHEN pv_kwh_contador > 0
           THEN 100 * (pv_kwh_integrado - pv_kwh_contador) / pv_kwh_contador END) STORED,
  disponibilidad_pct numeric(5,2) NOT NULL,
  PRIMARY KEY (sk_fecha, sk_inversor)
);
