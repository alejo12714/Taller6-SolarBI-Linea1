-- =====================================================================
-- SolarBI · 01 · Esquemas, roles y parámetros
-- Motor: PostgreSQL 16  ·  Administrado desde DBeaver
-- Bronze = archivo opencem-v1.0.0.sqlite (inmutable, fuera de PostgreSQL)
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS silver;   -- lecturas limpias agregadas a 1 minuto
CREATE SCHEMA IF NOT EXISTS gold;     -- modelo dimensional (estrella)
CREATE SCHEMA IF NOT EXISTS audit;    -- corridas ETL, conteos por regla, conciliación
CREATE SCHEMA IF NOT EXISTS stream;   -- aterrizaje del simulador near real-time

-- Roles de mínimo privilegio (las contraseñas llegan por Secret de K8s,
-- ver k8s/base/02-secret.example.yaml; aquí solo se crean sin LOGIN si no existen).
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bi_reader') THEN
    CREATE ROLE bi_reader NOLOGIN;          -- Power BI, Grafana, DBeaver (consulta)
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'etl_writer') THEN
    CREATE ROLE etl_writer NOLOGIN;         -- Job ETL y CronJob simulador
  END IF;
END $$;

GRANT USAGE ON SCHEMA gold, audit TO bi_reader;
GRANT USAGE, CREATE ON SCHEMA silver, gold, audit, stream TO etl_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold  GRANT SELECT ON TABLES TO bi_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA audit GRANT SELECT ON TABLES TO bi_reader;

-- Parámetros de negocio versionados en BD (los umbrales operativos de Grafana
-- y del simulador llegan por ConfigMap; estos son los del modelo analítico).
CREATE TABLE IF NOT EXISTS audit.parametro (
  clave        text PRIMARY KEY,
  valor        numeric NOT NULL,
  unidad       text    NOT NULL,
  justificacion text   NOT NULL
);
INSERT INTO audit.parametro (clave, valor, unidad, justificacion) VALUES
 ('dt_max_integracion_s', 120, 's',
  'p99 de la cadencia analógica = 111 s (validation_results.json). Un intervalo mayor se trata como hueco: no se integra energía.'),
 ('hueco_evento_s', 600, 's',
  'Mismo umbral que usan los autores para context_type=data_gap (import_derived_telemetry_context.sql).'),
 ('temp_centinela_c', 120, '°C',
  '99,93 % (inv.1) y 95,87 % (inv.2) de las filas reportan 120 °C: valor centinela, no temperatura física.'),
 ('tol_identidad_pv', 0.10, 'fracción',
  'Identidad P = V·I del canal PV; los autores reportan 96,8 % / 99,1 % de filas dentro de ±10 %.'),
 ('tol_conciliacion_pv', 0.20, 'fracción',
  'MAPE diario energía integrada vs contador del inversor reportado por los autores: 19,7 % / 19,8 %.'),
 ('red_ausente_v', 100, 'V',
  'Tensión de línea < 100 V = red no disponible (criterio de los autores: grid_unavailable_or_islanded).'),
 ('tarifa_cop_kwh', 920, 'COP/kWh',
  'Tarifa adoptada por el equipo para el indicador económico; parámetro what-if en Power BI.')
ON CONFLICT (clave) DO UPDATE
  SET valor = EXCLUDED.valor, unidad = EXCLUDED.unidad, justificacion = EXCLUDED.justificacion;
