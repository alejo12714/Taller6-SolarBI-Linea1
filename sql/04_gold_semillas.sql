-- =====================================================================
-- SolarBI · 04 · Semillas de dimensiones (idempotentes: ON CONFLICT)
-- =====================================================================

-- dim_fecha: cubre el contexto (desde 2024-05-01 local) hasta fin de 2027 (simulador)
INSERT INTO gold.dim_fecha
SELECT to_char(d, 'YYYYMMDD')::int,
       d::date,
       extract(year FROM d)::smallint,
       CASE WHEN extract(month FROM d) <= 6 THEN 1 ELSE 2 END,
       extract(quarter FROM d)::smallint,
       extract(month FROM d)::smallint,
       (ARRAY['Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto',
              'Septiembre','Octubre','Noviembre','Diciembre'])[extract(month FROM d)::int],
       to_char(d, 'YYYY-MM'),
       extract(week FROM d)::smallint,
       extract(day FROM d)::smallint,
       extract(isodow FROM d)::smallint,
       (ARRAY['Lunes','Martes','Miércoles','Jueves','Viernes','Sábado','Domingo'])[extract(isodow FROM d)::int],
       extract(isodow FROM d) IN (6, 7),
       CASE WHEN extract(month FROM d) IN (12,1,2) THEN 'Invierno'
            WHEN extract(month FROM d) IN (3,4,5)  THEN 'Primavera'
            WHEN extract(month FROM d) IN (6,7,8)  THEN 'Verano'
            ELSE 'Otoño' END
FROM generate_series('2024-04-30'::date, '2027-12-31'::date, interval '1 day') AS g(d)
ON CONFLICT (sk_fecha) DO NOTHING;

-- dim_tiempo: 1 440 minutos del día local
INSERT INTO gold.dim_tiempo
SELECT (h * 100 + m)::smallint, h, m, lpad(h::text, 2, '0') || ':' || lpad(m::text, 2, '0'),
       CASE WHEN h BETWEEN 0 AND 5   THEN '1. Madrugada (00–06)'
            WHEN h BETWEEN 6 AND 9   THEN '2. Rampa solar (06–10)'
            WHEN h BETWEEN 10 AND 14 THEN '3. Pico solar (10–15)'
            WHEN h BETWEEN 15 AND 18 THEN '4. Caída solar (15–19)'
            ELSE                          '5. Noche (19–24)' END,
       h BETWEEN 6 AND 18
FROM generate_series(0, 23) h CROSS JOIN generate_series(0, 59) m
ON CONFLICT (sk_tiempo) DO NOTHING;

-- Miembro desconocido del inversor (contexto sin inversor asociado)
INSERT INTO gold.dim_inversor (sk_inversor, inversor_id, nombre, subsistema, hash_atributos,
                               vigente_desde, vigente_hasta, es_vigente)
VALUES (-1, 0, 'No aplica', 'Instalación', repeat('0', 32), '-infinity', 'infinity', false)
ON CONFLICT (sk_inversor) DO NOTHING;

-- Catálogo de reglas de calidad (dimensión construida)
INSERT INTO gold.dim_regla_calidad VALUES
 (1, 'R01_FRAME_NULO',     'Trama analógica vacía',               'Completitud', 'Fila',  'Rechazar fila',        3,
  'pv1power, outw_a, battsoc, battvolt y linevolta son NULL a la vez',
  '≈ 58 000 NULL por columna (1,94 %), casi idénticos entre columnas → tramas Modbus fallidas completas'),
 (2, 'R02_DUPLICADO',      'Lectura duplicada (inversor, read_ts)', 'Unicidad',   'Fila',  'Deduplicar',           3,
  'COUNT(*) OVER (PARTITION BY inverter, read_ts) > 1',
  'La fuente reporta 0 duplicados (índice único); la regla protege el streaming simulado'),
 (3, 'R03_SOC_RANGO',      'SoC fuera de [0, 100] %',              'Validez',    'Fila',  'Rechazar fila',        3,
  'battsoc NOT BETWEEN 0 AND 100', 'Validador de los autores: soc_outside = 0; guarda para datos nuevos'),
 (4, 'R04_FREC_RANGO',     'Frecuencia fuera de [45, 65] Hz',      'Validez',    'Fila',  'Rechazar fila',        3,
  'outfreq o linefreq (≠ 0) fuera de [45, 65]', 'Red de 50 Hz; los autores reportan 0 casos'),
 (5, 'R05_PV_NEGATIVA',    'Potencia PV negativa',                 'Validez',    'Fila',  'Rechazar fila',        3,
  'pv1power < 0', 'Físicamente imposible en la entrada MPPT; 0 casos en la fuente'),
 (6, 'R06_TEMP_CENTINELA', 'Temperatura centinela 120 °C',         'Exactitud',  'Campo', 'Anular campo',         1,
  'temper1..4 = 120 → NULL', '99,93 % (inv. 1) y 95,87 % (inv. 2) de filas; nota telemetry_quality_note de los autores'),
 (7, 'R07_IDENTIDAD_PV',   'Incumple P = V·I en PV (±10 %)',       'Consistencia','Fila', 'Marcar sospechosa',    2,
  'pv1power ≥ 100 AND |pv1power − pv1volt·pv1curr| > 0,10·pv1power',
  'Fuera de tolerancia: 3,2 % (inv. 1) y 0,9 % (inv. 2) de filas evaluadas'),
 (8, 'R08_ACTIVA_GT_APARENTE','Potencia activa > aparente + 50 VA','Consistencia','Fila',  'Marcar sospechosa',    2,
  '|outw_a| > outva_a + 50', '3 574 filas (inv. 1) y 237 (inv. 2) según el validador de los autores'),
 (9, 'R09_BAT_V_INCOHERENTE','Tensión de batería 0 V con SoC > 0', 'Consistencia','Campo','Anular campo',         2,
  'battvolt = 0 AND battsoc > 0 → battvolt, battcurr = NULL',
  'El validador reporta “mínimo no nulo” de tensión: existen ceros; 0 V con SoC > 0 es lectura imposible'),
 (10,'R10_HUECO',          'Intervalo entre lecturas > 120 s',     'Oportunidad','Intervalo','No integrar intervalo',1,
  'lead(read_ts) − read_ts > 120 s', 'p99 = 111 s; 117 (inv. 1) y 40 (inv. 2) huecos > 10 min; máximo 26,1 días')
ON CONFLICT (sk_regla) DO UPDATE SET
  codigo = EXCLUDED.codigo, nombre = EXCLUDED.nombre, expresion = EXCLUDED.expresion,
  evidencia = EXCLUDED.evidencia, severidad = EXCLUDED.severidad, accion = EXCLUDED.accion;

-- Tipos de evento operativo (dimensión construida)
INSERT INTO gold.dim_tipo_evento VALUES
 (1, 'RED_NO_DISPONIBLE', 'Red',      'Advertencia', 'analog_measurements (derivado)',
     'linevolta < 100 V en ≥ 80 % de las lecturas válidas del minuto; consecutivos ≤ 30 min se fusionan'),
 (2, 'SOC_BAJO',          'Bateria',  'Critica',     'analog_measurements + settings.batsoclowalarm',
     'soc_min del minuto ≤ umbral de alarma configurado en el propio inversor'),
 (3, 'SALIDA_AC_APAGADA', 'Inversor', 'Critica',     'analog_measurements (derivado)',
     'outvolta < 50 V durante ≥ 3 minutos con lecturas válidas (el equipo reporta, pero no alimenta)'),
 (4, 'ALARMA_SISTEMA',    'Inversor', 'Advertencia', 'status.sysalarmflag',
     'Arreglo JSON con algún elemento ≠ 0; bits sin decodificar (mapa de registros propietario)'),
 (5, 'FALLA_HISTORICA',   'Inversor', 'Critica',     'fault_history.faulthistoryrecordNN(_time)',
     'Payload de falla distinto, anclado a su tiempo calibrado (20 516 de 22 529 calibrados)'),
 (6, 'HUECO_REGISTRO',    'Registro', 'Info',        'analog_measurements (derivado)',
     'Sin lecturas > 600 s: falla del registrador, NO del sistema eléctrico')
ON CONFLICT (sk_tipo_evento) DO NOTHING;

INSERT INTO gold.dim_tipo_contexto VALUES (-1, 'sin_tipo', 'Otro', false)
ON CONFLICT DO NOTHING;   -- 61 registros fuente sin context_type

INSERT INTO gold.dim_tarifa VALUES
 (1, 'Tarifa adoptada por el equipo (escenario base)', 920.00, '2024-01-01', '9999-12-31',
  'Decisión del equipo; parámetro what-if en Power BI (rango 600–1 500 COP/kWh)')
ON CONFLICT (sk_tarifa) DO NOTHING;
