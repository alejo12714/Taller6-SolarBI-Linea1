-- =====================================================================
-- SolarBI · 06 · Consultas de negocio (DBeaver)
-- Funciones de ventana, comparativos entre periodos y operadores JSONB nativos.
-- =====================================================================

-- Q1 · [JSONB + ventanas] ¿Cuánto sube la carga AC cuando se lanza un entrenamiento GPU
--      o una compilación CPU frente a la hora previa? Ranking de las sesiones más costosas.
--      Operadores: @> (contención), ? (existencia de llave), ->> (extracción), && (rangos).
WITH sesiones AS (
  SELECT c.contexto_id, c.inicio_utc, c.fin_utc, c.sk_inversor,
         c.payload->>'task_type'    AS tarea,
         c.payload->>'context_type' AS tipo
  FROM gold.fact_contexto c
  WHERE c.payload ? 'task_type'
    AND (c.payload @> '{"context_type": "gpu_cnn_training_workload"}'
      OR c.payload @> '{"context_type": "cpu_compilation_workload"}')
    AND c.sk_inversor <> -1
),
medicion AS (
  SELECT s.contexto_id, s.tipo, s.tarea,
         avg(f.e_carga_kwh * 60000) FILTER (WHERE tstzrange(s.inicio_utc, s.fin_utc) @> f.ts_minuto_utc) AS p_durante_w,
         avg(f.e_carga_kwh * 60000) FILTER (WHERE f.ts_minuto_utc <  s.inicio_utc)                       AS p_previa_w,
         sum(f.e_carga_kwh)        FILTER (WHERE tstzrange(s.inicio_utc, s.fin_utc) @> f.ts_minuto_utc) AS e_sesion_kwh
  FROM sesiones s
  JOIN gold.dim_inversor di ON di.sk_inversor = s.sk_inversor
  JOIN gold.dim_inversor dv ON dv.inversor_id = di.inversor_id          -- todas las versiones SCD2
  JOIN gold.fact_telemetria_minuto f
    ON f.sk_inversor = dv.sk_inversor
   AND f.ts_minuto_utc >= s.inicio_utc - interval '1 hour'
   AND f.ts_minuto_utc <  s.fin_utc
  GROUP BY s.contexto_id, s.tipo, s.tarea
)
SELECT tipo, tarea, contexto_id,
       round(p_previa_w)  AS carga_previa_w,
       round(p_durante_w) AS carga_durante_w,
       round(p_durante_w - p_previa_w) AS delta_w,
       round(e_sesion_kwh, 3) AS energia_sesion_kwh,
       round(e_sesion_kwh * (SELECT valor FROM audit.parametro WHERE clave = 'tarifa_cop_kwh')) AS costo_cop,
       rank() OVER (PARTITION BY tipo ORDER BY p_durante_w - p_previa_w DESC NULLS LAST) AS ranking_impacto
FROM medicion
ORDER BY tipo, ranking_impacto;

-- Q2 · [Comparativo entre periodos] Balance mensual por inversor con variación mes a mes (LAG),
--      acumulado del año (SUM OVER) y participación solar.
WITH mes AS (
  SELECT d.anio, d.mes, d.anio_mes, k.inversor_id,
         sum(k.e_pv_kwh) pv, sum(k.e_carga_kwh) carga, sum(k.e_red_kwh) red
  FROM gold.vw_kpi_diario k JOIN gold.dim_fecha d USING (sk_fecha)
  GROUP BY d.anio, d.mes, d.anio_mes, k.inversor_id
)
SELECT anio_mes, inversor_id,
       round(pv, 1) AS pv_kwh, round(carga, 1) AS carga_kwh, round(red, 1) AS red_kwh,
       round(100 * (1 - red / NULLIF(carga, 0)), 1)                                       AS autosuficiencia_pct,
       round(100 * (pv - lag(pv) OVER w) / NULLIF(lag(pv) OVER w, 0), 1)                  AS var_pv_mom_pct,
       round(sum(pv) OVER (PARTITION BY inversor_id, anio ORDER BY mes), 1)               AS pv_acum_anio_kwh,
       round(100 * pv / NULLIF(sum(pv) OVER (PARTITION BY anio_mes), 0), 1)              AS participacion_pv_pct,
       round(red * (SELECT valor FROM audit.parametro WHERE clave = 'tarifa_cop_kwh'))    AS costo_red_cop
FROM mes
WINDOW w AS (PARTITION BY inversor_id ORDER BY anio, mes)
ORDER BY inversor_id, anio_mes;

-- Q3 · [Línea 1 · calidad] % de lecturas válidas con media móvil de 7 días, días bajo meta
--      (98 %) y caídas bruscas frente al día anterior.
SELECT d.fecha, i.inversor_id,
       round(100.0 * x.lecturas_validas / NULLIF(x.lecturas_recibidas, 0), 2)              AS pct_validas,
       round(avg(100.0 * x.lecturas_validas / NULLIF(x.lecturas_recibidas, 0))
             OVER (PARTITION BY i.inversor_id ORDER BY d.fecha
                   ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2)                           AS pct_validas_mm7,
       round(100.0 * x.seg_cubiertos_valid / x.seg_periodo, 2)                              AS disponibilidad_pct,
       x.huecos_mayor_10min,
       CASE WHEN 100.0 * x.lecturas_validas / NULLIF(x.lecturas_recibidas, 0) < 98 THEN 'BAJO META' END AS alerta_meta,
       round(100.0 * x.lecturas_validas / NULLIF(x.lecturas_recibidas, 0)
           - lag(100.0 * x.lecturas_validas / NULLIF(x.lecturas_recibidas, 0))
             OVER (PARTITION BY i.inversor_id ORDER BY d.fecha), 2)                          AS delta_vs_dia_anterior
FROM gold.fact_disponibilidad_diaria x
JOIN gold.dim_fecha d USING (sk_fecha)
JOIN gold.dim_inversor i USING (sk_inversor)
ORDER BY i.inversor_id, d.fecha;

-- Q4 · [Franjas] ¿En qué franja horaria se compra más energía a la red? Cuartiles por franja.
SELECT t.franja_horaria, i.inversor_id,
       round(sum(f.e_red_kwh), 2)                                                           AS red_kwh,
       round(100 * sum(f.e_red_kwh) / sum(sum(f.e_red_kwh)) OVER (PARTITION BY i.inversor_id), 1) AS pct_red_del_inversor,
       ntile(4) OVER (PARTITION BY i.inversor_id ORDER BY sum(f.e_red_kwh))                 AS cuartil
FROM gold.fact_telemetria_minuto f
JOIN gold.dim_tiempo t USING (sk_tiempo)
JOIN gold.dim_inversor i USING (sk_inversor)
WHERE f.origen = 'historico'
GROUP BY t.franja_horaria, i.inversor_id
ORDER BY i.inversor_id, t.franja_horaria;

-- Q5 · [Conciliación] Días fuera de la tolerancia del 20 % y su relación con la disponibilidad.
SELECT d.fecha, i.inversor_id, k.pv_kwh_integrado, k.pv_kwh_contador, k.desv_pv_pct, k.disponibilidad_pct,
       CASE WHEN k.pv_kwh_contador IS NULL              THEN 'SIN CONTADOR'
            WHEN abs(k.desv_pv_pct) <= 20              THEN 'CONCILIA'
            WHEN k.disponibilidad_pct < 90             THEN 'DIFERENCIA EXPLICADA POR HUECOS'
            ELSE 'INVESTIGAR' END AS dictamen
FROM gold.fact_conciliacion_diaria k
JOIN gold.dim_fecha d USING (sk_fecha)
JOIN gold.dim_inversor i USING (sk_inversor)
ORDER BY d.fecha, i.inversor_id;

-- Q6 · [JSONB] Inventario de llaves del payload por fuente (perfilado del JSON de contexto).
SELECT fuente, k AS llave, count(*) AS registros,
       count(*) FILTER (WHERE jsonb_typeof(payload -> k) = 'number') AS numericos
FROM gold.fact_contexto, jsonb_object_keys(payload) AS k
GROUP BY fuente, k
ORDER BY fuente, registros DESC;

-- Q7 · [Eventos vs calidad] Minutos de red no disponible por mes y su peso relativo.
SELECT d.anio_mes, i.inversor_id, te.codigo,
       count(*) AS eventos, round(sum(e.duracion_s) / 60) AS minutos,
       round(100 * sum(e.duracion_s) / sum(sum(e.duracion_s)) OVER (PARTITION BY d.anio_mes), 1) AS pct_del_mes
FROM gold.fact_evento_operativo e
JOIN gold.dim_fecha d USING (sk_fecha)
JOIN gold.dim_inversor i USING (sk_inversor)
JOIN gold.dim_tipo_evento te USING (sk_tipo_evento)
WHERE e.fin_utc IS NOT NULL
GROUP BY d.anio_mes, i.inversor_id, te.codigo
ORDER BY d.anio_mes, minutos DESC;
