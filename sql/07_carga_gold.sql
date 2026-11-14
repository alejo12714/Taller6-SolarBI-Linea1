-- =====================================================================
-- SolarBI · 07 · Carga Silver -> Gold (ejecutada por el ETL en UNA transacción)
-- Requiere la tabla temporal etl_ctx(run_id, desde, hasta) creada por el ETL.
-- Patrón de idempotencia:
--   * Hechos con llave natural  -> INSERT ... ON CONFLICT DO UPDATE ... WHERE IS DISTINCT FROM
--   * Hechos derivados (eventos)-> reemplazo transaccional de la ventana procesada
-- =====================================================================

-- 1) Telemetría por minuto ------------------------------------------------
INSERT INTO gold.fact_telemetria_minuto AS f
      (sk_fecha, sk_tiempo, sk_inversor, ts_minuto_utc, n_lecturas, n_validas,
       seg_cubiertos, seg_cubiertos_valid, e_pv_kwh, e_carga_kwh, e_red_kwh,
       e_bat_carga_kwh, e_bat_descarga_kwh, soc_prom_pct, soc_min_pct, p_pv_max_w, origen)
SELECT to_char(s.ts_minuto AT TIME ZONE 'Asia/Shanghai', 'YYYYMMDD')::int,
       to_char(s.ts_minuto AT TIME ZONE 'Asia/Shanghai', 'HH24MI')::smallint,
       i.sk_inversor, s.ts_minuto, s.n_lecturas, s.n_validas,
       s.seg_cubiertos, s.seg_cubiertos_valid, s.e_pv_kwh, s.e_carga_kwh, s.e_red_kwh,
       s.e_bat_carga_kwh, s.e_bat_descarga_kwh, s.soc_prom_pct, s.soc_min_pct, s.p_pv_max_w, s.origen
FROM silver.lectura_minuto s
JOIN etl_ctx c ON s.ts_minuto >= c.desde AND s.ts_minuto < c.hasta
JOIN gold.dim_inversor i
  ON i.inversor_id = s.inversor
 AND s.ts_minuto >= i.vigente_desde AND s.ts_minuto < i.vigente_hasta   -- SCD2: versión vigente en el instante
ON CONFLICT (sk_inversor, ts_minuto_utc) DO UPDATE SET
       n_lecturas = EXCLUDED.n_lecturas, n_validas = EXCLUDED.n_validas,
       seg_cubiertos = EXCLUDED.seg_cubiertos, seg_cubiertos_valid = EXCLUDED.seg_cubiertos_valid,
       e_pv_kwh = EXCLUDED.e_pv_kwh, e_carga_kwh = EXCLUDED.e_carga_kwh, e_red_kwh = EXCLUDED.e_red_kwh,
       e_bat_carga_kwh = EXCLUDED.e_bat_carga_kwh, e_bat_descarga_kwh = EXCLUDED.e_bat_descarga_kwh,
       soc_prom_pct = EXCLUDED.soc_prom_pct, soc_min_pct = EXCLUDED.soc_min_pct,
       p_pv_max_w = EXCLUDED.p_pv_max_w, origen = EXCLUDED.origen
WHERE (f.n_lecturas, f.n_validas, f.e_pv_kwh, f.e_carga_kwh, f.e_red_kwh, f.soc_prom_pct)
      IS DISTINCT FROM
      (EXCLUDED.n_lecturas, EXCLUDED.n_validas, EXCLUDED.e_pv_kwh, EXCLUDED.e_carga_kwh,
       EXCLUDED.e_red_kwh, EXCLUDED.soc_prom_pct);       -- no reescribe tuplas idénticas (sin bloat)

-- 2) Calidad diaria por regla (desde el staging que deja el ETL) ------------
INSERT INTO gold.fact_calidad_diaria AS q (sk_fecha, sk_inversor, sk_regla, lecturas_evaluadas, lecturas_afectadas)
SELECT to_char(t.fecha_local, 'YYYYMMDD')::int, i.sk_inversor, r.sk_regla, t.evaluadas, t.afectadas
FROM tmp_calidad t
JOIN gold.dim_regla_calidad r ON r.codigo = t.codigo_regla
JOIN gold.dim_inversor i ON i.inversor_id = t.inversor
 AND (t.fecha_local::timestamp AT TIME ZONE 'Asia/Shanghai') >= i.vigente_desde
 AND (t.fecha_local::timestamp AT TIME ZONE 'Asia/Shanghai') <  i.vigente_hasta
ON CONFLICT (sk_fecha, sk_inversor, sk_regla) DO UPDATE
   SET lecturas_evaluadas = EXCLUDED.lecturas_evaluadas, lecturas_afectadas = EXCLUDED.lecturas_afectadas
WHERE (q.lecturas_evaluadas, q.lecturas_afectadas) IS DISTINCT FROM
      (EXCLUDED.lecturas_evaluadas, EXCLUDED.lecturas_afectadas);

-- 3) Disponibilidad diaria --------------------------------------------------
-- Hechos de grano DIARIO: se agregan por inversor natural y se asocian a la versión
-- SCD2 vigente al INICIO del día local (evita partir un día en dos filas).
CREATE TEMP TABLE tmp_dia ON COMMIT DROP AS
SELECT f.sk_fecha, i.inversor_id, sum(f.n_lecturas) AS recibidas, sum(f.n_validas) AS validas,
       least(sum(f.seg_cubiertos), 86400) AS seg, least(sum(f.seg_cubiertos_valid), 86400) AS seg_valid,
       round(sum(f.e_pv_kwh), 3) AS pv_kwh, round(sum(f.e_carga_kwh), 3) AS carga_kwh
FROM gold.fact_telemetria_minuto f
JOIN etl_ctx c ON f.ts_minuto_utc >= c.desde AND f.ts_minuto_utc < c.hasta
JOIN gold.dim_inversor i ON i.sk_inversor = f.sk_inversor
WHERE f.origen = 'historico'
GROUP BY f.sk_fecha, i.inversor_id;

INSERT INTO gold.fact_disponibilidad_diaria AS d
      (sk_fecha, sk_inversor, lecturas_recibidas, lecturas_validas, seg_cubiertos,
       seg_cubiertos_valid, huecos_mayor_10min)
SELECT t.sk_fecha, i.sk_inversor, t.recibidas, t.validas, t.seg, t.seg_valid, coalesce(h.huecos, 0)
FROM tmp_dia t
JOIN gold.dim_fecha df ON df.sk_fecha = t.sk_fecha
JOIN gold.dim_inversor i ON i.inversor_id = t.inversor_id
 AND (df.fecha::timestamp AT TIME ZONE 'Asia/Shanghai') >= i.vigente_desde
 AND (df.fecha::timestamp AT TIME ZONE 'Asia/Shanghai') <  i.vigente_hasta
LEFT JOIN (SELECT inversor, to_char(fecha_local,'YYYYMMDD')::int AS sk_fecha, count(*) AS huecos
           FROM tmp_huecos WHERE gap_s > 600 GROUP BY 1, 2) h
       ON h.inversor = t.inversor_id AND h.sk_fecha = t.sk_fecha
ON CONFLICT (sk_fecha, sk_inversor) DO UPDATE SET
   lecturas_recibidas = EXCLUDED.lecturas_recibidas, lecturas_validas = EXCLUDED.lecturas_validas,
   seg_cubiertos = EXCLUDED.seg_cubiertos, seg_cubiertos_valid = EXCLUDED.seg_cubiertos_valid,
   huecos_mayor_10min = EXCLUDED.huecos_mayor_10min
WHERE (d.lecturas_recibidas, d.lecturas_validas, d.seg_cubiertos_valid, d.huecos_mayor_10min)
      IS DISTINCT FROM (EXCLUDED.lecturas_recibidas, EXCLUDED.lecturas_validas,
                        EXCLUDED.seg_cubiertos_valid, EXCLUDED.huecos_mayor_10min);

-- 4) Conciliación diaria (integrado vs contador del inversor) ---------------
INSERT INTO gold.fact_conciliacion_diaria AS k
      (sk_fecha, sk_inversor, pv_kwh_integrado, pv_kwh_contador, carga_kwh_integrado,
       uso_kwh_contador, disponibilidad_pct)
SELECT t.sk_fecha, i.sk_inversor, t.pv_kwh, cd.pv_kwh_contador, t.carga_kwh, cd.uso_kwh_contador,
       round(100.0 * t.seg_valid / 86400, 2)
FROM tmp_dia t
JOIN gold.dim_fecha df ON df.sk_fecha = t.sk_fecha
JOIN gold.dim_inversor i ON i.inversor_id = t.inversor_id
 AND (df.fecha::timestamp AT TIME ZONE 'Asia/Shanghai') >= i.vigente_desde
 AND (df.fecha::timestamp AT TIME ZONE 'Asia/Shanghai') <  i.vigente_hasta
LEFT JOIN silver.contador_diario cd ON cd.inversor = t.inversor_id AND cd.fecha_local = df.fecha
ON CONFLICT (sk_fecha, sk_inversor) DO UPDATE SET
   pv_kwh_integrado = EXCLUDED.pv_kwh_integrado, pv_kwh_contador = EXCLUDED.pv_kwh_contador,
   carga_kwh_integrado = EXCLUDED.carga_kwh_integrado, uso_kwh_contador = EXCLUDED.uso_kwh_contador,
   disponibilidad_pct = EXCLUDED.disponibilidad_pct
WHERE (k.pv_kwh_integrado, k.pv_kwh_contador, k.carga_kwh_integrado, k.disponibilidad_pct)
      IS DISTINCT FROM (EXCLUDED.pv_kwh_integrado, EXCLUDED.pv_kwh_contador,
                        EXCLUDED.carga_kwh_integrado, EXCLUDED.disponibilidad_pct);

-- 5) Eventos derivados de la telemetría (gaps-and-islands) ------------------
DELETE FROM gold.fact_evento_operativo e
USING etl_ctx c
WHERE e.inicio_utc >= c.desde AND e.inicio_utc < c.hasta
  AND e.sk_tipo_evento IN (1, 2, 3, 6)
  AND e.detalle->>'origen' = 'historico';

WITH base AS (
  SELECT s.inversor, s.ts_minuto, i.sk_inversor,
         (s.n_validas > 0 AND s.ratio_red_ausente >= 0.8)                     AS red_off,
         (s.n_validas > 0 AND s.soc_min_pct <= coalesce(i.soc_alarma_pct, 20)) AS soc_bajo,
         (s.n_validas > 0 AND s.v_salida_prom < 50)                            AS ac_off,
         s.soc_min_pct
  FROM silver.lectura_minuto s
  JOIN etl_ctx c ON s.ts_minuto >= c.desde AND s.ts_minuto < c.hasta
  JOIN gold.dim_inversor i ON i.inversor_id = s.inversor
   AND s.ts_minuto >= i.vigente_desde AND s.ts_minuto < i.vigente_hasta
  WHERE s.origen = 'historico'
),
marcas AS (
  SELECT inversor, sk_inversor, ts_minuto, 1::smallint AS tipo, soc_min_pct, 30 AS fusion_min FROM base WHERE red_off
  UNION ALL
  SELECT inversor, sk_inversor, ts_minuto, 2, soc_min_pct, 10 FROM base WHERE soc_bajo
  UNION ALL
  SELECT inversor, sk_inversor, ts_minuto, 3, soc_min_pct, 1  FROM base WHERE ac_off
),
islas AS (
  SELECT *, sum(nueva) OVER (PARTITION BY sk_inversor, tipo ORDER BY ts_minuto) AS isla
  FROM (SELECT *, CASE WHEN ts_minuto - lag(ts_minuto) OVER (PARTITION BY sk_inversor, tipo ORDER BY ts_minuto)
                            <= make_interval(mins => fusion_min) THEN 0 ELSE 1 END AS nueva
        FROM marcas) x
),
eventos AS (
  SELECT sk_inversor, tipo, min(ts_minuto) AS ini, max(ts_minuto) + interval '1 minute' AS fin,
         count(*) AS minutos, min(soc_min_pct) AS soc_min
  FROM islas GROUP BY sk_inversor, tipo, isla
)
INSERT INTO gold.fact_evento_operativo (sk_fecha, sk_tiempo, sk_inversor, sk_tipo_evento, inicio_utc, fin_utc, detalle)
SELECT to_char(ini AT TIME ZONE 'Asia/Shanghai', 'YYYYMMDD')::int,
       to_char(ini AT TIME ZONE 'Asia/Shanghai', 'HH24MI')::smallint,
       sk_inversor, tipo, ini, fin,
       jsonb_build_object('origen', 'historico', 'minutos_afectados', minutos, 'soc_min', soc_min)
FROM eventos
WHERE tipo <> 3 OR minutos >= 3;                       -- apagado: ≥ 3 minutos consecutivos

-- Huecos del registrador como evento de categoría 'Registro' (no es falla eléctrica)
INSERT INTO gold.fact_evento_operativo (sk_fecha, sk_tiempo, sk_inversor, sk_tipo_evento, inicio_utc, fin_utc, detalle)
SELECT to_char(h.desde_utc AT TIME ZONE 'Asia/Shanghai', 'YYYYMMDD')::int,
       to_char(h.desde_utc AT TIME ZONE 'Asia/Shanghai', 'HH24MI')::smallint,
       i.sk_inversor, 6, h.desde_utc, h.hasta_utc,
       jsonb_build_object('origen', 'historico', 'gap_s', round(h.gap_s::numeric, 1))
FROM tmp_huecos h
JOIN gold.dim_inversor i ON i.inversor_id = h.inversor
 AND h.desde_utc >= i.vigente_desde AND h.desde_utc < i.vigente_hasta
WHERE h.gap_s > 600
ON CONFLICT (sk_inversor, sk_tipo_evento, inicio_utc) DO NOTHING;
