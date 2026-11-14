-- =====================================================================
-- SolarBI · 05 · Vistas de consumo (Power BI usa vistas diarias; Grafana, el minuto)
-- =====================================================================

DROP VIEW IF EXISTS gold.vw_kpi_diario;
CREATE VIEW gold.vw_kpi_diario AS
SELECT f.sk_fecha, i.inversor_id,
       sum(f.e_pv_kwh)            AS e_pv_kwh,
       sum(f.e_carga_kwh)         AS e_carga_kwh,
       sum(f.e_red_kwh)           AS e_red_kwh,
       sum(f.e_bat_carga_kwh)     AS e_bat_carga_kwh,
       sum(f.e_bat_descarga_kwh)  AS e_bat_descarga_kwh,
       sum(f.n_lecturas)          AS lecturas,
       sum(f.n_validas)           AS lecturas_validas,
       round(100.0 * sum(f.n_validas) / NULLIF(sum(f.n_lecturas), 0), 2)            AS pct_validas,
       round(100.0 * least(sum(f.seg_cubiertos_valid), 86400) / 86400, 2)          AS disponibilidad_pct,
       -- La red también carga la batería (meses con red > carga): solo cuenta la red que cubre la carga.
       round(100 * (1 - least(sum(f.e_red_kwh), sum(f.e_carga_kwh)) / NULLIF(sum(f.e_carga_kwh), 0)), 2) AS autosuficiencia_pct,
       greatest(sum(f.e_red_kwh) - sum(f.e_carga_kwh), 0)                           AS red_excedente_kwh,
       min(f.soc_min_pct)         AS soc_min_dia
FROM gold.fact_telemetria_minuto f
JOIN gold.dim_inversor i USING (sk_inversor)
WHERE f.origen = 'historico'
GROUP BY f.sk_fecha, i.inversor_id;   -- por inversor natural: un día no se parte por SCD2

-- Línea 1: tablero de calidad por regla (tasa de violación con denominador completo)
CREATE OR REPLACE VIEW gold.vw_calidad_regla_mes AS
SELECT d.anio_mes, i.inversor_id, r.codigo, r.dimension_dq, r.severidad,
       sum(c.lecturas_evaluadas) AS evaluadas,
       sum(c.lecturas_afectadas) AS afectadas,
       round(100.0 * sum(c.lecturas_afectadas) / NULLIF(sum(c.lecturas_evaluadas), 0), 3) AS tasa_pct
FROM gold.fact_calidad_diaria c
JOIN gold.dim_fecha d USING (sk_fecha)
JOIN gold.dim_inversor i USING (sk_inversor)
JOIN gold.dim_regla_calidad r USING (sk_regla)
GROUP BY d.anio_mes, i.inversor_id, r.codigo, r.dimension_dq, r.severidad;

-- Grafana: estado actual por inversor (último minuto con datos)
CREATE OR REPLACE VIEW gold.vw_estado_actual AS
SELECT DISTINCT ON (i.inversor_id)
       i.inversor_id, f.ts_minuto_utc,
       round(f.e_pv_kwh    * 60000) AS p_pv_w,       -- kWh/min → W promedio
       round(f.e_carga_kwh * 60000) AS p_carga_w,
       round(f.e_red_kwh   * 60000) AS p_red_w,
       f.soc_prom_pct, f.n_lecturas, f.n_validas,
       extract(epoch FROM now() - f.ts_minuto_utc)::int AS segundos_desde_ultimo
FROM gold.fact_telemetria_minuto f
JOIN gold.dim_inversor i USING (sk_inversor)
ORDER BY i.inversor_id, f.ts_minuto_utc DESC;

GRANT SELECT ON ALL TABLES IN SCHEMA gold, audit TO bi_reader;
