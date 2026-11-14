"""Genera grafana/dashboards/solarbi-operativo.json (tablero como código)."""
import json, sys

DS = {"type": "grafana-postgresql-datasource", "uid": "solarbi-pg"}
FROM = ("FROM gold.fact_telemetria_minuto f JOIN gold.dim_inversor i USING (sk_inversor) "
        "WHERE $__timeFilter(f.ts_minuto_utc) AND i.inversor_id IN (${inversor:csv})")

def q(sql, fmt="time_series", ref="A"):
    return {"refId": ref, "datasource": DS, "rawSql": sql, "format": fmt, "rawQuery": True, "editorMode": "code"}

def panel(pid, title, typ, x, y, w, h, targets, field=None, options=None, desc=""):
    return {"id": pid, "title": title, "type": typ, "description": desc, "datasource": DS,
            "gridPos": {"x": x, "y": y, "w": w, "h": h}, "targets": targets,
            "fieldConfig": field or {"defaults": {}, "overrides": []}, "options": options or {}}

umbral = lambda steps, unit, mn=None, mx=None: {"defaults": {"unit": unit, "min": mn, "max": mx,
    "thresholds": {"mode": "absolute", "steps": steps}, "color": {"mode": "thresholds"}}, "overrides": []}

panels = [
  {"id": 100, "type": "row", "title": "Estado actual por inversor", "gridPos": {"x": 0, "y": 0, "w": 24, "h": 1}, "collapsed": False, "panels": []},
  panel(1, "SoC actual", "gauge", 0, 1, 6, 6,
        [q("SELECT 'Inv ' || inversor_id AS metric, soc_prom_pct AS value FROM gold.vw_estado_actual "
           "WHERE inversor_id IN (${inversor:csv}) ORDER BY 1", "table")],
        umbral([{"color": "red", "value": None}, {"color": "orange", "value": 20}, {"color": "green", "value": 40}], "percent", 0, 100),
        {"reduceOptions": {"calcs": ["lastNotNull"], "values": True}},
        "Estado de carga del último minuto recibido. Umbral rojo = ALERTA_SOC_MIN."),
  panel(2, "Potencias instantáneas (W)", "stat", 6, 1, 8, 6,
        [q("SELECT 'Inv ' || inversor_id || ' PV' AS metric, p_pv_w AS value FROM gold.vw_estado_actual WHERE inversor_id IN (${inversor:csv}) "
           "UNION ALL SELECT 'Inv ' || inversor_id || ' Carga', p_carga_w FROM gold.vw_estado_actual WHERE inversor_id IN (${inversor:csv}) "
           "UNION ALL SELECT 'Inv ' || inversor_id || ' Red', p_red_w FROM gold.vw_estado_actual WHERE inversor_id IN (${inversor:csv}) ORDER BY 1", "table")],
        {"defaults": {"unit": "watt", "color": {"mode": "palette-classic"}}, "overrides": []},
        {"reduceOptions": {"calcs": ["lastNotNull"], "values": True}, "colorMode": "background"}),
  panel(3, "Frescura del dato (s desde la última lectura)", "stat", 14, 1, 5, 6,
        [q("SELECT 'Inv ' || inversor_id AS metric, segundos_desde_ultimo AS value FROM gold.vw_estado_actual "
           "WHERE inversor_id IN (${inversor:csv}) ORDER BY 1", "table")],
        umbral([{"color": "green", "value": None}, {"color": "orange", "value": 180}, {"color": "red", "value": 300}], "s"),
        {"reduceOptions": {"calcs": ["lastNotNull"], "values": True}, "colorMode": "background"},
        "Rojo a 300 s = umbral de la alerta de frescura (≈ 2,7 × p99 de la cadencia histórica, 111 s)."),
  panel(4, "% lecturas válidas (últimos 15 min) · Línea 1", "stat", 19, 1, 5, 6,
        [q("SELECT 'Inv ' || i.inversor_id AS metric, round(100.0 * sum(n_validas) / NULLIF(sum(n_lecturas), 0), 1) AS value "
           "FROM gold.fact_telemetria_minuto f JOIN gold.dim_inversor i USING (sk_inversor) "
           "WHERE f.ts_minuto_utc > now() - interval '15 minutes' AND i.inversor_id IN (${inversor:csv}) GROUP BY 1 ORDER BY 1", "table")],
        umbral([{"color": "red", "value": None}, {"color": "orange", "value": 90}, {"color": "green", "value": 97}], "percent", 0, 100),
        {"reduceOptions": {"calcs": ["lastNotNull"], "values": True}, "colorMode": "background"}),
  {"id": 101, "type": "row", "title": "Series de tiempo", "gridPos": {"x": 0, "y": 7, "w": 24, "h": 1}, "collapsed": False, "panels": []},
  panel(5, "Balance de potencia (W promedio por minuto)", "timeseries", 0, 8, 16, 9,
        [q("SELECT f.ts_minuto_utc AS time, 'Inv ' || i.inversor_id || ' · PV' AS metric, f.e_pv_kwh * 60000 AS value " + FROM +
           " UNION ALL SELECT f.ts_minuto_utc, 'Inv ' || i.inversor_id || ' · Carga', f.e_carga_kwh * 60000 " + FROM +
           " UNION ALL SELECT f.ts_minuto_utc, 'Inv ' || i.inversor_id || ' · Red', f.e_red_kwh * 60000 " + FROM + " ORDER BY 1")],
        {"defaults": {"unit": "watt", "custom": {"lineWidth": 1, "fillOpacity": 10, "spanNulls": 180000}}, "overrides": []},
        desc="Potencia media = energía del minuto × 60 000. spanNulls=3 min: no une huecos reales."),
  panel(6, "Estado de carga (%)", "timeseries", 16, 8, 8, 9,
        [q("SELECT f.ts_minuto_utc AS time, 'Inv ' || i.inversor_id AS metric, f.soc_prom_pct AS value " + FROM + " ORDER BY 1")],
        {"defaults": {"unit": "percent", "min": 0, "max": 100,
                      "custom": {"thresholdsStyle": {"mode": "line+area"}},
                      "thresholds": {"mode": "absolute", "steps": [{"color": "transparent", "value": None}, {"color": "red", "value": 0}, {"color": "transparent", "value": 20}]}},
         "overrides": []}),
  panel(7, "Calidad por minuto: lecturas recibidas vs válidas", "timeseries", 0, 17, 16, 8,
        [q("SELECT f.ts_minuto_utc AS time, 'Inv ' || i.inversor_id || ' · recibidas' AS metric, f.n_lecturas AS value " + FROM +
           " UNION ALL SELECT f.ts_minuto_utc, 'Inv ' || i.inversor_id || ' · válidas', f.n_validas " + FROM + " ORDER BY 1")],
        {"defaults": {"unit": "short", "custom": {"drawStyle": "bars", "fillOpacity": 60}}, "overrides": []}),
  panel(8, "Eventos operativos recientes", "table", 16, 17, 8, 8,
        [q("SELECT e.inicio_utc AS \"Inicio\", 'Inv ' || i.inversor_id AS \"Inversor\", t.codigo AS \"Evento\", t.severidad AS \"Severidad\", "
           "round(e.duracion_s / 60, 1) AS \"Min\" FROM gold.fact_evento_operativo e JOIN gold.dim_inversor i USING (sk_inversor) "
           "JOIN gold.dim_tipo_evento t USING (sk_tipo_evento) WHERE $__timeFilter(e.inicio_utc) AND i.inversor_id IN (${inversor:csv}) "
           "ORDER BY 1 DESC LIMIT 50", "table")]),
]

dash = {
  "uid": "solarbi-operativo", "title": "SolarBI · Operación de la microrred (near real-time)",
  "tags": ["solarbi", "microrred", "linea-1"], "timezone": "browser", "refresh": "30s",
  "schemaVersion": 39, "version": 1, "editable": False,
  "time": {"from": "now-3h", "to": "now"},
  "templating": {"list": [{
      "name": "inversor", "label": "Inversor", "type": "query", "datasource": DS,
      "query": "SELECT DISTINCT inversor_id::text FROM gold.dim_inversor WHERE inversor_id > 0 ORDER BY 1",
      "definition": "SELECT DISTINCT inversor_id::text FROM gold.dim_inversor WHERE inversor_id > 0 ORDER BY 1",
      "multi": True, "includeAll": True, "current": {"text": "All", "value": "$__all"}, "refresh": 1}]},
  "annotations": {"list": [{"name": "Eventos", "datasource": DS, "enable": True, "iconColor": "red",
      "target": {"rawQuery": True, "format": "table", "refId": "Anno",
                 "rawSql": "SELECT e.inicio_utc AS time, e.fin_utc AS timeend, t.codigo AS text FROM gold.fact_evento_operativo e "
                           "JOIN gold.dim_tipo_evento t USING (sk_tipo_evento) WHERE $__timeFilter(e.inicio_utc)"}}]},
  "panels": panels,
}
json.dump(dash, open(sys.argv[1], "w", encoding="utf-8"), ensure_ascii=False, indent=2)
print("ok", len(panels), "paneles")
