"""
Genera el proyecto Power BI (PBIP) de SolarBI: modelo semántico en TMDL e informe en PBIR.
Uso: python generar_pbip.py <directorio_salida_powerbi> <columnas.tsv>
columnas.tsv: tabla|columna|tipo_postgres (de information_schema).
"""
import json
import os
import sys
import uuid

OUT, COLS = sys.argv[1], sys.argv[2]
SM = os.path.join(OUT, "SolarBI.SemanticModel", "definition")
RP = os.path.join(OUT, "SolarBI.Report", "definition")
os.makedirs(os.path.join(SM, "tables"), exist_ok=True)

TIPOS = {"integer": "int64", "smallint": "int64", "bigint": "int64", "numeric": "double", "text": "string",
         "character": "string", "date": "dateTime", "boolean": "boolean", "timestamp with time zone": "dateTime"}
EXCLUIR = {("dim_inversor", "hash_atributos"), ("fact_evento_operativo", "detalle")}
TABLAS = ["dim_fecha", "dim_tiempo", "dim_inversor", "dim_regla_calidad", "dim_tipo_evento",
          "fact_telemetria_minuto", "fact_calidad_diaria", "fact_disponibilidad_diaria",
          "fact_conciliacion_diaria", "fact_evento_operativo"]
cols = {}
for linea in open(COLS, encoding="utf-8"):
    t, c, ty = linea.strip().split("|")
    if (t, c) not in EXCLUIR:
        cols.setdefault(t, []).append((c, TIPOS[ty]))


def lid():
    return str(uuid.uuid5(uuid.NAMESPACE_URL, "solarbi/" + lid.n.__next__()))


lid.n = (str(i) for i in range(10 ** 6))

# ------------------------------------------------------------------ medidas por tabla
M = {
 "fact_telemetria_minuto": [
  ("E PV kWh", "SUM(fact_telemetria_minuto[e_pv_kwh])", "#,0.0", "Energía"),
  ("E Carga kWh", "SUM(fact_telemetria_minuto[e_carga_kwh])", "#,0.0", "Energía"),
  ("E Red kWh", "SUM(fact_telemetria_minuto[e_red_kwh])", "#,0.0", "Energía"),
  ("E Bat Carga kWh", "SUM(fact_telemetria_minuto[e_bat_carga_kwh])", "#,0.0", "Energía"),
  ("E Bat Descarga kWh", "SUM(fact_telemetria_minuto[e_bat_descarga_kwh])", "#,0.0", "Energía"),
  ("Autosuficiencia %", "DIVIDE([E Carga kWh] - MIN([E Red kWh], [E Carga kWh]), [E Carga kWh])", "0.0%", "Híbridos"),
  ("Red excedente kWh", "MAX(0, [E Red kWh] - [E Carga kWh])", "#,0.0", "Híbridos"),
  ("Cobertura solar %", "DIVIDE([E PV kWh], [E Carga kWh])", "0.0%", "Híbridos"),
  ("Balance no explicado kWh", "([E PV kWh] + [E Red kWh] + [E Bat Descarga kWh]) - ([E Carga kWh] + [E Bat Carga kWh])", "#,0.0", "Híbridos"),
  ("Cierre del balance %", "DIVIDE([Balance no explicado kWh], [E PV kWh] + [E Red kWh] + [E Bat Descarga kWh])", "0.0%", "Híbridos"),
  ("Eficiencia aparente batería %", "DIVIDE([E Bat Descarga kWh], [E Bat Carga kWh])", "0.0%", "Híbridos"),
  ("Ciclos equivalentes", "DIVIDE([E Bat Descarga kWh], MAX(dim_inversor[capacidad_bat_ah]) * 51.2 / 1000)", "#,0.0", "Híbridos"),
  ("Dependencia nocturna de red %", "DIVIDE(CALCULATE([E Red kWh], dim_tiempo[es_ventana_solar] = FALSE()), [E Red kWh])", "0.0%", "Híbridos"),
  ("E PV MTD", "TOTALMTD([E PV kWh], dim_fecha[fecha])", "#,0.0", "Inteligencia de tiempo"),
  ("E PV YTD", "TOTALYTD([E PV kWh], dim_fecha[fecha])", "#,0.0", "Inteligencia de tiempo"),
  ("E PV mes anterior", "CALCULATE([E PV kWh], DATEADD(dim_fecha[fecha], -1, MONTH))", "#,0.0", "Inteligencia de tiempo"),
  ("Var PV MoM %", "DIVIDE([E PV kWh] - [E PV mes anterior], [E PV mes anterior])", "0.0%", "Inteligencia de tiempo"),
  ("Autosuficiencia mes anterior %", "CALCULATE([Autosuficiencia %], DATEADD(dim_fecha[fecha], -1, MONTH))", "0.0%", "Inteligencia de tiempo"),
  ("Costo red COP", "[E Red kWh] * [Valor Tarifa]", "$ #,0", "Económico"),
  ("Ahorro autosuficiencia COP", "([E Carga kWh] - MIN([E Red kWh], [E Carga kWh])) * [Valor Tarifa]", "$ #,0", "Económico"),
  ("Ahorro YTD COP", "VAR ult = CALCULATE(MAX(dim_fecha[fecha]), fact_telemetria_minuto) RETURN CALCULATE([Ahorro autosuficiencia COP], DATESBETWEEN(dim_fecha[fecha], DATE(YEAR(ult), 1, 1), ult))", "$ #,0", "Económico"),
  ("E Red escenario kWh", "VAR f = [Valor Factor PV] RETURN SUMX(fact_telemetria_minuto, MAX(0, fact_telemetria_minuto[e_red_kwh] - (f - 1) * fact_telemetria_minuto[e_pv_kwh]))", "#,0.0", "Económico"),
  ("Ahorro adicional escenario COP", "([E Red kWh] - [E Red escenario kWh]) * [Valor Tarifa]", "$ #,0", "Económico"),
 ],
 "fact_disponibilidad_diaria": [
  ("Lecturas recibidas", "SUM(fact_disponibilidad_diaria[lecturas_recibidas])", "#,0", "Calidad"),
  ("Lecturas válidas", "SUM(fact_disponibilidad_diaria[lecturas_validas])", "#,0", "Calidad"),
  ("Lecturas inválidas", "[Lecturas recibidas] - [Lecturas válidas]", "#,0", "Calidad"),
  ("% Lecturas válidas", "DIVIDE([Lecturas válidas], [Lecturas recibidas])", "0.00%", "Calidad"),
  ("Días en ventana de registro", "COUNTROWS(FILTER(dim_fecha, dim_fecha[fecha] >= DATE(2025, 7, 12) && dim_fecha[fecha] <= DATE(2026, 7, 13)))", "#,0", "Calidad"),
  ("Inversores en contexto", "COUNTROWS(FILTER(VALUES(dim_inversor[inversor_id]), dim_inversor[inversor_id] > 0))", "0", "Calidad"),
  ("Disponibilidad registro %", "DIVIDE(SUM(fact_disponibilidad_diaria[seg_cubiertos_valid]), [Días en ventana de registro] * 86400 * [Inversores en contexto])", "0.0%", "Calidad"),
  ("Huecos > 10 min", "SUM(fact_disponibilidad_diaria[huecos_mayor_10min])", "#,0", "Calidad"),
  ("% Válidas MM7", "AVERAGEX(DATESINPERIOD(dim_fecha[fecha], MAX(dim_fecha[fecha]), -7, DAY), [% Lecturas válidas])", "0.00%", "Calidad"),
 ],
 "fact_calidad_diaria": [
  ("Lecturas evaluadas", "SUM(fact_calidad_diaria[lecturas_evaluadas])", "#,0", "Calidad"),
  ("Lecturas afectadas", "SUM(fact_calidad_diaria[lecturas_afectadas])", "#,0", "Calidad"),
  ("Tasa violación regla %", "DIVIDE([Lecturas afectadas], [Lecturas evaluadas])", "0.00%", "Calidad"),
 ],
 "fact_conciliacion_diaria": [
  ("Desviación conciliación PV %", "VAR d = FILTER(fact_conciliacion_diaria, NOT ISBLANK(fact_conciliacion_diaria[pv_kwh_contador]) && fact_conciliacion_diaria[pv_kwh_contador] > 0) RETURN DIVIDE(SUMX(d, fact_conciliacion_diaria[pv_kwh_integrado] - fact_conciliacion_diaria[pv_kwh_contador]), SUMX(d, fact_conciliacion_diaria[pv_kwh_contador]))", "0.0%", "Calidad"),
  ("Días que concilian %", "DIVIDE(COUNTROWS(FILTER(fact_conciliacion_diaria, NOT ISBLANK(fact_conciliacion_diaria[desv_pv_pct]) && ABS(fact_conciliacion_diaria[desv_pv_pct]) <= 20)), COUNTROWS(FILTER(fact_conciliacion_diaria, NOT ISBLANK(fact_conciliacion_diaria[desv_pv_pct]))))", "0.0%", "Calidad"),
  ("Disponibilidad día %", "AVERAGE(fact_conciliacion_diaria[disponibilidad_pct])", "#,0.0", "Calidad"),
  ("Desviación PV día %", "AVERAGE(fact_conciliacion_diaria[desv_pv_pct])", "#,0.0", "Calidad"),
 ],
 "fact_evento_operativo": [
  ("Eventos", "SUM(fact_evento_operativo[conteo])", "#,0", "Eventos"),
  ("Horas en evento", "DIVIDE(SUM(fact_evento_operativo[duracion_s]), 3600)", "#,0.0", "Eventos"),
 ],
}
OCULTAS = lambda c: c.startswith("sk_")

def tabla_tmdl(t):
    l = [f"table {t}", f"\tlineageTag: {lid()}"]
    if t == "dim_fecha":
        l.append("\tdataCategory: Time")
    l.append("")
    for (nombre, expr, fmt, folder) in M.get(t, []):
        l += [f"\tmeasure '{nombre}' = {expr}", f"\t\tformatString: {fmt}",
              f"\t\tdisplayFolder: {folder}", f"\t\tlineageTag: {lid()}", ""]
    for c, ty in cols[t]:
        l.append(f"\tcolumn {c}")
        l.append(f"\t\tdataType: {ty}")
        if t == "dim_fecha" and c == "fecha":
            l.append("\t\tisKey")
            l.append("\t\tformatString: dd/mm/yyyy")
        elif ty == "dateTime":
            l.append("\t\tformatString: General Date")
        if OCULTAS(c) or (t.startswith("fact_") and ty in ("int64", "double") and not c.startswith("sk_")):
            l.append("\t\tisHidden") if OCULTAS(c) else None
        if ty in ("int64", "double") and not OCULTAS(c):
            l.append("\t\tsummarizeBy: " + ("sum" if t.startswith("fact_") else "none"))
        else:
            l.append("\t\tsummarizeBy: none")
        if t == "dim_fecha" and c in ("nombre_mes",):
            l.append("\t\tsortByColumn: mes")
        if t == "dim_fecha" and c == "nombre_dia":
            l.append("\t\tsortByColumn: dia_semana_iso")
        l += [f"\t\tsourceColumn: {c}", f"\t\tlineageTag: {lid()}", ""]
    if t == "dim_fecha":
        l += ["\thierarchy 'Jerarquía fecha'", f"\t\tlineageTag: {lid()}", ""]
        for n in ("anio", "trimestre", "mes", "fecha"):
            l += [f"\t\tlevel {n}", f"\t\t\tlineageTag: {lid()}", f"\t\t\tcolumn: {n}", ""]
    seleccion = ", ".join(f'"{c}"' for c, _ in cols[t])
    l += [f"\tpartition {t} = m", "\t\tmode: import", "\t\tsource =",
          "\t\t\t\tlet",
          "\t\t\t\t    Origen = PostgreSQL.Database(ServidorPG, BaseDatosPG),",
          f"\t\t\t\t    Tabla = Origen{{[Schema=\"gold\",Item=\"{t}\"]}}[Data],",
          f"\t\t\t\t    Columnas = Table.SelectColumns(Tabla, {{{seleccion}}})",
          "\t\t\t\tin",
          "\t\t\t\t    Columnas", "", "\tannotation PBI_ResultType = Table", ""]
    return "\n".join(l)

for t in TABLAS:
    open(os.path.join(SM, "tables", f"{t}.tmdl"), "w", encoding="utf-8").write(tabla_tmdl(t))

# Parámetros what-if (tablas calculadas desconectadas)
def whatif(nombre, serie, defecto, fmt, medida):
    return "\n".join([
        f"table '{nombre}'", f"\tlineageTag: {lid()}", "",
        f"\tmeasure '{medida}' = SELECTEDVALUE('{nombre}'[{nombre}], {defecto})",
        f"\t\tformatString: {fmt}", f"\t\tlineageTag: {lid()}", "",
        f"\tcolumn '{nombre}'", "\t\tformatString: " + fmt, f"\t\tlineageTag: {lid()}",
        "\t\tsummarizeBy: none", f"\t\tsourceColumn: [{nombre}]", "",
        "\t\textendedProperty ParameterMetadata =", "\t\t\t\t{", '\t\t\t\t  "version": 0', "\t\t\t\t}", "",
        f"\tpartition '{nombre}' = calculated", "\t\tmode: import",
        f"\t\tsource = SELECTCOLUMNS({serie}, \"{nombre}\", [Value])", "",
        "\tannotation PBI_Id = " + uuid.uuid4().hex, ""])

open(os.path.join(SM, "tables", "Tarifa.tmdl"), "w", encoding="utf-8").write(
    whatif("Tarifa", "GENERATESERIES(600, 1500, 20)", 920, "#,0", "Valor Tarifa"))
open(os.path.join(SM, "tables", "Factor PV.tmdl"), "w", encoding="utf-8").write(
    whatif("Factor PV", "GENERATESERIES(1, 2, 0.1)", 1, "0.0", "Valor Factor PV"))

# Expresiones M (parámetros de conexión: k8s -> localhost:5432, Docker Desktop -> localhost:5433)
open(os.path.join(SM, "expressions.tmdl"), "w", encoding="utf-8").write("\n".join([
    'expression ServidorPG = "localhost:5433" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]',
    f"\tlineageTag: {lid()}", "", "\tannotation PBI_ResultType = Text", "",
    'expression BaseDatosPG = "solarbi" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]',
    f"\tlineageTag: {lid()}", "", "\tannotation PBI_ResultType = Text", ""]))

# Relaciones 1:* (dim -> fact), una sola dirección
rels = []
for f, dims in {"fact_telemetria_minuto": ["fecha", "tiempo", "inversor"],
                "fact_calidad_diaria": ["fecha", "inversor", "regla"],
                "fact_disponibilidad_diaria": ["fecha", "inversor"],
                "fact_conciliacion_diaria": ["fecha", "inversor"],
                "fact_evento_operativo": ["fecha", "tiempo", "inversor", "tipo_evento"]}.items():
    for d in dims:
        dim = {"fecha": "dim_fecha", "tiempo": "dim_tiempo", "inversor": "dim_inversor",
               "regla": "dim_regla_calidad", "tipo_evento": "dim_tipo_evento"}[d]
        rels += [f"relationship {uuid.uuid5(uuid.NAMESPACE_URL, f + d)}",
                 f"\tfromColumn: {f}.sk_{d}", f"\ttoColumn: {dim}.sk_{d}", ""]
open(os.path.join(SM, "relationships.tmdl"), "w", encoding="utf-8").write("\n".join(rels))

orden = TABLAS + ["Tarifa", "Factor PV"]
open(os.path.join(SM, "model.tmdl"), "w", encoding="utf-8").write("\n".join([
    "model Model", "\tculture: es-MX", "\tdefaultPowerBIDataSourceVersion: powerBI_V3",
    "\tsourceQueryCulture: es-MX", "\tvalueFilterBehavior: independent", "\tdataAccessOptions",
    "\t\tlegacyRedirects", "\t\treturnErrorValuesAsNull", "",
    "annotation __PBI_TimeIntelligenceEnabled = 0", "",
    'annotation PBI_ProTooling = ["DevMode"]', "",
    "annotation PBI_QueryOrder = " + json.dumps(["ServidorPG", "BaseDatosPG"] + TABLAS), "",
    "ref cultureInfo es-MX", ""] + [f"ref table '{t}'" if " " in t else f"ref table {t}" for t in orden] + [""]))

# ------------------------------------------------------------------ informe (PBIR)
VS = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.4.0/schema.json"
PS = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.1.0/schema.json"

def campo(ent, prop, medida=True):
    k = "Measure" if medida else "Column"
    return {"field": {k: {"Expression": {"SourceRef": {"Entity": ent}}, "Property": prop}},
            "queryRef": f"{ent}.{prop}", "nativeQueryRef": prop}

def lit(v):
    return {"expr": {"Literal": {"Value": v}}}

def titulo(texto):
    return {"title": [{"properties": {"show": lit("true"), "text": lit("'" + texto + "'")}}]}

def visual(nombre, tipo, x, y, w, h, buckets, t=None, objetos=None):
    v = {"$schema": VS, "name": nombre,
         "position": {"x": x, "y": y, "z": visual.z, "height": h, "width": w, "tabOrder": visual.z},
         "visual": {"visualType": tipo, "drillFilterOtherVisuals": True}}
    visual.z += 1
    if buckets:
        v["visual"]["query"] = {"queryState": {b: {"projections": p} for b, p in buckets.items()}}
    if objetos:
        v["visual"]["objects"] = objetos
    if t:
        v["visual"]["visualContainerObjects"] = titulo(t)
    return v
visual.z = 0

def texto(nombre, x, y, w, h, contenido, tam="20pt"):
    return visual(nombre, "textbox", x, y, w, h, None, objetos={"general": [{"properties": {"paragraphs": [
        {"textRuns": [{"value": contenido, "textStyle": {"fontWeight": "bold", "fontSize": tam}}]}]}}]})

T, D, C, K, E = "fact_telemetria_minuto", "fact_disponibilidad_diaria", "fact_calidad_diaria", "fact_conciliacion_diaria", "fact_evento_operativo"
F = lambda p: campo("dim_fecha", p, False)
INV = campo("dim_inversor", "nombre", False)

def tarjeta(n, x, y, ent, med, t, w=280):
    return visual(n, "card", x, y, w, 130, {"Values": [campo(ent, med)]}, t)

def segmentador(n, x, y, col, ent, t, w=280, h=130):
    return visual(n, "slicer", x, y, w, h, {"Values": [campo(ent, col, False)]}, t)

paginas = {
 "9dbde995c48769768c1b": ("1 · Confiabilidad del dato", [
    texto("tit1", 20, 10, 1500, 60, "Línea 1 · ¿Podemos confiar en la telemetría? (OpenCEM, 12-jul-2025 a 13-jul-2026)"),
    visual("nav1", "pageNavigator", 1540, 10, 360, 60, None),
    segmentador("seg1a", 20, 80, "nombre", "dim_inversor", "Inversor"),
    segmentador("seg1b", 20, 220, "dimension_dq", "dim_regla_calidad", "Dimensión de calidad", h=200),
    tarjeta("k1a", 320, 80, D, "% Lecturas válidas", "% lecturas válidas"),
    tarjeta("k1b", 610, 80, D, "Disponibilidad registro %", "Disponibilidad del registro (calendario)"),
    tarjeta("k1c", 900, 80, D, "Huecos > 10 min", "Huecos > 10 min"),
    tarjeta("k1d", 1190, 80, K, "Días que concilian %", "Días que concilian (±20 %)"),
    tarjeta("k1e", 1480, 80, D, "Lecturas inválidas", "Lecturas inválidas (tramas vacías)", w=420),
    visual("m1", "pivotTable", 320, 230, 820, 420,
           {"Rows": [campo("dim_regla_calidad", "codigo", False)], "Columns": [F("anio_mes")],
            "Values": [campo(C, "Tasa violación regla %")]}, "Tasa de violación por regla y mes"),
    visual("l1", "lineChart", 1160, 230, 740, 420,
           {"Category": [F("anio_mes"), F("fecha")],
            "Y": [campo(D, "% Lecturas válidas"), campo(D, "% Válidas MM7")]},
           "% válidas y media móvil 7 días (drill: año → mes → día)"),
    visual("s1", "scatterChart", 320, 670, 820, 390,
           {"Category": [F("fecha")], "X": [campo(K, "Disponibilidad día %")],
            "Y": [campo(K, "Desviación PV día %")]},
           "Conciliación: disponibilidad (X) vs desviación PV % (Y) por día"),
    visual("b1", "clusteredColumnChart", 1160, 670, 740, 390,
           {"Category": [F("anio_mes")], "Y": [campo(D, "Huecos > 10 min"), campo(D, "Lecturas inválidas")]},
           "Huecos y lecturas inválidas por mes"),
 ]),
 "pagBalanceEnergetico": ("2 · Balance energético", [
    texto("tit2", 20, 10, 1500, 60, "¿De dónde viene y a dónde va la energía?"),
    visual("nav2", "pageNavigator", 1540, 10, 360, 60, None),
    segmentador("seg2a", 20, 80, "nombre", "dim_inversor", "Inversor"),
    segmentador("seg2b", 20, 220, "franja_horaria", "dim_tiempo", "Franja horaria", h=260),
    tarjeta("k2a", 320, 80, T, "E PV kWh", "Energía solar (kWh)"),
    tarjeta("k2b", 610, 80, T, "E Carga kWh", "Energía a la carga (kWh)"),
    tarjeta("k2c", 900, 80, T, "E Red kWh", "Energía de red (kWh)"),
    tarjeta("k2d", 1190, 80, T, "Autosuficiencia %", "Autosuficiencia"),
    tarjeta("k2e", 1480, 80, T, "Cierre del balance %", "Balance no explicado (pérdidas)", w=420),
    visual("bal", "clusteredBarChart", 320, 230, 700, 420,
           {"Y": [campo(T, "E PV kWh"), campo(T, "E Red kWh"), campo(T, "E Bat Descarga kWh"),
                  campo(T, "E Carga kWh"), campo(T, "E Bat Carga kWh"), campo(T, "Balance no explicado kWh")]},
           "Balance: entradas (PV, red, descarga) vs salidas (carga, carga de batería, no explicado)"),
    visual("c2", "columnChart", 1040, 230, 860, 420,
           {"Category": [F("anio_mes"), F("fecha")],
            "Y": [campo(T, "E PV kWh"), campo(T, "E Red kWh"), campo(T, "E Bat Descarga kWh")]},
           "Fuentes de energía por mes (drill a día)"),
    visual("c3", "clusteredColumnChart", 320, 670, 860, 390,
           {"Category": [campo("dim_tiempo", "franja_horaria", False)],
            "Y": [campo(T, "E Red kWh"), campo(T, "E PV kWh")]}, "Red vs solar por franja horaria"),
    visual("l2", "lineChart", 1200, 670, 700, 390,
           {"Category": [F("anio_mes")], "Y": [campo(T, "Autosuficiencia %"), campo(T, "Autosuficiencia mes anterior %")]},
           "Autosuficiencia vs mes anterior"),
 ]),
 "pagEconomiaEscenarios": ("3 · Economía y escenarios", [
    texto("tit3", 20, 10, 1500, 60, "¿Cuánto cuesta la red y qué pasaría si…? (what-if de tarifa y capacidad PV)"),
    visual("nav3", "pageNavigator", 1540, 10, 360, 60, None),
    segmentador("seg3a", 20, 80, "Tarifa", "Tarifa", "Tarifa (COP/kWh) · base 920", h=200),
    segmentador("seg3b", 20, 300, "Factor PV", "Factor PV", "Factor de capacidad PV (1 = actual)", h=200),
    segmentador("seg3c", 20, 520, "nombre", "dim_inversor", "Inversor"),
    tarjeta("k3a", 320, 80, T, "Costo red COP", "Costo de energía de red"),
    tarjeta("k3b", 610, 80, T, "Ahorro autosuficiencia COP", "Ahorro por autosuficiencia"),
    tarjeta("k3c", 900, 80, T, "Ahorro YTD COP", "Ahorro acumulado del año"),
    tarjeta("k3d", 1190, 80, T, "Ahorro adicional escenario COP", "Ahorro adicional del escenario PV", w=420),
    tarjeta("k3e", 1630, 80, T, "Var PV MoM %", "Var. PV mes a mes", w=270),
    visual("l3", "lineChart", 320, 230, 1580, 400,
           {"Category": [F("anio_mes")], "Y": [campo(T, "Costo red COP"), campo(T, "Ahorro autosuficiencia COP")]},
           "Costo de red vs ahorro por mes"),
    visual("t3", "tableEx", 320, 650, 1580, 410,
           {"Values": [INV, campo(T, "E PV kWh"), campo(T, "E Red kWh"), campo(T, "Autosuficiencia %"),
                       campo(T, "Costo red COP"), campo(T, "E Red escenario kWh"), campo(T, "Ahorro adicional escenario COP"),
                       campo(T, "Ciclos equivalentes")]}, "Resumen por inversor"),
 ]),
}
os.makedirs(os.path.join(RP, "pages"), exist_ok=True)
for pid, (nombre, visuales) in paginas.items():
    base = os.path.join(RP, "pages", pid)
    os.makedirs(os.path.join(base, "visuals"), exist_ok=True)
    json.dump({"$schema": PS, "name": pid, "displayName": nombre, "displayOption": "FitToPage",
               "height": 1080, "width": 1920}, open(os.path.join(base, "page.json"), "w", encoding="utf-8"),
              ensure_ascii=False, indent=2)
    for v in visuales:
        d = os.path.join(base, "visuals", v["name"])
        os.makedirs(d, exist_ok=True)
        json.dump(v, open(os.path.join(d, "visual.json"), "w", encoding="utf-8"), ensure_ascii=False, indent=2)
json.dump({"$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.1.0/schema.json",
           "pageOrder": list(paginas), "activePageName": list(paginas)[0]},
          open(os.path.join(RP, "pages", "pages.json"), "w", encoding="utf-8"), indent=2)
print("ok")
