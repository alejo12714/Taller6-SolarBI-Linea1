"""
Genera una base SQLite DE PRUEBA con el esquema oficial de OpenCEM
(documentation/schema.sql) y datos SINTÉTICOS. Sirve solo para probar el ETL
en CI / portátil sin descargar los 4,4 GB. Ninguna cifra del informe sale de aquí.

Uso: python -I crear_sqlite_prueba.py <schema.sql> <salida.sqlite>
"""
import json
import math
import random
import sqlite3
import sys

random.seed(7)
schema, salida = sys.argv[1], sys.argv[2]
con = sqlite3.connect(salida)
con.executescript(open(schema, encoding="utf-8").read())

T0 = 1781280000.0          # 2026-06-12 16:00 UTC = 13-jun 00:00 local
DIAS = 3
filas = []
for inv in (1, 2):
    t, soc = T0, 60.0
    while t < T0 + DIAS * 86400:
        hora_local = ((t + 8 * 3600) % 86400) / 3600
        sol = max(0.0, math.sin(math.pi * (hora_local - 6) / 13)) if 6 <= hora_local <= 19 else 0.0
        pv = round(1800 * inv * sol * random.uniform(0.85, 1.0))
        carga = round(random.uniform(300, 900) * inv)
        red_on = not (inv == 1 and T0 + 86400 + 3600 * 3 < t < T0 + 86400 + 3600 * 4)  # 1 h sin red
        red = max(0, carga - pv) if (red_on and soc < 30) else 0
        bat_w = carga - pv - red                       # >0 descarga
        vbat = 51.2 + (soc - 50) * 0.05
        icurr = -bat_w / vbat                          # convención "negativo = descarga" (como el pipeline derivado)
        soc = min(100, max(5, soc - bat_w * 12 / 3600 / (inv * 5000) * 100))
        fila = dict(read_ts=t, inverter=inv, battvolt=round(vbat, 1), battcurr=round(icurr, 1), battsoc=round(soc),
                    pv1volt=380.0 if pv else 0.0, pv1curr=round(pv / 380, 2), pv1power=pv,
                    outvolta=230.0, outw_a=carga, outva_a=round(carga * 1.05), linevolta=225.0 if red_on else 0.0,
                    linepowerw_a=red, outfreq=50.0, linefreq=50.0 if red_on else 0.0,
                    temper1=120.0, temper2=120.0, temper3=41.0, temper4=120.0)
        r = random.random()
        if r < 0.02:                                   # trama nula (R01)
            fila = {k: (v if k in ("read_ts", "inverter") else None) for k, v in fila.items()}
        elif r < 0.025:                                # identidad PV rota (R07)
            fila["pv1power"] = (fila["pv1power"] or 0) + 400
        filas.append(fila)
        dt = random.choice([12.5, 12.6, 12.7, 60])
        if inv == 2 and abs(t - (T0 + 2 * 86400)) < 13:
            dt = 1800                                  # hueco de 30 min (R10 + evento HUECO_REGISTRO)
        t += dt

cols = list(filas[0].keys())
con.executemany(f"INSERT INTO analog_measurements ({','.join(cols)}) VALUES ({','.join('?'*len(cols))})",
                [tuple(f[c] for c in cols) for f in filas])
for inv in (1, 2):
    for k in range(DIAS * 24):
        ts = T0 + k * 3600
        alarma = 20 if k < 30 else 25                  # cambio de configuración -> SCD2
        con.execute("INSERT INTO settings (read_ts, inverter, battpackrateah, batsoclowalarm, batstopsoc) "
                    "VALUES (?,?,?,?,?)", (ts, inv, 100.0 * inv, alarma, 10))
        con.execute("INSERT INTO specs (read_ts, inverter, protocolver) VALUES (?,?,?)", (ts, inv, 101))
        con.execute("INSERT INTO statistics (read_ts, inverter, pvgeneratenergytoday, usedenergytoday) "
                    "VALUES (?,?,?,?)", (ts, inv, round(4 * inv * ((k % 24) / 24), 1), round(10 * inv * ((k % 24) / 24), 1)))
        con.execute("INSERT INTO status (read_ts, inverter, sysalarmflag) VALUES (?,?,?)",
                    (ts, inv, json.dumps([0, 0, 4 if k == 40 else 0])))
ctx = [
    {"source": "manual_workload_entry", "task_type": "CNN fitting, GPU load",
     "context_type": "gpu_cnn_training_workload", "device": "workstation2", "inverter": 2,
     "textual_description": "texto libre que NO debe llegar a Gold"},
    {"source": "derived_context_pipeline", "context_type": "data_gap", "inverter": 2, "gap_seconds": 1800.0},
    {"source": "derived_context_pipeline", "context_type": "solar_daylight"},
    {"source": "manual entry", "task_type": "compilation, CPU load", "context_type": "cpu_compilation_workload",
     "device": "workstation1", "inverter": 1},
]
for n, v in enumerate(ctx):
    ini = int(T0 + 86400 + n * 7200)
    con.execute('INSERT INTO context (recorded, start, "end", value) VALUES (?,?,?,?)',
                (ini, ini, ini + 5400, json.dumps(v)))
con.commit()
print(f"{len(filas)} lecturas sintéticas -> {salida}")
