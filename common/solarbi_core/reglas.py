"""
Reglas de calidad y agregación a 1 minuto, compartidas por el ETL histórico
y el simulador de streaming. Un solo código => el dato histórico y el simulado
pasan por exactamente las mismas reglas (consistencia entre capas).

Convenciones:
  * read_ts en UTC (datetime64[ns, UTC]).
  * Potencias en W, tensiones en V, corrientes en A, SoC en %.
  * Energía por retención de orden cero: cada lectura sostiene su potencia
    hasta la siguiente lectura del mismo inversor, si Δt <= DT_MAX_S.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

ZONA_SITIO = "Asia/Shanghai"   # Longgang, Shenzhen (UTC+8, sin horario de verano)

COLUMNAS_FUENTE = [
    "read_ts", "inverter", "battvolt", "battcurr", "battsoc",
    "pv1volt", "pv1curr", "pv1power", "outvolta", "outw_a", "outva_a",
    "linevolta", "linepowerw_a", "outfreq", "linefreq",
    "temper1", "temper2", "temper3", "temper4",
]
CAMPOS_FRAME = ["pv1power", "outw_a", "battsoc", "battvolt", "linevolta"]


@dataclass
class Parametros:
    dt_max_s: float = 120.0
    hueco_evento_s: float = 600.0
    temp_centinela: float = 120.0
    tol_identidad_pv: float = 0.10
    red_ausente_v: float = 100.0
    signo_descarga: int = 1        # +1: battcurr > 0 es descarga (diccionario de la fuente)


@dataclass
class ConteoRegla:
    """Acumula (inversor, fecha_local, regla) -> [evaluadas, afectadas]."""
    diario: dict = field(default_factory=dict)
    # (orden, regla, inversor) -> [antes, afectadas, despues]
    global_: dict = field(default_factory=dict)

    def sumar(self, orden, regla, df_eval: pd.DataFrame, mask_afectadas: pd.Series, elimina: bool):
        if df_eval.empty:
            return
        g = pd.DataFrame({
            "inv": df_eval["inverter"].to_numpy(),
            "fecha": df_eval["fecha_local"].to_numpy(),
            "afe": mask_afectadas.to_numpy().astype(np.int64),
        }).groupby(["inv", "fecha"])["afe"].agg(["size", "sum"])
        for (inv, fecha), (n, a) in g.iterrows():
            k = (int(inv), fecha, regla)
            v = self.diario.setdefault(k, [0, 0])
            v[0] += int(n)
            v[1] += int(a)
        for inv, sub in df_eval.groupby("inverter"):
            a = int(mask_afectadas.loc[sub.index].sum())
            k = (orden, regla, int(inv))
            v = self.global_.setdefault(k, [0, 0, 0])
            v[0] += len(sub)
            v[1] += a
            v[2] += len(sub) - a if elimina else len(sub)


def preparar(df: pd.DataFrame) -> pd.DataFrame:
    """Tipos y llaves de tiempo. read_ts puede llegar como epoch (float) o datetime."""
    df = df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df["read_ts"]):
        df["read_ts"] = pd.to_datetime(df["read_ts"], unit="s", utc=True)
    df["inverter"] = df["inverter"].astype("int16")
    local = df["read_ts"].dt.tz_convert(ZONA_SITIO)
    df["fecha_local"] = local.dt.date
    df["ts_minuto"] = df["read_ts"].dt.floor("min")
    for c in COLUMNAS_FUENTE[2:]:
        if c in df:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def aplicar_reglas(df: pd.DataFrame, p: Parametros, conteo: ConteoRegla | None,
                   rechazos: list | None = None) -> pd.DataFrame:
    """
    Aplica R01..R09 en orden y devuelve TODAS las filas con columnas:
      valida (bool), sospechosa (bool). Los campos anulados quedan NaN.
    R10 (hueco) se evalúa en integrar() porque depende de la lectura siguiente.
    """
    df = df.copy()
    df["valida"] = True
    df["sospechosa"] = False

    def registrar(orden, codigo, base, mask, elimina):
        if conteo is not None:
            conteo.sumar(orden, codigo, base, mask, elimina)
        if rechazos is not None and elimina and mask.any():
            rech = base.loc[mask, ["inverter", "read_ts", "pv1power", "outw_a", "battsoc", "battvolt"]]
            for r in rech.itertuples(index=False):  # noqa
                rechazos.append((int(r.inverter), r.read_ts.to_pydatetime(warn=False), codigo, {
                    "pv1power": None if pd.isna(r.pv1power) else float(r.pv1power),
                    "outw_a": None if pd.isna(r.outw_a) else float(r.outw_a),
                    "battsoc": None if pd.isna(r.battsoc) else float(r.battsoc),
                    "battvolt": None if pd.isna(r.battvolt) else float(r.battvolt),
                }))

    # R02 · duplicados (se conserva la primera aparición)
    dup = df.duplicated(subset=["inverter", "read_ts"], keep="first")
    registrar(1, "R02_DUPLICADO", df, dup, True)
    df = df.loc[~dup]

    viva = df["valida"]
    # R01 · trama vacía
    m = df[CAMPOS_FRAME].isna().all(axis=1)
    registrar(2, "R01_FRAME_NULO", df[viva], m[viva], True)
    df.loc[m, "valida"] = False

    viva = df["valida"]
    # R03 · SoC fuera de rango
    m = df["battsoc"].notna() & ~df["battsoc"].between(0, 100)
    registrar(3, "R03_SOC_RANGO", df[viva], m[viva], True)
    df.loc[m & viva, "valida"] = False

    viva = df["valida"]
    # R04 · frecuencia fuera de rango (0 Hz en línea = red ausente, es evento, no inválido)
    def fuera(col):
        return df[col].notna() & (df[col] != 0) & ~df[col].between(45, 65)
    m = fuera("outfreq") | fuera("linefreq")
    registrar(4, "R04_FREC_RANGO", df[viva], m[viva], True)
    df.loc[m & viva, "valida"] = False

    viva = df["valida"]
    # R05 · PV negativa
    m = df["pv1power"] < 0
    registrar(5, "R05_PV_NEGATIVA", df[viva], m[viva], True)
    df.loc[m & viva, "valida"] = False

    viva = df["valida"]
    # R06 · centinela de temperatura -> anular campo (la fila sigue siendo válida)
    temps = [c for c in ("temper1", "temper2", "temper3", "temper4") if c in df]
    m = (df[temps] == p.temp_centinela).any(axis=1)
    registrar(6, "R06_TEMP_CENTINELA", df[viva], m[viva], False)
    for c in temps:
        df.loc[df[c] == p.temp_centinela, c] = np.nan

    # R07 · identidad P = V·I en PV -> sospechosa
    elig = df["pv1power"] >= 100
    calc = df["pv1volt"] * df["pv1curr"]
    m = elig & ((df["pv1power"] - calc).abs() > p.tol_identidad_pv * df["pv1power"])
    registrar(7, "R07_IDENTIDAD_PV", df[viva], m[viva], False)
    df.loc[m, "sospechosa"] = True

    # R08 · activa > aparente + 50 VA -> sospechosa
    m = df["outva_a"].notna() & df["outw_a"].notna() & (df["outw_a"].abs() > df["outva_a"] + 50)
    registrar(8, "R08_ACTIVA_GT_APARENTE", df[viva], m[viva], False)
    df.loc[m, "sospechosa"] = True

    # R09 · 0 V de batería con SoC > 0 -> anular tensión y corriente
    m = (df["battvolt"] == 0) & (df["battsoc"] > 0)
    registrar(9, "R09_BAT_V_INCOHERENTE", df[viva], m[viva], False)
    df.loc[m, ["battvolt", "battcurr"]] = np.nan
    return df


def integrar(df: pd.DataFrame, p: Parametros, conteo: ConteoRegla | None) -> pd.DataFrame:
    """
    df debe venir ordenado por (inverter, read_ts) y traer la columna dt_s
    (segundos hasta la siguiente lectura del mismo inversor; NaN si no se conoce).
    Calcula energías por lectura (kWh) respetando R10.
    """
    df = df.copy()
    hueco = df["dt_s"] > p.dt_max_s
    if conteo is not None:
        base = df[df["dt_s"].notna()]
        conteo.sumar(10, "R10_HUECO", base, hueco[base.index], False)
    dt = df["dt_s"].where(~hueco & df["dt_s"].notna(), 0.0)
    df["seg"] = dt
    df["seg_valid"] = dt.where(df["valida"], 0.0)
    k = df["seg_valid"] / 3.6e6                         # W·s -> kWh
    df["e_pv"] = df["pv1power"].clip(lower=0).fillna(0) * k
    df["e_carga"] = df["outw_a"].clip(lower=0).fillna(0) * k
    df["e_red"] = df["linepowerw_a"].clip(lower=0).fillna(0) * k
    p_bat = (df["battvolt"] * df["battcurr"] * p.signo_descarga).fillna(0)   # >0 descarga
    df["e_bat_desc"] = p_bat.clip(lower=0) * k
    df["e_bat_carga"] = (-p_bat).clip(lower=0) * k
    df["red_ausente"] = (df["linevolta"].fillna(0) < p.red_ausente_v).astype(float)
    return df


def agregar_minuto(df: pd.DataFrame) -> pd.DataFrame:
    """Agregados parciales por (inverter, ts_minuto) con sumas combinables."""
    v = df["valida"]
    tmp = pd.DataFrame({
        "inverter": df["inverter"], "ts_minuto": df["ts_minuto"],
        "n_lecturas": 1, "n_validas": v.astype(int),
        "n_sospechosas": (df["sospechosa"] & v).astype(int),
        "seg_cubiertos": df["seg"], "seg_cubiertos_valid": df["seg_valid"],
        "e_pv_kwh": df["e_pv"], "e_carga_kwh": df["e_carga"], "e_red_kwh": df["e_red"],
        "e_bat_carga_kwh": df["e_bat_carga"], "e_bat_descarga_kwh": df["e_bat_desc"],
        "p_pv_max_w": df["pv1power"].where(v), "p_carga_max_w": df["outw_a"].where(v),
        "soc_sum": df["battsoc"].where(v), "soc_n": df["battsoc"].where(v).notna().astype(int),
        "soc_min_pct": df["battsoc"].where(v),
        "vbat_sum": df["battvolt"].where(v), "vbat_n": df["battvolt"].where(v).notna().astype(int),
        "vred_sum": df["linevolta"].where(v), "vred_n": df["linevolta"].where(v).notna().astype(int),
        "ausente_sum": df["red_ausente"].where(v), "vout_sum": df["outvolta"].where(v),
        "vout_n": df["outvolta"].where(v).notna().astype(int),
    })
    return combinar(tmp)


SUMABLES = ["n_lecturas", "n_validas", "n_sospechosas", "seg_cubiertos", "seg_cubiertos_valid",
            "e_pv_kwh", "e_carga_kwh", "e_red_kwh", "e_bat_carga_kwh", "e_bat_descarga_kwh",
            "soc_sum", "soc_n", "vbat_sum", "vbat_n", "vred_sum", "vred_n", "ausente_sum",
            "vout_sum", "vout_n"]


def combinar(parciales: pd.DataFrame) -> pd.DataFrame:
    agg = {c: "sum" for c in SUMABLES}
    agg.update({"p_pv_max_w": "max", "p_carga_max_w": "max", "soc_min_pct": "min"})
    return parciales.groupby(["inverter", "ts_minuto"], as_index=False).agg(agg)


def finalizar_minuto(m: pd.DataFrame) -> pd.DataFrame:
    """Convierte sumas parciales en el registro Silver definitivo."""
    out = m[["inverter", "ts_minuto", "n_lecturas", "n_validas", "n_sospechosas",
             "seg_cubiertos", "seg_cubiertos_valid", "e_pv_kwh", "e_carga_kwh", "e_red_kwh",
             "e_bat_carga_kwh", "e_bat_descarga_kwh", "p_pv_max_w", "p_carga_max_w",
             "soc_min_pct"]].copy()
    out["soc_prom_pct"] = (m["soc_sum"] / m["soc_n"].replace(0, np.nan)).round(2)
    out["v_bat_prom"] = (m["vbat_sum"] / m["vbat_n"].replace(0, np.nan)).round(2)
    out["v_red_prom"] = (m["vred_sum"] / m["vred_n"].replace(0, np.nan)).round(2)
    out["ratio_red_ausente"] = (m["ausente_sum"] / m["n_validas"].replace(0, np.nan)).round(3)
    out["v_salida_prom"] = (m["vout_sum"] / m["vout_n"].replace(0, np.nan)).round(2)
    out["seg_cubiertos"] = out["seg_cubiertos"].clip(upper=180).round(2)
    out["seg_cubiertos_valid"] = out["seg_cubiertos_valid"].clip(upper=180).round(2)
    for c in ("e_pv_kwh", "e_carga_kwh", "e_red_kwh", "e_bat_carga_kwh", "e_bat_descarga_kwh"):
        out[c] = out[c].round(6)
    return out
