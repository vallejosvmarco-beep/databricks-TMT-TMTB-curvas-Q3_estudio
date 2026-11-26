from pyspark import pipelines as dp
from pyspark.sql import functions as F
import numpy as np
import pandas as pd

"""
Despacho optimo de BESS por tarifa (estrategia PUNTA_OPTIMO), alineado a
conocimiento_base/CNFL/estudio_financiero_solar/dispatch_optimization_bess_solar.py:

  - Programacion lineal por cliente y mes (dias tipicos laboral y fin_de_semana juntos,
    96 intervalos de 15 min): minimiza costo de compra de energia por franja (tarifa
    de compra + peaje) x dias con datos + cargo por potencia maxima por franja del mes.
  - El BESS carga desde la red o desde el excedente solar y descarga a la carga o a la
    red (CNFL es duena de solar y BESS: lo exportado reduce su compra).
  - Balance: E[t] = E[t-1] + carga x ef_carga - descarga / ef_descarga.
  - SOC entre SOC_MIN (reserva 10%) y SOC_MAX; ciclo diario cerrado (E final = E inicial).
  - Potencia de carga/descarga = FRACCION_POTENCIA_FV x kWp del cliente; capacidad =
    potencia x HORAS_DURACION (parametros_bateria).
  - Diferencia con el estudio: se relaja la restriccion binaria de no cargar y
    descargar a la vez (con eficiencias < 1 el LP no lo hace; se valida eficiencia
    de ciclo <= ef_carga x ef_descarga en la calidad).
  - La potencia se mide sobre la curva promedio del dia tipico (no el maximo del mes).
  - Potencia de PUNTA: solo dias laborales (consolidado_tarifas_28102026.pdf, T-MT/T-MTb:
    la demanda punta excluye sabados y domingos); valle y noche: todos los dias.

Tarifas: deben coincidir con COMPRA/PEAJE de src/financiero/financiero_solar.py
(config.py del estudio, CON CVG, 22/04/2026).

El LP se resuelve UNA sola vez, en bateria_despacho_optimo_intervalo (curva de 15 min).
bateria_despacho_optimo (resumen mensual) se arma desde esas curvas con las mismas
formulas (_costo, kWh cargados/descargados), sin volver a resolver el LP.
bateria_despacho_optimo_validacion compara ambos (KWH_CARGA, KWH_DESCARGA, KW_PICO_PUNTA_CON).
"""

OUT_CAT = spark.conf.get("project.output_catalog")
OUT_SCH = spark.conf.get("project.output_schema")
SOLAR_INTERVALO = f"{OUT_CAT}.{OUT_SCH}.impacto_solar_q3_intervalo"
PARAM_BATERIA = f"{OUT_CAT}.{OUT_SCH}.parametros_bateria"

# Compra CON CVG (config.py, revisadas 22/04/2026), igual que financiero_solar.py
TC = {"PUNTA": 36.85, "VALLE": 30.20, "NOCHE": 25.63}
TPC = {"PUNTA": 1954.51, "VALLE": 1954.51, "NOCHE": 0.0}
PEAJE = 10.36
FRANJAS = ["PUNTA", "VALLE", "NOCHE"]


def _franja(b):
    if 41 <= b <= 50 or 71 <= b <= 80:
        return "PUNTA"
    if 25 <= b <= 40 or 51 <= b <= 70:
        return "VALLE"
    return "NOCHE"


FR = [_franja(b) for b in range(96)]
PRECIO = np.array([TC[f] + PEAJE for f in FR])
FIDX = {f: np.array([b for b in range(96) if FR[b] == f]) for f in FRANJAS}

OUT_SCHEMA = (
    "NISE string, PLAN string, PERIODO_MES date, ESCENARIO_BATERIA string, KWP double, "
    "POTENCIA_BESS_KW double, CAPACIDAD_KWH double, COSTO_ENERGIA_SIN_BESS_CRC double, "
    "COSTO_ENERGIA_CON_BESS_CRC double, COSTO_POTENCIA_SIN_BESS_CRC double, "
    "COSTO_POTENCIA_CON_BESS_CRC double, AHORRO_ENERGIA_CRC double, AHORRO_POTENCIA_CRC double, "
    "AHORRO_TOTAL_CRC double, KWH_CARGA double, KWH_CARGA_RED double, KWH_DESCARGA double, "
    "KWH_DESCARGA_PUNTA double, EFICIENCIA_CICLO double, KW_PICO_PUNTA_SIN double, "
    "KW_PICO_PUNTA_CON double, ESTADO_LP string"
)


def _cuenta_pico(f, tipo):
    """La demanda de punta no se factura en fin de semana (feriado = fin_de_semana)."""
    return not (f == "PUNTA" and tipo != "laboral")


def _costo(nets, dias, tipos):
    e = sum(d * float((PRECIO * n).sum()) for n, d in zip(nets, dias))
    p = {f: max([0.0] + [4 * float(n[FIDX[f]].max()) for n, tp in zip(nets, tipos) if _cuenta_pico(f, tp)])
         for f in FRANJAS}
    return e, sum(TPC[f] * p[f] for f in FRANJAS), p


def _despacho(L_list, S_list, dias, tipos, cap, pq, ec, ed, soc_min, soc_max):
    from scipy.optimize import linprog
    from scipy.sparse import lil_matrix
    K, nb = len(L_list), 96
    nvd = 3 * nb + 1                      # carga, descarga, E, E0 por tipo de dia
    nv = K * nvd + 3                      # + pico por franja (compartido en el mes)
    c = np.zeros(nv)
    bounds = []
    for k in range(K):
        o = k * nvd
        c[o:o + nb] = dias[k] * PRECIO
        c[o + nb:o + 2 * nb] = -dias[k] * PRECIO
        bounds += [(0, pq)] * (2 * nb) + [(soc_min * cap, soc_max * cap)] * (nb + 1)
    pk = {f: K * nvd + i for i, f in enumerate(FRANJAS)}
    for f in FRANJAS:
        c[pk[f]] = TPC[f]
    bounds += [(0, None)] * 3
    aeq = lil_matrix((K * (nb + 1), nv))
    aub = lil_matrix((K * nb, nv))
    bub = np.zeros(K * nb)
    r = ru = 0
    for k in range(K):
        o = k * nvd
        neto = np.asarray(L_list[k]) - np.asarray(S_list[k])
        for t in range(nb):
            aeq[r, o + 2 * nb + t] = 1
            aeq[r, o + t] = -ec
            aeq[r, o + nb + t] = 1 / ed
            aeq[r, (o + 2 * nb + t - 1) if t > 0 else (o + 3 * nb)] = -1
            r += 1
            if _cuenta_pico(FR[t], tipos[k]):
                aub[ru, o + t] = 4
                aub[ru, o + nb + t] = -4
                aub[ru, pk[FR[t]]] = -1
                bub[ru] = -4 * neto[t]
                ru += 1
        aeq[r, o + 3 * nb - 1] = 1        # ciclo cerrado: E[95] = E0
        aeq[r, o + 3 * nb] = -1
        r += 1
    aub, bub = aub[:ru, :], bub[:ru]
    res = linprog(c, A_ub=aub.tocsr(), b_ub=bub, A_eq=aeq.tocsr(), b_eq=np.zeros(K * (nb + 1)),
                  bounds=bounds, method="highs")
    if res.status != 0:
        return None
    x = res.x
    ch = [x[k * nvd:k * nvd + nb] for k in range(K)]
    dis = [x[k * nvd + nb:k * nvd + 2 * nb] for k in range(K)]
    ene = [x[k * nvd + 2 * nb:k * nvd + 3 * nb] for k in range(K)]   # energia almacenada al final del bloque
    return ch, dis, ene


INT_COLS = [
    ("NISE", "string"), ("PLAN", "string"), ("PERIODO_MES", "date"), ("ESCENARIO_BATERIA", "string"),
    ("TIPO_DIA", "string"), ("BLOQUE_15M", "int"), ("HORA", "int"), ("MINUTO", "int"), ("FRANJA", "string"),
    ("DIAS_CON_DATOS", "double"), ("KWP", "double"), ("POTENCIA_BESS_KW", "double"), ("CAPACIDAD_KWH", "double"),
    ("DEMANDA_KWH", "double"), ("GENERACION_SOLAR_KWH", "double"), ("NETO_SIN_BESS_KWH", "double"),
    ("CARGA_KWH", "double"), ("CARGA_RED_KWH", "double"), ("CARGA_SOLAR_KWH", "double"),
    ("DESCARGA_KWH", "double"), ("ENERGIA_BESS_KWH", "double"), ("SOC", "double"),
    ("NETO_CON_BESS_KWH", "double"), ("ESTADO_LP", "string"),
]
INT_SCHEMA = ", ".join(f"{n} {t}" for n, t in INT_COLS)


def _make_fn_intervalo(escenarios):
    """Resuelve el LP por NISE x mes x escenario y devuelve la curva de 15 min."""
    def fn(pdf):
        filas = []
        for (nise, plan, mes), g in pdf.groupby(["NISE", "PLAN", "PERIODO_MES"]):
            g = g.sort_values(["TIPO_DIA", "BLOQUE_15M"])
            tipos = [t for t, h in g.groupby("TIPO_DIA") if len(h) == 96]
            if not tipos:
                continue
            L = [g[g.TIPO_DIA == t]["DEMANDA_PROM_KWH"].to_numpy(float) for t in tipos]
            S = [g[g.TIPO_DIA == t]["GENERACION_SOLAR_SIMULADA_KWH"].to_numpy(float) for t in tipos]
            D = [float(g[g.TIPO_DIA == t]["DIAS_CON_DATOS"].max()) for t in tipos]
            kwp = float(g["POTENCIA_SOLAR_PICO_CLIENTE"].max() or 0.0) * 4
            for esc in escenarios:
                pot = esc["FRACCION_POTENCIA_FV"] * kwp
                cap = pot * esc["HORAS_DURACION"]
                if cap <= 0:
                    continue
                out = _despacho(L, S, D, tipos, cap, pot / 4, esc["EFICIENCIA_CARGA"], esc["EFICIENCIA_DESCARGA"],
                                esc["SOC_MIN"], esc["SOC_MAX"])
                for k, tp in enumerate(tipos):
                    l, s = L[k], S[k]
                    neto = l - s
                    if out is None:
                        c = d = e = None
                    else:
                        c, d, e = out[0][k], out[1][k], out[2][k]
                    for b in range(96):
                        if c is None:
                            bat, estado = (None,) * 7, "NO_OPTIMO"
                        else:
                            # carga desde la red = carga que excede el excedente solar del bloque
                            cr = max(c[b] - max(s[b] - l[b], 0.0), 0.0)
                            bat = (float(c[b]), float(cr), float(c[b] - cr), float(d[b]), float(e[b]),
                                   float(e[b] / cap), float(neto[b] + c[b] - d[b]))
                            estado = "OPTIMO"
                        filas.append((nise, plan, mes, esc["ESCENARIO"], tp, b, b // 4, (b % 4) * 15, FR[b],
                                      D[k], kwp, pot, cap, float(l[b]), float(s[b]), float(neto[b]))
                                     + bat + (estado,))
        return pd.DataFrame(filas, columns=[n for n, _ in INT_COLS])
    return fn


def _resumen_fn(pdf):
    """Resumen mensual desde la curva del LP (mismas formulas que antes, sin re-resolver)."""
    filas = []
    for (nise, plan, mes, esc), g in pdf.groupby(["NISE", "PLAN", "PERIODO_MES", "ESCENARIO_BATERIA"]):
        g = g.sort_values(["TIPO_DIA", "BLOQUE_15M"])
        tipos = sorted(g["TIPO_DIA"].unique())
        h = {t: g[g.TIPO_DIA == t] for t in tipos}
        L = [h[t]["DEMANDA_KWH"].to_numpy(float) for t in tipos]
        S = [h[t]["GENERACION_SOLAR_KWH"].to_numpy(float) for t in tipos]
        D = [float(h[t]["DIAS_CON_DATOS"].max()) for t in tipos]
        kwp, pot, cap = (float(g[c].iloc[0]) for c in ("KWP", "POTENCIA_BESS_KW", "CAPACIDAD_KWH"))
        netos = [l - s for l, s in zip(L, S)]
        e0, p0, pk0 = _costo(netos, D, tipos)
        if (g["ESTADO_LP"] != "OPTIMO").any():
            filas.append((nise, plan, mes, esc, kwp, pot, cap, e0, None, p0, None,
                          None, None, None, None, None, None, None, None, pk0["PUNTA"], None, "NO_OPTIMO"))
            continue
        ch = [h[t]["CARGA_KWH"].to_numpy(float) for t in tipos]
        dis = [h[t]["DESCARGA_KWH"].to_numpy(float) for t in tipos]
        netos_b = [n + c - d for n, c, d in zip(netos, ch, dis)]
        e1, p1, pk1 = _costo(netos_b, D, tipos)
        kc = sum(dd * float(c.sum()) for c, dd in zip(ch, D))
        kd = sum(dd * float(d.sum()) for d, dd in zip(dis, D))
        # carga desde la red = carga que excede el excedente solar del intervalo
        kcr = sum(dd * float(np.maximum(c - np.maximum(s - l, 0.0), 0.0).sum())
                  for c, s, l, dd in zip(ch, S, L, D))
        kdp = sum(dd * float(d[FIDX["PUNTA"]].sum()) for d, dd in zip(dis, D))
        filas.append((nise, plan, mes, esc, kwp, pot, cap, e0, e1, p0, p1,
                      e0 - e1, p0 - p1, (e0 - e1) + (p0 - p1), kc, kcr, kd, kdp,
                      (kd / kc) if kc > 0 else None, pk0["PUNTA"], pk1["PUNTA"], "OPTIMO"))
    return pd.DataFrame(filas, columns=[c.split()[0] for c in OUT_SCHEMA.split(", ")])


@dp.materialized_view(
    name="bateria_despacho_optimo_intervalo",
    comment=(
        "Curva de 15 min del despacho optimo de BESS (LP mensual por cliente, estrategia PUNTA_OPTIMO): "
        "por NISE x mes x escenario x tipo de dia x bloque. Demanda, solar, neto sin BESS, carga "
        "(red/solar), descarga, energia almacenada y SOC al final del bloque, neto con BESS (kWh por "
        "15 min; kW = kWh x 4). BLOQUE_15M con etiqueta de fin de intervalo; HORA/MINUTO = BLOQUE."
    ),
)
def bateria_despacho_optimo_intervalo():
    escenarios = [r.asDict() for r in spark.read.table(PARAM_BATERIA).filter("ACTIVO").collect()]
    si = spark.read.table(SOLAR_INTERVALO).select(
        "NISE", "PLAN", "PERIODO_MES", "TIPO_DIA", "BLOQUE_15M", "DEMANDA_PROM_KWH",
        "GENERACION_SOLAR_SIMULADA_KWH", "DIAS_CON_DATOS", "POTENCIA_SOLAR_PICO_CLIENTE")
    return si.groupBy("NISE").applyInPandas(_make_fn_intervalo(escenarios), INT_SCHEMA)


@dp.materialized_view(
    name="bateria_despacho_optimo",
    comment=(
        "Despacho optimo de BESS por tarifa (LP mensual por cliente, estrategia PUNTA_OPTIMO del "
        "estudio): costos de compra de energia y potencia sin/con BESS sobre la curva neta "
        "(carga - solar), ahorros, kWh cargados (red/solar) y descargados. Por NISE x mes x escenario. "
        "Resumen de bateria_despacho_optimo_intervalo (el LP no se vuelve a resolver)."
    ),
)
def bateria_despacho_optimo():
    return (spark.read.table(f"{OUT_CAT}.{OUT_SCH}.bateria_despacho_optimo_intervalo")
            .groupBy("NISE").applyInPandas(_resumen_fn, OUT_SCHEMA))


@dp.materialized_view(
    name="bateria_despacho_optimo_validacion",
    comment=(
        "Valida que la curva de 15 min (dias x kWh) cuadre con el resumen mensual: KWH_CARGA, "
        "KWH_DESCARGA y KW_PICO_PUNTA_CON (max 4 x neto con BESS en punta, dias laborales). OK si |dif| <= 1e-6 relativo."
    ),
)
def bateria_despacho_optimo_validacion():
    k = ["NISE", "PLAN", "PERIODO_MES", "ESCENARIO_BATERIA"]
    it = (spark.read.table(f"{OUT_CAT}.{OUT_SCH}.bateria_despacho_optimo_intervalo")
          .filter("ESTADO_LP = 'OPTIMO'")
          .groupBy(*k).agg(
              F.sum(F.col("DIAS_CON_DATOS") * F.col("CARGA_KWH")).alias("KWH_CARGA_CURVA"),
              F.sum(F.col("DIAS_CON_DATOS") * F.col("DESCARGA_KWH")).alias("KWH_DESCARGA_CURVA"),
              F.max(F.when((F.col("FRANJA") == "PUNTA") & (F.col("TIPO_DIA") == "laboral"),
                           4 * F.col("NETO_CON_BESS_KWH"))).alias("KW_PICO_PUNTA_CON_CURVA")))
    m = (spark.read.table(f"{OUT_CAT}.{OUT_SCH}.bateria_despacho_optimo").filter("ESTADO_LP = 'OPTIMO'")
         .select(*k, "KWH_CARGA", "KWH_DESCARGA", "KW_PICO_PUNTA_CON"))
    rel = lambda a, b: F.abs(F.col(a) - F.col(b)) / F.greatest(F.abs(F.col(b)), F.lit(1.0))  # noqa: E731
    out = (m.join(it, k, "full_outer")
           .withColumn("KW_PICO_PUNTA_CON_CURVA", F.greatest(F.coalesce("KW_PICO_PUNTA_CON_CURVA", F.lit(0.0)), F.lit(0.0)))
           .withColumn("DIF_KWH_CARGA", rel("KWH_CARGA_CURVA", "KWH_CARGA"))
           .withColumn("DIF_KWH_DESCARGA", rel("KWH_DESCARGA_CURVA", "KWH_DESCARGA"))
           .withColumn("DIF_KW_PICO_PUNTA_CON", rel("KW_PICO_PUNTA_CON_CURVA", "KW_PICO_PUNTA_CON")))
    return out.withColumn("OK", F.coalesce(
        (F.col("DIF_KWH_CARGA") <= 1e-6) & (F.col("DIF_KWH_DESCARGA") <= 1e-6) & (F.col("DIF_KW_PICO_PUNTA_CON") <= 1e-6),
        F.lit(False)))
