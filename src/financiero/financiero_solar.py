from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import numpy as np
import pandas as pd

"""
Evaluacion financiera de dos escenarios para clientes TMT/TMTB (alineado a la
base de conocimiento: conocimiento_base/CNFL/estudio_financiero_solar).

E1 - El cliente instala solar por su cuenta:
     CNFL pierde ventas (energia por franja x tarifa TMT/TMTB + reduccion de la
     potencia facturada = suma de maximos por franja) y evita compra (energia por
     franja x tarifa de compra + peaje de transmision + potencia coincidente).
     Resultado: impacto neto en el margen de CNFL (sin CAPEX).
E2 - CNFL instala solar (y opcionalmente BESS) en el cliente, igual que la
     microrred del estudio: el cliente sigue pagando su tarifa; el beneficio de CNFL
     es la compra evitada de TODA la generacion. VAN, TIR, RBC y payback con las
     formulas de financial_analysis.py (escenario CNFL 100% CAPEX, valor_salvamento).

Fuentes de parametros (tabla parametros_estudio_financiero):
  - config.py del estudio: tarifas de compra CON CVG (22/04/2026), peaje, tasas,
    CAPEX/OPEX, vida util, reinversion, IPC, degradacion, tipo de cambio.
  - Franjas: SLICE_PUNTA/VALLE/NOCHE del estudio (indice = BLOQUE_15M, etiqueta de
    fin de intervalo). Validado contra facturacion oficial junio 2026.
  - Tarifas de venta TMT/TMTB: oro_margen_comercializacion (bundle tmt_tmtb_oro),
    con factor de calibracion de energia contra la facturacion oficial junio 2026.

Supuestos (no cubiertos por la base de conocimiento):
  - Excedente solar inyectado por el cliente (E1): medicion neta (parametro
    EXCEDENTE_E1_MEDICION_NETA): CNFL evita su compra al inyectarse y pierde su venta
    cuando el cliente consume el credito (tarifa media de energia del cliente).
    En E2 el excedente es generacion de CNFL y evita compra en su franja.
  - E2 potencia: reduccion del maximo por franja de la curva neta del cliente (como el
    estudio); sensibilidad OPTIMISTA con potencia coincidente (E2_VAN_COINCIDENTE): el
    pico agregado TMT/TMTB cae a mediodia (bloques 44-54), donde la solar es maxima.
  - BESS: despacho optimo por tarifa (bateria_despacho_optimo, pipeline de baterias);
    por cliente se elige el escenario con mayor VAN si es positivo (E2B = solar + BESS).
  - Potencia de compra evitada = reduccion en el bloque pico laboral del agregado
    TMT/TMTB por mes y franja (proxy del pico de compra de CNFL).
  - Potencia facturada del cliente: maximo Q3 por franja (x4 = kW); con solar se
    resta la generacion promedio del bloque.
  - Anualizacion: suma de los meses con datos x 12 / meses.
"""

OUT_CAT = spark.conf.get("project.output_catalog")
OUT_SCH = spark.conf.get("project.output_schema")


def T(nombre):
    return f"{OUT_CAT}.{OUT_SCH}.{nombre}"


# ---------------------------------------------------------------------------
# Parametros (config.py del estudio salvo indicacion)
# ---------------------------------------------------------------------------
IPC = 1.03
CRECIMIENTO_TARIFAS_IPC = [1.0207] + [IPC] * 24
CRECIMIENTO_INFLACION_IPC = [1.0207] + [IPC] * 24
DEGRADACION_SOLAR = [100, 99, 98.6, 98.2, 97.8, 97.4, 97, 96.6, 96.2, 95.8, 95.4, 95,
                     94.6, 94.2, 93.8, 93.4, 93, 92.6, 92.2, 91.8]
DEGRADACION_BATERIA = [100.00, 94.33, 88.56, 87.42, 86.29, 85.17, 84.07, 82.99, 81.92,
                       80.86, 79.81, 78.78, 77.77, 76.76, 75.77]

PARAMS = [
    # (PARAMETRO, VALOR, UNIDAD, FUENTE)
    ("TASA_COSTO_CAPITAL", 0.0897, "tasa anual", "config.py"),
    ("CANTIDAD_ANIOS_ANALISIS", 10, "anios", "config.py (METODO_VALOR_TERMINAL=valor_salvamento)"),
    ("DOLAR_A_COLONES", 470.14, "CRC/USD", "config.py"),
    ("COSTO_KW_SOLAR_USD", 1055.077217438840 * 1.10, "USD/kWp", "config.py (incluye x1.10)"),
    ("OPEX_SOLAR", 3.74 / 277.92, "fraccion CAPEX/anio", "config.py"),
    ("VIDA_UTIL_SOLAR", 20, "anios", "config.py"),
    ("ANIO_REINVERSION_SOLAR", 11, "anio", "config.py"),
    ("COSTO_REINVERSION_SOLAR", 0.1241, "fraccion CAPEX", "config.py"),
    ("CPKWH_BESS_USD", 195, "USD/kWh", "config.py (RANGO_CPKWH)"),
    ("OPEX_BESS", 0.021, "fraccion CAPEX/anio", "config.py"),
    ("VIDA_UTIL_BESS", 15, "anios", "config.py"),
    ("ANIO_REINVERSION_BESS", 11, "anio", "config.py"),
    ("COSTO_REINVERSION_BESS", 0.1241, "fraccion CAPEX", "config.py"),
    ("IPC_ANIO_1", 1.0207, "factor", "config.py (CRECIMIENTO_TARIFAS_IPC[0])"),
    ("IPC_ANIOS_SIGUIENTES", IPC, "factor", "config.py (IPC_CRECIMIENTO)"),
    ("PEAJE_TRANSMISION", 10.36, "CRC/kWh", "config.py (con CVG, 22/04/2026; sin CVG = 6.59)"),
    ("CALIBRACION_ENERGIA_TMT", round(1 / 0.986, 4), "factor", "facturacion oficial jun-2026 vs tarifas oro_margen"),
    ("CALIBRACION_ENERGIA_TMTB", round(1 / 0.897, 4), "factor", "facturacion oficial jun-2026 vs tarifas oro_margen"),
    # Excedentes E1 (cliente duenio): medicion neta = el kWh inyectado se acredita y el
    # cliente lo consume despues (CNFL deja de venderlo a su tarifa media de energia),
    # mientras CNFL evita comprarlo en la franja en que se inyecta y cobra la tarifa de
    # acceso T-A al retiro (consolidado_tarifas_28102026.pdf). Supuesto: todo el credito
    # se retira dentro del periodo anual (el remanente al 31-dic se pierde).
    ("EXCEDENTE_E1_MEDICION_NETA", 1, "1 = medicion neta; 0 = sin compensacion",
     "consolidado_tarifas_28102026.pdf: Tarifa Acceso T-A, medicion neta sencilla"),
    # T-A: el productor-consumidor paga por cada kWh que retira de la energia depositada
    ("TARIFA_ACCESO_T_A", 25.67, "CRC/kWh retirado del deposito", "consolidado_tarifas_28102026.pdf (rige 01-01-2026)"),
]
P = {p[0]: float(p[1]) for p in PARAMS}

# Tarifas por franja (CRC/kWh y CRC/kW-mes)
# Compra CON CVG (config.py, bloque "Tarifas con CVG", revisadas 22/04/2026), coherente con
# las tarifas de venta que incluyen CVG (consolidado_tarifas_28102026.pdf).
# Sin CVG (bloque activo del estudio): 39.95/32.74/27.79, 2119.17/2119.17/0, peaje 6.59.
COMPRA = {
    "PUNTA": (36.85, 1954.51), "VALLE": (30.20, 1954.51), "NOCHE": (25.63, 0.0)}
VENTA = {  # consolidado_tarifas_28102026.pdf (rige 01-01-2026, incluyen CVG): precios MAXIMOS de la banda
    # (coinciden con oro_margen_comercializacion). Minimos T-MT: 47.84/23.91/17.22 y
    # 8387.95/5968.31/3788.77; T-MTb: 90.75/31.17/20.02 y 2665.35/1860.67/1192.34.
    ("TMT", "PUNTA"): (50.14, 8791.48), ("TMT", "VALLE"): (25.06, 6255.43),
    ("TMT", "NOCHE"): (18.05, 3971.05), ("TMTB", "PUNTA"): (95.12, 2793.58),
    ("TMTB", "VALLE"): (32.67, 1950.19), ("TMTB", "NOCHE"): (20.99, 1249.71)}
CALIB = {"TMT": P["CALIBRACION_ENERGIA_TMT"], "TMTB": P["CALIBRACION_ENERGIA_TMTB"]}


def franja_col(bloque):
    """SLICE_PUNTA = 41-50 y 71-80; SLICE_VALLE = 25-40 y 51-70; resto NOCHE."""
    return (
        F.when(bloque.between(41, 50) | bloque.between(71, 80), F.lit("PUNTA"))
        .when(bloque.between(25, 40) | bloque.between(51, 70), F.lit("VALLE"))
        .otherwise(F.lit("NOCHE"))
    )


# ---------------------------------------------------------------------------
# Formulas financieras (financial_analysis.py)
# ---------------------------------------------------------------------------
N_ANIOS = int(P["CANTIDAD_ANIOS_ANALISIS"])
R = P["TASA_COSTO_CAPITAL"]
ANIOS = np.arange(1, N_ANIOS + 1)
DESC = (1 + R) ** ANIOS
F_TARIFA = np.cumprod(CRECIMIENTO_TARIFAS_IPC[:N_ANIOS])      # precio anio y
F_INFL = np.cumprod(CRECIMIENTO_INFLACION_IPC[:N_ANIOS])       # opex anio y
DEG_SOLAR = np.array(DEGRADACION_SOLAR[:N_ANIOS]) / 100.0       # anio 1 = indice 0
DEG_BESS = np.array(DEGRADACION_BATERIA[:N_ANIOS]) / 100.0


def valor_salvamento(capex, vida_util, anio_reinv, costo_reinv):
    """calcular_valor_salvamento: activo + reinversion (anio_reinv >= horizonte)."""
    vs = 0.0
    if N_ANIOS < vida_util:
        vs += capex * (vida_util - N_ANIOS) / vida_util
    else:
        return vs
    monto = capex * costo_reinv
    if N_ANIOS <= anio_reinv:
        vs += monto * (anio_reinv - N_ANIOS) / anio_reinv
    return vs


def tir(flujos):
    """TIR por biseccion (flujos[0] = -CAPEX). None si no hay cambio de signo."""
    def vpn(t):
        return sum(f / (1 + t) ** i for i, f in enumerate(flujos))
    lo, hi = -0.99, 10.0
    f_lo, f_hi = vpn(lo), vpn(hi)
    if np.isnan(f_lo) or f_lo * f_hi > 0:
        return None
    for _ in range(200):
        mid = (lo + hi) / 2
        f_mid = vpn(mid)
        if f_lo * f_mid <= 0:
            hi = mid
        else:
            lo, f_lo = mid, f_mid
    return (lo + hi) / 2


def payback(flujos):
    """calculate_payback_period: FNE descontado acumulado con interpolacion lineal."""
    desc = np.array(flujos) / (1 + R) ** np.arange(0, len(flujos))
    acum = np.cumsum(desc)
    idx = np.where(acum >= 0)[0]
    if len(idx) == 0 or idx[0] == 0:
        return None
    k = idx[0]
    prev, cur = acum[k - 1], acum[k]
    return float((k - 1) + (-prev / (cur - prev))) if cur != prev else float(k)


def evaluar(ahorro_anual_base, capex, opex_frac, deg, vida_util, anio_reinv, costo_reinv):
    """Escenario CNFL (100% CAPEX): FNE = ahorro - O&M (+ valor de salvamento)."""
    ahorro = ahorro_anual_base * F_TARIFA * deg
    opex = opex_frac * capex * F_INFL
    opex[vida_util:] = 0
    vs = valor_salvamento(capex, vida_util, anio_reinv, costo_reinv)
    terminal = np.zeros(N_ANIOS)
    terminal[-1] = vs
    fne = ahorro - opex + terminal
    flujos = [-capex] + list(fne)
    van = float((fne / DESC).sum() - capex)
    vp_benef = float(((ahorro + terminal) / DESC).sum())
    vp_cost = float((opex / DESC).sum() + capex)
    rbc = vp_benef / vp_cost if capex > 0 else None
    t = tir(flujos) if capex > 0 else None
    return {
        "VAN": van, "TIR": None if t is None else t * 100, "RBC": rbc,
        "PAYBACK": payback(flujos) if capex > 0 else None,
        "VALOR_SALVAMENTO": vs, "FLUJOS": flujos, "VP_BENEF": vp_benef, "VP_COST": vp_cost,
    }


# ---------------------------------------------------------------------------
# 1. Parametros y tarifas
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name="parametros_estudio_financiero",
    comment="Parametros del estudio financiero solar (config.py) y calibraciones, con su fuente.",
)
def parametros_estudio_financiero():
    return spark.createDataFrame(
        [(a, float(b), c, d) for a, b, c, d in PARAMS],
        "PARAMETRO string, VALOR double, UNIDAD string, FUENTE string",
    )


@dp.materialized_view(
    name="tarifas_franja_estudio",
    comment=(
        "Tarifas por PLAN y FRANJA: venta TMT/TMTB (oro_margen, energia calibrada con "
        "facturacion jun-2026), compra sin CVG y peaje (config.py). CRC/kWh y CRC/kW-mes."
    ),
)
def tarifas_franja_estudio():
    return _tarifas_df()


def _tarifas_df():
    filas = []
    for (plan, fr), (te, tp) in VENTA.items():
        tc, tpc = COMPRA[fr]
        filas.append((plan, fr, te, CALIB[plan], te * CALIB[plan], tp, tc, tpc, P["PEAJE_TRANSMISION"]))
    return spark.createDataFrame(
        filas,
        "PLAN string, FRANJA string, TARIFA_VENTA_ENERGIA double, CALIBRACION_ENERGIA double, "
        "TARIFA_VENTA_ENERGIA_CAL double, TARIFA_VENTA_POTENCIA double, "
        "TARIFA_COMPRA_ENERGIA double, TARIFA_COMPRA_POTENCIA double, PEAJE_TRANSMISION double",
    )


# ---------------------------------------------------------------------------
# 2. Valoracion por cliente, mes y franja (E1 y E2)
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name="financiero_cliente_franja_mes",
    comment=(
        "Energia (kWh medidos) y potencia (kW) por NISE x mes x franja, ingreso de venta "
        "perdido (E1) y compra evitada (E1 autoconsumo; E2 generacion total), en CRC."
    ),
)
def financiero_cliente_franja_mes():
    return _franja_df()


# Las vistas que hacen calculos en pandas (acciones eager) recalculan desde las tablas
# fuente de otros pipelines: en una ejecucion de validacion las vistas de este mismo
# pipeline todavia no tienen datos.
def _franja_df():
    si = (
        spark.read.table(T("impacto_solar_q3_intervalo"))
        .withColumn("FRANJA", franja_col(F.col("BLOQUE_15M")))
    )
    d = F.col("DIAS_CON_DATOS")
    energia = si.groupBy("NISE", "PLAN", "PERIODO_MES", "FRANJA").agg(
        F.sum(F.col("DEMANDA_PROM_KWH") * d).alias("KWH_BASE"),
        F.sum(F.col("AUTOCONSUMO_SOLAR_KWH") * d).alias("KWH_AUTOCONSUMO"),
        F.sum(F.col("GENERACION_SOLAR_SIMULADA_KWH") * d).alias("KWH_GENERACION"),
        F.sum(F.col("EXCEDENTE_SOLAR_KWH") * d).alias("KWH_EXCEDENTE"),
    )
    # Potencia facturable del cliente (Q3 para picos), kW = kWh/15min x 4. Punta solo en
    # dias laborales (consolidado de tarifas: la demanda punta excluye sabados y domingos).
    potencia = (
        si.filter(~((F.col("FRANJA") == "PUNTA") & (F.col("TIPO_DIA") != "laboral")))
        .groupBy("NISE", "PLAN", "PERIODO_MES", "FRANJA")
        .agg(
            F.max(F.col("DEMANDA_Q3_KWH") * 4).alias("KW_BASE"),
            F.max(F.greatest(F.col("DEMANDA_Q3_KWH") - F.col("GENERACION_SOLAR_SIMULADA_KWH"), F.lit(0.0)) * 4)
            .alias("KW_CON_SOLAR"),
        )
    )
    energia = energia.join(potencia, ["NISE", "PLAN", "PERIODO_MES", "FRANJA"], "left").fillna(
        0.0, subset=["KW_BASE", "KW_CON_SOLAR"])

    # Bloque pico laboral del agregado TMT/TMTB por mes y franja (proxy pico de compra)
    lab = si.filter(F.col("TIPO_DIA") == "laboral")
    agregado = lab.groupBy("PERIODO_MES", "FRANJA", "BLOQUE_15M").agg(
        F.sum("DEMANDA_PROM_KWH").alias("AGREGADO"))
    w = Window.partitionBy("PERIODO_MES", "FRANJA").orderBy(F.col("AGREGADO").desc())
    pico = agregado.withColumn("rn", F.row_number().over(w)).filter("rn = 1").select(
        "PERIODO_MES", "FRANJA", F.col("BLOQUE_15M").alias("BLOQUE_PICO"))
    coinc = (
        lab.join(pico, ["PERIODO_MES", "FRANJA"])
        .filter(F.col("BLOQUE_15M") == F.col("BLOQUE_PICO"))
        .groupBy("NISE", "PERIODO_MES", "FRANJA")
        .agg(
            F.max(F.col("AUTOCONSUMO_SOLAR_KWH") * 4).alias("KW_COINC_AUTOCONSUMO"),
            F.max(F.col("GENERACION_SOLAR_SIMULADA_KWH") * 4).alias("KW_COINC_GENERACION"),
            F.max("BLOQUE_PICO").alias("BLOQUE_PICO"),
        )
    )

    tar = _tarifas_df()
    base = (
        energia.join(coinc, ["NISE", "PERIODO_MES", "FRANJA"], "left")
        .join(tar, ["PLAN", "FRANJA"], "left")
        .fillna(0.0, subset=["KW_COINC_AUTOCONSUMO", "KW_COINC_GENERACION"])
    )
    te, tp = F.col("TARIFA_VENTA_ENERGIA_CAL"), F.col("TARIFA_VENTA_POTENCIA")
    tc, tpc, peaje = F.col("TARIFA_COMPRA_ENERGIA"), F.col("TARIFA_COMPRA_POTENCIA"), F.col("PEAJE_TRANSMISION")
    kw_red = F.greatest(F.col("KW_BASE") - F.col("KW_CON_SOLAR"), F.lit(0.0))
    return base.select(
        "NISE", "PLAN", "PERIODO_MES", "FRANJA", "BLOQUE_PICO",
        "KWH_BASE", "KWH_AUTOCONSUMO", "KWH_GENERACION", "KWH_EXCEDENTE",
        "KW_BASE", "KW_CON_SOLAR", kw_red.alias("KW_REDUCCION_CLIENTE"),
        "KW_COINC_AUTOCONSUMO", "KW_COINC_GENERACION",
        (F.col("KWH_BASE") * te + F.col("KW_BASE") * tp).alias("INGRESO_BASE_CRC"),
        (F.col("KWH_BASE") * te).alias("INGRESO_BASE_ENERGIA_CRC"),
        (F.col("KWH_BASE") * (tc + peaje)).alias("COSTO_COMPRA_ENERGIA_BASE_CRC"),
        # Excedente inyectado (E1): compra evitada en la franja de inyeccion
        (F.col("KWH_EXCEDENTE") * (tc + peaje)).alias("COMPRA_EVITADA_E1_EXCEDENTE_CRC"),
        # E2 potencia segun el estudio (reduccion del maximo por franja de la curva neta)
        (kw_red * tpc).alias("COMPRA_EVITADA_E2_POTENCIA_ESTUDIO_CRC"),
        (F.col("KWH_AUTOCONSUMO") * te).alias("INGRESO_PERDIDO_ENERGIA_CRC"),
        (kw_red * tp).alias("INGRESO_PERDIDO_POTENCIA_CRC"),
        (F.col("KWH_AUTOCONSUMO") * (tc + peaje)).alias("COMPRA_EVITADA_E1_ENERGIA_CRC"),
        (F.col("KW_COINC_AUTOCONSUMO") * tpc).alias("COMPRA_EVITADA_E1_POTENCIA_CRC"),
        (F.col("KWH_GENERACION") * (tc + peaje)).alias("COMPRA_EVITADA_E2_ENERGIA_CRC"),
        (F.col("KW_COINC_GENERACION") * tpc).alias("COMPRA_EVITADA_E2_POTENCIA_CRC"),
    )


# ---------------------------------------------------------------------------
# 3. Indicadores por cliente: E1 (margen) y E2 (VAN, TIR, RBC, payback)
# ---------------------------------------------------------------------------
E_COLS = ["KWH_BASE", "KWH_AUTOCONSUMO", "KWH_GENERACION", "INGRESO_BASE_CRC",
          "INGRESO_PERDIDO_ENERGIA_CRC", "INGRESO_PERDIDO_POTENCIA_CRC",
          "COMPRA_EVITADA_E1_ENERGIA_CRC", "COMPRA_EVITADA_E1_POTENCIA_CRC",
          "COMPRA_EVITADA_E2_ENERGIA_CRC", "COMPRA_EVITADA_E2_POTENCIA_CRC",
          "KWH_EXCEDENTE", "INGRESO_BASE_ENERGIA_CRC", "COMPRA_EVITADA_E1_EXCEDENTE_CRC",
          "COMPRA_EVITADA_E2_POTENCIA_ESTUDIO_CRC"]


def _clientes_pdf():
    fm = _franja_df().toPandas()
    meses = max(int(fm["PERIODO_MES"].nunique()), 1)
    anual = fm.groupby(["NISE", "PLAN"])[E_COLS].sum() * (12.0 / meses)
    kwp = (
        spark.read.table(T("impacto_solar_q3_cliente_resumen"))
        .select("NISE", F.col("POTENCIA_SOLAR_PICO_CLIENTE_KW").alias("KWP"))
        .toPandas().set_index("NISE")
    )
    df = anual.reset_index().join(kwp, on="NISE", how="left")
    df["MESES_DATOS"] = meses
    # Excedente E1 con medicion neta: credito consumido despues a la tarifa media de energia
    tarifa_media = (df["INGRESO_BASE_ENERGIA_CRC"] / df["KWH_BASE"].where(df["KWH_BASE"] > 0)).fillna(0.0)
    df["TARIFA_MEDIA_ENERGIA_CRC_KWH"] = tarifa_media
    # Medicion neta (T-A): al retirar el credito el cliente deja de pagar su tarifa de energia
    # pero paga la tarifa de acceso por cada kWh retirado.
    df["INGRESO_TARIFA_ACCESO_CRC"] = df["KWH_EXCEDENTE"] * P["TARIFA_ACCESO_T_A"] * P["EXCEDENTE_E1_MEDICION_NETA"]
    df["INGRESO_PERDIDO_EXCEDENTE_CRC"] = (df["KWH_EXCEDENTE"] * tarifa_media * P["EXCEDENTE_E1_MEDICION_NETA"]
                                           - df["INGRESO_TARIFA_ACCESO_CRC"])
    df["INGRESO_PERDIDO_ANUAL_CRC"] = (df["INGRESO_PERDIDO_ENERGIA_CRC"] + df["INGRESO_PERDIDO_POTENCIA_CRC"]
                                       + df["INGRESO_PERDIDO_EXCEDENTE_CRC"])
    df["COMPRA_EVITADA_E1_ANUAL_CRC"] = (df["COMPRA_EVITADA_E1_ENERGIA_CRC"] + df["COMPRA_EVITADA_E1_POTENCIA_CRC"]
                                         + df["COMPRA_EVITADA_E1_EXCEDENTE_CRC"])
    df["E1_MARGEN_ANUAL_CRC"] = df["COMPRA_EVITADA_E1_ANUAL_CRC"] - df["INGRESO_PERDIDO_ANUAL_CRC"]
    df["E1_MARGEN_SIN_COMPENSACION_CRC"] = (df["COMPRA_EVITADA_E1_ANUAL_CRC"] - df["INGRESO_PERDIDO_ENERGIA_CRC"]
                                            - df["INGRESO_PERDIDO_POTENCIA_CRC"])
    # E2: energia de toda la generacion (incluye excedente) + potencia segun el estudio
    df["COMPRA_EVITADA_E2_ANUAL_CRC"] = df["COMPRA_EVITADA_E2_ENERGIA_CRC"] + df["COMPRA_EVITADA_E2_POTENCIA_ESTUDIO_CRC"]
    # Sensibilidad optimista: potencia coincidente con el pico agregado TMT/TMTB (cae a mediodia)
    df["COMPRA_EVITADA_E2_COINCIDENTE_CRC"] = df["COMPRA_EVITADA_E2_ENERGIA_CRC"] + df["COMPRA_EVITADA_E2_POTENCIA_CRC"]
    df["CAPEX_SOLAR_CRC"] = df["KWP"].fillna(0) * P["COSTO_KW_SOLAR_USD"] * P["DOLAR_A_COLONES"]
    return df


def _bess_pdf():
    """BESS con despacho optimo (bateria_despacho_optimo): indicadores por NISE x escenario."""
    b = spark.read.table(T("bateria_despacho_optimo")).filter("ESTADO_LP = 'OPTIMO'").toPandas()
    if b.empty:
        return pd.DataFrame(columns=["NISE", "ESCENARIO_BATERIA"])
    meses = max(int(b["PERIODO_MES"].nunique()), 1)
    g = b.groupby(["NISE", "ESCENARIO_BATERIA"]).agg(
        CAPACIDAD_KWH=("CAPACIDAD_KWH", "max"), POTENCIA_BESS_KW=("POTENCIA_BESS_KW", "max"),
        AHORRO_ENERGIA_CRC=("AHORRO_ENERGIA_CRC", "sum"), AHORRO_POTENCIA_CRC=("AHORRO_POTENCIA_CRC", "sum"),
        KWH_CARGA=("KWH_CARGA", "sum"), KWH_CARGA_RED=("KWH_CARGA_RED", "sum"),
        KWH_DESCARGA=("KWH_DESCARGA", "sum"), KWH_DESCARGA_PUNTA=("KWH_DESCARGA_PUNTA", "sum"),
    ).reset_index()
    for c in ["AHORRO_ENERGIA_CRC", "AHORRO_POTENCIA_CRC", "KWH_CARGA", "KWH_CARGA_RED", "KWH_DESCARGA",
              "KWH_DESCARGA_PUNTA"]:
        g[c] = g[c] * 12.0 / meses
    res = []
    for _, r in g.iterrows():
        capex = float(r["CAPACIDAD_KWH"]) * P["CPKWH_BESS_USD"] * P["DOLAR_A_COLONES"]
        e = evaluar(float(r["AHORRO_ENERGIA_CRC"] + r["AHORRO_POTENCIA_CRC"]), capex, P["OPEX_BESS"], DEG_BESS,
                    int(P["VIDA_UTIL_BESS"]), P["ANIO_REINVERSION_BESS"], P["COSTO_REINVERSION_BESS"])
        res.append({"CAPEX_CRC": capex, "VAN_CRC": e["VAN"], "TIR_PCT": e["TIR"], "RBC": e["RBC"],
                    "PAYBACK_ANIOS": e["PAYBACK"], "VP_BENEF": e["VP_BENEF"], "VP_COST": e["VP_COST"],
                    "FLUJOS": e["FLUJOS"]})
    return pd.concat([g, pd.DataFrame(res, index=g.index)], axis=1)


def _indicadores(df):
    bess = _bess_pdf()
    mejor = {}
    if len(bess):
        for nise, h in bess.groupby("NISE"):
            top = h.sort_values("VAN_CRC", ascending=False).iloc[0]
            if top["VAN_CRC"] > 0:
                mejor[nise] = top
    filas, flujos_e1, flujos_e2, flujos_e2b = [], [], [], []
    cero = np.zeros(N_ANIOS + 1)
    for _, r in df.iterrows():
        e1 = r["E1_MARGEN_ANUAL_CRC"] * F_TARIFA * DEG_SOLAR
        e1_sc = r["E1_MARGEN_SIN_COMPENSACION_CRC"] * F_TARIFA * DEG_SOLAR
        args = (r["CAPEX_SOLAR_CRC"], P["OPEX_SOLAR"], DEG_SOLAR, int(P["VIDA_UTIL_SOLAR"]),
                P["ANIO_REINVERSION_SOLAR"], P["COSTO_REINVERSION_SOLAR"])
        e2 = evaluar(r["COMPRA_EVITADA_E2_ANUAL_CRC"], *args)
        e2c = evaluar(r["COMPRA_EVITADA_E2_COINCIDENTE_CRC"], *args)
        bm = mejor.get(r["NISE"])
        fb = np.array(bm["FLUJOS"]) if bm is not None else cero
        f2b = np.array(e2["FLUJOS"]) + fb
        capex_b = float(bm["CAPEX_CRC"]) if bm is not None else 0.0
        vpb_b = e2["VP_BENEF"] + (float(bm["VP_BENEF"]) if bm is not None else 0.0)
        vpc_b = e2["VP_COST"] + (float(bm["VP_COST"]) if bm is not None else 0.0)
        t_b = tir(list(f2b)) if (r["CAPEX_SOLAR_CRC"] + capex_b) > 0 else None
        filas.append((
            float((e1 / DESC).sum()), float((e1_sc / DESC).sum()),
            e2["VAN"], e2["TIR"], e2["RBC"], e2["PAYBACK"], e2["VALOR_SALVAMENTO"], e2["VP_BENEF"], e2["VP_COST"],
            e2c["VAN"], e2c["RBC"],
            None if bm is None else bm["ESCENARIO_BATERIA"], capex_b,
            None if bm is None else float(bm["VAN_CRC"]), None if bm is None else bm["RBC"],
            float(e2["VAN"] + (float(bm["VAN_CRC"]) if bm is not None else 0.0)),
            float(r["CAPEX_SOLAR_CRC"] + capex_b), None if t_b is None else t_b * 100,
            vpb_b / vpc_b if vpc_b > 0 else None, vpb_b, vpc_b))
        flujos_e1.append([0.0] + list(e1))
        flujos_e2.append(e2["FLUJOS"])
        flujos_e2b.append(list(f2b))
    ind = pd.DataFrame(filas, columns=[
        "E1_VAN_CRC", "E1_VAN_SIN_COMPENSACION_CRC",
        "E2_VAN_CRC", "E2_TIR_PCT", "E2_RBC", "E2_PAYBACK_ANIOS", "E2_VALOR_SALVAMENTO_CRC",
        "E2_VP_BENEFICIOS_CRC", "E2_VP_COSTOS_CRC", "E2_VAN_COINCIDENTE_CRC", "E2_RBC_COINCIDENTE",
        "BESS_ESCENARIO_OPTIMO", "BESS_CAPEX_CRC", "BESS_VAN_CRC", "BESS_RBC",
        "E2B_VAN_CRC", "E2B_CAPEX_CRC", "E2B_TIR_PCT", "E2B_RBC", "E2B_VP_BENEFICIOS_CRC", "E2B_VP_COSTOS_CRC",
    ], index=df.index)
    out = pd.concat([df, ind], axis=1)
    out["VALOR_ACTUAR_PRIMERO_CRC"] = out["E2B_VAN_CRC"] - out["E1_VAN_CRC"]
    return out, np.array(flujos_e1), np.array(flujos_e2), np.array(flujos_e2b)


@dp.materialized_view(
    name="financiero_cliente_escenarios",
    comment=(
        "Por NISE: E1 (cliente instala; impacto anual y VAN 10 anios en el margen de CNFL) y "
        "E2 (CNFL instala solar; CAPEX, VAN, TIR, RBC, payback). VALOR_ACTUAR_PRIMERO = "
        "E2_VAN - E1_VAN. PRIORIDAD por E2_VAN. Montos en CRC, anualizados."
    ),
)
def financiero_cliente_escenarios():
    df, _, _, _ = _indicadores(_clientes_pdf())
    df = df.sort_values("E2B_VAN_CRC", ascending=False).reset_index(drop=True)
    df["PRIORIDAD"] = np.arange(1, len(df) + 1)
    df["E2_VIABLE"] = (df["E2_RBC"] >= 1.0) & (df["E2_VAN_CRC"] > 0)
    df["E2B_VIABLE"] = (df["E2B_RBC"] >= 1.0) & (df["E2B_VAN_CRC"] > 0)
    texto = ("NISE", "PLAN", "BESS_ESCENARIO_OPTIMO")
    enteros = ("PRIORIDAD", "MESES_DATOS")
    booleanos = ("E2_VIABLE", "E2B_VIABLE")
    cols = ["PRIORIDAD", "NISE", "PLAN", "KWP", "MESES_DATOS", "KWH_BASE", "KWH_AUTOCONSUMO", "KWH_GENERACION",
            "KWH_EXCEDENTE", "INGRESO_BASE_CRC", "TARIFA_MEDIA_ENERGIA_CRC_KWH",
            "INGRESO_PERDIDO_ENERGIA_CRC", "INGRESO_PERDIDO_POTENCIA_CRC", "INGRESO_PERDIDO_EXCEDENTE_CRC",
            "INGRESO_PERDIDO_ANUAL_CRC", "COMPRA_EVITADA_E1_EXCEDENTE_CRC", "COMPRA_EVITADA_E1_ANUAL_CRC",
            "E1_MARGEN_ANUAL_CRC", "E1_MARGEN_SIN_COMPENSACION_CRC", "E1_VAN_CRC", "E1_VAN_SIN_COMPENSACION_CRC",
            "CAPEX_SOLAR_CRC", "COMPRA_EVITADA_E2_ENERGIA_CRC", "COMPRA_EVITADA_E2_POTENCIA_ESTUDIO_CRC",
            "COMPRA_EVITADA_E2_POTENCIA_CRC", "COMPRA_EVITADA_E2_ANUAL_CRC", "E2_VAN_CRC", "E2_TIR_PCT", "E2_RBC",
            "E2_PAYBACK_ANIOS", "E2_VALOR_SALVAMENTO_CRC", "E2_VIABLE", "E2_VAN_COINCIDENTE_CRC",
            "E2_RBC_COINCIDENTE", "BESS_ESCENARIO_OPTIMO", "BESS_CAPEX_CRC", "BESS_VAN_CRC", "BESS_RBC",
            "E2B_CAPEX_CRC", "E2B_VAN_CRC", "E2B_TIR_PCT", "E2B_RBC", "E2B_VIABLE", "VALOR_ACTUAR_PRIMERO_CRC"]
    df = df[cols].astype({"PRIORIDAD": "int64", "MESES_DATOS": "int64"})
    num = [c for c in cols if c not in texto + enteros + booleanos]
    df[num] = df[num].astype("float64")
    df = df.astype(object).where(pd.notnull(df), None)
    schema = ", ".join(
        f"{c} {'bigint' if c in enteros else 'string' if c in texto else 'boolean' if c in booleanos else 'double'}"
        for c in cols)
    return spark.createDataFrame(df.values.tolist(), schema)


# ---------------------------------------------------------------------------
# 4. Matriz de adopcion: penetracion solar x captura de CNFL
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name="financiero_matriz_adopcion",
    comment=(
        "Si una PENETRACION de clientes TMT/TMTB adopta solar y CNFL capta la fraccion CAPTURA "
        "(E2) mientras el resto se autoinstala (E1): VAN total 10 anios para CNFL, partes E1/E2, "
        "CAPEX, TIR y RBC del portafolio E2, y ganancia vs no actuar. ESTRATEGIA PROMEDIO "
        "(clientes promedio) o PRIORIZADA (CNFL capta primero los de mayor VAN E2)."
    ),
)
def financiero_matriz_adopcion():
    df, fe1, fe2, fe2b = _indicadores(_clientes_pdf())
    n = len(df)
    filas = []
    # (estrategia, columnas E2, flujos E2, priorizar)
    variantes = [
        ("PROMEDIO", ("E2_VAN_CRC", "CAPEX_SOLAR_CRC", "E2_VP_BENEFICIOS_CRC", "E2_VP_COSTOS_CRC"), fe2, False),
        ("PRIORIZADA", ("E2_VAN_CRC", "CAPEX_SOLAR_CRC", "E2_VP_BENEFICIOS_CRC", "E2_VP_COSTOS_CRC"), fe2, True),
        ("PRIORIZADA_SOLAR_BESS", ("E2B_VAN_CRC", "E2B_CAPEX_CRC", "E2B_VP_BENEFICIOS_CRC", "E2B_VP_COSTOS_CRC"),
         fe2b, True),
    ]
    for p in [0.10, 0.25, 0.50, 0.75, 1.00]:
        n_tot = int(round(p * n))
        for c in [round(x * 0.05, 2) for x in range(21)]:
            n_cnfl = int(round(c * n_tot))
            n_e1 = n_tot - n_cnfl
            for estrategia, (c_van, c_capex, c_vpb, c_vpc), fl2, priorizar in variantes:
                if not priorizar:
                    f2 = fl2.mean(axis=0) * n_cnfl
                    van_e2 = df[c_van].mean() * n_cnfl
                    van_e1 = df["E1_VAN_CRC"].mean() * n_e1
                    capex = df[c_capex].mean() * n_cnfl
                    vpb = df[c_vpb].mean() * n_cnfl
                    vpc = df[c_vpc].mean() * n_cnfl
                else:
                    orden = np.argsort(-df[c_van].values)
                    sel, resto = orden[:n_cnfl], orden[n_cnfl:]
                    f2 = fl2[sel].sum(axis=0) if n_cnfl else np.zeros(N_ANIOS + 1)
                    van_e2 = float(df[c_van].values[sel].sum())
                    van_e1 = float(df["E1_VAN_CRC"].values[resto].mean() * n_e1) if len(resto) else 0.0
                    capex = float(df[c_capex].values[sel].sum())
                    vpb = float(df[c_vpb].values[sel].sum())
                    vpc = float(df[c_vpc].values[sel].sum())
                van_no_actuar = float(df["E1_VAN_CRC"].mean() * n_tot)
                t2 = tir(list(f2)) if n_cnfl else None
                # RBC del estudio: VP(beneficios + valor terminal) / (CAPEX + VP(O&M))
                rbc2 = vpb / vpc if n_cnfl and vpc > 0 else None
                van_total = van_e1 + van_e2
                def _f(x):
                    return None if x is None else float(x)
                filas.append((estrategia, float(p), float(c), int(n_tot), int(n_cnfl), int(n_e1),
                              _f(van_e1), _f(van_e2), _f(van_total), _f(van_no_actuar),
                              _f(van_total - van_no_actuar), _f(capex),
                              None if t2 is None else float(t2 * 100), _f(rbc2), bool(van_total >= 0)))
    return spark.createDataFrame(
        filas,
        "ESTRATEGIA string, PENETRACION double, CAPTURA_CNFL double, CLIENTES_ADOPTAN bigint, "
        "CLIENTES_CNFL bigint, CLIENTES_AUTOINSTALAN bigint, VAN_E1_CRC double, VAN_E2_CRC double, "
        "VAN_TOTAL_CRC double, VAN_NO_ACTUAR_CRC double, GANANCIA_VS_NO_ACTUAR_CRC double, "
        "CAPEX_E2_CRC double, TIR_E2_PCT double, RBC_E2 double, BALANCE_NO_NEGATIVO boolean",
    )


# ---------------------------------------------------------------------------
# 5. BESS marginal sobre E2 (CNFL duena de solar + BESS)
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name="financiero_bess_marginal",
    comment=(
        "BESS marginal en E2 con despacho optimo por tarifa (bateria_despacho_optimo, estrategia "
        "PUNTA_OPTIMO del estudio), por escenario de bateria (portafolio): ahorro de energia y "
        "potencia, CAPEX = kWh x CPKWH x TC, VAN, TIR, RBC del estudio y clientes con VAN > 0."
    ),
)
def financiero_bess_marginal():
    b = _bess_pdf()
    filas = []
    for esc, h in b.groupby("ESCENARIO_BATERIA"):
        flujos = np.sum(np.array(list(h["FLUJOS"])), axis=0)
        t = tir(list(flujos))
        vpc = float(h["VP_COST"].sum())
        filas.append((
            esc, int(len(h)), int((h["VAN_CRC"] > 0).sum()), float(h["CAPACIDAD_KWH"].sum()),
            float(h["POTENCIA_BESS_KW"].sum()), float(h["KWH_CARGA"].sum()), float(h["KWH_CARGA_RED"].sum()),
            float(h["KWH_DESCARGA"].sum()), float(h["KWH_DESCARGA_PUNTA"].sum()),
            float(h["AHORRO_ENERGIA_CRC"].sum()), float(h["AHORRO_POTENCIA_CRC"].sum()),
            float(h["CAPEX_CRC"].sum()), float(h["VAN_CRC"].sum()), None if t is None else float(t * 100),
            float(h["VP_BENEF"].sum()) / vpc if vpc > 0 else None,
            float(h.loc[h["VAN_CRC"] > 0, "VAN_CRC"].sum()), float(h.loc[h["VAN_CRC"] > 0, "CAPEX_CRC"].sum())))
    return spark.createDataFrame(
        filas,
        "ESCENARIO_BATERIA string, CLIENTES bigint, CLIENTES_VAN_POSITIVO bigint, CAPACIDAD_KWH double, "
        "POTENCIA_KW double, KWH_CARGA_ANUAL double, KWH_CARGA_RED_ANUAL double, KWH_DESCARGA_ANUAL double, "
        "KWH_DESCARGA_PUNTA_ANUAL double, AHORRO_ENERGIA_ANUAL_CRC double, AHORRO_POTENCIA_ANUAL_CRC double, "
        "CAPEX_CRC double, VAN_CRC double, TIR_PCT double, RBC double, VAN_SOLO_POSITIVOS_CRC double, "
        "CAPEX_SOLO_POSITIVOS_CRC double",
    )


@dp.materialized_view(
    name="financiero_bess_autoconsumo",
    comment=(
        "Comparacion: BESS con estrategia AUTOCONSUMO_MAX (impacto_bateria_q3, solo carga con "
        "excedente solar). BESS marginal en E2 por escenario de bateria (portafolio): arbitraje de energia "
        "(descarga - carga valoradas a tarifa de compra + peaje por franja) + potencia "
        "coincidente en punta; CAPEX = kWh x CPKWH x TC; VAN, TIR, RBC, payback del estudio."
    ),
)
def financiero_bess_autoconsumo():
    bi = (
        spark.read.table(T("impacto_bateria_q3_intervalo"))
        .withColumn("FRANJA", franja_col(F.col("BLOQUE_15M")))
    )
    tc = (F.when(F.col("FRANJA") == "PUNTA", F.lit(COMPRA["PUNTA"][0]))
          .when(F.col("FRANJA") == "VALLE", F.lit(COMPRA["VALLE"][0]))
          .otherwise(F.lit(COMPRA["NOCHE"][0])) + F.lit(P["PEAJE_TRANSMISION"]))
    d = F.col("DIAS_CON_DATOS")
    meses = max(bi.select("PERIODO_MES").distinct().count(), 1)
    energia = bi.groupBy("ESCENARIO_BATERIA").agg(
        F.sum((F.col("ENERGIA_DESCARGA_KWH") - F.col("ENERGIA_CARGA_KWH")) * tc * d).alias("ARBITRAJE_CRC"),
        F.sum(F.col("ENERGIA_DESCARGA_KWH") * d).alias("KWH_DESCARGA"),
        F.sum(F.col("ENERGIA_CARGA_KWH") * d).alias("KWH_CARGA"),
    )
    cap = (bi.groupBy("ESCENARIO_BATERIA", "NISE").agg(F.max("CAPACIDAD_KWH").alias("C"))
           .groupBy("ESCENARIO_BATERIA").agg(F.sum("C").alias("CAPACIDAD_KWH")))
    # Potencia coincidente: descarga en el bloque pico laboral de PUNTA (mismo pico que E2)
    pico = (_franja_df()
            .filter(F.col("FRANJA") == "PUNTA").select("PERIODO_MES", "BLOQUE_PICO").distinct())
    coinc = (bi.filter(F.col("TIPO_DIA") == "laboral").join(pico, "PERIODO_MES")
             .filter(F.col("BLOQUE_15M") == F.col("BLOQUE_PICO"))
             .groupBy("ESCENARIO_BATERIA")
             .agg(F.sum(F.col("ENERGIA_DESCARGA_KWH") * 4 * F.lit(COMPRA["PUNTA"][1])).alias("POTENCIA_CRC")))
    pdf = energia.join(cap, "ESCENARIO_BATERIA").join(coinc, "ESCENARIO_BATERIA", "left").toPandas().fillna(0.0)
    filas = []
    for _, r in pdf.iterrows():
        ahorro = (r["ARBITRAJE_CRC"] + r["POTENCIA_CRC"]) * 12.0 / meses
        capex = r["CAPACIDAD_KWH"] * P["CPKWH_BESS_USD"] * P["DOLAR_A_COLONES"]
        e = evaluar(ahorro, capex, P["OPEX_BESS"], DEG_BESS, int(P["VIDA_UTIL_BESS"]),
                    P["ANIO_REINVERSION_BESS"], P["COSTO_REINVERSION_BESS"])
        filas.append((r["ESCENARIO_BATERIA"], float(r["CAPACIDAD_KWH"]),
                      float(r["KWH_CARGA"] * 12.0 / meses), float(r["KWH_DESCARGA"] * 12.0 / meses),
                      float(r["ARBITRAJE_CRC"] * 12.0 / meses), float(r["POTENCIA_CRC"] * 12.0 / meses),
                      float(ahorro), float(capex), float(e["VAN"]),
                      None if e["TIR"] is None else float(e["TIR"]),
                      None if e["RBC"] is None else float(e["RBC"]),
                      None if e["PAYBACK"] is None else float(e["PAYBACK"])))
    return spark.createDataFrame(
        filas,
        "ESCENARIO_BATERIA string, CAPACIDAD_KWH double, KWH_CARGA_ANUAL double, "
        "KWH_DESCARGA_ANUAL double, ARBITRAJE_ANUAL_CRC double, POTENCIA_ANUAL_CRC double, "
        "AHORRO_ANUAL_CRC double, CAPEX_CRC double, VAN_CRC double, TIR_PCT double, "
        "RBC double, PAYBACK_ANIOS double",
    )


# ---------------------------------------------------------------------------
# 6. Calidad
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name="financiero_calidad",
    comment="Validaciones del modelo financiero (0 = OK).",
)
def financiero_calidad():
    fm = _franja_df()
    cm = spark.read.table(T("impacto_solar_q3_cliente_mes"))
    bo = spark.read.table(T("bateria_despacho_optimo"))
    # Tarifas del despacho (despacho_optimo_bateria.py) = tarifas del financiero
    esc0 = bo.agg(F.min("ESCENARIO_BATERIA")).first()[0]
    e_lp = float(bo.filter(F.col("ESCENARIO_BATERIA") == esc0)
                 .agg(F.sum("COSTO_ENERGIA_SIN_BESS_CRC")).first()[0] or 0.0)
    e_fin = float(fm.agg(F.sum(F.col("COSTO_COMPRA_ENERGIA_BASE_CRC") - F.col("COMPRA_EVITADA_E2_ENERGIA_CRC")))
                  .first()[0] or 0.0)
    a = fm.agg(F.sum("KWH_BASE").alias("F"), F.sum("KWH_AUTOCONSUMO").alias("FA")).first()
    b = cm.agg(F.sum("CONSUMO_BASE_KWH").alias("B"), F.sum("AUTOCONSUMO_SOLAR_KWH").alias("BA")).first()
    checks = [
        ("CONSERVACION_ENERGIA_BASE", f"kWh base franjas vs cliente_mes: {a['F']:.1f} vs {b['B']:.1f}",
         int(abs(a["F"] - b["B"]) > 1.0)),
        ("CONSERVACION_AUTOCONSUMO", f"kWh autoconsumo franjas vs cliente_mes: {a['FA']:.1f} vs {b['BA']:.1f}",
         int(abs(a["FA"] - b["BA"]) > 1.0)),
        ("AUTOCONSUMO_MAYOR_GENERACION", "KWH_AUTOCONSUMO > KWH_GENERACION",
         fm.filter(F.col("KWH_AUTOCONSUMO") > F.col("KWH_GENERACION") + 0.01).count()),
        ("KW_SOLAR_MAYOR_BASE", "KW_CON_SOLAR > KW_BASE",
         fm.filter(F.col("KW_CON_SOLAR") > F.col("KW_BASE") + 0.001).count()),
        ("INGRESO_PERDIDO_MAYOR_BASE", "ingreso perdido > ingreso base",
         fm.filter(F.col("INGRESO_PERDIDO_ENERGIA_CRC") + F.col("INGRESO_PERDIDO_POTENCIA_CRC")
                   > F.col("INGRESO_BASE_CRC") + 0.01).count()),
        ("TARIFA_FALTANTE", "filas sin tarifa",
         fm.filter(F.col("INGRESO_BASE_CRC").isNull()).count()),
        ("BESS_LP_NO_OPTIMO", "cliente-mes-escenario sin solucion optima del LP",
         bo.filter(F.col("ESTADO_LP") != "OPTIMO").count()),
        ("BESS_EFICIENCIA_CICLO", "descarga / carga > ef_carga x ef_descarga (0.9025)",
         bo.filter(F.col("EFICIENCIA_CICLO") > 0.9026).count()),
        ("BESS_AHORRO_NEGATIVO", "despacho optimo con costo mayor que sin BESS",
         bo.filter(F.col("AHORRO_TOTAL_CRC") < -1.0).count()),
        ("BESS_TARIFAS_DISTINTAS",
         f"costo energia neta sin BESS: despacho {e_lp:,.0f} vs financiero {e_fin:,.0f} CRC (tolerancia 0.1%)",
         int(abs(e_lp - e_fin) > 0.001 * max(abs(e_fin), 1.0))),
    ]
    return spark.createDataFrame(
        [(n, d, int(v)) for n, d, v in checks],
        "VALIDACION string, DESCRIPCION string, REGISTROS_AFECTADOS bigint",
    ).withColumn("FECHA_VALIDACION", F.current_timestamp())
