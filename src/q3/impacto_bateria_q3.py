import pandas as pd
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    DateType,
)

"""
Simulacion de impacto de baterias sobre el impacto solar ya calculado.

Para cada escenario de bateria, simula el almacenamiento de excedentes
solares y la descarga durante deficit, calculando:
  - Estado de carga (SOC) iterativo por intervalo
  - Energia cargada/descargada
  - Demanda neta con bateria
  - Excedente neto reducido
  - Energia adicional no vendida gracias a la bateria

Estrategia: AUTOCONSUMO_MAX
  - Si hay excedente solar: cargar bateria
  - Si hay deficit: descargar bateria
  - Sino: bateria inactiva

Unidades: kWh por intervalo de 15 minutos (ENERGIA, no DEMANDA).

Escalado (2026-11-20): la simulacion corre sobre el dia tipico PROMEDIO
(DEMANDA_PROM_KWH); cliente_mes multiplica por DIAS_CON_DATOS (dias con
lectura) para obtener energia medida del mes, sin extrapolar dias sin datos. DEMANDA_Q3_KWH se conserva solo como referencia de picos.

Convencion de columnas:
  - ENERGIA_BATERIA_KWH > 0: CARGA (energia tomada del excedente solar)
  - ENERGIA_BATERIA_KWH < 0: DESCARGA (energia entregada a la carga)
  - ENERGIA_CARGA_KWH: energia tomada del excedente (antes de eficiencia)
  - ENERGIA_DESCARGA_KWH: energia entregada a la carga (despues de eficiencia)
  - DEMANDA_NETA_CON_BATERIA_KWH: lo que la distribuidora vende al cliente
  - EXCEDENTE_NETO_KWH: excedente solar que va a la red despues de bateria
"""

OUTPUT_CATALOG = spark.conf.get("project.output_catalog")
OUTPUT_SCHEMA = spark.conf.get("project.output_schema")

SOLAR_INTERVALO_TABLE = f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.impacto_solar_q3_intervalo"
BATERIA_PARAM_TABLE = f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.parametros_bateria"


# Schema for applyInPandas output
BATTERY_INTERVALO_SCHEMA = StructType([
    StructField("ESCENARIO_SOLAR", StringType(), True),
    StructField("ESCENARIO_BATERIA", StringType(), True),
    StructField("NISE", StringType(), True),
    StructField("PERIODO_MES", DateType(), True),
    StructField("TIPO_DIA", StringType(), True),
    StructField("PLAN", StringType(), True),
    StructField("TIPO_CLIENTE_SOLAR", StringType(), True),
    StructField("ANIO", IntegerType(), True),
    StructField("MES", IntegerType(), True),
    StructField("BLOQUE_15M", IntegerType(), True),
    StructField("HORA", IntegerType(), True),
    StructField("MINUTO", IntegerType(), True),
    StructField("DEMANDA_Q3_KWH", DoubleType(), True),
    StructField("DEMANDA_PROM_KWH", DoubleType(), True),
    StructField("GENERACION_SOLAR_SIMULADA_KWH", DoubleType(), True),
    StructField("EXCEDENTE_SOLAR_KWH", DoubleType(), True),
    StructField("DEMANDA_REMANENTE_KWH", DoubleType(), True),
    StructField("CAPACIDAD_KWH", DoubleType(), True),
    StructField("POTENCIA_CARGA_15MIN", DoubleType(), True),
    StructField("POTENCIA_DESCARGA_15MIN", DoubleType(), True),
    StructField("EFICIENCIA_CARGA", DoubleType(), True),
    StructField("EFICIENCIA_DESCARGA", DoubleType(), True),
    StructField("SOC", DoubleType(), True),
    StructField("ENERGIA_BATERIA_KWH", DoubleType(), True),
    StructField("ENERGIA_CARGA_KWH", DoubleType(), True),
    StructField("ENERGIA_DESCARGA_KWH", DoubleType(), True),
    StructField("DEMANDA_NETA_CON_BATERIA_KWH", DoubleType(), True),
    StructField("EXCEDENTE_NETO_KWH", DoubleType(), True),
    StructField("ESTADO_BATERIA", StringType(), True),
    StructField("MUESTRAS", LongType(), True),
    StructField("DIAS_CON_DATOS", LongType(), True),
    StructField("DIAS_TIPO_MES", LongType(), True),
])


def _simulate_battery(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Simulacion iterativa de bateria para un grupo
    (ESCENARIO_SOLAR, ESCENARIO_BATERIA, NISE, PERIODO_MES, TIPO_DIA).

    Recibe un pandas DataFrame con todos los intervalos del grupo (96
    bloques de 15 min) y aplica la logica de bateria secuencialmente,
    manteniendo el SOC entre intervalos.
    """
    pdf = pdf.sort_values("BLOQUE_15M").reset_index(drop=True)

    if len(pdf) == 0:
        return pdf

    capacidad = pdf.iloc[0]["CAPACIDAD_KWH"]
    pot_carga = pdf.iloc[0]["POTENCIA_CARGA_15MIN"]
    pot_descarga = pdf.iloc[0]["POTENCIA_DESCARGA_15MIN"]
    eff_carga = pdf.iloc[0]["EFICIENCIA_CARGA"]
    eff_descarga = pdf.iloc[0]["EFICIENCIA_DESCARGA"]
    soc_min = pdf.iloc[0]["SOC_MIN"]
    soc_max = pdf.iloc[0]["SOC_MAX"]
    soc_inicial = pdf.iloc[0]["SOC_INICIAL"]
    autodescarga = pdf.iloc[0]["AUTODESCARGA_DIARIA"]

    soc = soc_inicial
    autodescarga_factor = 1.0 - autodescarga / 96.0

    soc_list = []
    energia_bateria_list = []
    energia_carga_list = []
    energia_descarga_list = []
    demanda_neta_list = []
    excedente_neto_list = []
    estado_list = []

    # Dia tipico ciclico: se simula el dia dos veces y se conserva la segunda
    # pasada, que arranca con el SOC del final del primer dia. El primer dia
    # arranca vacio (SOC_MIN, no SOC_INICIAL): como la dinamica es monotona en el
    # SOC, el SOC final de la segunda pasada es >= su SOC inicial, por lo que la
    # descarga nunca supera la carga x eficiencia (no se "regala" energia al
    # escalar el dia tipico x dias del mes).
    soc = soc_min
    n_filas = len(pdf)
    filas = pd.concat([pdf, pdf], ignore_index=True)

    for _, row in filas.iterrows():
        demanda = row["DEMANDA_PROM_KWH"]
        generacion = row["GENERACION_SOLAR_SIMULADA_KWH"]
        balance = generacion - demanda

        energia_carga = 0.0
        descarga = 0.0
        energia_bateria = 0.0
        excedente_neto = 0.0
        estado = "INACTIVA"

        if balance > 1e-9:
            # Hay excedente solar: cargar bateria
            espacio_bateria = (soc_max - soc) * capacidad
            energia_carga = min(
                balance,
                pot_carga,
                espacio_bateria / eff_carga if eff_carga > 0 else espacio_bateria,
            )
            energia_cargada_real = energia_carga * eff_carga
            soc = soc + (energia_cargada_real / capacidad if capacidad > 0 else 0.0)
            excedente_neto = balance - energia_carga
            energia_bateria = energia_carga
            estado = "CARGA"

        elif balance < -1e-9:
            # Hay deficit: descargar bateria
            deficit = -balance
            energia_disponible_bateria = (soc - soc_min) * capacidad
            energia_descarga = min(
                deficit / eff_descarga if eff_descarga > 0 else deficit,
                pot_descarga,
                energia_disponible_bateria,
            )
            descarga = energia_descarga * eff_descarga
            soc = soc - (energia_descarga / capacidad if capacidad > 0 else 0.0)
            excedente_neto = 0.0
            energia_bateria = -descarga
            estado = "DESCARGA"

        # Aplicar autodescarga diaria (por intervalo)
        soc = soc * autodescarga_factor
        # Limitar SOC dentro de [soc_min, soc_max]
        soc = max(soc_min, min(soc_max, soc))

        demanda_neta = max(0.0, demanda - generacion - descarga)

        soc_list.append(soc)
        energia_bateria_list.append(energia_bateria)
        energia_carga_list.append(energia_carga)
        energia_descarga_list.append(descarga)
        demanda_neta_list.append(demanda_neta)
        excedente_neto_list.append(excedente_neto)
        estado_list.append(estado)

    # Solo la segunda pasada (estado estacionario)
    pdf["SOC"] = soc_list[n_filas:]
    pdf["ENERGIA_BATERIA_KWH"] = energia_bateria_list[n_filas:]
    pdf["ENERGIA_CARGA_KWH"] = energia_carga_list[n_filas:]
    pdf["ENERGIA_DESCARGA_KWH"] = energia_descarga_list[n_filas:]
    pdf["DEMANDA_NETA_CON_BATERIA_KWH"] = demanda_neta_list[n_filas:]
    pdf["EXCEDENTE_NETO_KWH"] = excedente_neto_list[n_filas:]
    pdf["ESTADO_BATERIA"] = estado_list[n_filas:]

    output_cols = [
        "ESCENARIO_SOLAR", "ESCENARIO_BATERIA", "NISE", "PERIODO_MES",
        "TIPO_DIA", "PLAN", "TIPO_CLIENTE_SOLAR", "ANIO", "MES",
        "BLOQUE_15M", "HORA", "MINUTO", "DEMANDA_Q3_KWH", "DEMANDA_PROM_KWH",
        "GENERACION_SOLAR_SIMULADA_KWH", "EXCEDENTE_SOLAR_KWH",
        "DEMANDA_REMANENTE_KWH", "CAPACIDAD_KWH", "POTENCIA_CARGA_15MIN",
        "POTENCIA_DESCARGA_15MIN", "EFICIENCIA_CARGA", "EFICIENCIA_DESCARGA",
        "SOC", "ENERGIA_BATERIA_KWH", "ENERGIA_CARGA_KWH",
        "ENERGIA_DESCARGA_KWH", "DEMANDA_NETA_CON_BATERIA_KWH",
        "EXCEDENTE_NETO_KWH", "ESTADO_BATERIA", "MUESTRAS", "DIAS_CON_DATOS",
        "DIAS_TIPO_MES",
    ]
    return pdf[output_cols]


# ---------------------------------------------------------------------------
# 1. Simulacion a nivel de intervalo (15 min)
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name="impacto_bateria_q3_intervalo",
    comment=(
        "Simulacion de impacto de baterias a nivel de intervalo de 15 min "
        "por cliente y escenario de bateria. Incluye SOC iterativo, "
        "carga/descarga, demanda neta y excedente neto. Unidades: kWh por "
        "intervalo."
    ),
)
def impacto_bateria_q3_intervalo():
    """
    Simulacion a nivel de intervalo usando applyInPandas para tracking
    iterativo de SOC. Cada grupo (NISE x ESCENARIO_BATERIA x MES x TIPO_DIA)
    se procesa secuencialmente por BLOQUE_15M.
    """
    solar = spark.read.table(SOLAR_INTERVALO_TABLE)
    bateria = spark.read.table(BATERIA_PARAM_TABLE).filter(
        F.col("ACTIVO") == True
    )

    combined = (
        solar.alias("s")
        .crossJoin(bateria.alias("b"))
        .select(
            F.col("s.ESCENARIO").alias("ESCENARIO_SOLAR"),
            F.col("b.ESCENARIO").alias("ESCENARIO_BATERIA"),
            F.col("s.NISE"),
            F.col("s.PLAN"),
            F.col("s.TIPO_CLIENTE_SOLAR"),
            F.col("s.PERIODO_MES"),
            F.col("s.ANIO"),
            F.col("s.MES"),
            F.col("s.TIPO_DIA"),
            F.col("s.BLOQUE_15M"),
            F.col("s.HORA"),
            F.col("s.MINUTO"),
            F.col("s.DEMANDA_Q3_KWH"),
            F.col("s.DEMANDA_PROM_KWH"),
            F.col("s.GENERACION_SOLAR_SIMULADA_KWH"),
            F.col("s.EXCEDENTE_SOLAR_KWH"),
            F.col("s.DEMANDA_REMANENTE_KWH"),
            # Dimension por cliente: kWp = POTENCIA_SOLAR_PICO_CLIENTE (kWh/15 min) x 4
            # POTENCIA = FRACCION x kWp; CAPACIDAD = POTENCIA x HORAS
            (
                F.coalesce(F.col("s.POTENCIA_SOLAR_PICO_CLIENTE"), F.lit(0.0)) * 4.0
                * F.col("b.FRACCION_POTENCIA_FV") * F.col("b.HORAS_DURACION")
            ).alias("CAPACIDAD_KWH"),
            # Energia maxima por intervalo de 15 min = POTENCIA_KW / 4 = FRACCION x kWh/15min
            (
                F.coalesce(F.col("s.POTENCIA_SOLAR_PICO_CLIENTE"), F.lit(0.0))
                * F.col("b.FRACCION_POTENCIA_FV")
            ).alias("POTENCIA_CARGA_15MIN"),
            (
                F.coalesce(F.col("s.POTENCIA_SOLAR_PICO_CLIENTE"), F.lit(0.0))
                * F.col("b.FRACCION_POTENCIA_FV")
            ).alias("POTENCIA_DESCARGA_15MIN"),
            F.col("b.EFICIENCIA_CARGA"),
            F.col("b.EFICIENCIA_DESCARGA"),
            F.col("b.SOC_MIN"),
            F.col("b.SOC_MAX"),
            F.col("b.SOC_INICIAL"),
            F.col("b.AUTODESCARGA_DIARIA"),
            F.col("s.MUESTRAS"),
            F.col("s.DIAS_CON_DATOS"),
            F.col("s.DIAS_TIPO_MES"),
        )
    )

    result = combined.groupBy(
        "ESCENARIO_SOLAR",
        "ESCENARIO_BATERIA",
        "NISE",
        "PERIODO_MES",
        "TIPO_DIA",
    ).applyInPandas(_simulate_battery, schema=BATTERY_INTERVALO_SCHEMA)

    return result


# ---------------------------------------------------------------------------
# 2. Resumen mensual por cliente
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name="impacto_bateria_q3_cliente_mes",
    comment=(
        "Resumen mensual de impacto de baterias por cliente y escenario. "
        "Agrega intervalos a nivel NISE x PERIODO_MES respetando "
        "TIPO_DIA laboral y fin_de_semana (ref_calendario; feriado = fin_de_semana)."
    ),
)
def impacto_bateria_q3_cliente_mes():
    intervalo = spark.read.table(
        f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.impacto_bateria_q3_intervalo"
    )

    return (
        intervalo.groupBy(
            "ESCENARIO_SOLAR",
            "ESCENARIO_BATERIA",
            "NISE",
            "PLAN",
            "PERIODO_MES",
            "MES",
            "TIPO_DIA",
        )
        .agg(
            # Energia del mes = dia tipico promedio x dias con datos (no extrapola)
            F.sum(F.col("DEMANDA_PROM_KWH") * F.col("DIAS_CON_DATOS")).alias("CONSUMO_BASE_KWH"),
            F.sum(F.col("GENERACION_SOLAR_SIMULADA_KWH") * F.col("DIAS_CON_DATOS")).alias("GENERACION_SOLAR_KWH"),
            F.sum(F.col("ENERGIA_BATERIA_KWH") * F.col("DIAS_CON_DATOS")).alias("ENERGIA_BATERIA_NETA_KWH"),
            F.sum(F.col("ENERGIA_CARGA_KWH") * F.col("DIAS_CON_DATOS")).alias("ENERGIA_CARGADA_KWH"),
            F.sum(F.col("ENERGIA_DESCARGA_KWH") * F.col("DIAS_CON_DATOS")).alias("ENERGIA_DESCARGADA_KWH"),
            F.sum(F.col("DEMANDA_NETA_CON_BATERIA_KWH") * F.col("DIAS_CON_DATOS")).alias("DEMANDA_NETA_KWH"),
            F.sum(F.col("EXCEDENTE_NETO_KWH") * F.col("DIAS_CON_DATOS")).alias("EXCEDENTE_NETO_KWH"),
            F.sum(
                (F.col("DEMANDA_REMANENTE_KWH") - F.col("DEMANDA_NETA_CON_BATERIA_KWH"))
                * F.col("DIAS_CON_DATOS")
            ).alias("ENERGIA_NO_VENDIDA_ADICIONAL_KWH"),
            F.max("DIAS_TIPO_MES").alias("DIAS_TIPO_MES"),
            F.count("*").alias("INTERVALOS"),
            F.max("CAPACIDAD_KWH").alias("CAPACIDAD_BATERIA_KWH"),
        )
    )


# ---------------------------------------------------------------------------
# 3. Resumen global por cliente
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name="impacto_bateria_q3_cliente_resumen",
    comment=(
        "Resumen global por cliente y escenario de bateria: capacidad, "
        "consumo, generacion, carga/descarga, demanda neta, excedente y "
        "metricas de eficiencia."
    ),
)
def impacto_bateria_q3_cliente_resumen():
    cli_mes = spark.read.table(
        f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.impacto_bateria_q3_cliente_mes"
    )

    resumen = (
        cli_mes.groupBy(
            "ESCENARIO_SOLAR", "ESCENARIO_BATERIA", "NISE", "PLAN"
        )
        .agg(
            F.max("CAPACIDAD_BATERIA_KWH").alias("CAPACIDAD_BATERIA_KWH"),
            F.sum("CONSUMO_BASE_KWH").alias("CONSUMO_BASE_KWH"),
            F.sum("GENERACION_SOLAR_KWH").alias("GENERACION_SOLAR_KWH"),
            F.sum("ENERGIA_CARGADA_KWH").alias("ENERGIA_CARGADA_KWH"),
            F.sum("ENERGIA_DESCARGADA_KWH").alias("ENERGIA_DESCARGADA_KWH"),
            F.sum("DEMANDA_NETA_KWH").alias("DEMANDA_NETA_KWH"),
            F.sum("EXCEDENTE_NETO_KWH").alias("EXCEDENTE_NETO_KWH"),
            F.sum("ENERGIA_NO_VENDIDA_ADICIONAL_KWH").alias(
                "ENERGIA_NO_VENDIDA_ADICIONAL_KWH"
            ),
        )
    )

    return resumen.select(
        "ESCENARIO_SOLAR",
        "ESCENARIO_BATERIA",
        "NISE",
        "PLAN",
        "CAPACIDAD_BATERIA_KWH",
        "CONSUMO_BASE_KWH",
        "GENERACION_SOLAR_KWH",
        "ENERGIA_CARGADA_KWH",
        "ENERGIA_DESCARGADA_KWH",
        "DEMANDA_NETA_KWH",
        "EXCEDENTE_NETO_KWH",
        "ENERGIA_NO_VENDIDA_ADICIONAL_KWH",
        F.round(
            F.col("ENERGIA_DESCARGADA_KWH")
            / F.when(F.col("ENERGIA_CARGADA_KWH") > 0, F.col("ENERGIA_CARGADA_KWH"))
            .otherwise(F.lit(None)),
            4,
        ).alias("EFICIENCIA_CICLO_BATERIA"),
        F.round(
            F.col("ENERGIA_NO_VENDIDA_ADICIONAL_KWH")
            / F.when(F.col("CONSUMO_BASE_KWH") > 0, F.col("CONSUMO_BASE_KWH"))
            .otherwise(F.lit(None)) * 100,
            2,
        ).alias("PORCENTAJE_REDUCCION_ADICIONAL"),
        F.round(
            F.col("EXCEDENTE_NETO_KWH")
            / F.when(F.col("GENERACION_SOLAR_KWH") > 0, F.col("GENERACION_SOLAR_KWH"))
            .otherwise(F.lit(None)) * 100,
            2,
        ).alias("PORCENTAJE_EXCEDENTE_NETO"),
    )


# ---------------------------------------------------------------------------
# 4. Curva de pato con baterias
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name="impacto_bateria_q3_curva_pato",
    comment=(
        "Curva de pato agregada con baterias: demanda base, generacion "
        "solar, flujo de bateria (carga/descarga), demanda neta y "
        "excedente neto por MES x TIPO_DIA x BLOQUE_15M."
    ),
)
def impacto_bateria_q3_curva_pato():
    intervalo = spark.read.table(
        f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.impacto_bateria_q3_intervalo"
    )

    return (
        intervalo.groupBy(
            "ESCENARIO_BATERIA",
            "MES",
            "TIPO_DIA",
            "BLOQUE_15M",
            "HORA",
            "MINUTO",
        )
        .agg(
            F.sum("DEMANDA_PROM_KWH").alias("CURVA_BASE"),
            F.sum("GENERACION_SOLAR_SIMULADA_KWH").alias("GENERACION_SOLAR"),
            F.sum("ENERGIA_CARGA_KWH").alias("CARGA_BATERIA"),
            F.sum("ENERGIA_DESCARGA_KWH").alias("DESCARGA_BATERIA"),
            F.sum("DEMANDA_NETA_CON_BATERIA_KWH").alias("CURVA_NETA"),
            F.sum("EXCEDENTE_NETO_KWH").alias("EXCEDENTE_NETO"),
        )
        .select(
            "ESCENARIO_BATERIA",
            "MES",
            "TIPO_DIA",
            "BLOQUE_15M",
            "HORA",
            "MINUTO",
            "CURVA_BASE",
            "GENERACION_SOLAR",
            "CARGA_BATERIA",
            "DESCARGA_BATERIA",
            "CURVA_NETA",
            "EXCEDENTE_NETO",
        )
    )


# ---------------------------------------------------------------------------
# 5. Resumen global del sistema
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name="impacto_bateria_q3_global",
    comment=(
        "Resumen global de impacto de baterias: clientes simulados, "
        "capacidad total, energia cargada/descargada, reduccion de "
        "excedentes y eficiencia en GWh."
    ),
)
def impacto_bateria_q3_global():
    cli_resumen = spark.read.table(
        f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.impacto_bateria_q3_cliente_resumen"
    )

    global_row = (
        cli_resumen.groupBy("ESCENARIO_SOLAR", "ESCENARIO_BATERIA")
        .agg(
            F.countDistinct("NISE").alias("CLIENTES_SIMULADOS"),
            F.sum("CAPACIDAD_BATERIA_KWH").alias("CAPACIDAD_TOTAL_KWH"),
            F.sum("CONSUMO_BASE_KWH").alias("CONSUMO_BASE_KWH"),
            F.sum("GENERACION_SOLAR_KWH").alias("GENERACION_SOLAR_KWH"),
            F.sum("ENERGIA_CARGADA_KWH").alias("ENERGIA_CARGADA_KWH"),
            F.sum("ENERGIA_DESCARGADA_KWH").alias("ENERGIA_DESCARGADA_KWH"),
            F.sum("DEMANDA_NETA_KWH").alias("DEMANDA_NETA_KWH"),
            F.sum("EXCEDENTE_NETO_KWH").alias("EXCEDENTE_NETO_KWH"),
            F.sum("ENERGIA_NO_VENDIDA_ADICIONAL_KWH").alias(
                "ENERGIA_NO_VENDIDA_ADICIONAL_KWH"
            ),
        )
    )

    return global_row.select(
        "ESCENARIO_SOLAR",
        "ESCENARIO_BATERIA",
        "CLIENTES_SIMULADOS",
        F.round(F.col("CAPACIDAD_TOTAL_KWH") / 1000, 3).alias("CAPACIDAD_TOTAL_MWH"),
        F.round(F.col("CONSUMO_BASE_KWH") / 1e6, 3).alias("CONSUMO_BASE_GWH"),
        F.round(F.col("GENERACION_SOLAR_KWH") / 1e6, 3).alias("GENERACION_SOLAR_GWH"),
        F.round(F.col("ENERGIA_CARGADA_KWH") / 1e6, 3).alias("ENERGIA_CARGADA_GWH"),
        F.round(F.col("ENERGIA_DESCARGADA_KWH") / 1e6, 3).alias("ENERGIA_DESCARGADA_GWH"),
        F.round(F.col("DEMANDA_NETA_KWH") / 1e6, 3).alias("DEMANDA_NETA_GWH"),
        F.round(F.col("EXCEDENTE_NETO_KWH") / 1e6, 3).alias("EXCEDENTE_NETO_GWH"),
        F.round(F.col("ENERGIA_NO_VENDIDA_ADICIONAL_KWH") / 1e6, 3).alias(
            "REDUCCION_VENTAS_ADICIONAL_GWH"
        ),
        F.round(
            F.col("ENERGIA_DESCARGADA_KWH")
            / F.when(F.col("ENERGIA_CARGADA_KWH") > 0, F.col("ENERGIA_CARGADA_KWH"))
            .otherwise(F.lit(None)),
            4,
        ).alias("EFICIENCIA_CICLO_GLOBAL"),
        F.round(
            F.col("EXCEDENTE_NETO_KWH")
            / F.when(F.col("GENERACION_SOLAR_KWH") > 0, F.col("GENERACION_SOLAR_KWH"))
            .otherwise(F.lit(None)) * 100,
            2,
        ).alias("PORCENTAJE_EXCEDENTE_NETO"),
    )
