from pyspark import pipelines as dp
from pyspark.sql import functions as F

"""
Validaciones de calidad del modelo de impacto de baterias.

Verifica:
  - SOC dentro de limites [0, 1]
  - Potencia de carga/descarga dentro de limites por intervalo
  - Excedente neto no negativo
  - Demanda neta con bateria no negativa
  - Eficiencia de ciclo > 0 (>1 esperado por SOC inicial)
  - Clientes sin 96 intervalos por mes
  - Consistencia: SUM(clientes) = resultado global
"""

OUTPUT_CATALOG = spark.conf.get("project.output_catalog")
OUTPUT_SCHEMA = spark.conf.get("project.output_schema")

INTERVALO_TABLE = f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.impacto_bateria_q3_intervalo"
CLIENTE_TABLE = f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.impacto_bateria_q3_cliente_resumen"
GLOBAL_TABLE = f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.impacto_bateria_q3_global"
PARAM_TABLE = f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.parametros_bateria"


@dp.materialized_view(
    name="impacto_bateria_q3_calidad",
    comment=(
        "Tabla de validaciones de calidad del modelo de impacto de "
        "baterias. Incluye validaciones de SOC, balance energetico, "
        "excedente neto, demanda neta y consistencia de agregaciones."
    ),
)
def impacto_bateria_q3_calidad():
    """
    Genera una tabla con todas las validaciones de calidad del modelo
    de baterias.
    """
    intervalo = spark.read.table(INTERVALO_TABLE)
    cliente = spark.read.table(CLIENTE_TABLE)
    global_tbl = spark.read.table(GLOBAL_TABLE)

    # 1. SOC fuera de rango [SOC_MIN, SOC_MAX]
    soc_invalido = (
        intervalo.filter(
            (F.col("SOC") < 0)
            | (F.col("SOC") > 1.0001)
        )
        .agg(F.count("*").alias("REGISTROS_AFECTADOS"))
        .select(
            F.lit("SOC_FUERA_RANGO").alias("VALIDACION"),
            F.lit("SOC debe estar entre 0 y 1").alias("DESCRIPCION"),
            F.col("REGISTROS_AFECTADOS"),
            F.lit("impacto_bateria_q3_intervalo").alias("TABLA"),
        )
    )

    # 2. Potencia de carga/descarga dentro de limites por intervalo
    potencia_excedida = (
        intervalo.filter(
            (F.col("ENERGIA_CARGA_KWH") > F.col("POTENCIA_CARGA_15MIN") + 0.001)
            | (
                F.col("ENERGIA_DESCARGA_KWH")
                > F.col("POTENCIA_DESCARGA_15MIN") + 0.001
            )
        )
        .agg(F.count("*").alias("REGISTROS_AFECTADOS"))
        .select(
            F.lit("POTENCIA_DENTRO_LIMITES").alias("VALIDACION"),
            F.lit(
                "Energia de carga/descarga no debe superar la "
                "potencia maxima por intervalo de 15 min"
            ).alias("DESCRIPCION"),
            F.col("REGISTROS_AFECTADOS"),
            F.lit("impacto_bateria_q3_intervalo").alias("TABLA"),
        )
    )

    # 3. Excedente neto negativo
    excedente_negativo = (
        intervalo.filter(F.col("EXCEDENTE_NETO_KWH") < -0.001)
        .agg(F.count("*").alias("REGISTROS_AFECTADOS"))
        .select(
            F.lit("EXCEDENTE_NETO_NEGATIVO").alias("VALIDACION"),
            F.lit("EXCEDENTE_NETO_KWH debe ser >= 0").alias("DESCRIPCION"),
            F.col("REGISTROS_AFECTADOS"),
            F.lit("impacto_bateria_q3_intervalo").alias("TABLA"),
        )
    )

    # 4. Demanda neta con bateria negativa
    demanda_neta_negativa = (
        intervalo.filter(F.col("DEMANDA_NETA_CON_BATERIA_KWH") < -0.001)
        .agg(F.count("*").alias("REGISTROS_AFECTADOS"))
        .select(
            F.lit("DEMANDA_NETA_NEGATIVA").alias("VALIDACION"),
            F.lit("DEMANDA_NETA_CON_BATERIA_KWH debe ser >= 0").alias("DESCRIPCION"),
            F.col("REGISTROS_AFECTADOS"),
            F.lit("impacto_bateria_q3_intervalo").alias("TABLA"),
        )
    )

    # 5. Eficiencia de ciclo > 0 (>1 esperado por SOC inicial)
    # La bateria arranca con 50% SOC, por lo que puede descargar mas
    # energia de la que carga en un periodo. La eficiencia > 1 es
    # valida y refleja el uso de energia inicial almacenada.
    eff_invalida = (
        cliente.filter(F.col("EFICIENCIA_CICLO_BATERIA") < 0)
        .agg(F.count("*").alias("REGISTROS_AFECTADOS"))
        .select(
            F.lit("EFICIENCIA_NEGATIVA").alias("VALIDACION"),
            F.lit(
                "EFICIENCIA_CICLO_BATERIA debe ser > 0 "
                "(valores > 1 son validos por SOC inicial)"
            ).alias("DESCRIPCION"),
            F.col("REGISTROS_AFECTADOS"),
            F.lit("impacto_bateria_q3_cliente_resumen").alias("TABLA"),
        )
    )

    # 6. Clientes sin 96 intervalos por mes
    intervalos_por_mes = (
        intervalo.groupBy(
            "ESCENARIO_BATERIA", "NISE", "MES", "TIPO_DIA"
        )
        .agg(F.count("*").alias("NUM_INTERVALOS"))
    )
    sin_96 = (
        intervalos_por_mes.filter(F.col("NUM_INTERVALOS") != 96)
        .agg(F.count("*").alias("REGISTROS_AFECTADOS"))
        .select(
            F.lit("SIN_96_INTERVALOS").alias("VALIDACION"),
            F.lit("Clientes/meses sin 96 intervalos completos").alias(
                "DESCRIPCION"
            ),
            F.col("REGISTROS_AFECTADOS"),
            F.lit("impacto_bateria_q3_intervalo").alias("TABLA"),
        )
    )

    # 7. Consistencia: SUM(clientes) por escenario = resultado global
    sum_por_escenario = (
        cliente.groupBy("ESCENARIO_BATERIA")
        .agg(
            F.sum("ENERGIA_NO_VENDIDA_ADICIONAL_KWH").alias("SUM_NO_VENDIDA"),
        )
    )
    global_sums = (
        global_tbl.select(
            "ESCENARIO_BATERIA",
            (F.col("REDUCCION_VENTAS_ADICIONAL_GWH") * 1e6).alias("GLOBAL_NO_VENDIDA"),
        )
    )
    diff = (
        sum_por_escenario.join(global_sums, "ESCENARIO_BATERIA", "left")
        .withColumn(
            "DIFERENCIA",
            F.abs(F.col("SUM_NO_VENDIDA") - F.coalesce(F.col("GLOBAL_NO_VENDIDA"), F.lit(0))),
        )
        .filter(F.col("DIFERENCIA") > 1000.0)
    )
    consistencia = (
        diff.agg(F.count("*").alias("REGISTROS_AFECTADOS"))
        .select(
            F.lit("CONSISTENCIA_GLOBAL").alias("VALIDACION"),
            F.lit(
                "SUM(ENERGIA_NO_VENDIDA_ADICIONAL por escenario) "
                "debe coincidir con resultado global"
            ).alias("DESCRIPCION"),
            F.col("REGISTROS_AFECTADOS"),
            F.lit("impacto_bateria_q3_global").alias("TABLA"),
        )
    )

    # Unir todas las validaciones
    all_validations = (
        soc_invalido
        .unionByName(potencia_excedida)
        .unionByName(excedente_negativo)
        .unionByName(demanda_neta_negativa)
        .unionByName(eff_invalida)
        .unionByName(sin_96)
        .unionByName(consistencia)
    )

    return all_validations.withColumn(
        "FECHA_VALIDACION",
        F.current_timestamp(),
    )
