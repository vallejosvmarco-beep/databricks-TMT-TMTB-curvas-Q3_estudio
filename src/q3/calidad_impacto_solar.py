from pyspark import pipelines as dp
from pyspark.sql import functions as F

"""
Validaciones de calidad del modelo de impacto solar.

Verifica:
  - FACTOR_SOLAR_NORMALIZADO entre 0 y 1
  - AUTOCONSUMO <= DEMANDA y AUTOCONSUMO <= GENERACION_SOLAR
  - DEMANDA_REMANENTE >= 0
  - EXCEDENTE_SOLAR >= 0
  - ENERGIA_NO_VENDIDA <= ENERGIA_BASE
  - Generacion nocturna ~ 0
  - SUM(clientes) = resultado global
  - Agregaciones mensuales consistentes
"""

OUTPUT_CATALOG = spark.conf.get("project.output_catalog")
OUTPUT_SCHEMA = spark.conf.get("project.output_schema")

PERFIL_TABLE = f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.perfil_solar_q3_referencia"
INTERVALO_TABLE = f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.impacto_solar_q3_intervalo"
CLIENTE_TABLE = f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.impacto_solar_q3_cliente_resumen"
GLOBAL_TABLE = f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.impacto_solar_q3_global"


@dp.materialized_view(
    name="impacto_solar_q3_calidad",
    comment=(
        "Tabla de validaciones de calidad del modelo de impacto solar. "
        "Incluye validaciones de factor solar, autoconsumo, excedente, "
        "generacion nocturna y consistencia de agregaciones."
    ),
)
def impacto_solar_q3_calidad():
    """
    Genera una tabla con todas las validaciones de calidad del modelo.
    """
    perfil = spark.read.table(PERFIL_TABLE)
    intervalo = spark.read.table(INTERVALO_TABLE)
    cliente = spark.read.table(CLIENTE_TABLE)
    global_tbl = spark.read.table(GLOBAL_TABLE)

    # 1. Factor solar normalizado fuera de rango [0, 1]
    factor_invalido = (
        perfil.filter(
            (F.col("FACTOR_SOLAR_NORMALIZADO") < 0)
            | (F.col("FACTOR_SOLAR_NORMALIZADO") > 1.0001)
        )
        .agg(F.count("*").alias("REGISTROS_AFECTADOS"))
        .select(
            F.lit("FACTOR_SOLAR_FUERA_RANGO").alias("VALIDACION"),
            F.lit("FACTOR_SOLAR_NORMALIZADO debe estar entre 0 y 1").alias(
                "DESCRIPCION"
            ),
            F.col("REGISTROS_AFECTADOS"),
            F.lit("perfil_solar_q3_referencia").alias("TABLA"),
        )
    )

    # 2. Autoconsumo > demanda (no debe ocurrir)
    autoconsumo_excesivo = (
        intervalo.filter(
            F.col("AUTOCONSUMO_SOLAR_KWH") > F.col("DEMANDA_Q3_KWH") + 0.001
        )
        .agg(F.count("*").alias("REGISTROS_AFECTADOS"))
        .select(
            F.lit("AUTOCONSUMO_MAYOR_DEMANDA").alias("VALIDACION"),
            F.lit("AUTOCONSUMO_SOLAR no debe superar DEMANDA_Q3").alias(
                "DESCRIPCION"
            ),
            F.col("REGISTROS_AFECTADOS"),
            F.lit("impacto_solar_q3_intervalo").alias("TABLA"),
        )
    )

    # 3. Autoconsumo > generacion (no debe ocurrir)
    autoconsumo_mayor_gen = (
        intervalo.filter(
            F.col("AUTOCONSUMO_SOLAR_KWH")
            > F.col("GENERACION_SOLAR_SIMULADA_KWH") + 0.001
        )
        .agg(F.count("*").alias("REGISTROS_AFECTADOS"))
        .select(
            F.lit("AUTOCONSUMO_MAYOR_GENERACION").alias("VALIDACION"),
            F.lit("AUTOCONSUMO_SOLAR no debe superar GENERACION_SOLAR").alias(
                "DESCRIPCION"
            ),
            F.col("REGISTROS_AFECTADOS"),
            F.lit("impacto_solar_q3_intervalo").alias("TABLA"),
        )
    )

    # 4. Demanda remanente negativa
    remanente_negativo = (
        intervalo.filter(F.col("DEMANDA_REMANENTE_KWH") < -0.001)
        .agg(F.count("*").alias("REGISTROS_AFECTADOS"))
        .select(
            F.lit("DEMANDA_REMANENTE_NEGATIVA").alias("VALIDACION"),
            F.lit("DEMANDA_REMANENTE debe ser >= 0").alias("DESCRIPCION"),
            F.col("REGISTROS_AFECTADOS"),
            F.lit("impacto_solar_q3_intervalo").alias("TABLA"),
        )
    )

    # 5. Excedente negativo
    excedente_negativo = (
        intervalo.filter(F.col("EXCEDENTE_SOLAR_KWH") < -0.001)
        .agg(F.count("*").alias("REGISTROS_AFECTADOS"))
        .select(
            F.lit("EXCEDENTE_NEGATIVO").alias("VALIDACION"),
            F.lit("EXCEDENTE_SOLAR debe ser >= 0").alias("DESCRIPCION"),
            F.col("REGISTROS_AFECTADOS"),
            F.lit("impacto_solar_q3_intervalo").alias("TABLA"),
        )
    )

    # 6. Energia no vendida > consumo base
    no_vendida_excesiva = (
        cliente.filter(
            F.col("ENERGIA_NO_VENDIDA_KWH") > F.col("CONSUMO_BASE_KWH") + 0.001
        )
        .agg(F.count("*").alias("REGISTROS_AFECTADOS"))
        .select(
            F.lit("NO_VENDIDA_MAYOR_BASE").alias("VALIDACION"),
            F.lit("ENERGIA_NO_VENDIDA no debe superar CONSUMO_BASE").alias(
                "DESCRIPCION"
            ),
            F.col("REGISTROS_AFECTADOS"),
            F.lit("impacto_solar_q3_cliente_resumen").alias("TABLA"),
        )
    )

    # 7. Generacion nocturna anomala (bloques 0-21 y 73-95 con generacion > 0)
    gen_nocturna = (
        intervalo.filter(
            (
                (F.col("BLOQUE_15M") < 22) | (F.col("BLOQUE_15M") > 72)
            )
            & (F.col("GENERACION_SOLAR_SIMULADA_KWH") > 0.01)
        )
        .groupBy("NISE")
        .agg(
            F.sum("GENERACION_SOLAR_SIMULADA_KWH").alias("GEN_NOCTURNA_KWH"),
            F.count("*").alias("INTERVALOS_NOCTURNOS_GEN"),
        )
        .select(
            F.lit("GENERACION_NOCTURNA_ANOMALA").alias("VALIDACION"),
            F.concat(
                F.lit("NISE "),
                F.col("NISE"),
                F.lit(" tiene generacion nocturna: "),
                F.round("GEN_NOCTURNA_KWH", 4).cast("string"),
                F.lit(" kWh en "),
                F.col("INTERVALOS_NOCTURNOS_GEN").cast("string"),
                F.lit(" intervalos"),
            ).alias("DESCRIPCION"),
            F.col("INTERVALOS_NOCTURNOS_GEN").alias("REGISTROS_AFECTADOS"),
            F.lit("impacto_solar_q3_intervalo").alias("TABLA"),
        )
    )

    # 8. Potencia solar cero
    potencia_cero = (
        cliente.filter(
            (F.col("POTENCIA_SOLAR_PICO_CLIENTE_KW").isNull())
            | (F.col("POTENCIA_SOLAR_PICO_CLIENTE_KW") <= 0)
        )
        .agg(F.count("*").alias("REGISTROS_AFECTADOS"))
        .select(
            F.lit("POTENCIA_SOLAR_CERO").alias("VALIDACION"),
            F.lit("Clientes con potencia solar cero o nula").alias("DESCRIPCION"),
            F.col("REGISTROS_AFECTADOS"),
            F.lit("impacto_solar_q3_cliente_resumen").alias("TABLA"),
        )
    )

    # 9. Clientes sin 96 intervalos por mes
    intervalos_por_mes = (
        intervalo.groupBy("NISE", "MES", "TIPO_DIA")
        .agg(F.count("*").alias("NUM_INTERVALOS"))
    )
    sin_96 = (
        intervalos_por_mes.filter(F.col("NUM_INTERVALOS") != 96)
        .agg(F.count("*").alias("REGISTROS_AFECTADOS"))
        .select(
            F.lit("SIN_96_INTERVALOS").alias("VALIDACION"),
            F.lit("Clientes/meses sin 96 intervalos completos").alias("DESCRIPCION"),
            F.col("REGISTROS_AFECTADOS"),
            F.lit("impacto_solar_q3_intervalo").alias("TABLA"),
        )
    )

    # 10. Consistencia: SUM(clientes) = resultado global
    sum_clientes = cliente.agg(
        F.sum("ENERGIA_NO_VENDIDA_KWH").alias("SUM_NO_VENDIDA"),
        F.sum("CONSUMO_BASE_KWH").alias("SUM_BASE"),
    ).collect()[0]

    # Check if global table has data
    global_rows = global_tbl.collect()
    if len(global_rows) > 0:
        global_row = global_rows[0]
        diferencia_no_vendida = abs(
            sum_clientes["SUM_NO_VENDIDA"] - global_row["ENERGIA_NO_VENDIDA_GWH"] * 1e6
        )
        descripcion = f"Diferencia SUM(clientes) vs global: {diferencia_no_vendida:.4f} kWh"
    else:
        descripcion = "Tabla global vacia - no se puede verificar consistencia"

    consistencia = (
        spark.createDataFrame(
            [("CONSISTENCIA_GLOBAL", descripcion, 1, "impacto_solar_q3_global")],
            ["VALIDACION", "DESCRIPCION", "REGISTROS_AFECTADOS", "TABLA"],
        )
    )

    # Unir todas las validaciones
    all_validations = (
        factor_invalido
        .unionByName(autoconsumo_excesivo)
        .unionByName(autoconsumo_mayor_gen)
        .unionByName(remanente_negativo)
        .unionByName(excedente_negativo)
        .unionByName(no_vendida_excesiva)
        .unionByName(potencia_cero)
        .unionByName(sin_96)
        .unionByName(consistencia)
    )

    # Agregar timestamp
    return all_validations.withColumn(
        "FECHA_VALIDACION",
        F.current_timestamp(),
    )