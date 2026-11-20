from pyspark import pipelines as dp
from pyspark.sql import functions as F

SOURCE_CATALOG = spark.conf.get("project.source_catalog")
SOURCE_SCHEMA = spark.conf.get("project.source_schema_950")
SOURCE_TABLE = spark.conf.get("project.source_table_950")

PERIODO_INICIO = spark.conf.get("project.periodo_inicio")
PERIODO_FIN = spark.conf.get("project.periodo_fin")

SOURCE_FQN = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{SOURCE_TABLE}"
# Calendario comun (conocimiento_base/src/referencias/ref_calendario.sql), en el mismo
# catalogo/esquema que plata_950_carga. Se une por FECHA = dia de la curva de plata_950
# (no la fecha de FECHA_LECTURA, que es fin de intervalo: 00:00 = intervalo 96 del dia
# anterior). TIPO_DIA: laboral / fin_de_semana (feriado = fin_de_semana).
CALENDARIO_FQN = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.ref_calendario"


@dp.materialized_view(
    name="curvas_q3_mensuales",
    comment=(
        "Curvas mensuales Q3 por NISE, operacion, metrica, tipo de dia y "
        "estampilla de 15 minutos, usando una plata_950_carga existente."
    ),
)
def curvas_q3_mensuales():
    df = spark.read.table(SOURCE_FQN)
    calendario = spark.read.table(CALENDARIO_FQN).select(
        "FECHA",
        F.col("ANIO").alias("ANIO_CAL"),
        F.col("MES").alias("MES_CAL"),
        F.col("TIPO_DIA").alias("TIPO_DIA_CAL"),
    )

    base = (
        df
        .filter(
            F.col("NISE").isNotNull()
            & F.col("OPERACION").isNotNull()
            & F.col("FECHA_LECTURA").isNotNull()
        )
        .filter(
            (F.col("FECHA") >= F.lit(PERIODO_INICIO).cast("date"))
            & (F.col("FECHA") <= F.lit(PERIODO_FIN).cast("date"))
        )
        .join(calendario, "FECHA", "inner")
        .withColumn("PERIODO_MES", F.trunc(F.col("FECHA"), "month"))
        .withColumn("ANIO", F.col("ANIO_CAL"))
        .withColumn("MES", F.col("MES_CAL"))
        .withColumn("HORA", F.hour("FECHA_LECTURA"))
        .withColumn("MINUTO", F.minute("FECHA_LECTURA"))
        .withColumn(
            "BLOQUE_15M",
            (
                F.hour("FECHA_LECTURA") * 4
                + F.floor(F.minute("FECHA_LECTURA") / 15)
            ).cast("int"),
        )
        .withColumn("ENERGIA", F.col("ENERGIA").cast("double"))
        .withColumn("DEMANDA", F.col("DEMANDA").cast("double"))
        .withColumn("TIPO_DIA", F.col("TIPO_DIA_CAL"))
    )

    long_df = (
        base.select(
            "NISE",
            "PLAN",
            "OPERACION",
            "FECHA",
            "PERIODO_MES",
            "ANIO",
            "MES",
            "TIPO_DIA",
            "HORA",
            "MINUTO",
            "BLOQUE_15M",
            F.expr(
                """
                stack(
                    2,
                    'ENERGIA', ENERGIA,
                    'DEMANDA', DEMANDA
                ) as (METRICA, VALOR)
                """
            ),
        )
        .filter(F.col("VALOR").isNotNull())
    )

    return (
        long_df
        .groupBy(
            "NISE",
            "OPERACION",
            "PERIODO_MES",
            "ANIO",
            "MES",
            "TIPO_DIA",
            "HORA",
            "MINUTO",
            "BLOQUE_15M",
            "METRICA",
        )
        .agg(
            F.first("PLAN", ignorenulls=True).alias("PLAN"),
            F.expr("percentile_approx(VALOR, 0.75, 10000)").alias("Q3"),
            F.count("*").alias("MUESTRAS"),
            F.countDistinct("FECHA").alias("DIAS_CON_DATOS"),
        )
        .select(
            "NISE",
            "PLAN",
            "OPERACION",
            "PERIODO_MES",
            "ANIO",
            "MES",
            "TIPO_DIA",
            "HORA",
            "MINUTO",
            "BLOQUE_15M",
            "METRICA",
            "Q3",
            "MUESTRAS",
            "DIAS_CON_DATOS",
        )
    )
