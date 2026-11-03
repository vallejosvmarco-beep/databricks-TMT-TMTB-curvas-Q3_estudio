from pyspark import pipelines as dp
from pyspark.sql import functions as F

SOURCE_CATALOG = spark.conf.get("project.source_catalog")
SOURCE_SCHEMA = spark.conf.get("project.source_schema_950")
SOURCE_TABLE = spark.conf.get("project.source_table_950")

PERIODO_INICIO = spark.conf.get("project.periodo_inicio")
PERIODO_FIN = spark.conf.get("project.periodo_fin")

SOURCE_FQN = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{SOURCE_TABLE}"


@dp.materialized_view(
    name="curvas_q3_mensuales",
    comment=(
        "Curvas mensuales Q3 por NISE, operacion, metrica, tipo de dia y "
        "estampilla de 15 minutos, usando una plata_950_carga existente."
    ),
)
def curvas_q3_mensuales():
    df = spark.read.table(SOURCE_FQN)

    base = (
        df
        .filter(
            F.col("NISE").isNotNull()
            & F.col("OPERACION").isNotNull()
            & F.col("FECHA_LECTURA").isNotNull()
        )
        .withColumn("FECHA", F.to_date("FECHA_LECTURA"))
        .filter(
            (F.col("FECHA") >= F.lit(PERIODO_INICIO).cast("date"))
            & (F.col("FECHA") <= F.lit(PERIODO_FIN).cast("date"))
        )
        .withColumn("PERIODO_MES", F.trunc(F.col("FECHA"), "month"))
        .withColumn("ANIO", F.year("FECHA_LECTURA"))
        .withColumn("MES", F.month("FECHA_LECTURA"))
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
        .withColumn(
            "TIPO_DIA",
            F.when(
                F.dayofweek("FECHA").isin([2, 3, 4, 5, 6]),
                F.lit("ENTRE_SEMANA")
            ).otherwise(F.lit("FIN_DE_SEMANA"))
        )
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
