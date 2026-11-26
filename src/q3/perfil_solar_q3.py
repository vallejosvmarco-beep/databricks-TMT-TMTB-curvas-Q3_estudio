from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

"""
Perfil solar de referencia para Costa Rica.

Construye un perfil fotovoltaico real a partir de clientes que ya poseen
generacion solar medida (OPERACION = 'Generacion_SGD_Activa_kWh';
antes 'Generacion_SGDA_Activo_kWh', renombrada en plata_950_carga 2026).

El perfil se normaliza a un factor 0-1 para poder escalarlo por cliente
segun su potencia pico dimensionada.

Unidades (validadas en inspeccion de datos):
  - METRICA = 'ENERGIA': kWh por intervalo de 15 minutos (ya es energia)
  - METRICA = 'DEMANDA': kW promedio del intervalo
  - ENERGIA = DEMANDA * 0.25 (confirmado por ratio en datos reales)
  - Por tanto, Q3 de ENERGIA ya esta en kWh y NO requiere conversion.
"""

OUTPUT_CATALOG = spark.conf.get("project.output_catalog")
OUTPUT_SCHEMA = spark.conf.get("project.output_schema")

SOURCE_TABLE = f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.curvas_q3_mensuales"
OPERACION_GENERACION = "Generacion_SGD_Activa_kWh"
# Se excluyen generadores no solares: si mas de este porcentaje de su energia
# promedio cae de noche (HORA < 5 o HORA >= 19), no se usan en el perfil FV.
UMBRAL_GENERACION_NOCTURNA = 0.05


@dp.materialized_view(
    name="perfil_solar_q3_referencia",
    comment=(
        "Perfil solar FV de referencia para Costa Rica, construido desde "
        "clientes con generacion SGDA medida. Granularidad: "
        "MES x TIPO_DIA x BLOQUE_15M. Incluye factor normalizado 0-1."
    ),
)
def perfil_solar_q3_referencia():
    """
    Construye el perfil solar de referencia desde clientes con generacion
    solar medida (Generacion_SGD_Activa_kWh, METRICA=ENERGIA).
    """
    df = spark.read.table(SOURCE_TABLE)

    generacion = df.filter(
        (F.col("OPERACION") == OPERACION_GENERACION)
        & (F.col("METRICA") == "ENERGIA")
        & F.col("Q3").isNotNull()
    )

    # Generadores con perfil solar (energia nocturna <= umbral)
    es_noche = (F.col("HORA") < 5) | (F.col("HORA") >= 19)
    generadores_fv = (
        generacion.groupBy("NISE")
        .agg(
            F.sum("PROMEDIO").alias("TOTAL"),
            F.sum(F.when(es_noche, F.col("PROMEDIO")).otherwise(F.lit(0.0))).alias("NOCHE"),
        )
        .filter(
            (F.col("TOTAL") > 0)
            & (F.col("NOCHE") / F.col("TOTAL") <= F.lit(UMBRAL_GENERACION_NOCTURNA))
        )
        .select("NISE")
    )

    # Filtrar solo generacion solar activa, metrica de energia (kWh por 15 min)
    solar = (
        generacion.join(generadores_fv, "NISE", "inner")
        .select(
            "ANIO",
            "MES",
            "TIPO_DIA",
            "BLOQUE_15M",
            "HORA",
            "MINUTO",
            F.col("Q3").alias("Q3_SOLAR_REFERENCIA"),
            F.col("PROMEDIO").alias("PROM_SOLAR_REFERENCIA"),
        )
    )

    # Agregar promedio entre clientes solares por MES x TIPO_DIA x BLOQUE_15M
    perfil = (
        solar.groupBy(
            "ANIO",
            "MES",
            "TIPO_DIA",
            "BLOQUE_15M",
            "HORA",
            "MINUTO",
        )
        .agg(
            F.avg("Q3_SOLAR_REFERENCIA").alias("Q3_SOLAR_REFERENCIA"),
            F.avg("PROM_SOLAR_REFERENCIA").alias("PROM_SOLAR_REFERENCIA"),
            F.count("*").alias("MUESTRAS_SOLARES"),
        )
    )

    # Normalizar: FACTOR_SOLAR_NORMALIZADO = Q3_SOLAR / MAX(Q3_SOLAR)
    # El maximo se calcula sobre todo el perfil (todos los bloques)
    max_q3 = perfil.agg(F.max("Q3_SOLAR_REFERENCIA")).collect()[0][0]

    perfil = perfil.withColumn(
        "FACTOR_SOLAR_NORMALIZADO",
        F.when(F.lit(max_q3) > 0, F.col("Q3_SOLAR_REFERENCIA") / F.lit(max_q3))
        .otherwise(F.lit(0.0)),
    )
    # Factor de ENERGIA: generacion promedio (dia tipico) relativa al mismo pico Q3.
    # Potencia (dimensionada con Q3) x FACTOR_SOLAR_ENERGIA = kWh promedio por intervalo.
    perfil = perfil.withColumn(
        "FACTOR_SOLAR_ENERGIA",
        F.when(F.lit(max_q3) > 0, F.col("PROM_SOLAR_REFERENCIA") / F.lit(max_q3))
        .otherwise(F.lit(0.0)),
    )

    # Determinar pico solar global
    pico = (
        perfil.filter(F.col("Q3_SOLAR_REFERENCIA") > 0)
        .orderBy(F.col("Q3_SOLAR_REFERENCIA").desc())
        .select("BLOQUE_15M", "HORA", "MINUTO")
        .first()
    )

    if pico is not None:
        bloque_pico = int(pico["BLOQUE_15M"])
        hora_pico = int(pico["HORA"])
        minuto_pico = int(pico["MINUTO"])
    else:
        bloque_pico = 48  # default: mediodia (12:00)
        hora_pico = 12
        minuto_pico = 0

    # Pico mensual
    pico_mensual = (
        perfil.filter(F.col("Q3_SOLAR_REFERENCIA") > 0)
        .groupBy("MES")
        .agg(
            F.max("Q3_SOLAR_REFERENCIA").alias("Q3_MAX_MES"),
            F.first("BLOQUE_15M", ignorenulls=True).alias("BLOQUE_PICO_MES"),
            F.first("HORA", ignorenulls=True).alias("HORA_PICO_MES"),
            F.first("MINUTO", ignorenulls=True).alias("MINUTO_PICO_MES"),
        )
    )

    # Determinar pico mensual correctamente (bloque donde ocurre el maximo)
    w_pico = Window.partitionBy("MES").orderBy(F.col("Q3_SOLAR_REFERENCIA").desc())
    pico_mensual = (
        perfil.filter(F.col("Q3_SOLAR_REFERENCIA") > 0)
        .withColumn("rn", F.row_number().over(w_pico))
        .filter(F.col("rn") == 1)
        .select(
            "MES",
            F.col("BLOQUE_15M").alias("BLOQUE_PICO_MES"),
            F.col("HORA").alias("HORA_PICO_MES"),
            F.col("MINUTO").alias("MINUTO_PICO_MES"),
            F.col("Q3_SOLAR_REFERENCIA").alias("Q3_MAX_MES"),
        )
    )

    # Unir perfil con picos mensuales
    resultado = (
        perfil.join(pico_mensual, "MES", "left")
        .withColumn("BLOQUE_PICO_SOLAR_CR", F.lit(bloque_pico))
        .withColumn("HORA_PICO_SOLAR_CR", F.lit(hora_pico))
        .withColumn("MINUTO_PICO_SOLAR_CR", F.lit(minuto_pico))
        .withColumn("MAX_Q3_SOLAR_GLOBAL", F.lit(max_q3))
        .select(
            "ANIO",
            "MES",
            "TIPO_DIA",
            "BLOQUE_15M",
            "HORA",
            "MINUTO",
            "Q3_SOLAR_REFERENCIA",
            "FACTOR_SOLAR_NORMALIZADO",
            "PROM_SOLAR_REFERENCIA",
            "FACTOR_SOLAR_ENERGIA",
            "MUESTRAS_SOLARES",
            "BLOQUE_PICO_SOLAR_CR",
            "HORA_PICO_SOLAR_CR",
            "MINUTO_PICO_SOLAR_CR",
            "MAX_Q3_SOLAR_GLOBAL",
            "BLOQUE_PICO_MES",
            "HORA_PICO_MES",
            "MINUTO_PICO_MES",
            "Q3_MAX_MES",
        )
    )

    return resultado