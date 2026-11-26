from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

"""
Simulacion de impacto de generacion solar fotovoltaica.

Responde: ?Cuant energia dejaria de vender la distribuidora si cada
cliente elegible instalara generacion solar FV?

Unidades (validadas en inspeccion de datos):
  - METRICA = 'ENERGIA': kWh por intervalo de 15 minutos
  - METRICA = 'DEMANDA': kW promedio del intervalo
  - Q3 de ENERGIA ya esta en kWh: NO requiere conversion * 0.25
  - La conversion kWh = kW * 0.25 NO se aplica porque ENERGIA ya es kWh.

Clasificacion de clientes:
  - SOLAR_EXISTENTE: NISE con OPERACION = 'Generacion_SGD_Activa_kWh'
    (antes 'Generacion_SGDA_Activo_kWh')
  - INYECTA_RED: NISE con OPERACION = 'Inyeccion_Activo_kWh'
  - SIN_SOLAR: resto de clientes (elegibles para simulacion)

Regla fundamental:
  ENERGIA_DEJADA_DE_VENDER = AUTOCONSUMO_SOLAR
  (NO es GENERACION_SOLAR_TOTAL)

Escalado (2026-11-20):
  - Energia: curva PROMEDIO (dia tipico) x DIAS_CON_DATOS (dias con lectura del
    tipo de dia en el mes, por intervalo). Equivale a la energia medida en
    plata_950_carga; no extrapola dias sin datos. DIAS_TIPO_MES (dias calendario)
    se conserva como referencia de completitud. Las columnas *_KWH por intervalo
    son de un dia tipico; cliente_mes/resumen/global estan en kWh medidos del mes.
  - Picos y dimensionamiento: curva Q3 (POTENCIA_SOLAR_PICO_CLIENTE, GENERACION_SOLAR_Q3_KWH).
"""

OUTPUT_CATALOG = spark.conf.get("project.output_catalog")
OUTPUT_SCHEMA = spark.conf.get("project.output_schema")
ESCENARIO = spark.conf.get("project.escenario", "ADOPCION_100")

SOURCE_TABLE = f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.curvas_q3_mensuales"
PERFIL_SOLAR_TABLE = f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.perfil_solar_q3_referencia"
OPERACION_GENERACION = "Generacion_SGD_Activa_kWh"
# Unidades: POTENCIA_SOLAR_PICO_CLIENTE (intervalo) esta en kWh por 15 min;
# x INTERVALOS_POR_HORA = kW (kWp). Las tablas cliente_mes/resumen/global reportan kW.
INTERVALOS_POR_HORA = 4


def _load_consumo_energia():
    """Carga el consumo activo en energia (kWh por 15 min) para todos los clientes."""
    df = spark.read.table(SOURCE_TABLE)
    return (
        df.filter(
            (F.col("OPERACION") == "Consumo_Activo_kWh")
            & (F.col("METRICA") == "ENERGIA")
            & F.col("Q3").isNotNull()
        )
        .select(
            "NISE",
            "PLAN",
            "PERIODO_MES",
            "ANIO",
            "MES",
            "TIPO_DIA",
            "BLOQUE_15M",
            "HORA",
            "MINUTO",
            F.col("Q3").alias("DEMANDA_Q3_KWH"),
            F.col("PROMEDIO").alias("DEMANDA_PROM_KWH"),
            "MUESTRAS",
            "DIAS_CON_DATOS",
            "DIAS_TIPO_MES",
        )
    )


def _load_nise_solar():
    """Identifica NISE con generacion solar (SGDA) e inyeccion a la red."""
    df = spark.read.table(SOURCE_TABLE)
    solar_sgda = (
        df.filter(F.col("OPERACION") == OPERACION_GENERACION)
        .select("NISE")
        .distinct()
    )
    inyeccion = (
        df.filter(F.col("OPERACION") == "Inyeccion_Activo_kWh")
        .select("NISE")
        .distinct()
    )
    return solar_sgda, inyeccion


@dp.materialized_view(
    name="impacto_solar_q3_intervalo",
    comment=(
        "Simulacion de impacto solar a nivel de intervalo de 15 min por "
        "cliente. Incluye generacion solar simulada, autoconsumo, "
        "excedente y energia remanente. Unidades: kWh por intervalo."
    ),
)
def impacto_solar_q3_intervalo():
    """
    Simulacion a nivel de intervalo (NISE x PERIODO_MES x BLOQUE_15M).
    """
    consumo = _load_consumo_energia()
    perfil = spark.read.table(PERFIL_SOLAR_TABLE)
    solar_sgda, inyeccion = _load_nise_solar()

    # Clasificar clientes
    solar_sgda_list = solar_sgda.withColumn("TIPO_CLIENTE_SOLAR", F.lit("SOLAR_EXISTENTE"))
    inyeccion_list = inyeccion.withColumn("TIPO_CLIENTE_SOLAR", F.lit("INYECTA_RED"))

    tipo_cliente = (
        solar_sgda_list.unionByName(inyeccion_list)
        .groupBy("NISE")
        .agg(F.first("TIPO_CLIENTE_SOLAR").alias("TIPO_CLIENTE_SOLAR"))
    )

    # Unir consumo con tipo de cliente
    consumo_cli = consumo.join(tipo_cliente, "NISE", "left")
    consumo_cli = consumo_cli.withColumn(
        "TIPO_CLIENTE_SOLAR",
        F.coalesce(F.col("TIPO_CLIENTE_SOLAR"), F.lit("SIN_SOLAR")),
    )

    # Para simulacion: solo clientes SIN_SOLAR son elegibles
    elegibles = consumo_cli.filter(F.col("TIPO_CLIENTE_SOLAR") == "SIN_SOLAR")

    # Unir con perfil solar (MES x TIPO_DIA x BLOQUE_15M)
    simulacion = (
        elegibles.join(
            perfil.select(
                "MES",
                "TIPO_DIA",
                "BLOQUE_15M",
                "FACTOR_SOLAR_NORMALIZADO",
                "FACTOR_SOLAR_ENERGIA",
            ),
            ["MES", "TIPO_DIA", "BLOQUE_15M"],
            "left",
        )
    )

    # Dimensionamiento: POTENCIA_SOLAR_PICO_CLIENTE
    # La potencia pico FV no debe superar la demanda Q3 maxima del cliente
    # durante el periodo de maxima generacion solar.
    # Usamos la demanda en el bloque pico solar.
    pico_info = (
        perfil.select("MES", "TIPO_DIA", "BLOQUE_PICO_MES")
        .distinct()
    )

    # Demanda del cliente en el bloque pico solar mensual
    demanda_pico = (
        elegibles.join(pico_info, ["MES", "TIPO_DIA"], "inner")
        .filter(F.col("BLOQUE_15M") == F.col("BLOQUE_PICO_MES"))
        .groupBy("NISE", "MES", "TIPO_DIA")
        .agg(F.max("DEMANDA_Q3_KWH").alias("DEMANDA_PICO_SOLAR_KWH"))
    )

    # Potencia solar pico por cliente: maxima demanda en bloque pico solar
    potencia_cliente = (
        demanda_pico.groupBy("NISE")
        .agg(F.max("DEMANDA_PICO_SOLAR_KWH").alias("POTENCIA_SOLAR_PICO_CLIENTE"))
    )

    # Unir simulacion con potencia del cliente
    simulacion = simulacion.join(potencia_cliente, "NISE", "left")

    # Calcular generacion solar simulada
    # GENERACION_SOLAR_SIMULADA = POTENCIA * FACTOR_SOLAR_NORMALIZADO
    # Como ENERGIA ya esta en kWh y el factor es adimensional,
    # el resultado esta en kWh por intervalo.
    # Energia (dia tipico promedio): POTENCIA x FACTOR_SOLAR_ENERGIA
    # Pico (Q3): POTENCIA x FACTOR_SOLAR_NORMALIZADO
    simulacion = (
        simulacion
        .withColumn(
            "GENERACION_SOLAR_SIMULADA_KWH",
            F.coalesce(F.col("POTENCIA_SOLAR_PICO_CLIENTE"), F.lit(0.0))
            * F.coalesce(F.col("FACTOR_SOLAR_ENERGIA"), F.lit(0.0)),
        )
        .withColumn(
            "GENERACION_SOLAR_Q3_KWH",
            F.coalesce(F.col("POTENCIA_SOLAR_PICO_CLIENTE"), F.lit(0.0))
            * F.coalesce(F.col("FACTOR_SOLAR_NORMALIZADO"), F.lit(0.0)),
        )
    )

    # Autoconsumo, remanente y excedente
    simulacion = (
        simulacion
        .withColumn(
            "AUTOCONSUMO_SOLAR_KWH",
            F.least(F.col("DEMANDA_PROM_KWH"), F.col("GENERACION_SOLAR_SIMULADA_KWH")),
        )
        .withColumn(
            "DEMANDA_REMANENTE_KWH",
            F.greatest(
                F.col("DEMANDA_PROM_KWH") - F.col("GENERACION_SOLAR_SIMULADA_KWH"),
                F.lit(0.0),
            ),
        )
        .withColumn(
            "EXCEDENTE_SOLAR_KWH",
            F.greatest(
                F.col("GENERACION_SOLAR_SIMULADA_KWH") - F.col("DEMANDA_PROM_KWH"),
                F.lit(0.0),
            ),
        )
        # ENERGIA_NO_VENDIDA = AUTOCONSUMO_SOLAR (regla fundamental)
        .withColumn("ENERGIA_NO_VENDIDA_KWH", F.col("AUTOCONSUMO_SOLAR_KWH"))
        .withColumn("ESCENARIO", F.lit(ESCENARIO))
    )

    return simulacion.select(
        "ESCENARIO",
        "NISE",
        "PLAN",
        "TIPO_CLIENTE_SOLAR",
        "PERIODO_MES",
        "ANIO",
        "MES",
        "TIPO_DIA",
        "BLOQUE_15M",
        "HORA",
        "MINUTO",
        "DEMANDA_Q3_KWH",
        "DEMANDA_PROM_KWH",
        "POTENCIA_SOLAR_PICO_CLIENTE",
        "FACTOR_SOLAR_NORMALIZADO",
        "FACTOR_SOLAR_ENERGIA",
        "GENERACION_SOLAR_SIMULADA_KWH",
        "GENERACION_SOLAR_Q3_KWH",
        "AUTOCONSUMO_SOLAR_KWH",
        "DEMANDA_REMANENTE_KWH",
        "EXCEDENTE_SOLAR_KWH",
        "ENERGIA_NO_VENDIDA_KWH",
        "MUESTRAS",
        "DIAS_CON_DATOS",
        "DIAS_TIPO_MES",
    )


@dp.materialized_view(
    name="impacto_solar_q3_cliente_mes",
    comment=(
        "Resumen mensual de impacto solar por cliente. Agrega intervalos "
        "a nivel NISE x PERIODO_MES respetando TIPO_DIA laboral y fin_de_semana (ref_calendario)."
    ),
)
def impacto_solar_q3_cliente_mes():
    """
    Agregacion mensual por cliente, separada por tipo de dia.
    """
    intervalo = spark.read.table(
        f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.impacto_solar_q3_intervalo"
    )

    return (
        intervalo.groupBy(
            "ESCENARIO",
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
            F.sum(F.col("AUTOCONSUMO_SOLAR_KWH") * F.col("DIAS_CON_DATOS")).alias("AUTOCONSUMO_SOLAR_KWH"),
            F.sum(F.col("EXCEDENTE_SOLAR_KWH") * F.col("DIAS_CON_DATOS")).alias("EXCEDENTE_SOLAR_KWH"),
            F.sum(F.col("DEMANDA_REMANENTE_KWH") * F.col("DIAS_CON_DATOS")).alias("ENERGIA_REMANENTE_KWH"),
            F.sum(F.col("ENERGIA_NO_VENDIDA_KWH") * F.col("DIAS_CON_DATOS")).alias("ENERGIA_NO_VENDIDA_KWH"),
            # Potencias en kW (kWh por 15 min x 4)
            (F.max("POTENCIA_SOLAR_PICO_CLIENTE") * INTERVALOS_POR_HORA).alias("POTENCIA_SOLAR_PICO_KW"),
            # Pico (Q3) del dia tipico, en kW
            (F.max("DEMANDA_Q3_KWH") * INTERVALOS_POR_HORA).alias("DEMANDA_PICO_Q3_KW"),
            F.max("DIAS_TIPO_MES").alias("DIAS_TIPO_MES"),
            F.count("*").alias("INTERVALOS"),
        )
    )


@dp.materialized_view(
    name="impacto_solar_q3_cliente_resumen",
    comment=(
        "Resumen global por cliente: potencia, consumo, generacion, "
        "autoconsumo, excedente y energia dejada de vender."
    ),
)
def impacto_solar_q3_cliente_resumen():
    """
    Resumen total por NISE con porcentajes.
    """
    cli_mes = spark.read.table(
        f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.impacto_solar_q3_cliente_mes"
    )

    resumen = (
        cli_mes.groupBy("ESCENARIO", "NISE", "PLAN")
        .agg(
            F.sum("CONSUMO_BASE_KWH").alias("CONSUMO_BASE_KWH"),
            F.sum("GENERACION_SOLAR_KWH").alias("GENERACION_SOLAR_KWH"),
            F.sum("AUTOCONSUMO_SOLAR_KWH").alias("AUTOCONSUMO_SOLAR_KWH"),
            F.sum("EXCEDENTE_SOLAR_KWH").alias("EXCEDENTE_SOLAR_KWH"),
            F.sum("ENERGIA_REMANENTE_KWH").alias("ENERGIA_REMANENTE_KWH"),
            F.sum("ENERGIA_NO_VENDIDA_KWH").alias("ENERGIA_NO_VENDIDA_KWH"),
            F.max("POTENCIA_SOLAR_PICO_KW").alias(
                "POTENCIA_SOLAR_PICO_CLIENTE_KW"
            ),
        )
    )

    return resumen.select(
        "ESCENARIO",
        "NISE",
        "PLAN",
        "POTENCIA_SOLAR_PICO_CLIENTE_KW",
        "CONSUMO_BASE_KWH",
        "GENERACION_SOLAR_KWH",
        "AUTOCONSUMO_SOLAR_KWH",
        "EXCEDENTE_SOLAR_KWH",
        "ENERGIA_REMANENTE_KWH",
        "ENERGIA_NO_VENDIDA_KWH",
        F.round(
            F.col("ENERGIA_NO_VENDIDA_KWH") / F.col("CONSUMO_BASE_KWH") * 100,
            2,
        ).alias("PORCENTAJE_REDUCCION_VENTAS"),
        F.round(
            F.col("AUTOCONSUMO_SOLAR_KWH") / F.col("GENERACION_SOLAR_KWH") * 100,
            2,
        ).alias("PORCENTAJE_AUTOCONSUMO"),
        F.round(
            F.col("EXCEDENTE_SOLAR_KWH") / F.col("GENERACION_SOLAR_KWH") * 100,
            2,
        ).alias("PORCENTAJE_EXCEDENTE"),
    )


@dp.materialized_view(
    name="impacto_solar_q3_global",
    comment=(
        "Resumen global de impacto solar: clientes simulados, potencia "
        "total, energia base, generacion, autoconsumo, excedente y "
        "reduccion de ventas en GWh."
    ),
)
def impacto_solar_q3_global():
    """
    Agregacion global de todos los clientes simulados.
    """
    cli_resumen = spark.read.table(
        f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.impacto_solar_q3_cliente_resumen"
    )

    global_row = (
        cli_resumen.groupBy("ESCENARIO")
        .agg(
            F.countDistinct("NISE").alias("CLIENTES_SIMULADOS"),
            F.sum("POTENCIA_SOLAR_PICO_CLIENTE_KW").alias(
                "POTENCIA_FOTOVOLTAICA_TOTAL_KW"
            ),
            F.sum("CONSUMO_BASE_KWH").alias("CONSUMO_BASE_KWH"),
            F.sum("GENERACION_SOLAR_KWH").alias("GENERACION_SOLAR_KWH"),
            F.sum("AUTOCONSUMO_SOLAR_KWH").alias("AUTOCONSUMO_SOLAR_KWH"),
            F.sum("EXCEDENTE_SOLAR_KWH").alias("EXCEDENTE_SOLAR_KWH"),
            F.sum("ENERGIA_REMANENTE_KWH").alias("ENERGIA_REMANENTE_KWH"),
            F.sum("ENERGIA_NO_VENDIDA_KWH").alias("ENERGIA_NO_VENDIDA_KWH"),
        )
    )

    return global_row.select(
        "ESCENARIO",
        "CLIENTES_SIMULADOS",
        F.round(F.col("POTENCIA_FOTOVOLTAICA_TOTAL_KW") / 1000, 3).alias(
            "POTENCIA_FOTOVOLTAICA_TOTAL_MW"
        ),
        F.round(F.col("CONSUMO_BASE_KWH") / 1e6, 3).alias("CONSUMO_BASE_GWH"),
        F.round(F.col("GENERACION_SOLAR_KWH") / 1e6, 3).alias(
            "GENERACION_SOLAR_GWH"
        ),
        F.round(F.col("AUTOCONSUMO_SOLAR_KWH") / 1e6, 3).alias(
            "AUTOCONSUMO_SOLAR_GWH"
        ),
        F.round(F.col("EXCEDENTE_SOLAR_KWH") / 1e6, 3).alias("EXCEDENTE_SOLAR_GWH"),
        F.round(F.col("ENERGIA_NO_VENDIDA_KWH") / 1e6, 3).alias(
            "ENERGIA_NO_VENDIDA_GWH"
        ),
        F.round(F.col("ENERGIA_REMANENTE_KWH") / 1e6, 3).alias(
            "ENERGIA_REMANENTE_GWH"
        ),
        F.round(
            F.col("ENERGIA_NO_VENDIDA_KWH") / F.col("CONSUMO_BASE_KWH") * 100,
            2,
        ).alias("PORCENTAJE_REDUCCION_VENTAS"),
    )


@dp.materialized_view(
    name="impacto_solar_q3_curva_pato",
    comment=(
        "Curva de pato agregada: demanda base, generacion solar, "
        "autoconsumo, demanda neta y excedente por MES x TIPO_DIA x BLOQUE_15M."
    ),
)
def impacto_solar_q3_curva_pato():
    """
    Curva de pato: agregacion de todos los clientes por bloque de 15 min.
    CURVA_NETA = CURVA_BASE - AUTOCONSUMO_SOLAR
    """
    intervalo = spark.read.table(
        f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.impacto_solar_q3_intervalo"
    )

    return (
        intervalo.groupBy("MES", "TIPO_DIA", "BLOQUE_15M", "HORA", "MINUTO")
        .agg(
            # Curva de dia tipico promedio (energia); CURVA_BASE_Q3 para picos
            F.sum("DEMANDA_PROM_KWH").alias("CURVA_BASE"),
            F.sum("DEMANDA_Q3_KWH").alias("CURVA_BASE_Q3"),
            F.sum("GENERACION_SOLAR_SIMULADA_KWH").alias("GENERACION_SOLAR"),
            F.sum("AUTOCONSUMO_SOLAR_KWH").alias("AUTOCONSUMO_SOLAR"),
            F.sum("EXCEDENTE_SOLAR_KWH").alias("EXCEDENTE_SOLAR"),
        )
        .withColumn(
            "CURVA_NETA",
            F.col("CURVA_BASE") - F.col("AUTOCONSUMO_SOLAR"),
        )
        .select(
            "MES",
            "TIPO_DIA",
            "BLOQUE_15M",
            "HORA",
            "MINUTO",
            "CURVA_BASE",
            "CURVA_BASE_Q3",
            "GENERACION_SOLAR",
            "AUTOCONSUMO_SOLAR",
            "CURVA_NETA",
            "EXCEDENTE_SOLAR",
        )
    )