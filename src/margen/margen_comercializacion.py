from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

"""
Estudio de margen de comercializacion por cliente TMT/TMTB: que clientes le dejan
mas ganancia a CNFL con su consumo actual (sin solar ni baterias).

Margen = ingreso por venta - costo de compra, con datos MEDIDOS (plata_950_carga):
  Ingreso (tarifa del cliente, consolidado_tarifas_28102026.pdf, incluye CVG)
    - Energia: kWh por franja x tarifa de energia del plan.
    - Potencia: kW maximo del mes por franja (kWh 15 min x 4) x tarifa de potencia;
      la punta solo cuenta dias laborales (sabado, domingo y feriado no).
    Validado contra la facturacion oficial (tablas tmt/tmtb): mediana por cliente
    = 1.000 en energia y potencia (junio 2026) -> tarifas SIN calibrar.
  Costo (compra de CNFL al ICE, tarifa T-CB, segun las FACTURAS mensuales del ICE;
  tabla compra_ice_factura de facturas_ice.py; el CVG ya viene en los precios)
    - Energia: kWh por franja x precio de energia de la factura del mes (monto / kWh).
    - Transmision: kWh x (transmision variable G + cargo fijo H) / kWh total facturado
      del mes; el fijo se reparte por kWh. Se guardan por separado ambos costos.
    - Potencia: kW del cliente en el instante del pico que FACTURA EL ICE por mes y franja
      x precio de demanda de la factura (monto / kW). Si un mes no tiene factura se usa
      el pico PME (suma de puntos Subestacion) y las tarifas de tarifas_franja_estudio.
      PME marca inicio de intervalo: instante AMI = FECHA_LECTURA PME + 15 min
      (correlacion 0.996 contra la suma AMI, verificado 2026-11-23).
  Medicion neta (clientes con inyeccion): el kWh inyectado evita compra en su franja
    (energia + transmision variable; el cargo fijo no se evita) y se pierde su venta cuando el cliente retira el credito
    (tarifa media de energia del cliente - tarifa de acceso T-A). Mismo supuesto que
    el escenario E1 del estudio financiero.

Tarifas y parametros: tablas tarifas_franja_estudio y parametros_estudio_financiero
(pipeline TMT_TMTB_Solar_Financiero), una sola fuente de tarifas para el bundle.
No incluye costos de distribucion, perdidas, cobranza ni cargos que son traslado
(impuestos, alumbrado publico).
"""

SRC_CAT = spark.conf.get("project.source_catalog")
SRC_SCH = spark.conf.get("project.source_schema_950")
SRC_TAB = spark.conf.get("project.source_table_950")
OUT_CAT = spark.conf.get("project.output_catalog")
OUT_SCH = spark.conf.get("project.output_schema")
PERIODO_INICIO = spark.conf.get("project.periodo_inicio")
PERIODO_FIN = spark.conf.get("project.periodo_fin")
PME_FQN = spark.conf.get("project.pme_tabla")

SOURCE_FQN = f"{SRC_CAT}.{SRC_SCH}.{SRC_TAB}"
CALENDARIO_FQN = f"{SRC_CAT}.{SRC_SCH}.ref_calendario"
PLANES = ["TMT", "TMTB"]
CONSUMO = "Consumo_Activo_kWh"
INYECCION = "Inyeccion_Activo_kWh"
# Umbral de completitud de un mes (dias con datos / dias del mes)
COMPLETITUD_MIN = 0.9


def T(nombre):
    return f"{OUT_CAT}.{OUT_SCH}.{nombre}"


def bloque_col(ts):
    """Bloque 15 min con etiqueta de fin de intervalo (igual que curvas_q3)."""
    return F.hour(ts) * 4 + F.floor(F.minute(ts) / 15)


def franja_col(bloque):
    """SLICE_PUNTA = 41-50 y 71-80; SLICE_VALLE = 25-40 y 51-70; resto NOCHE."""
    return (
        F.when(bloque.between(41, 50) | bloque.between(71, 80), F.lit("PUNTA"))
        .when(bloque.between(25, 40) | bloque.between(51, 70), F.lit("VALLE"))
        .otherwise(F.lit("NOCHE"))
    )


def cuenta_para_potencia():
    """La potencia de punta solo se mide en dias laborales."""
    return ~((F.col("FRANJA") == "PUNTA") & (F.col("TIPO_DIA") != "laboral"))


def _lecturas():
    """Lecturas de 15 min por NISE (suma de medidores), consumo e inyeccion."""
    return (
        spark.read.table(SOURCE_FQN)
        .filter(
            F.col("PLAN").isin(PLANES)
            & F.col("NISE").isNotNull()
            & F.col("OPERACION").isin(CONSUMO, INYECCION)
            & (F.col("FECHA") >= F.lit(PERIODO_INICIO).cast("date"))
            & (F.col("FECHA") <= F.lit(PERIODO_FIN).cast("date"))
        )
        .groupBy("NISE", "PLAN", "OPERACION", "FECHA", "TIPO_DIA", "FECHA_LECTURA")
        .agg(F.sum("ENERGIA").alias("ENERGIA"))
        .withColumn("PERIODO_MES", F.trunc("FECHA", "month"))
        .withColumn("BLOQUE_15M", bloque_col(F.col("FECHA_LECTURA")))
        .withColumn("FRANJA", franja_col(F.col("BLOQUE_15M")))
        .withColumn("KW", F.col("ENERGIA") * 4)
    )


# ---------------------------------------------------------------------------
# 1. Pico del sistema (proxy de la demanda de compra de CNFL)
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name="margen_pico_sistema",
    comment=(
        "Instante del pico usado para la potencia de compra por mes y franja: el que factura "
        "el ICE (FUENTE_PICO = factura_ICE); si falta la factura, el pico PME (suma de puntos "
        "Subestacion, punta solo dias laborales). INSTANTE_PICO en convencion AMI (fin de "
        "intervalo). Incluye el pico PME y su razon contra el kW facturado."
    ),
)
def margen_pico_sistema():
    cal = spark.read.table(CALENDARIO_FQN).select(
        F.col("FECHA").alias("FECHA_CURVA"), "TIPO_DIA")
    pme = (
        spark.read.table(PME_FQN)
        .filter(F.col("TIPO_PUNTO") == "Subestacion")
        .withColumn("INSTANTE", F.col("FECHA_LECTURA") + F.expr("INTERVAL 15 MINUTES"))
        .groupBy("INSTANTE")
        .agg(F.sum("ENERGIA_ACTIVA").alias("KWH_SISTEMA"),
             F.countDistinct("CIRCUITO").alias("PUNTOS_PME"))
        # Dia de la curva: el instante 00:00 es el ultimo intervalo del dia anterior
        .withColumn("FECHA_CURVA", F.to_date(F.col("INSTANTE") - F.expr("INTERVAL 15 MINUTES")))
        .filter(
            (F.col("FECHA_CURVA") >= F.lit(PERIODO_INICIO).cast("date"))
            & (F.col("FECHA_CURVA") <= F.lit(PERIODO_FIN).cast("date"))
        )
        .join(cal, "FECHA_CURVA", "inner")
        .withColumn("PERIODO_MES", F.trunc("FECHA_CURVA", "month"))
        .withColumn("BLOQUE_15M", bloque_col(F.col("INSTANTE")))
        .withColumn("FRANJA", franja_col(F.col("BLOQUE_15M")))
        .filter(cuenta_para_potencia())
    )
    w = Window.partitionBy("PERIODO_MES", "FRANJA").orderBy(F.col("KWH_SISTEMA").desc())
    pico_pme = (
        pme.withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .select(
            "PERIODO_MES", "FRANJA",
            F.col("INSTANTE").alias("INSTANTE_PICO_PME"),
            (F.col("KWH_SISTEMA") * 4 / 1000).alias("MW_PME"),
            "PUNTOS_PME",
        )
    )
    # Pico que factura el ICE (convencion AMI): es el que se usa; el PME queda de respaldo
    # (mes sin factura) y para comparar.
    ice = (
        spark.read.table("compra_ice_factura")
        .filter((F.col("PERIODO_MES") >= F.trunc(F.lit(PERIODO_INICIO).cast("date"), "month"))
                & (F.col("PERIODO_MES") <= F.lit(PERIODO_FIN).cast("date")))
        .select("PERIODO_MES", "FRANJA",
                F.col("INSTANTE_PICO_AMI").alias("INSTANTE_PICO_ICE"),
                (F.col("KW_FACTURA") / 1000).alias("MW_FACTURA_ICE"))
    )
    return (
        ice.join(pico_pme, ["PERIODO_MES", "FRANJA"], "full")
        .withColumn("INSTANTE_PICO", F.coalesce("INSTANTE_PICO_ICE", "INSTANTE_PICO_PME"))
        .withColumn("FUENTE_PICO", F.when(F.col("INSTANTE_PICO_ICE").isNotNull(), F.lit("factura_ICE"))
                    .otherwise(F.lit("PME")))
        .withColumn("FECHA_CURVA", F.to_date(F.col("INSTANTE_PICO") - F.expr("INTERVAL 15 MINUTES")))
        .join(cal, "FECHA_CURVA", "left")
        .select(
            "PERIODO_MES", "FRANJA", "INSTANTE_PICO",
            bloque_col(F.col("INSTANTE_PICO")).alias("BLOQUE_PICO"),
            F.col("TIPO_DIA").alias("TIPO_DIA_PICO"),
            "FUENTE_PICO", "MW_FACTURA_ICE",
            "INSTANTE_PICO_PME", "MW_PME", "PUNTOS_PME",
            (F.col("INSTANTE_PICO_PME") == F.col("INSTANTE_PICO_ICE")).alias("MISMO_INSTANTE_PME"),
            (F.col("MW_PME") / F.col("MW_FACTURA_ICE")).alias("RAZON_PME_FACTURA"),
        )
    )


# ---------------------------------------------------------------------------
# 2. Margen por cliente, mes y franja
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name="margen_cliente_franja_mes",
    comment=(
        "Por NISE x mes x franja: kWh consumidos e inyectados, kW facturado (maximo), kW "
        "coincidente con el pico del sistema, ingreso de venta y costo de compra (CRC)."
    ),
)
def margen_cliente_franja_mes():
    lec = _lecturas()
    cons = lec.filter(F.col("OPERACION") == CONSUMO)
    claves = ["NISE", "PLAN", "PERIODO_MES", "FRANJA"]

    energia = lec.groupBy(*claves).agg(
        F.sum(F.when(F.col("OPERACION") == CONSUMO, F.col("ENERGIA"))).alias("KWH"),
        F.sum(F.when(F.col("OPERACION") == INYECCION, F.col("ENERGIA"))).alias("KWH_INYECCION"),
        F.count(F.when(F.col("OPERACION") == CONSUMO, True)).alias("LECTURAS"),
    )
    potencia = cons.filter(cuenta_para_potencia()).groupBy(*claves).agg(
        F.max("KW").alias("KW_FACTURADO"))

    pico = spark.read.table("margen_pico_sistema").select(
        "PERIODO_MES", "FRANJA", "INSTANTE_PICO", "BLOQUE_PICO")
    coinc = (
        cons.join(pico, ["PERIODO_MES", "FRANJA"])
        .filter(F.col("FECHA_LECTURA") == F.col("INSTANTE_PICO"))
        .groupBy(*claves).agg(F.sum("KW").alias("KW_COINCIDENTE_EXACTO"))
    )
    # Respaldo si el cliente no tiene lectura en el instante pico: su kW promedio en el
    # bloque del pico, dias laborales del mismo mes (el pico del sistema es laboral).
    respaldo = (
        cons.filter(F.col("TIPO_DIA") == "laboral")
        .join(pico, ["PERIODO_MES", "FRANJA"])
        .filter(F.col("BLOQUE_15M") == F.col("BLOQUE_PICO"))
        .groupBy(*claves).agg(F.avg("KW").alias("KW_COINCIDENTE_PROMEDIO"))
    )

    tar = spark.read.table(T("tarifas_franja_estudio")).select(
        "PLAN", "FRANJA", "TARIFA_VENTA_ENERGIA", "TARIFA_VENTA_POTENCIA",
        F.col("TARIFA_COMPRA_ENERGIA").alias("TCE_ESTUDIO"),
        F.col("TARIFA_COMPRA_POTENCIA").alias("TCP_ESTUDIO"),
        F.col("PEAJE_TRANSMISION").alias("PEAJE_ESTUDIO"))
    # Compra segun la factura del ICE del mes (precio = monto / cantidad)
    compra = spark.read.table("compra_ice_factura").select(
        "PERIODO_MES", "FRANJA",
        F.col("TARIFA_COMPRA_ENERGIA").alias("TCE_FACTURA"),
        F.col("TARIFA_COMPRA_POTENCIA").alias("TCP_FACTURA"),
        "PEAJE_VARIABLE_KWH", "PEAJE_FIJO_KWH")

    base = (
        energia.join(potencia, claves, "left")
        .join(coinc, claves, "left")
        .join(respaldo, claves, "left")
        .join(tar, ["PLAN", "FRANJA"], "left")
        .join(compra, ["PERIODO_MES", "FRANJA"], "left")
        .withColumn("FUENTE_TARIFA_COMPRA",
                    F.when(F.col("TCE_FACTURA").isNotNull(), F.lit("factura_ICE"))
                    .otherwise(F.lit("tarifas_franja_estudio")))
        .withColumn("TARIFA_COMPRA_ENERGIA", F.coalesce("TCE_FACTURA", "TCE_ESTUDIO"))
        .withColumn("TARIFA_COMPRA_POTENCIA",
                    F.coalesce("TCP_FACTURA",
                               F.when(F.col("TCE_FACTURA").isNotNull(), F.lit(0.0)),
                               "TCP_ESTUDIO"))
        .withColumn("PEAJE_TRANSMISION_VARIABLE", F.coalesce("PEAJE_VARIABLE_KWH", "PEAJE_ESTUDIO"))
        .withColumn("PEAJE_TRANSMISION_FIJO", F.coalesce("PEAJE_FIJO_KWH", F.lit(0.0)))
        .withColumn("PEAJE_TRANSMISION",
                    F.col("PEAJE_TRANSMISION_VARIABLE") + F.col("PEAJE_TRANSMISION_FIJO"))
        .join(pico.drop("BLOQUE_PICO"), ["PERIODO_MES", "FRANJA"], "left")
        .withColumn("SIN_LECTURA_EN_PICO",
                    F.col("KW_COINCIDENTE_EXACTO").isNull() & (F.col("LECTURAS") > 0))
        .withColumn("FUENTE_COINCIDENTE",
                    F.when(F.col("KW_COINCIDENTE_EXACTO").isNotNull(), F.lit("instante_pico"))
                    .when(F.col("KW_COINCIDENTE_PROMEDIO").isNotNull(), F.lit("promedio_bloque_pico"))
                    .otherwise(F.lit("sin_datos")))
        .withColumn("KW_COINCIDENTE",
                    F.coalesce("KW_COINCIDENTE_EXACTO", "KW_COINCIDENTE_PROMEDIO", F.lit(0.0)))
        .fillna(0.0, subset=["KWH", "KWH_INYECCION", "KW_FACTURADO"])
    )
    return base.select(
        *claves, "LECTURAS", "INSTANTE_PICO", "SIN_LECTURA_EN_PICO", "FUENTE_COINCIDENTE",
        "KWH", "KWH_INYECCION", "KW_FACTURADO", "KW_COINCIDENTE",
        "TARIFA_VENTA_ENERGIA", "TARIFA_VENTA_POTENCIA",
        "FUENTE_TARIFA_COMPRA", "TARIFA_COMPRA_ENERGIA", "TARIFA_COMPRA_POTENCIA",
        "PEAJE_TRANSMISION_VARIABLE", "PEAJE_TRANSMISION_FIJO", "PEAJE_TRANSMISION",
        (F.col("KWH") * F.col("TARIFA_VENTA_ENERGIA")).alias("INGRESO_ENERGIA_CRC"),
        (F.col("KW_FACTURADO") * F.col("TARIFA_VENTA_POTENCIA")).alias("INGRESO_POTENCIA_CRC"),
        (F.col("KWH") * F.col("TARIFA_COMPRA_ENERGIA")).alias("COSTO_ENERGIA_CRC"),
        (F.col("KWH") * F.col("PEAJE_TRANSMISION_VARIABLE")).alias("COSTO_TRANSMISION_VARIABLE_CRC"),
        (F.col("KWH") * F.col("PEAJE_TRANSMISION_FIJO")).alias("COSTO_TRANSMISION_FIJO_CRC"),
        (F.col("KWH") * F.col("PEAJE_TRANSMISION")).alias("COSTO_PEAJE_CRC"),
        (F.col("KW_COINCIDENTE") * F.col("TARIFA_COMPRA_POTENCIA")).alias("COSTO_POTENCIA_CRC"),
        (F.col("KWH_INYECCION") * (F.col("TARIFA_COMPRA_ENERGIA") + F.col("PEAJE_TRANSMISION_VARIABLE")))
        .alias("COMPRA_EVITADA_INYECCION_CRC"),
    )


# ---------------------------------------------------------------------------
# 3. Margen por cliente y mes
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name="margen_cliente_mes",
    comment=(
        "Margen de comercializacion por NISE x mes (CRC): ingreso, costo de compra, ajuste "
        "de medicion neta, margen bruto y neto, completitud de datos y factores de carga "
        "y coincidencia."
    ),
)
def margen_cliente_mes():
    fm = spark.read.table("margen_cliente_franja_mes")
    ta = (spark.read.table(T("parametros_estudio_financiero"))
          .filter(F.col("PARAMETRO") == "TARIFA_ACCESO_T_A")
          .select(F.col("VALOR").alias("TARIFA_ACCESO_T_A")))
    lec = _lecturas().filter(F.col("OPERACION") == CONSUMO)
    dias = lec.groupBy("NISE", "PERIODO_MES").agg(
        F.countDistinct("FECHA").alias("DIAS_CON_DATOS"),
        F.max("KW").alias("KW_MAXIMO_MES"))
    atributos = (
        spark.read.table(SOURCE_FQN)
        .filter(F.col("PLAN").isin(PLANES)
                & (F.col("FECHA") >= F.lit(PERIODO_INICIO).cast("date"))
                & (F.col("FECHA") <= F.lit(PERIODO_FIN).cast("date")))
        .groupBy("NISE")
        .agg(F.max("DESC_SECTOR").alias("DESC_SECTOR"),
             F.max(F.col("TIENE_GD").cast("int")).alias("TIENE_GD"),
             F.max("ESTADO_SERVICIO").alias("ESTADO_SERVICIO"))
    )
    s = lambda c: F.sum(c).alias(c)  # noqa: E731
    pf = lambda fr, c: F.sum(F.when(F.col("FRANJA") == fr, F.col(c)))  # noqa: E731
    m = fm.groupBy("NISE", "PLAN", "PERIODO_MES").agg(
        s("KWH"), s("KWH_INYECCION"),
        pf("PUNTA", "KWH").alias("KWH_PUNTA"), pf("VALLE", "KWH").alias("KWH_VALLE"),
        pf("NOCHE", "KWH").alias("KWH_NOCHE"),
        pf("PUNTA", "KW_FACTURADO").alias("KW_FACTURADO_PUNTA"),
        pf("VALLE", "KW_FACTURADO").alias("KW_FACTURADO_VALLE"),
        pf("NOCHE", "KW_FACTURADO").alias("KW_FACTURADO_NOCHE"),
        pf("PUNTA", "KW_COINCIDENTE").alias("KW_COINCIDENTE_PUNTA"),
        pf("VALLE", "KW_COINCIDENTE").alias("KW_COINCIDENTE_VALLE"),
        F.max(F.col("SIN_LECTURA_EN_PICO").cast("int")).alias("FLAG_SIN_LECTURA_EN_PICO"),
        s("INGRESO_ENERGIA_CRC"), s("INGRESO_POTENCIA_CRC"),
        s("COSTO_ENERGIA_CRC"), s("COSTO_PEAJE_CRC"), s("COSTO_POTENCIA_CRC"),
        s("COSTO_TRANSMISION_VARIABLE_CRC"), s("COSTO_TRANSMISION_FIJO_CRC"),
        s("COMPRA_EVITADA_INYECCION_CRC"),
    )
    ingreso = F.col("INGRESO_ENERGIA_CRC") + F.col("INGRESO_POTENCIA_CRC")
    costo = F.col("COSTO_ENERGIA_CRC") + F.col("COSTO_PEAJE_CRC") + F.col("COSTO_POTENCIA_CRC")
    tarifa_media = F.when(F.col("KWH") > 0, F.col("INGRESO_ENERGIA_CRC") / F.col("KWH"))
    venta_perdida = F.col("KWH_INYECCION") * F.greatest(
        F.coalesce(tarifa_media, F.lit(0.0)) - F.col("TARIFA_ACCESO_T_A"), F.lit(0.0))
    dias_mes = F.dayofmonth(F.last_day("PERIODO_MES"))
    out = (
        m.join(dias, ["NISE", "PERIODO_MES"], "left")
        .join(atributos, "NISE", "left")
        .crossJoin(ta)
        .withColumn("DIAS_MES", dias_mes)
        .withColumn("COMPLETITUD", F.col("DIAS_CON_DATOS") / F.col("DIAS_MES"))
        .withColumn("MES_COMPLETO", F.col("COMPLETITUD") >= COMPLETITUD_MIN)
        .withColumn("INGRESO_TOTAL_CRC", ingreso)
        .withColumn("COSTO_TOTAL_CRC", costo)
        .withColumn("MARGEN_BRUTO_CRC", ingreso - costo)
        .withColumn("TARIFA_MEDIA_ENERGIA", tarifa_media)
        .withColumn("VENTA_PERDIDA_INYECCION_CRC", venta_perdida)
        .withColumn("AJUSTE_MEDICION_NETA_CRC",
                    F.col("COMPRA_EVITADA_INYECCION_CRC") - F.col("VENTA_PERDIDA_INYECCION_CRC"))
        .withColumn("MARGEN_NETO_CRC", F.col("MARGEN_BRUTO_CRC") + F.col("AJUSTE_MEDICION_NETA_CRC"))
        # Componentes que escalan por dia (energia) y por mes (cargos de potencia)
        .withColumn("MARGEN_ENERGIA_CRC",
                    F.col("INGRESO_ENERGIA_CRC") - F.col("COSTO_ENERGIA_CRC") - F.col("COSTO_PEAJE_CRC")
                    + F.col("AJUSTE_MEDICION_NETA_CRC"))
        .withColumn("MARGEN_POTENCIA_CRC", F.col("INGRESO_POTENCIA_CRC") - F.col("COSTO_POTENCIA_CRC"))
        .withColumn("MARGEN_PCT", F.when(ingreso > 0, F.col("MARGEN_NETO_CRC") / ingreso * 100))
        .withColumn("MARGEN_CRC_KWH", F.when(F.col("KWH") > 0, F.col("MARGEN_NETO_CRC") / F.col("KWH")))
        .withColumn("FACTOR_CARGA", F.when(
            F.col("KW_MAXIMO_MES") > 0,
            F.col("KWH") / (F.col("KW_MAXIMO_MES") * 24 * F.col("DIAS_CON_DATOS"))))
        .withColumn("FACTOR_COINCIDENCIA_PUNTA", F.when(
            F.col("KW_FACTURADO_PUNTA") > 0,
            F.col("KW_COINCIDENTE_PUNTA") / F.col("KW_FACTURADO_PUNTA")))
    )
    return out.drop("TARIFA_ACCESO_T_A")


# ---------------------------------------------------------------------------
# 4. Ranking de clientes en el periodo
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name="margen_cliente",
    comment=(
        "Ranking de clientes TMT/TMTB por margen neto anualizado (margen del periodo / dias "
        "con datos x 365). Incluye participacion, Pareto (segmento A = 80% del margen, B = "
        "hasta 95%, C = resto, NEGATIVO = margen < 0) y calidad de datos."
    ),
)
def margen_cliente():
    cm = spark.read.table("margen_cliente_mes")
    s = lambda c: F.sum(c).alias(c)  # noqa: E731
    c = cm.groupBy("NISE", "PLAN").agg(
        F.max("DESC_SECTOR").alias("DESC_SECTOR"),
        F.max("TIENE_GD").alias("TIENE_GD"),
        F.max("ESTADO_SERVICIO").alias("ESTADO_SERVICIO"),
        F.count("*").alias("MESES"),
        F.sum(F.col("MES_COMPLETO").cast("int")).alias("MESES_COMPLETOS"),
        s("DIAS_CON_DATOS"), s("DIAS_MES"),
        s("KWH"), s("KWH_PUNTA"), s("KWH_VALLE"), s("KWH_NOCHE"), s("KWH_INYECCION"),
        F.avg("KW_FACTURADO_PUNTA").alias("KW_FACTURADO_PUNTA_PROMEDIO"),
        F.avg("KW_COINCIDENTE_PUNTA").alias("KW_COINCIDENTE_PUNTA_PROMEDIO"),
        F.avg("FACTOR_CARGA").alias("FACTOR_CARGA"),
        F.avg("FACTOR_COINCIDENCIA_PUNTA").alias("FACTOR_COINCIDENCIA_PUNTA"),
        F.max("FLAG_SIN_LECTURA_EN_PICO").alias("FLAG_SIN_LECTURA_EN_PICO"),
        s("INGRESO_ENERGIA_CRC"), s("INGRESO_POTENCIA_CRC"), s("INGRESO_TOTAL_CRC"),
        s("COSTO_ENERGIA_CRC"), s("COSTO_PEAJE_CRC"), s("COSTO_POTENCIA_CRC"), s("COSTO_TOTAL_CRC"),
        s("COSTO_TRANSMISION_VARIABLE_CRC"), s("COSTO_TRANSMISION_FIJO_CRC"),
        s("MARGEN_BRUTO_CRC"), s("AJUSTE_MEDICION_NETA_CRC"), s("MARGEN_NETO_CRC"),
        s("MARGEN_ENERGIA_CRC"), s("MARGEN_POTENCIA_CRC"),
    )
    # Anualizacion: energia por dia con datos x 365; cargos de potencia (mensuales) x 12 / meses
    por_dia = 365 / F.col("DIAS_CON_DATOS")
    por_mes = 12 / F.col("MESES")
    anual = F.col("MARGEN_ENERGIA_CRC") * por_dia + F.col("MARGEN_POTENCIA_CRC") * por_mes
    c = (
        c.withColumn("COMPLETITUD", F.col("DIAS_CON_DATOS") / F.col("DIAS_MES"))
        .withColumn("MARGEN_NETO_ANUALIZADO_CRC", anual)
        .withColumn("INGRESO_ANUALIZADO_CRC",
                    F.col("INGRESO_ENERGIA_CRC") * por_dia + F.col("INGRESO_POTENCIA_CRC") * por_mes)
        .withColumn("KWH_ANUALIZADO", F.col("KWH") / F.col("DIAS_CON_DATOS") * 365)
        .withColumn("MARGEN_PCT", F.when(F.col("INGRESO_TOTAL_CRC") > 0,
                                         F.col("MARGEN_NETO_CRC") / F.col("INGRESO_TOTAL_CRC") * 100))
        .withColumn("MARGEN_CRC_KWH", F.when(F.col("KWH") > 0, F.col("MARGEN_NETO_CRC") / F.col("KWH")))
        .withColumn("DATOS_CONFIABLES", F.col("COMPLETITUD") >= COMPLETITUD_MIN)
    )
    w_all = Window.orderBy(F.col("MARGEN_NETO_ANUALIZADO_CRC").desc(), F.col("NISE"))
    w_plan = Window.partitionBy("PLAN").orderBy(F.col("MARGEN_NETO_ANUALIZADO_CRC").desc(), F.col("NISE"))
    w_tot = Window.partitionBy()
    # Ranking solo entre clientes con datos confiables (completitud >= 90%)
    w_conf = Window.partitionBy("DATOS_CONFIABLES").orderBy(
        F.col("MARGEN_NETO_ANUALIZADO_CRC").desc(), F.col("NISE"))
    positivo = F.greatest(F.col("MARGEN_NETO_ANUALIZADO_CRC"), F.lit(0.0))
    c = (
        c.withColumn("RANKING", F.row_number().over(w_all))
        .withColumn("RANKING_PLAN", F.row_number().over(w_plan))
        .withColumn("RANKING_CONFIABLE",
                    F.when(F.col("DATOS_CONFIABLES"), F.row_number().over(w_conf)))
        .withColumn("PARTICIPACION_MARGEN_PCT",
                    F.col("MARGEN_NETO_ANUALIZADO_CRC") / F.sum("MARGEN_NETO_ANUALIZADO_CRC").over(w_tot) * 100)
        .withColumn("PARETO_ACUMULADO_PCT",
                    F.sum(positivo).over(w_all.rowsBetween(Window.unboundedPreceding, Window.currentRow))
                    / F.sum(positivo).over(w_tot) * 100)
    )
    return c.withColumn(
        "SEGMENTO",
        F.when(F.col("MARGEN_NETO_ANUALIZADO_CRC") < 0, F.lit("NEGATIVO"))
        .when(F.col("PARETO_ACUMULADO_PCT") - F.col("PARTICIPACION_MARGEN_PCT") < 80, F.lit("A"))
        .when(F.col("PARETO_ACUMULADO_PCT") - F.col("PARTICIPACION_MARGEN_PCT") < 95, F.lit("B"))
        .otherwise(F.lit("C")),
    )


# ---------------------------------------------------------------------------
# 5. Validacion contra la facturacion oficial (tablas tmt / tmtb de AMI_base)
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name="margen_validacion_facturacion",
    comment=(
        "Ingreso calculado vs facturacion oficial TMT/TMTB por NISE y mes de lectura: kWh, "
        "monto de energia y de demanda, con razones calculado/facturado."
    ),
)
def margen_validacion_facturacion():
    cols = ["nis", "plan", "fecha_lectura", "consumo_energia", "monto_total_energia",
            "consumo_demanda_facturada", "monto_demanda", "valor_factura"]
    fac = (
        spark.read.table(f"{SRC_CAT}.{SRC_SCH}.tmt").select(*cols)
        .unionByName(spark.read.table(f"{SRC_CAT}.{SRC_SCH}.tmtb").select(*cols))
        .select(
            F.col("nis").cast("string").alias("NISE"),
            F.upper(F.col("plan")).alias("PLAN_FACTURA"),
            F.trunc(F.col("fecha_lectura").cast("date"), "month").alias("PERIODO_MES"),
            F.col("consumo_energia").cast("double").alias("KWH_FACTURADO"),
            F.col("monto_total_energia").cast("double").alias("MONTO_ENERGIA_FACTURADO"),
            F.col("consumo_demanda_facturada").cast("double").alias("KW_DEMANDA_FACTURADA"),
            F.col("monto_demanda").cast("double").alias("MONTO_DEMANDA_FACTURADO"),
            F.col("valor_factura").cast("double").alias("VALOR_FACTURA"),
        )
    )
    cm = spark.read.table("margen_cliente_mes").select(
        "NISE", "PERIODO_MES", "PLAN", "COMPLETITUD", "KWH",
        "INGRESO_ENERGIA_CRC", "INGRESO_POTENCIA_CRC")
    r = lambda a, b: F.when(F.col(b) > 0, F.col(a) / F.col(b))  # noqa: E731
    return (
        fac.join(cm, ["NISE", "PERIODO_MES"], "left")
        .withColumn("EN_AMI", F.col("KWH").isNotNull())
        .withColumn("RAZON_KWH", r("KWH", "KWH_FACTURADO"))
        .withColumn("RAZON_ENERGIA", r("INGRESO_ENERGIA_CRC", "MONTO_ENERGIA_FACTURADO"))
        .withColumn("RAZON_DEMANDA", r("INGRESO_POTENCIA_CRC", "MONTO_DEMANDA_FACTURADO"))
        .withColumn("COINCIDE_2PCT",
                    (F.abs(F.col("RAZON_ENERGIA") - 1) <= 0.02) & (F.abs(F.col("RAZON_DEMANDA") - 1) <= 0.02))
    )


# ---------------------------------------------------------------------------
# 6. Calidad (0 = OK)
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name="margen_calidad",
    comment="Validaciones del estudio de margen (FALLA 0 = OK, VALOR = detalle).",
)
def margen_calidad():
    pico = spark.read.table("margen_pico_sistema")
    cm = spark.read.table("margen_cliente_mes")
    cl = spark.read.table("margen_cliente")
    val = spark.read.table("margen_validacion_facturacion")

    def fila(nombre, valor, falla, detalle):
        return (valor.select(F.lit(nombre).alias("CHECK"), F.col("v").cast("double").alias("VALOR"),
                             F.when(falla, 1).otherwise(0).alias("FALLA"), F.lit(detalle).alias("DETALLE")))

    picos = pico.agg((F.countDistinct("PERIODO_MES") * 3 - F.count("*")).alias("v"))
    sin_pico = cm.agg(F.sum("FLAG_SIN_LECTURA_EN_PICO").alias("v"))
    suma = (cm.agg(F.sum("MARGEN_NETO_CRC").alias("a"))
            .crossJoin(cl.agg(F.sum("MARGEN_NETO_CRC").alias("b")))
            .select(F.abs(F.col("a") - F.col("b")).alias("v")))
    con_ami = val.filter("EN_AMI AND COMPLETITUD >= 0.9")
    med_e = con_ami.agg(F.percentile_approx("RAZON_ENERGIA", 0.5).alias("v"))
    med_d = con_ami.agg(F.percentile_approx("RAZON_DEMANDA", 0.5).alias("v"))
    sin_ami = val.agg(F.sum(F.when(~F.col("EN_AMI"), 1).otherwise(0)).alias("v"))
    negativos = cl.agg(F.sum(F.when(F.col("MARGEN_NETO_CRC") < 0, 1).otherwise(0)).alias("v"))

    # Facturas ICE: meses del estudio sin factura, cuadre y precios vs tarifas del estudio
    fac = spark.read.table("compra_ice_factura")
    meses_fac = fac.select("PERIODO_MES").distinct()
    sin_factura = (cm.select("PERIODO_MES").distinct()
                   .join(meses_fac, "PERIODO_MES", "left_anti").agg(F.count("*").alias("v")))
    cuadre = fac.agg(F.max(F.abs("CUADRE_DIF_CRC")).alias("v"))
    est = (spark.read.table(T("tarifas_franja_estudio"))
           .groupBy("FRANJA").agg(F.max("TARIFA_COMPRA_ENERGIA").alias("TCE"),
                                   F.max("TARIFA_COMPRA_POTENCIA").alias("TCP")))
    fac_est = fac.join(meses_fac.join(cm.select("PERIODO_MES").distinct(), "PERIODO_MES"),
                       "PERIODO_MES").join(est, "FRANJA", "left")
    dif_e = fac_est.agg(F.max(F.abs(F.col("TARIFA_COMPRA_ENERGIA") - F.col("TCE"))).alias("v"))
    dif_p = fac_est.agg(F.max(F.abs(F.coalesce("TARIFA_COMPRA_POTENCIA", F.lit(0.0))
                                    - F.coalesce("TCP", F.lit(0.0)))).alias("v"))
    con_ice = pico.filter(F.col("MW_FACTURA_ICE").isNotNull() & F.col("MW_PME").isNotNull())
    razon_pico = con_ice.agg(F.avg("RAZON_PME_FACTURA").alias("v"))
    mismo = con_ice.agg(F.sum(F.col("MISMO_INSTANTE_PME").cast("int")).alias("v"))
    pico_pme = pico.agg(F.sum(F.when(F.col("FUENTE_PICO") != "factura_ICE", 1).otherwise(0)).alias("v"))
    fuera_grilla = fac.agg(F.sum(F.col("HORA_FUERA_DE_GRILLA").cast("int")).alias("v"))
    return (
        fila("PICOS_SISTEMA_FALTANTES", picos, F.col("v") != 0, "mes x franja sin pico (factura ICE ni PME)")
        .unionByName(fila("FACTURAS_ICE_MESES_FALTANTES", sin_factura, F.col("v") > 0,
                          "meses del estudio sin factura ICE (usan pico PME y tarifas del estudio)"))
        .unionByName(fila("FACTURAS_ICE_CUADRE_MAX_DIF_CRC", cuadre, F.col("v") > 1,
                          "max |total - suma de componentes| por factura, CRC"))
        .unionByName(fila("FACTURA_VS_ESTUDIO_PRECIO_ENERGIA", dif_e, F.col("v") > 0.01,
                          "max |precio energia factura - tarifas_franja_estudio|, CRC/kWh"))
        .unionByName(fila("FACTURA_VS_ESTUDIO_PRECIO_POTENCIA", dif_p, F.col("v") > 0.01,
                          "max |precio demanda factura - tarifas_franja_estudio|, CRC/kW"))
        .unionByName(fila("PICO_PME_VS_FACTURA_RAZON_MW", razon_pico, F.lit(False),
                          "informativo: promedio MW pico PME / MW facturado ICE"))
        .unionByName(fila("PICO_PME_MISMO_INSTANTE_QUE_FACTURA", mismo, F.lit(False),
                          "informativo: mes x franja donde el pico PME coincide con el del ICE"))
        .unionByName(fila("PICOS_CON_RESPALDO_PME", pico_pme, F.col("v") > 0,
                          "mes x franja sin factura ICE que usan el pico PME"))
        .unionByName(fila("FACTURA_HORA_PICO_FUERA_DE_GRILLA", fuera_grilla, F.lit(False),
                          "informativo: hora de pico no multiplo de 15 min, redondeada (revisar PDF)"))
        .unionByName(fila("CLIENTE_MES_SIN_LECTURA_EN_PICO", sin_pico, F.lit(False),
                          "informativo: kW coincidente = 0 en alguna franja"))
        .unionByName(fila("SUMA_MARGEN_CLIENTE_VS_MES", suma, F.col("v") > 1, "CRC de diferencia"))
        .unionByName(fila("MEDIANA_RAZON_ENERGIA_FACTURA", med_e, F.abs(F.col("v") - 1) > 0.02,
                          "clientes con datos completos; 1 = igual a la factura"))
        .unionByName(fila("MEDIANA_RAZON_DEMANDA_FACTURA", med_d, F.abs(F.col("v") - 1) > 0.02,
                          "clientes con datos completos; 1 = igual a la factura"))
        .unionByName(fila("FACTURAS_SIN_DATOS_AMI", sin_ami, F.lit(False),
                          "informativo: NIS facturados que no estan en plata_950 ese mes"))
        .unionByName(fila("CLIENTES_MARGEN_NEGATIVO", negativos, F.lit(False), "informativo"))
    )
