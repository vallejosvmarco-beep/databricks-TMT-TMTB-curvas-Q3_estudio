from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

"""
Facturas de compra de energia del ICE a CNFL (tarifa T-CB), una por mes.

1. compra_ice_facturas_parseadas (streaming table): Auto Loader binaryFile sobre la carpeta
   de facturas; cada PDF se parsea UNA sola vez con ai_parse_document (v2.0). Agregar un PDF
   nuevo solo parsea ese archivo.
2. compra_ice_factura (MV, mes x franja): extrae del texto parseado kWh y monto de energia,
   kW, monto, dia y hora de la demanda maxima, transmision (variable G y cargo fijo H),
   CVG (informativo, ya incluido en los precios), otros cargos y el total; calcula precios
   unitarios (monto / cantidad), transmision por kWh del mes (variable + fijo repartido por
   kWh facturado) y el cuadre de la factura.

Hora de la factura: HORA_FACTURA_AJUSTE_MIN convierte la hora de la factura a la convencion
AMI (FECHA_LECTURA = fin del intervalo de 15 min). 0 = la factura ya marca el fin del
intervalo; 15 = marca el inicio. Verificado 2026-11-26: en 8 de 9 picos de punta (ene-oct
2026, abril ilegible) la curva PME Subestacion en convencion AMI tiene su maximo local justo
en la hora de la factura (en junio empata con +15 min) -> la hora marca el FIN -> 0.
Horas fuera de la grilla de 15 min (p. ej. 1211 en abril, lectura dudosa del PDF) se
redondean al cuarto de hora mas cercano y quedan marcadas en HORA_FUERA_DE_GRILLA.
"""

FACTURAS_PATH = spark.conf.get("project.facturas_ice_path")
HORA_FACTURA_AJUSTE_MIN = 0

FRANJAS = [("Punta", "PUNTA"), ("Valle", "VALLE"), ("Noche", "NOCHE")]


@dp.table(
    name="compra_ice_facturas_parseadas",
    comment=(
        "Facturas ICE -> CNFL (PDF) parseadas una sola vez con ai_parse_document v2.0 "
        "(incremental por archivo)."
    ),
)
def compra_ice_facturas_parseadas():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("pathGlobFilter", "*.pdf")
        .load(FACTURAS_PATH)
        .select(
            F.col("path").alias("RUTA"),
            F.regexp_extract(F.col("path"), r"([^/]+)$", 1).alias("ARCHIVO"),
            F.col("modificationTime").alias("FECHA_MODIFICACION"),
            F.col("length").alias("BYTES"),
            F.expr("ai_parse_document(content, map('version', '2.0'))").alias("PARSEO"),
            F.current_timestamp().alias("FECHA_INGESTA"),
        )
    )


def _num(txt, patron, grupo=1):
    """Numero con separador de miles ',' -> double (null si no hay coincidencia)."""
    return F.regexp_replace(F.regexp_extract(txt, patron, grupo), ",", "").cast("double")


def _monto(etiqueta):
    return r"" + etiqueta + r"[^:]*:[^0-9-]*(-?[0-9][0-9,]*\.?[0-9]*)"


@dp.materialized_view(
    name="compra_ice_factura",
    comment=(
        "Factura ICE -> CNFL por mes y franja: kWh, monto de energia, kW y monto de demanda, "
        "instante del pico facturado (y en convencion AMI), precios unitarios, transmision "
        "variable y cargo fijo (CRC/kWh del mes), CVG informativo, total y cuadre."
    ),
)
def compra_ice_factura():
    p = spark.read.table("compra_ice_facturas_parseadas")
    texto = F.expr(
        "concat_ws(' | ', transform(try_cast(PARSEO:document:elements AS ARRAY<VARIANT>), "
        "x -> cast(x:content AS STRING)))"
    )
    t = F.col("TEXTO")
    periodo = r"Per[ií]odo facturado\s*:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})\s*al\s*([0-9]{2}/[0-9]{2}/[0-9]{4})"
    filas = []
    for etiqueta, franja in FRANJAS:
        pe = etiqueta + r":</td><td>([0-9,.]+)</td><td>([0-9,.]+)</td></tr>"
        pd = etiqueta + r":</td><td>([0-9,.]+)</td><td>([0-9,.]+)</td><td>([0-9/]+)</td><td>([0-9]+)</td>"
        filas.append(F.struct(
            F.lit(franja).alias("FRANJA"),
            _num(t, pe, 1).alias("KWH_FACTURA"),
            _num(t, pe, 2).alias("MONTO_ENERGIA_CRC"),
            _num(t, pd, 1).alias("KW_FACTURA"),
            _num(t, pd, 2).alias("MONTO_DEMANDA_CRC"),
            F.regexp_extract(t, pd, 3).alias("DIA_PICO"),
            F.regexp_extract(t, pd, 4).alias("HORA_PICO"),
        ))
    mes = (
        p.withColumn("TEXTO", texto)
        .withColumn("PERIODO_DESDE", F.to_date(F.regexp_extract(t, periodo, 1), "dd/MM/yyyy"))
        .withColumn("PERIODO_HASTA", F.to_date(F.regexp_extract(t, periodo, 2), "dd/MM/yyyy"))
        .withColumn("PERIODO_MES", F.trunc("PERIODO_HASTA", "month"))
        .withColumn("NUMERO_FACTURA", F.regexp_extract(t, r"Factura\s*\n?\s*([0-9]{8,})", 1))
        .withColumn("TARIFA", F.regexp_extract(t, r"tarifa:\s*([A-Z]-[A-Z]+)", 1))
        .withColumn("TRANSMISION_VARIABLE_CRC", _num(t, _monto(r"Servicio de Transmisi[oó]n El[eé]ctrica")))
        .withColumn("CARGO_FIJO_TRANSMISION_CRC",
                    F.coalesce(_num(t, _monto(r"Cargo fijo Transmisi[oó]n El[eé]ctrica")), F.lit(0.0)))
        .withColumn("IMPUESTOS_CRC",
                    F.coalesce(_num(t, _monto(r"Impuesto (de Ventas|Valor Agregado)"), 2), F.lit(0.0))
                    + F.coalesce(_num(t, _monto(r"Impuesto de Bomberos")), F.lit(0.0))
                    + F.coalesce(_num(t, _monto(r"Alumbrado")), F.lit(0.0)))
        .withColumn("RECARGO_FP_CRC", F.coalesce(_num(t, _monto(r"Recargo por Bajo Factor de Potencia")), F.lit(0.0)))
        .withColumn("ALQUILERES_CRC", F.coalesce(_num(t, _monto(r"Alquileres Transformador")), F.lit(0.0)))
        .withColumn("OTROS_DEBITOS_CRC", F.coalesce(_num(t, _monto(r"Otros D[eé]bitos")), F.lit(0.0)))
        .withColumn("OTROS_CREDITOS_CRC", F.coalesce(_num(t, _monto(r"Otros Cr[eé]ditos")), F.lit(0.0)))
        .withColumn("CVG_ENERGIA_CRC", _num(t, _monto(r"Costo Variable Combustible Energ[ií]a")))
        .withColumn("CVG_DEMANDA_CRC", _num(t, _monto(r"Costo Variable Combustible Demanda")))
        .withColumn("MONTO_TOTAL_CRC", _num(t, r"Monto total en colones:[^0-9]*([0-9][0-9,]*\.?[0-9]*)"))
        .withColumn("f", F.explode(F.array(*filas)))
        .select("*", "f.*")
        .drop("f", "PARSEO", "TEXTO")
    )
    # Si un mes tiene mas de una factura (reemision), se usa el archivo mas reciente.
    w_mes = Window.partitionBy("PERIODO_MES").orderBy(F.col("FECHA_MODIFICACION").desc(), F.col("ARCHIVO").desc())
    w_fac = Window.partitionBy("ARCHIVO")
    instante = F.to_timestamp(
        F.concat_ws(" ", F.col("DIA_PICO"), F.lpad(F.col("HORA_PICO"), 4, "0")), "dd/MM/yyyy HHmm")
    return (
        mes.withColumn("_rn", F.dense_rank().over(w_mes))
        .filter("_rn = 1").drop("_rn")
        .withColumn("KWH_TOTAL_MES", F.sum("KWH_FACTURA").over(w_fac))
        .withColumn("MONTO_ENERGIA_TOTAL_CRC", F.sum("MONTO_ENERGIA_CRC").over(w_fac))
        .withColumn("MONTO_DEMANDA_TOTAL_CRC", F.sum("MONTO_DEMANDA_CRC").over(w_fac))
        .withColumn("INSTANTE_PICO_FACTURA", instante)
        .withColumn("HORA_FUERA_DE_GRILLA", F.minute("INSTANTE_PICO_FACTURA") % 15 != 0)
        .withColumn("INSTANTE_PICO_AMI",
                    F.timestamp_seconds(F.round(F.unix_timestamp("INSTANTE_PICO_FACTURA") / 900) * 900)
                    + F.expr(f"INTERVAL {HORA_FACTURA_AJUSTE_MIN} MINUTES"))
        .withColumn("TARIFA_COMPRA_ENERGIA",
                    F.when(F.col("KWH_FACTURA") > 0, F.col("MONTO_ENERGIA_CRC") / F.col("KWH_FACTURA")))
        .withColumn("TARIFA_COMPRA_POTENCIA",
                    F.when(F.col("KW_FACTURA") > 0, F.col("MONTO_DEMANDA_CRC") / F.col("KW_FACTURA")))
        .withColumn("PEAJE_VARIABLE_KWH", F.col("TRANSMISION_VARIABLE_CRC") / F.col("KWH_TOTAL_MES"))
        .withColumn("PEAJE_FIJO_KWH", F.col("CARGO_FIJO_TRANSMISION_CRC") / F.col("KWH_TOTAL_MES"))
        .withColumn("PEAJE_TRANSMISION", F.col("PEAJE_VARIABLE_KWH") + F.col("PEAJE_FIJO_KWH"))
        # Cuadre: total = energia + demanda + recargo FP + impuestos + transmision (G + H)
        # + alquileres + otros debitos - otros creditos. El CVG es informativo (ya en precios).
        .withColumn("CUADRE_DIF_CRC",
                    F.col("MONTO_TOTAL_CRC") - (
                        F.col("MONTO_ENERGIA_TOTAL_CRC") + F.col("MONTO_DEMANDA_TOTAL_CRC")
                        + F.col("RECARGO_FP_CRC") + F.col("IMPUESTOS_CRC")
                        + F.col("TRANSMISION_VARIABLE_CRC") + F.col("CARGO_FIJO_TRANSMISION_CRC")
                        + F.col("ALQUILERES_CRC") + F.col("OTROS_DEBITOS_CRC") - F.col("OTROS_CREDITOS_CRC")))
        .select(
            "PERIODO_MES", "FRANJA", "ARCHIVO", "NUMERO_FACTURA", "TARIFA",
            "PERIODO_DESDE", "PERIODO_HASTA",
            "KWH_FACTURA", "MONTO_ENERGIA_CRC", "TARIFA_COMPRA_ENERGIA",
            "KW_FACTURA", "MONTO_DEMANDA_CRC", "TARIFA_COMPRA_POTENCIA",
            "DIA_PICO", "HORA_PICO", "INSTANTE_PICO_FACTURA", "HORA_FUERA_DE_GRILLA", "INSTANTE_PICO_AMI",
            "KWH_TOTAL_MES", "TRANSMISION_VARIABLE_CRC", "CARGO_FIJO_TRANSMISION_CRC",
            "PEAJE_VARIABLE_KWH", "PEAJE_FIJO_KWH", "PEAJE_TRANSMISION",
            "CVG_ENERGIA_CRC", "CVG_DEMANDA_CRC", "RECARGO_FP_CRC", "IMPUESTOS_CRC",
            "ALQUILERES_CRC", "OTROS_DEBITOS_CRC", "OTROS_CREDITOS_CRC",
            "MONTO_ENERGIA_TOTAL_CRC", "MONTO_DEMANDA_TOTAL_CRC", "MONTO_TOTAL_CRC", "CUADRE_DIF_CRC",
            "FECHA_MODIFICACION",
        )
    )
