from pyspark import pipelines as dp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType,
)

"""
Parametros de configuracion para escenarios de baterias.

Define escenarios de bateria con diferentes capacidades y caracteristicas.
Esta tabla sirve como input para la simulacion de impacto de baterias
sobre el impacto solar ya calculado.

Escenarios relativos al tamano de cada cliente (2026-11-20; antes 2.5-30 kWh fijos,
demasiado pequenos para clientes TMT/TMTB de cientos de kW):
  POTENCIA_KW  = FRACCION_POTENCIA_FV x kWp solar del cliente
  CAPACIDAD_KWH = POTENCIA_KW x HORAS_DURACION
  - BESS_25PCT_2H:  25% del kWp, 2 h
  - BESS_50PCT_2H:  50% del kWp, 2 h
  - BESS_50PCT_4H:  50% del kWp, 4 h
  - BESS_100PCT_2H: 100% del kWp, 2 h
  - BESS_100PCT_4H: 100% del kWp, 4 h

Eficiencias 95%/95%, SOC inicial 50% y reserva 10% (SOC_MIN) segun
estudio_financiero_solar/config.py.
Estrategia por defecto: AUTOCONSUMO_MAX (maximiza autoconsumo solar)
"""

BATTERY_SCHEMA = StructType([
    StructField("ESCENARIO", StringType(), False),
    StructField("FRACCION_POTENCIA_FV", DoubleType(), False),
    StructField("HORAS_DURACION", DoubleType(), False),
    StructField("EFICIENCIA_CARGA", DoubleType(), False),
    StructField("EFICIENCIA_DESCARGA", DoubleType(), False),
    StructField("SOC_MIN", DoubleType(), False),
    StructField("SOC_MAX", DoubleType(), False),
    StructField("SOC_INICIAL", DoubleType(), False),
    StructField("AUTODESCARGA_DIARIA", DoubleType(), False),
    StructField("ESTRATEGIA", StringType(), False),
    StructField("ACTIVO", BooleanType(), False),
])


BATTERY_SCENARIOS = [
    ("BESS_25PCT_2H",  0.25, 2.0, 0.95, 0.95, 0.10, 0.90, 0.50, 0.001, "AUTOCONSUMO_MAX", True),
    ("BESS_50PCT_2H",  0.50, 2.0, 0.95, 0.95, 0.10, 0.90, 0.50, 0.001, "AUTOCONSUMO_MAX", True),
    ("BESS_50PCT_4H",  0.50, 4.0, 0.95, 0.95, 0.10, 0.90, 0.50, 0.001, "AUTOCONSUMO_MAX", True),
    ("BESS_100PCT_2H", 1.00, 2.0, 0.95, 0.95, 0.10, 0.90, 0.50, 0.001, "AUTOCONSUMO_MAX", True),
    ("BESS_100PCT_4H", 1.00, 4.0, 0.95, 0.95, 0.10, 0.90, 0.50, 0.001, "AUTOCONSUMO_MAX", True),
]


@dp.materialized_view(
    name="parametros_bateria",
    comment=(
        "Parametros de configuracion para diferentes escenarios de "
        "baterias. Incluye capacidad, potencia, eficiencia, limites de "
        "SOC y estrategia de control."
    ),
)
def parametros_bateria():
    return spark.createDataFrame(BATTERY_SCENARIOS, schema=BATTERY_SCHEMA)
