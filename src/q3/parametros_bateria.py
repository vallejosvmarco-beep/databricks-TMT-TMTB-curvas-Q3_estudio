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

Escenarios incluidos (7 escenarios para analisis de sensibilidad):
  - BATERIA_2_5:   2.5 kWh, 1.25 kW carga/descarga
  - BATERIA_5:     5.0 kWh, 2.5  kW carga/descarga
  - BATERIA_7_5:   7.5 kWh, 3.75 kW carga/descarga
  - BATERIA_10:   10.0 kWh, 5.0  kW carga/descarga
  - BATERIA_15:   15.0 kWh, 7.5  kW carga/descarga
  - BATERIA_20:   20.0 kWh, 10.0 kW carga/descarga
  - BATERIA_30:   30.0 kWh, 15.0 kW carga/descarga

Estrategia por defecto: AUTOCONSUMO_MAX (maximiza autoconsumo solar)
Potencia = Capacidad / 2 (C-rate 0.5) en todos los escenarios
"""

BATTERY_SCHEMA = StructType([
    StructField("ESCENARIO", StringType(), False),
    StructField("CAPACIDAD_KWH", DoubleType(), False),
    StructField("POTENCIA_CARGA_KW", DoubleType(), False),
    StructField("POTENCIA_DESCARGA_KW", DoubleType(), False),
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
    ("BATERIA_2_5",   2.5, 1.25, 1.25, 0.95, 0.95, 0.10, 0.90, 0.50, 0.001, "AUTOCONSUMO_MAX", True),
    ("BATERIA_5",     5.0, 2.5,  2.5,  0.95, 0.95, 0.10, 0.90, 0.50, 0.001, "AUTOCONSUMO_MAX", True),
    ("BATERIA_7_5",   7.5, 3.75, 3.75, 0.95, 0.95, 0.10, 0.90, 0.50, 0.001, "AUTOCONSUMO_MAX", True),
    ("BATERIA_10",   10.0, 5.0,  5.0,  0.95, 0.95, 0.10, 0.90, 0.50, 0.001, "AUTOCONSUMO_MAX", True),
    ("BATERIA_15",   15.0, 7.5,  7.5,  0.95, 0.95, 0.10, 0.90, 0.50, 0.001, "AUTOCONSUMO_MAX", True),
    ("BATERIA_20",   20.0, 10.0, 10.0, 0.95, 0.95, 0.10, 0.90, 0.50, 0.001, "AUTOCONSUMO_MAX", True),
    ("BATERIA_30",   30.0, 15.0, 15.0, 0.95, 0.95, 0.10, 0.90, 0.50, 0.001, "AUTOCONSUMO_MAX", True),
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
