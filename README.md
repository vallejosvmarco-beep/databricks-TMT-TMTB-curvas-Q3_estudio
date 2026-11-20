# TMT/TMTB - Curvas Q3 e Impacto Solar Fotovoltaico

Paquete Databricks (Declarative Automation Bundle) con dos flujos analiticos:

1. **Curvas Q3 mensuales a 15 minutos** (pipeline existente)
2. **Impacto de generacion solar fotovoltaica** (nuevo)

Y dos dashboards AI/BI:

1. **Curvas Q3 Mensuales - Analisis Multidimensional**
2. **Impacto Solar - Curvas Q3**

---

## Configuracion inicial

### 1. Configurar el SQL Warehouse por nombre

Copiar la plantilla y editar el nombre del warehouse:

```bash
cp databricks.local.example.yml databricks.local.yml
```

Editar `databricks.local.yml`:

```yaml
variables:
  warehouse_id:
    lookup:
      warehouse: "MI SQL WAREHOUSE"
```

Databricks resuelve el ID del warehouse automaticamente por nombre.
El archivo `databricks.local.yml` esta en `.gitignore` y no se commitea.

Si el warehouse no existe o hay varios con el mismo nombre, `bundle validate` fallara con un error claro.

### 2. Validar el bundle

```bash
databricks bundle validate -t free_dev
```

### 3. Desplegar

```bash
databricks bundle deploy -t free_dev
```

Esto despliega:
- Pipeline Q3 (`TMT_TMTB_Q3_CURVAS`)
- Pipeline Solar (`TMT_TMTB_Q3_IMPACTO_SOLAR`)
- Dashboard Curvas Q3 (`Curvas Q3 Mensuales - Analisis Multidimensional`)
- Dashboard Solar (`Impacto Solar - Curvas Q3`)

Ambos dashboards se ubican en `/Shared/curvas-Q3/`.

### 4. Ejecutar pipelines

```bash
databricks bundle run q3_curvas -t free_dev
databricks bundle run impacto_solar_q3 -t free_dev
```

---

## Flujo 1: Curvas Q3

### Entrada
Consume una `plata_950_carga` ya existente:
- `source_catalog.source_schema_950.source_table_950`
- Default: `workspace.plata_analisis.plata_950_carga`

### Salida
`workspace.oro_q3.curvas_q3_mensuales`

Una fila = un punto Q3 mensual por:
`NISE + PLAN + OPERACION + PERIODO_MES + TIPO_DIA + BLOQUE_15M + METRICA`

### Unidades de datos (validadas por inspeccion)

| Columna | Unidad |
| --- | --- |
| Q3 (METRICA=ENERGIA) | kWh por intervalo de 15 min |
| Q3 (METRICA=DEMANDA) | kW promedio del intervalo |
| ENERGIA | = DEMANDA * 0.25 (confirmado) |

### OPERACIONES encontradas

| OPERACION | Significado |
| --- | --- |
| Consumo_Activo_kWh | Consumo de energia activa |
| Consumo_Reactivo_kVARh | Consumo de energia reactiva |
| Inyeccion_Activo_kWh | Inyeccion a la red (excedente solar) |
| Generacion_SGDA_Activo_kWh | Generacion solar total (SGDA) |
| Generacion_SGDA_Reactivo_kVARh | Generacion solar reactiva |

### TIPO_DIA
Se toma de la tabla comun `datosami.datos.ref_calendario` (definida en
`databricks-conocimiento_base_estudio/src/referencias/ref_calendario.sql`), unida por
`FECHA` = dia de la curva de `plata_950_carga`:
- `laboral`: lunes a viernes que no son feriado
- `fin_de_semana`: sabado, domingo o feriado (Codigo de Trabajo art. 148, con traslados de la Ley 9875)

HORA, MINUTO y BLOQUE_15M siguen siendo la estampilla de fin de intervalo de `FECHA_LECTURA`
(el intervalo 96, 00:00, queda como bloque 0 del mismo dia de la curva).

---

## Flujo 2: Impacto Solar FV

Responde: ?Cuant energia dejaria de vender la distribuidora si cada cliente elegible instalara generacion solar FV?

### Clasificacion de clientes

| Tipo | Criterio |
| --- | --- |
| SIN_SOLAR | Sin generacion solar (elegibles para simulacion) |
| SOLAR_EXISTENTE | Con Generacion_SGDA_Activo_kWh |
| INYECTA_RED | Con Inyeccion_Activo_kWh |

### Perfil solar de referencia

Se construye desde clientes con generacion SGDA medida, a granularidad:
`MES x TIPO_DIA x BLOQUE_15M`

- `FACTOR_SOLAR_NORMALIZADO = Q3_SOLAR_REFERENCIA / MAX(Q3_SOLAR_REFERENCIA)`
- Rango: 0 <= FACTOR_SOLAR_NORMALIZADO <= 1
- Pico solar determinado por los datos (no hardcodeado)

### Dimensionamiento por cliente

`POTENCIA_SOLAR_PICO_CLIENTE` = maxima demanda Q3 del cliente en el bloque de maxima generacion solar.
La generacion simulada no supera la demanda del cliente.

### Generacion solar simulada

`GENERACION_SOLAR_SIMULADA_KWH = POTENCIA_SOLAR_PICO_CLIENTE * FACTOR_SOLAR_NORMALIZADO`

Como ENERGIA ya esta en kWh por intervalo, NO se aplica conversion * 0.25.

### Formulas clave

```
AUTOCONSUMO_SOLAR_KWH   = LEAST(DEMANDA_Q3_KWH, GENERACION_SOLAR_SIMULADA_KWH)
DEMANDA_REMANENTE_KWH   = GREATEST(DEMANDA_Q3_KWH - GENERACION_SOLAR_SIMULADA_KWH, 0)
EXCEDENTE_SOLAR_KWH     = GREATEST(GENERACION_SOLAR_SIMULADA_KWH - DEMANDA_Q3_KWH, 0)
ENERGIA_NO_VENDIDA_KWH = AUTOCONSUMO_SOLAR_KWH  (regla fundamental)
```

### Escenario

- `ESCENARIO = 'ADOPCION_100'`: todos los clientes elegibles instalan su sistema.
- Arquitectura preparada para 10%, 25%, 50%, 75%, 100%.

### Tablas de salida

| Tabla | Descripcion |
| --- | --- |
| perfil_solar_q3_referencia | Perfil FV de referencia normalizado |
| impacto_solar_q3_intervalo | Simulacion por NISE x intervalo |
| impacto_solar_q3_cliente_mes | Resumen mensual por cliente |
| impacto_solar_q3_cliente_resumen | Resumen global por cliente |
| impacto_solar_q3_global | Resumen global en GWh/MW |
| impacto_solar_q3_curva_pato | Curva de pato agregada |
| impacto_solar_q3_calidad | Validaciones de calidad |

### Curva de pato

```
CURVA_NETA = CURVA_BASE - AUTOCONSUMO_SOLAR
```

Separada por MES x TIPO_DIA x BLOQUE_15M.

---

## Estructura del paquete

```text
databricks.yml
databricks.local.yml          (no versionado)
databricks.local.example.yml  (plantilla)
.gitignore
README.md

resources/
├── curvas_q3_dashboard.yml
├── impacto_solar_dashboard.yml
└── pipelines/
    ├── q3_curvas.pipeline.yml
    └── impacto_solar.pipeline.yml

src/q3/
├── curvas_q3.py
├── perfil_solar_q3.py
├── impacto_solar_q3.py
└── calidad_impacto_solar.py

sql/
├── 01_validar_fuente.sql
└── 02_validar_salida.sql

dashboards/
├── curvas_q3_src.json
└── impacto_solar_src.json
```

### Parametrizacion de dashboards

Los dashboards usan `dataset_catalog` y `dataset_schema` del bundle:

```yaml
dataset_catalog: ${var.output_catalog}
dataset_schema: ${var.output_schema}
```

Las queries SQL usan nombres de tabla sin cualificar (ej: `FROM curvas_q3_mensuales`),
funcionando en dev/test/prod cambiando solo la configuracion del bundle.

---

## Variables del bundle

| Variable | Default | Descripcion |
| --- | --- | --- |
| source_catalog | workspace | Catalogo de la tabla fuente |
| source_schema_950 | plata_analisis | Schema de la tabla fuente |
| source_table_950 | plata_950_carga | Tabla fuente |
| output_catalog | workspace | Catalogo de salida |
| output_schema | oro_q3 | Schema de salida |
| periodo_inicio | 2026-01-01 | Fecha inicial inclusive |
| periodo_fin | 2026-06-30 | Fecha final inclusive |
| warehouse_id | (lookup por nombre) | ID del SQL Warehouse |

El warehouse se resuelve por nombre mediante `lookup` en `databricks.local.yml`.
