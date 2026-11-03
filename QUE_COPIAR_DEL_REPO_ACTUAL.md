# Qué necesito del repositorio actual

NO copies Bronce, Plata incremental, Jobs, dashboard ni boxes.sql.

Solo confirma la ubicación exacta de la tabla `plata_950_carga`.

## 1. En `databricks.yml`
Confirma los valores de:
- `catalog`
- `schema_analisis`

## 2. En `resources/pipelines/tmt_tmtb_analisis.pipeline.yml`
Confirma:
```yaml
catalog: ${var.catalog}
schema: ${var.schema_analisis}
```
y cualquier configuración:
```yaml
project.catalog
project.schema_analisis
```

## 3. En `src/tmt_tmtb_analisis/plata/plata_950_carga.sql`
Confirma el nombre exacto de la tabla de salida y si se crea con un nombre totalmente calificado.

## Lo único que este nuevo repo necesita
Construir:
`<CATALOGO>.<SCHEMA_ANALISIS>.<TABLA_950>`

Ejemplo:
`workspace.plata_analisis.plata_950_carga`
