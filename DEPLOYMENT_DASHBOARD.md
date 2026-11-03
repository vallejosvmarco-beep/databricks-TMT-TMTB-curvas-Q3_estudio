# Deployment del Dashboard Curvas Q3

## Configuración completada:

✓ Archivo `databricks.yml` actualizado con:
  - Variable `warehouse_id` para el dashboard
  - Include de `resources/curvas_q3_dashboard.yml`

✓ Archivo `resources/curvas_q3_dashboard.yml` creado con:
  - Definición del recurso dashboard
  - Permisos de lectura para usuarios

## 🚀 Deployment Automático (RECOMENDADO)

Ejecuta el script de deployment que automatiza todos los pasos:

```bash
# Desde la raíz del bundle
cd /Users/mvallejos3100@gmail.com/databricks-TMT-TMTB-curvas-Q3

# Hacer ejecutable el script
chmod +x export_dashboard.sh

# Ejecutar deployment automático
./export_dashboard.sh
```

El script:
1. ✅ Exporta el dashboard completo
2. ✅ Ajusta las referencias a variables del bundle
3. ✅ Detecta automáticamente el SQL Warehouse ID
4. ✅ Valida la configuración del bundle
5. ✅ Despliega el bundle completo

---

## ⚙️ Pasos Manuales (Alternativa):

### 1. Exportar el dashboard actual

```bash
# Desde la raíz del bundle
cd /Users/mvallejos3100@gmail.com/databricks-TMT-TMTB-curvas-Q3

# Exportar el dashboard completo (incluye toda la configuración, páginas y widgets)
databricks api get /api/2.0/lakeview/dashboards/01f1a0c4012d155c869b1b019d8d1978 > curvas_q3_dashboard.lvdash.json
```

### 2. Ajustar el archivo curvas_q3_dashboard.lvdash.json

Edita `curvas_q3_dashboard.lvdash.json` y reemplaza las referencias a tablas para usar variables:

```bash
# Buscar y reemplazar en el archivo (macOS usa -i '' en lugar de -i)
sed -i 's/workspace\.oro_q3/${var.output_catalog}.${var.output_schema}/g' curvas_q3_dashboard.lvdash.json

# Si estás en macOS:
sed -i '' 's/workspace\.oro_q3/${var.output_catalog}.${var.output_schema}/g' curvas_q3_dashboard.lvdash.json
```

O manualmente, cambia:
- `workspace.oro_q3.curvas_q3_mensuales` → `${var.output_catalog}.${var.output_schema}.curvas_q3_mensuales`

**IMPORTANTE**: Busca TODAS las apariciones de "workspace.oro_q3" en el archivo JSON exportado. El dashboard tiene múltiples datasets y queries que referencian esta tabla.

### 3. Configurar Warehouse ID

Obtener el ID de tu SQL Warehouse:

```bash
# Listar warehouses disponibles
databricks sql-warehouses list

# Copiar el ID del warehouse que quieres usar
```

OPCIÓN A - Configurar en databricks.yml:
```yaml
variables:
  warehouse_id:
    description: ID del SQL Warehouse para el dashboard
    default: "abc123def456"  # Reemplaza con tu warehouse ID
```

OPCIÓN B - Pasar al desplegar:
```bash
databricks bundle deploy -t free_dev --var="warehouse_id=abc123def456"
```

### 4. Validar la configuración

```bash
databricks bundle validate -t free_dev
```

### 5. Desplegar el bundle

```bash
# Desplegar todo (pipeline + dashboard)
databricks bundle deploy -t free_dev

# O si configuraste warehouse_id en el comando
databricks bundle deploy -t free_dev --var="warehouse_id=abc123def456"
```

El dashboard se creará en: `/Shared/curvas-Q3/Curvas Q3 Mensuales - Análisis Multidimensional`

### 6. Verificar el deployment

```bash
# Ver el estado
databricks bundles resources list -t free_dev
```

## Troubleshooting:

### Error: "curvas_q3_dashboard.lvdash.json not found"
- Asegúrate de haber exportado el dashboard (paso 1)
- Verifica que el archivo esté en la raíz del bundle: `/Users/mvallejos3100@gmail.com/databricks-TMT-TMTB-curvas-Q3/curvas_q3_dashboard.lvdash.json`

### Error: "Warehouse not found"
- Lista los warehouses disponibles: `databricks sql-warehouses list`
- Verifica que tengas permisos de uso en el warehouse

### Error: "Table not found"
- Ejecuta el pipeline primero
- Verifica que la tabla existe: `SELECT * FROM workspace.oro_q3.curvas_q3_mensuales LIMIT 1`

## Actualizar el dashboard:

1. Exporta de nuevo el dashboard actualizado
2. Ajusta las referencias a variables
3. Despliega: `databricks bundle deploy -t free_dev`
