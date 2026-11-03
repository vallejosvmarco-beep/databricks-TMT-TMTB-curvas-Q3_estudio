#!/bin/bash
# Script para exportar, ajustar y desplegar el dashboard Curvas Q3

set -e  # Salir si hay error

echo "=== Deployment Dashboard Curvas Q3 ==="
echo ""

# 1. Exportar dashboard
echo "[1/5] Exportando dashboard..."
databricks api get /api/2.0/lakeview/dashboards/01f1a0c4012d155c869b1b019d8d1978 > curvas_q3_dashboard.lvdash.json
echo "  ✓ Dashboard exportado a curvas_q3_dashboard.lvdash.json"
echo ""

# 2. Ajustar referencias a variables
echo "[2/5] Ajustando referencias a variables..."
if [[ "$OSTYPE" == "darwin"* ]]; then
  # macOS
  sed -i '' 's/workspace\.oro_q3/${var.output_catalog}.${var.output_schema}/g' curvas_q3_dashboard.lvdash.json
else
  # Linux
  sed -i 's/workspace\.oro_q3/${var.output_catalog}.${var.output_schema}/g' curvas_q3_dashboard.lvdash.json
fi
echo "  ✓ Referencias ajustadas: workspace.oro_q3 -> \${var.output_catalog}.\${var.output_schema}"
echo ""

# 3. Obtener warehouse ID
echo "[3/5] Obteniendo SQL Warehouse ID..."
WAREHOUSE_ID=$(databricks sql-warehouses list --output json | jq -r '.[0].id')

if [ -z "$WAREHOUSE_ID" ] || [ "$WAREHOUSE_ID" = "null" ]; then
  echo "  ⚠️  No se encontró un warehouse automáticamente."
  echo "  Lista de warehouses disponibles:"
  databricks sql-warehouses list
  echo ""
  echo "  Por favor, ejecuta manualmente con:"
  echo "  databricks bundle deploy -t free_dev --var=\"warehouse_id=TU_WAREHOUSE_ID\""
  exit 1
fi

echo "  ✓ Warehouse ID encontrado: $WAREHOUSE_ID"
echo ""

# 4. Validar bundle
echo "[4/5] Validando bundle..."
databricks bundle validate -t free_dev --var="warehouse_id=$WAREHOUSE_ID"
echo "  ✓ Bundle válido"
echo ""

# 5. Desplegar bundle
echo "[5/5] Desplegando bundle..."
databricks bundle deploy -t free_dev --var="warehouse_id=$WAREHOUSE_ID"
echo ""

echo "=== Deployment completado ==="
echo "Dashboard desplegado en: /Shared/curvas-Q3/Curvas Q3 Mensuales - Análisis Multidimensional"
echo "Warehouse ID usado: $WAREHOUSE_ID"
echo ""
echo "Para ver el estado:"
echo "  databricks bundles resources list -t free_dev"
