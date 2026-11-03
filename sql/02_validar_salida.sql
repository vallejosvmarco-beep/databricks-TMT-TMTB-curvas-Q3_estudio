SELECT
  NISE,
  OPERACION,
  METRICA,
  PERIODO_MES,
  COUNT(*) AS puntos_curva,
  MIN(BLOQUE_15M) AS bloque_min,
  MAX(BLOQUE_15M) AS bloque_max,
  MIN(DIAS_CON_DATOS) AS min_dias_con_datos,
  MAX(DIAS_CON_DATOS) AS max_dias_con_datos
FROM workspace.oro_q3.curvas_q3_mensuales
GROUP BY NISE, OPERACION, METRICA, PERIODO_MES
ORDER BY NISE, OPERACION, METRICA, PERIODO_MES;
