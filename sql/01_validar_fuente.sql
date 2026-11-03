SELECT
  MIN(FECHA_LECTURA) AS desde,
  MAX(FECHA_LECTURA) AS hasta,
  COUNT(*) AS filas,
  COUNT(DISTINCT DATE_TRUNC('MONTH', FECHA_LECTURA)) AS meses,
  COUNT(DISTINCT NISE) AS clientes
FROM workspace.plata_analisis.plata_950_carga;
