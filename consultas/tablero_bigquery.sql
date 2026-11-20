-- =====================================================================
-- Paso 8 - Tablero epidemiológico en BigQuery
-- Indicadores que pide el Reto 5: ALCANCE, TIEMPO DE DETECCIÓN y NODOS CRÍTICOS.
-- Cada consulta se guarda como VISTA para conectarla a Looker Studio (tablero).
-- ${PROYECTO} y ${DATASET} los reemplaza analitica/cargar_bigquery.py
-- =====================================================================

-- V1. KPIs generales (una fila: tarjetas del tablero)
CREATE OR REPLACE VIEW `${PROYECTO}.${DATASET}.v_kpis` AS
SELECT
  (SELECT COUNT(*) FROM `${PROYECTO}.${DATASET}.personas`) AS personas,
  (SELECT COUNTIF(fecha_positivo IS NOT NULL) FROM `${PROYECTO}.${DATASET}.personas`) AS casos_confirmados,
  (SELECT ROUND(100 * COUNTIF(fecha_positivo IS NOT NULL) / COUNT(*), 1) FROM `${PROYECTO}.${DATASET}.personas`) AS pct_confirmados,
  (SELECT COUNTIF(desenlace = 'HOSPITALIZADO') FROM `${PROYECTO}.${DATASET}.personas`) AS hospitalizados,
  (SELECT COUNTIF(desenlace = 'FALLECIDO') FROM `${PROYECTO}.${DATASET}.personas`) AS fallecidos,
  (SELECT ROUND(AVG(dias_deteccion), 1) FROM `${PROYECTO}.${DATASET}.deteccion`) AS dias_deteccion_promedio,
  (SELECT MAX(acumulado) FROM `${PROYECTO}.${DATASET}.simulacion`) AS alcance_simulado;

-- V2. ALCANCE por municipio: casos, porcentaje de la población y desenlaces
CREATE OR REPLACE VIEW `${PROYECTO}.${DATASET}.v_alcance_municipio` AS
SELECT
  municipio,
  COUNT(*) AS personas,
  COUNTIF(fecha_positivo IS NOT NULL) AS confirmados,
  ROUND(100 * SAFE_DIVIDE(COUNTIF(fecha_positivo IS NOT NULL), COUNT(*)), 1) AS pct_confirmados,
  COUNTIF(desenlace = 'HOSPITALIZADO') AS hospitalizados,
  COUNTIF(desenlace = 'FALLECIDO') AS fallecidos,
  MIN(fecha_positivo) AS primer_caso
FROM `${PROYECTO}.${DATASET}.personas`
GROUP BY municipio;

-- V3. Curva epidémica diaria del Tolima (casos, media móvil de 7 días y acumulado)
CREATE OR REPLACE VIEW `${PROYECTO}.${DATASET}.v_curva_epidemica` AS
SELECT
  fecha,
  casos,
  contactos,
  ROUND(AVG(casos) OVER (ORDER BY fecha ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1) AS media_movil_7d,
  SUM(casos) OVER (ORDER BY fecha) AS casos_acumulados
FROM (
  SELECT fecha, SUM(casos_confirmados) AS casos, SUM(contactos) AS contactos
  FROM `${PROYECTO}.${DATASET}.indicadores_diarios`
  GROUP BY fecha
);

-- V4. Efecto de las medidas: contactos y casos por semana
--     (las medidas de contención empiezan la semana del 2025-04-14)
CREATE OR REPLACE VIEW `${PROYECTO}.${DATASET}.v_semanal` AS
SELECT
  DATE_TRUNC(fecha, WEEK(MONDAY)) AS semana,
  SUM(contactos) AS contactos,
  SUM(contactos_cercanos) AS contactos_cercanos,
  SUM(casos_confirmados) AS casos,
  IF(DATE_TRUNC(fecha, WEEK(MONDAY)) >= '2025-04-14', 'con medidas', 'sin medidas') AS periodo
FROM `${PROYECTO}.${DATASET}.indicadores_diarios`
GROUP BY semana, periodo;

-- V5. TIEMPO DE DETECCIÓN por municipio (días entre exposición y prueba positiva)
CREATE OR REPLACE VIEW `${PROYECTO}.${DATASET}.v_deteccion_municipio` AS
SELECT
  municipio,
  COUNT(*) AS casos_con_exposicion,
  ROUND(AVG(dias_deteccion), 1) AS dias_promedio,
  APPROX_QUANTILES(dias_deteccion, 2)[OFFSET(1)] AS dias_mediana,
  MAX(dias_deteccion) AS dias_maximo
FROM `${PROYECTO}.${DATASET}.deteccion`
GROUP BY municipio
HAVING COUNT(*) >= 5;

-- V6. NODOS CRÍTICOS: PageRank (Neo4j) + contactos distintos (Spark)
CREATE OR REPLACE VIEW `${PROYECTO}.${DATASET}.v_nodos_criticos` AS
SELECT
  p.persona_id,
  p.municipio,
  p.edad,
  p.grado AS contactos_distintos,
  ROUND(r.pagerank, 3) AS pagerank,
  r.comunidad,
  p.fecha_positivo IS NOT NULL AS confirmado,
  RANK() OVER (ORDER BY r.pagerank DESC) AS puesto
FROM `${PROYECTO}.${DATASET}.personas` p
JOIN `${PROYECTO}.${DATASET}.pagerank` r USING (persona_id);

-- V7. Lugares por tipo: dónde ocurren los contactos
CREATE OR REPLACE VIEW `${PROYECTO}.${DATASET}.v_lugares_tipo` AS
SELECT
  tipo_lugar,
  COUNT(*) AS lugares,
  SUM(n_contactos) AS contactos,
  ROUND(100 * SUM(n_contactos) / SUM(SUM(n_contactos)) OVER (), 1) AS pct_contactos
FROM `${PROYECTO}.${DATASET}.lugares`
GROUP BY tipo_lugar;

-- V8. Simulación de propagación por oleadas (Spark)
CREATE OR REPLACE VIEW `${PROYECTO}.${DATASET}.v_simulacion` AS
SELECT oleada, nuevos_infectados, acumulado,
       ROUND(SAFE_DIVIDE(nuevos_infectados, LAG(nuevos_infectados) OVER (ORDER BY oleada)), 2) AS factor_crecimiento
FROM `${PROYECTO}.${DATASET}.simulacion`;
