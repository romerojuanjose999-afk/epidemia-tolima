-- =====================================================================
-- Consultas en Cassandra (para el informe y la demo en vivo)
-- Ejecutar en:  docker exec -it cassandra cqlsh
-- =====================================================================

-- 1. Conteo de filas guardadas (evidencia de volumen)
--    (Cassandra no está hecha para COUNT; en tablas grandes puede tardar o dar timeout.
--     Se usa solo como verificación puntual)
SELECT COUNT(*) FROM epidemia.contactos_por_lugar_dia;

-- 2. RASTREO INMEDIATO: con quién tuvo contacto la persona P032710
--    (lee UNA sola partición -> respuesta en milisegundos)
SELECT fecha_hora, otra_persona, tipo_lugar, municipio, duracion_min, fecha_positivo_otra
FROM epidemia.contactos_por_persona
WHERE persona_id = 'P032710';

-- 3. Contactos en su ventana infecciosa: P032710 dio positivo el 2025-05-30 -> 14 días antes
SELECT fecha_hora, otra_persona, tipo_lugar, duracion_min
FROM epidemia.contactos_por_persona
WHERE persona_id = 'P032710'
  AND fecha_hora >= '2025-05-16' AND fecha_hora < '2025-05-31';

-- 4. BROTE EN UN LUGAR: todos los contactos en el mercado L00417 (Chaparral) el 2025-04-10
SELECT fecha_hora, persona_a, persona_b, duracion_min, distancia_m
FROM epidemia.contactos_por_lugar_dia
WHERE lugar_id = 'L00417' AND fecha = '2025-04-10';

-- 5. Contactos cercanos (< 1 m) en ese lugar y día
--    ALLOW FILTERING es aceptable aquí porque filtra DENTRO de una sola partición
SELECT fecha_hora, persona_a, persona_b, distancia_m
FROM epidemia.contactos_por_lugar_dia
WHERE lugar_id = 'L00417' AND fecha = '2025-04-10' AND distancia_m < 1.0
ALLOW FILTERING;

-- Activar el tiempo de respuesta para la evidencia de rendimiento:
--   TRACING ON;     (luego ejecutar la consulta 2 y ver "Request complete" en microsegundos)
