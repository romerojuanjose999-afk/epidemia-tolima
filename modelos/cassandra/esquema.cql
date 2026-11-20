-- =====================================================================
-- Paso 4 - Modelo de datos en Cassandra
-- En Cassandra se diseña UNA TABLA POR CONSULTA: primero se definen las
-- preguntas y luego la tabla que las responde leyendo una sola partición.
-- =====================================================================

CREATE KEYSPACE IF NOT EXISTS epidemia
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
-- replication_factor = 1 porque es un solo nodo local (en producción: NetworkTopologyStrategy y RF = 3)

-- ---------------------------------------------------------------------
-- CONSULTA 1: "¿Con quién tuvo contacto la persona X (y cuándo, dónde)?"
--   Uso: rastreo inmediato cuando una persona da positivo.
--   Clave de partición : persona_id  -> todos los contactos de una persona juntos
--   Clustering          : fecha_hora DESC -> los más recientes primero, permite filtrar por rango
--   Cada contacto se guarda 2 veces (una para cada persona), porque en
--   Cassandra se duplica el dato para que cada consulta lea una sola partición.
--   Tamaño de partición: ~28 contactos por persona en 90 días -> muy pequeño.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS epidemia.contactos_por_persona (
    persona_id       text,
    fecha_hora       timestamp,
    contacto_id      text,
    otra_persona     text,
    edad_otra        int,
    lugar_id         text,
    tipo_lugar       text,
    municipio        text,
    duracion_min     int,
    distancia_m      float,
    fecha_positivo_otra date,
    PRIMARY KEY ((persona_id), fecha_hora, contacto_id)
) WITH CLUSTERING ORDER BY (fecha_hora DESC, contacto_id ASC);

-- ---------------------------------------------------------------------
-- CONSULTA 2: "¿Quiénes estuvieron en contacto en el lugar L el día D?"
--   Uso: brote en un lugar (colegio, mercado, transporte) -> todos los expuestos.
--   Clave de partición : (lugar_id, fecha) -> se agrega el DÍA a la clave para
--                        evitar particiones gigantes (un mercado acumularía
--                        miles de contactos en 90 días; por día son cientos).
--   Clustering          : fecha_hora, contacto_id -> orden cronológico en el día
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS epidemia.contactos_por_lugar_dia (
    lugar_id         text,
    fecha            date,
    fecha_hora       timestamp,
    contacto_id      text,
    persona_a        text,
    persona_b        text,
    tipo_lugar       text,
    municipio        text,
    duracion_min     int,
    distancia_m      float,
    PRIMARY KEY ((lugar_id, fecha), fecha_hora, contacto_id)
);
