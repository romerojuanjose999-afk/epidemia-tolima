// =====================================================================
// Paso 7 - Modelo de grafo en Neo4j
//
//   (:Persona)-[:CONTACTO]-(:Persona)     contacto cercano agregado por pareja
//   (:Persona)-[:VISITO]->(:Lugar)        dónde estuvo cada persona
//   (:Persona)-[:VIVE_EN]->(:Municipio)
//   (:Lugar)-[:UBICADO_EN]->(:Municipio)
//
// Por qué grafo: rastrear "quién contagió a quién" son recorridos de varios
// saltos (amigo del amigo del amigo). En SQL serían JOIN repetidos sobre
// cientos de miles de filas; en un grafo cada salto sigue punteros directos.
//
// Origen: data/curated/*.csv generados por Spark (Paso 6). La carpeta data/
// está montada en Neo4j como /var/lib/neo4j/import (ver docker-compose.yml).
// Se ejecuta con:  python grafo/cargar_neo4j.py
// =====================================================================

// 0. Limpiar el grafo (permite volver a cargar sin duplicados)
MATCH (n) CALL (n) { DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS;

// 1. Restricciones de unicidad (crean índices: búsquedas por id en milisegundos)
CREATE CONSTRAINT persona_id IF NOT EXISTS FOR (p:Persona) REQUIRE p.persona_id IS UNIQUE;
CREATE CONSTRAINT lugar_id IF NOT EXISTS FOR (l:Lugar) REQUIRE l.lugar_id IS UNIQUE;
CREATE CONSTRAINT municipio_nombre IF NOT EXISTS FOR (m:Municipio) REQUIRE m.nombre IS UNIQUE;
CREATE INDEX persona_positivo IF NOT EXISTS FOR (p:Persona) ON (p.fecha_positivo);

// 2. Personas + municipio donde vive
LOAD CSV WITH HEADERS FROM 'file:///curated/personas.csv' AS r
CALL (r) {
  MERGE (p:Persona {persona_id: r.persona_id})
  SET p.edad = toInteger(r.edad),
      p.sexo = r.sexo,
      p.municipio = r.municipio,
      p.desenlace = r.desenlace,
      p.grado = toInteger(r.grado),
      p.fecha_positivo = CASE WHEN r.fecha_positivo IS NULL OR r.fecha_positivo = '' THEN null
                              ELSE date(r.fecha_positivo) END,
      p.confirmado = (r.fecha_positivo IS NOT NULL AND r.fecha_positivo <> '')
  MERGE (m:Municipio {nombre: r.municipio})
  MERGE (p)-[:VIVE_EN]->(m)
} IN TRANSACTIONS OF 5000 ROWS;

// 3. Lugares + municipio donde están
LOAD CSV WITH HEADERS FROM 'file:///curated/lugares.csv' AS r
CALL (r) {
  MERGE (l:Lugar {lugar_id: r.lugar_id})
  SET l.tipo = r.tipo_lugar, l.municipio = r.municipio, l.n_contactos = toInteger(r.n_contactos)
  MERGE (m:Municipio {nombre: r.municipio})
  MERGE (l)-[:UBICADO_EN]->(m)
} IN TRANSACTIONS OF 2000 ROWS;

// 4. Contactos persona-persona (~435 000 relaciones)
LOAD CSV WITH HEADERS FROM 'file:///curated/aristas_contacto.csv' AS r
CALL (r) {
  MATCH (a:Persona {persona_id: r.persona_1})
  MATCH (b:Persona {persona_id: r.persona_2})
  CREATE (a)-[:CONTACTO {
    n_contactos:     toInteger(r.n_contactos),
    minutos_total:   toInteger(r.minutos_total),
    distancia_min:   toFloat(r.distancia_min),
    primer_contacto: localdatetime(replace(r.primer_contacto, ' ', 'T')),
    ultimo_contacto: localdatetime(replace(r.ultimo_contacto, ' ', 'T'))
  }]->(b)
} IN TRANSACTIONS OF 10000 ROWS;

// 5. Visitas persona-lugar (~188 000 relaciones)
LOAD CSV WITH HEADERS FROM 'file:///curated/visitas.csv' AS r
CALL (r) {
  MATCH (p:Persona {persona_id: r.persona_id})
  MATCH (l:Lugar {lugar_id: r.lugar_id})
  CREATE (p)-[:VISITO {n_contactos: toInteger(r.n_contactos),
                       primera_vez: localdatetime(replace(r.primera_vez, ' ', 'T'))}]->(l)
} IN TRANSACTIONS OF 10000 ROWS;

// 6. Resultado de la simulación de Spark (qué personas alcanzó y en qué oleada)
LOAD CSV WITH HEADERS FROM 'file:///curated/simulacion_infectados.csv' AS r
MATCH (p:Persona {persona_id: r.persona_id})
SET p.simulado_infectado = true, p.oleada_simulada = toInteger(r.oleada);
