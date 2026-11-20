// =====================================================================
// Consultas de grafo en Neo4j (Reto 5: trazabilidad y propagación)
// Se pueden pegar en http://localhost:7474 o ejecutar todas con:
//     python consultas/consultas_neo4j.py
// =====================================================================

// ---------------------------------------------------------------------
// Q1. RASTREO HACIA ATRÁS (origen probable)
// Para un caso confirmado, buscar cadenas de hasta 3 saltos por contactos
// con personas que dieron positivo ANTES (fechas positivas decrecientes).
// El último eslabón es el posible origen del contagio.
// ---------------------------------------------------------------------
MATCH p = (x:Persona {persona_id: 'P032710'})-[:CONTACTO*1..3]-(origen:Persona)
WHERE all(n IN nodes(p) WHERE n.fecha_positivo IS NOT NULL)
  AND all(i IN range(0, length(p) - 1)
          WHERE nodes(p)[i + 1].fecha_positivo <= nodes(p)[i].fecha_positivo)
RETURN [n IN nodes(p) | n.persona_id + ' ' + toString(n.fecha_positivo)] AS cadena_hacia_atras,
       length(p) AS saltos
ORDER BY origen.fecha_positivo ASC, saltos ASC
LIMIT 5;

// ---------------------------------------------------------------------
// Q2. RASTREO HACIA ADELANTE (expuestos)
// Personas que tuvieron contacto con el caso en los 10 días previos a su
// prueba positiva (nivel 1) y, a su vez, sus contactos posteriores (nivel 2).
// ---------------------------------------------------------------------
MATCH (s:Persona {persona_id: 'P012966'})-[c1:CONTACTO]-(h1:Persona)
WHERE date(c1.ultimo_contacto) >= s.fecha_positivo - duration({days: 10})
  AND date(c1.ultimo_contacto) <= s.fecha_positivo
OPTIONAL MATCH (h1)-[c2:CONTACTO]-(h2:Persona)
WHERE h2 <> s
  AND c2.ultimo_contacto >= c1.ultimo_contacto
  AND c2.ultimo_contacto <= c1.ultimo_contacto + duration({days: 14})
RETURN s.fecha_positivo AS fecha_positivo_caso,
       count(DISTINCT h1) AS expuestos_nivel_1,
       count(DISTINCT CASE WHEN h1.confirmado THEN h1 END) AS confirmados_nivel_1,
       count(DISTINCT h2) AS expuestos_nivel_2,
       count(DISTINCT CASE WHEN h2.confirmado THEN h2 END) AS confirmados_nivel_2;

// ---------------------------------------------------------------------
// Q3. NODOS CRÍTICOS con PageRank (Graph Data Science)
// PageRank da más peso a quien está conectado con personas que a su vez
// están muy conectadas: los "superpropagadores" potenciales.
// ---------------------------------------------------------------------
CALL gds.graph.drop('contactos', false) YIELD graphName RETURN graphName;

CALL gds.graph.project('contactos', 'Persona',
  {CONTACTO: {orientation: 'UNDIRECTED', properties: 'n_contactos'}})
YIELD nodeCount, relationshipCount
RETURN nodeCount, relationshipCount;

CALL gds.pageRank.write('contactos', {relationshipWeightProperty: 'n_contactos', writeProperty: 'pagerank'})
YIELD nodePropertiesWritten, ranIterations
RETURN nodePropertiesWritten, ranIterations;

MATCH (p:Persona)
RETURN p.persona_id AS persona, p.municipio AS municipio, p.grado AS contactos_distintos,
       round(p.pagerank, 3) AS pagerank, p.confirmado AS confirmado
ORDER BY p.pagerank DESC
LIMIT 10;

// ---------------------------------------------------------------------
// Q4. COMUNIDADES con Louvain: grupos de personas muy conectadas entre sí
// (barrios, colegios, empresas). Muestra dónde se concentra el brote.
// ---------------------------------------------------------------------
CALL gds.louvain.write('contactos', {writeProperty: 'comunidad'})
YIELD communityCount, modularity
RETURN communityCount, round(modularity, 3) AS modularity;

MATCH (p:Persona)
WITH p.comunidad AS comunidad, count(*) AS personas,
     sum(CASE WHEN p.confirmado THEN 1 ELSE 0 END) AS confirmados,
     collect(DISTINCT p.municipio)[0..3] AS municipios
WHERE personas >= 50
RETURN comunidad, personas, confirmados,
       round(100.0 * confirmados / personas, 1) AS pct_confirmados, municipios
ORDER BY pct_confirmados DESC
LIMIT 10;

// ---------------------------------------------------------------------
// Q5. LUGARES CRÍTICOS: lugares visitados por más casos confirmados
// ---------------------------------------------------------------------
MATCH (p:Persona {confirmado: true})-[:VISITO]->(l:Lugar)
RETURN l.lugar_id AS lugar, l.tipo AS tipo, l.municipio AS municipio,
       count(p) AS casos_confirmados_que_lo_visitaron
ORDER BY casos_confirmados_que_lo_visitaron DESC
LIMIT 10;

// ---------------------------------------------------------------------
// Q6. PROPAGACIÓN ENTRE MUNICIPIOS: contactos entre personas de
// municipios distintos (rutas por donde el brote salta de un municipio a otro)
// ---------------------------------------------------------------------
MATCH (a:Persona)-[c:CONTACTO]-(b:Persona)
WHERE a.municipio < b.municipio
RETURN a.municipio AS municipio_1, b.municipio AS municipio_2,
       count(c) AS contactos,
       sum(CASE WHEN a.confirmado AND b.confirmado THEN 1 ELSE 0 END) AS entre_confirmados
ORDER BY contactos DESC
LIMIT 10;

// ---------------------------------------------------------------------
// Q7. CADENA MÁS CORTA entre dos casos confirmados (¿están conectados?)
// ---------------------------------------------------------------------
MATCH (a:Persona {persona_id: 'P012966'}), (b:Persona {persona_id: 'P032710'})
MATCH p = shortestPath((a)-[:CONTACTO*..8]-(b))
RETURN [n IN nodes(p) | n.persona_id + CASE WHEN n.confirmado THEN ' (+)' ELSE '' END] AS cadena,
       length(p) AS saltos;
