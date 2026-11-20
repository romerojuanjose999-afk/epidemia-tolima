#!/usr/bin/env python3
"""
Paso 7 - Carga del grafo en Neo4j
Ejecuta modelos/neo4j/cargar_grafo.cypher sentencia por sentencia y mide el tiempo.

Requisitos: Neo4j encendido (docker compose up -d neo4j) y data/curated/*.csv (Paso 6).
Uso (desde la carpeta del proyecto):
    python grafo/cargar_neo4j.py
"""
import os
import time

from neo4j import GraphDatabase

URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
AUTH = (os.environ.get("NEO4J_USER", "neo4j"), os.environ.get("NEO4J_PASSWORD", "epidemia2025"))
ARCHIVO = os.path.join("modelos", "neo4j", "cargar_grafo.cypher")


def sentencias(ruta):
    """Separa el archivo .cypher por ';' ignorando comentarios //."""
    texto = "\n".join(l for l in open(ruta, encoding="utf-8") if not l.strip().startswith("//"))
    return [s.strip() for s in texto.split(";") if s.strip()]


def main():
    faltan = [f for f in ("personas", "lugares", "aristas_contacto", "visitas", "simulacion_infectados")
              if not os.path.exists(os.path.join("data", "curated", f + ".csv"))]
    if faltan:
        raise SystemExit(f"Faltan archivos de Spark en data/curated/: {faltan}. Ejecute primero el Paso 6.")

    driver = GraphDatabase.driver(URI, auth=AUTH)
    driver.verify_connectivity()
    print("Conectado a Neo4j\n")
    t_total = time.time()
    with driver.session() as s:
        for i, q in enumerate(sentencias(ARCHIVO)):
            t0 = time.time()
            resumen = s.run(q).consume()
            c = resumen.counters
            print(f"  [{i:2d}] {q.splitlines()[0][:65]:65s} {time.time()-t0:6.1f} s  "
                  f"(+{c.nodes_created} nodos, +{c.relationships_created} relaciones)")

        print("\nContenido del grafo:")
        for r in s.run("MATCH (n) RETURN labels(n)[0] AS tipo, count(*) AS total ORDER BY total DESC"):
            print(f"  Nodos {r['tipo']:10s}: {r['total']:>8,d}")
        for r in s.run("MATCH ()-[r]->() RETURN type(r) AS tipo, count(*) AS total ORDER BY total DESC"):
            print(f"  Rel.  {r['tipo']:10s}: {r['total']:>8,d}")
    driver.close()
    print(f"\nTiempo total de carga: {time.time()-t_total:,.1f} s")


if __name__ == "__main__":
    main()
