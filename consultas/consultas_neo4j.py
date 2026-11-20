#!/usr/bin/env python3
"""
Ejecuta las consultas de consultas/consultas_neo4j.cypher, muestra los resultados
y el tiempo de cada una (evidencia para el informe y la demo).

Uso (desde la carpeta del proyecto, con Neo4j encendido y el grafo cargado):
    python consultas/consultas_neo4j.py
"""
import os
import time

from neo4j import GraphDatabase

URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
AUTH = (os.environ.get("NEO4J_USER", "neo4j"), os.environ.get("NEO4J_PASSWORD", "epidemia2025"))
ARCHIVO = os.path.join("consultas", "consultas_neo4j.cypher")


def bloques(ruta):
    """Devuelve (titulo, consulta) usando las líneas '// Qn.' como títulos."""
    titulo, actual, salida = "", [], []
    for linea in open(ruta, encoding="utf-8"):
        l = linea.strip()
        if l.startswith("// Q"):
            titulo = l[3:]
        if l.startswith("//") or not l:
            continue
        actual.append(linea.rstrip())
        if l.endswith(";"):
            salida.append((titulo, "\n".join(actual).rstrip(";")))
            actual = []
    return salida


def mostrar(filas):
    if not filas:
        print("    (sin resultados)")
        return
    for f in filas[:10]:
        print("    " + " | ".join(f"{k}={v}" for k, v in f.items()))


def main():
    driver = GraphDatabase.driver(URI, auth=AUTH)
    driver.verify_connectivity()
    ultimo = None
    with driver.session() as s:
        for titulo, q in bloques(ARCHIVO):
            if titulo != ultimo:
                print("\n" + "=" * 70 + f"\n{titulo}\n" + "=" * 70)
                ultimo = titulo
            t0 = time.perf_counter()
            filas = s.run(q).data()
            ms = (time.perf_counter() - t0) * 1000
            mostrar(filas)
            print(f"    -> {len(filas)} filas en {ms:,.0f} ms")
    driver.close()


if __name__ == "__main__":
    main()
