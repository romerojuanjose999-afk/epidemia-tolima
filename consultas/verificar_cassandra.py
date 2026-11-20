#!/usr/bin/env python3
"""
Verifica la carga en Cassandra y mide tiempos de respuesta (evidencia para el informe).

- Cuenta las filas recorriendo la tabla por páginas (COUNT(*) falla en Cassandra
  con tablas grandes: no está diseñada para recorridos completos).
- Mide el tiempo de las consultas por clave (rastreo por persona y por lugar-día).

Uso:  python consultas/verificar_cassandra.py
"""
import statistics
import time
from datetime import date, datetime

from cassandra.cluster import Cluster
from cassandra.io.asyncioreactor import AsyncioConnection
from cassandra.query import SimpleStatement

cluster = Cluster(["127.0.0.1"], connection_class=AsyncioConnection)
s = cluster.connect("epidemia")
s.default_timeout = 120

# 1. Conteo por páginas (solo lee la columna de la clave, 5 000 filas por página)
print("1) Contando filas por páginas (puede tardar 1-3 minutos)...")
t0 = time.time()
total = 0
for _ in s.execute(SimpleStatement("SELECT contacto_id FROM contactos_por_lugar_dia", fetch_size=5000)):
    total += 1
    if total % 100000 == 0:
        print(f"   {total:,d} filas...")
print(f"   contactos_por_lugar_dia : {total:,d} filas ({time.time()-t0:.0f} s)")
print(f"   {'OK' if total == 526037 else 'REVISAR'}: se esperaban 526,037\n")


# 2. Tiempos de respuesta de las consultas por clave (20 repeticiones)
def medir(nombre, cql, params):
    st = s.prepare(cql)
    tiempos, filas = [], 0
    for _ in range(20):
        t = time.perf_counter()
        filas = len(list(s.execute(st, params)))
        tiempos.append((time.perf_counter() - t) * 1000)
    print(f"   {nombre:38s} {filas:4d} filas | mediana {statistics.median(tiempos):6.1f} ms "
          f"| máx {max(tiempos):6.1f} ms")


print("2) Tiempo de respuesta de consultas por clave (20 repeticiones):")
medir("Rastreo persona P032710",
      "SELECT * FROM contactos_por_persona WHERE persona_id = ?", ["P032710"])
medir("Ventana infecciosa P032710 (14 días)",
      "SELECT * FROM contactos_por_persona WHERE persona_id = ? AND fecha_hora >= ? AND fecha_hora < ?",
      ["P032710", datetime(2025, 5, 16), datetime(2025, 5, 31)])
medir("Brote mercado L00417 el 2025-04-10",
      "SELECT * FROM contactos_por_lugar_dia WHERE lugar_id = ? AND fecha = ?", ["L00417", date(2025, 4, 10)])

cluster.shutdown()
