#!/usr/bin/env python3
"""
Consulta las vistas del tablero en BigQuery y muestra resultados, tiempo y
datos procesados (evidencia para el informe).

Uso (desde la carpeta del proyecto, después de analitica/cargar_bigquery.py):
    python consultas/consultas_bigquery.py
"""
import os
import time

from google.cloud import bigquery

VISTAS = [
    ("KPIs generales", "SELECT * FROM `{d}.v_kpis`"),
    ("ALCANCE: top 10 municipios por % de confirmados",
     "SELECT * FROM `{d}.v_alcance_municipio` WHERE personas >= 100 ORDER BY pct_confirmados DESC LIMIT 10"),
    ("Curva epidémica (cada 10 días)",
     "SELECT * FROM `{d}.v_curva_epidemica` WHERE MOD(DATE_DIFF(fecha, DATE '2025-03-03', DAY), 10) = 0 ORDER BY fecha"),
    ("Efecto de las medidas (por semana)", "SELECT * FROM `{d}.v_semanal` ORDER BY semana"),
    ("TIEMPO DE DETECCIÓN: municipios más lentos",
     "SELECT * FROM `{d}.v_deteccion_municipio` ORDER BY dias_promedio DESC LIMIT 10"),
    ("NODOS CRÍTICOS: top 10 PageRank", "SELECT * FROM `{d}.v_nodos_criticos` WHERE puesto <= 10 ORDER BY puesto"),
    ("Contactos por tipo de lugar", "SELECT * FROM `{d}.v_lugares_tipo` ORDER BY contactos DESC"),
    ("Simulación por oleadas", "SELECT * FROM `{d}.v_simulacion` ORDER BY oleada"),
]


def cargar_env(ruta=".env"):
    for linea in open(ruta, encoding="utf-8"):
        linea = linea.strip()
        if linea and not linea.startswith("#") and "=" in linea:
            k, v = linea.split("=", 1)
            if v.strip():
                os.environ[k.strip()] = v.strip()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.abspath(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])


def main():
    cargar_env()
    proyecto = os.environ["GCP_PROJECT_ID"]
    d = f"{proyecto}.{os.environ.get('BQ_DATASET', 'epidemia_tolima')}"
    bq = bigquery.Client(project=proyecto)
    for titulo, sql in VISTAS:
        print("\n" + "=" * 70 + f"\n{titulo}\n" + "=" * 70)
        t0 = time.perf_counter()
        try:
            job = bq.query(sql.format(d=d))
            filas = [dict(r) for r in job.result()]
        except Exception as e:
            print(f"    Error: {str(e)[:150]}")
            continue
        ms = (time.perf_counter() - t0) * 1000
        for f in filas:
            print("    " + " | ".join(f"{k}={v}" for k, v in f.items()))
        print(f"    -> {len(filas)} filas en {ms:,.0f} ms | {(job.total_bytes_processed or 0)/1024:,.0f} KB procesados")


if __name__ == "__main__":
    main()
