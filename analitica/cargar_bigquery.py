#!/usr/bin/env python3
"""
Paso 8 - Carga de resultados a BigQuery (capa analítica / tablero)

Sube a BigQuery los resultados de Spark (data/curated/*.csv) y el PageRank /
comunidades calculados en Neo4j, y crea las VISTAS del tablero definidas en
consultas/tablero_bigquery.sql.

Requisitos en .env:
    GOOGLE_APPLICATION_CREDENTIALS=credenciales/gcp-sa.json
    GCP_PROJECT_ID=<id de su proyecto de Google Cloud>
    BQ_DATASET=epidemia_tolima

Uso (desde la carpeta del proyecto):
    python analitica/cargar_bigquery.py
"""
import csv
import os
import time

from google.cloud import bigquery

C = os.path.join("data", "curated")

# Esquemas explícitos (más seguro que dejar que BigQuery "adivine" los tipos)
S = bigquery.SchemaField
TABLAS = {
    "personas": [S("persona_id", "STRING"), S("edad", "INT64"), S("sexo", "STRING"),
                 S("fecha_positivo", "DATE"), S("desenlace", "STRING"), S("grado", "INT64"),
                 S("n_contactos", "INT64"), S("municipio", "STRING")],
    "indicadores_diarios": [S("fecha", "DATE"), S("municipio", "STRING"), S("contactos", "INT64"),
                            S("contactos_cercanos", "INT64"), S("casos_confirmados", "INT64"),
                            S("hospitalizados", "INT64"), S("fallecidos", "INT64")],
    "deteccion": [S("persona_id", "STRING"), S("municipio", "STRING"), S("fecha_positivo", "DATE"),
                  S("fecha_exposicion", "DATE"), S("dias_deteccion", "INT64")],
    "lugares": [S("lugar_id", "STRING"), S("tipo_lugar", "STRING"), S("municipio", "STRING"),
                S("n_contactos", "INT64"), S("dias_activo", "INT64")],
    "simulacion": [S("oleada", "INT64"), S("nuevos_infectados", "INT64"), S("acumulado", "INT64")],
    "simulacion_infectados": [S("persona_id", "STRING"), S("t_inf", "DATETIME"), S("oleada", "INT64")],
    "pagerank": [S("persona_id", "STRING"), S("pagerank", "FLOAT64"), S("comunidad", "INT64")],
}


def cargar_env(ruta=".env"):
    if not os.path.exists(ruta):
        raise SystemExit("Falta el archivo .env")
    for linea in open(ruta, encoding="utf-8"):
        linea = linea.strip()
        if linea and not linea.startswith("#") and "=" in linea:
            k, v = linea.split("=", 1)
            if v.strip():
                os.environ[k.strip()] = v.strip()
    cred = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    if not cred or not os.path.exists(cred):
        raise SystemExit(f"No se encuentra la llave de Google: '{cred}'. Revise GOOGLE_APPLICATION_CREDENTIALS en .env")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.abspath(cred)
    if not os.environ.get("GCP_PROJECT_ID"):
        raise SystemExit("Falta GCP_PROJECT_ID en .env")


def exportar_pagerank():
    """Trae de Neo4j el PageRank y la comunidad de cada persona (Paso 7) -> data/curated/pagerank.csv"""
    try:
        from neo4j import GraphDatabase
        drv = GraphDatabase.driver("bolt://localhost:7687",
                                   auth=("neo4j", os.environ.get("NEO4J_PASSWORD", "epidemia2025")))
        with drv.session() as s:
            filas = s.run("MATCH (p:Persona) WHERE p.pagerank IS NOT NULL "
                          "RETURN p.persona_id AS persona_id, p.pagerank AS pagerank, "
                          "p.comunidad AS comunidad").data()
        drv.close()
    except Exception as e:
        print(f"   Aviso: no se pudo leer Neo4j ({type(e).__name__}). Se omite la tabla pagerank.")
        return False
    if not filas:
        print("   Aviso: Neo4j no tiene PageRank (ejecute consultas_neo4j.py). Se omite pagerank.")
        return False
    with open(os.path.join(C, "pagerank.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["persona_id", "pagerank", "comunidad"])
        w.writeheader()
        w.writerows(filas)
    print(f"   PageRank exportado desde Neo4j: {len(filas):,d} personas")
    return True


def sentencias_sql(ruta, proyecto, dataset):
    texto = open(ruta, encoding="utf-8").read().replace("${PROYECTO}", proyecto).replace("${DATASET}", dataset)
    texto = "\n".join(l for l in texto.splitlines() if not l.strip().startswith("--"))
    return [s.strip() for s in texto.split(";") if s.strip()]


def main():
    cargar_env()
    proyecto, dataset = os.environ["GCP_PROJECT_ID"], os.environ.get("BQ_DATASET", "epidemia_tolima")
    t0 = time.time()

    print("1) Exportando PageRank y comunidades desde Neo4j...")
    hay_pagerank = exportar_pagerank()

    print(f"2) Cargando tablas en BigQuery ({proyecto}.{dataset})...")
    bq = bigquery.Client(project=proyecto)
    ds = bigquery.Dataset(f"{proyecto}.{dataset}")
    ds.location = "US"
    bq.create_dataset(ds, exists_ok=True)

    for tabla, esquema in TABLAS.items():
        ruta = os.path.join(C, f"{tabla}.csv")
        if tabla == "pagerank" and not hay_pagerank:
            continue
        if not os.path.exists(ruta):
            raise SystemExit(f"Falta {ruta}. Ejecute primero el Paso 6 (Spark).")
        cfg = bigquery.LoadJobConfig(schema=esquema, source_format=bigquery.SourceFormat.CSV,
                                     skip_leading_rows=1, write_disposition="WRITE_TRUNCATE")
        with open(ruta, "rb") as f:
            job = bq.load_table_from_file(f, f"{proyecto}.{dataset}.{tabla}", job_config=cfg)
        job.result()
        print(f"   {tabla:22s} {bq.get_table(job.destination).num_rows:>8,d} filas")

    print("3) Creando las vistas del tablero...")
    for sql in sentencias_sql(os.path.join("consultas", "tablero_bigquery.sql"), proyecto, dataset):
        nombre = sql.split("VIEW", 1)[1].split("AS", 1)[0].strip().strip("`").split(".")[-1]
        if "pagerank" in sql and not hay_pagerank:
            print(f"   {nombre:28s} omitida (sin PageRank)")
            continue
        bq.query(sql).result()
        print(f"   {nombre:28s} creada")

    print(f"\nListo en {time.time()-t0:,.1f} s. Vea las tablas en: "
          f"https://console.cloud.google.com/bigquery?project={proyecto}")


if __name__ == "__main__":
    main()
