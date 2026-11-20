"""
Paso 6 - Procesamiento masivo con Spark (lee el histórico desde S3)

Qué hace:
  1. LEE el data lake en S3 (Parquet particionado por fecha).
  2. LIMPIA: quita duplicados, nulos y registros inválidos.
  3. CALCULA los indicadores que pide el reto:
       - alcance del brote por municipio y día
       - tiempo de detección (exposición -> prueba positiva)
       - nodos críticos (personas con más contactos distintos)
  4. SIMULA la propagación por oleadas desde los primeros casos confirmados.
  5. ESCRIBE los resultados:
       - en S3 (capa curated/, Parquet) -> histórico procesado
       - en data/curated/*.csv          -> para cargar en Neo4j y BigQuery

Se ejecuta DENTRO del contenedor de Spark (ver instrucciones del Paso 6):
  docker exec spark-master /opt/spark/bin/spark-submit ... /opt/app/procesamiento/spark_procesamiento.py
Prueba local sin S3:
  spark-submit procesamiento/spark_procesamiento.py --origen data/lake_local
"""
import argparse
import glob
import os
import shutil
import time

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

APP = "/opt/app" if os.path.isdir("/opt/app") else "."


def cargar_env(ruta):
    if os.path.exists(ruta):
        for linea in open(ruta, encoding="utf-8"):
            linea = linea.strip()
            if linea and not linea.startswith("#") and "=" in linea:
                k, v = linea.split("=", 1)
                if v.strip():
                    os.environ.setdefault(k.strip(), v.strip())


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--origen", default=None, help="Ruta local de prueba (si no se da, lee de S3)")
    p.add_argument("--oleadas", type=int, default=5, help="Oleadas de la simulación")
    return p.parse_args()


def guardar_csv(df, nombre):
    """Escribe UN solo CSV con nombre fijo en data/curated/ (Neo4j necesita nombres fijos)."""
    destino_tmp = os.path.join(APP, "data", "curated", f"_{nombre}_tmp")
    (df.coalesce(1).write.mode("overwrite").option("header", True)
       .option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv(destino_tmp))
    parte = glob.glob(os.path.join(destino_tmp, "part-*.csv"))[0]
    final = os.path.join(APP, "data", "curated", f"{nombre}.csv")
    shutil.move(parte, final)
    shutil.rmtree(destino_tmp, ignore_errors=True)


def main():
    a = parse_args()
    cargar_env(os.path.join(APP, ".env"))
    t0 = time.time()

    b = SparkSession.builder.appName("epidemia-tolima")
    if not a.origen:
        region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
        b = (b.config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
              .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
              .config("spark.hadoop.fs.s3a.endpoint", f"s3.{region}.amazonaws.com")
              .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"))
    spark = (b.config("spark.sql.shuffle.partitions", "8")
              .config("spark.sql.session.timeZone", "UTC")     # mismas horas en cualquier PC
              # Algunas versiones de pandas guardan fechas en NANOsegundos y Spark 3.5 no las lee
              # ("Illegal Parquet type: TIMESTAMP(NANOS)"); con esto se leen como número y se convierten
              .config("spark.sql.legacy.parquet.nanosAsLong", "true")
              .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    bucket = os.environ.get("S3_BUCKET")
    origen = a.origen or f"s3a://{bucket}/raw/contactos/"
    salida_s3 = None if a.origen else f"s3a://{bucket}/curated/"
    os.makedirs(os.path.join(APP, "data", "curated"), exist_ok=True)

    # ------------------------------------------------------------------ 1. LEER
    print(f"\n1) Leyendo histórico: {origen}")
    raw = spark.read.parquet(origen)
    if dict(raw.dtypes)["fecha_hora"] == "bigint":          # nanosegundos -> timestamp
        raw = raw.withColumn("fecha_hora", F.timestamp_micros((F.col("fecha_hora") / 1000).cast("long")))
    n_raw = raw.count()
    print(f"   Registros leídos: {n_raw:,d}  |  particiones (días): {raw.select('fecha').distinct().count()}")

    # ------------------------------------------------------------------ 2. LIMPIAR
    print("2) Limpieza")
    c = (raw.dropDuplicates(["contacto_id"])
            .filter(F.col("persona_a").isNotNull() & F.col("persona_b").isNotNull())
            .filter(F.col("persona_a") != F.col("persona_b"))
            .filter((F.col("duracion_min") > 0) & (F.col("distancia_m") > 0))
            .withColumn("fecha_hora", F.col("fecha_hora").cast("timestamp"))
            .withColumn("fecha", F.date_format("fecha_hora", "yyyy-MM-dd"))
            .withColumn("fecha_positivo_a", F.to_date("fecha_positivo_a"))
            .withColumn("fecha_positivo_b", F.to_date("fecha_positivo_b"))
            .cache())
    n = c.count()
    print(f"   Registros válidos: {n:,d}  (descartados: {n_raw - n:,d})")

    # Vista "por persona": cada contacto visto desde las dos personas
    lado_a = c.select(F.col("persona_a").alias("persona_id"), F.col("persona_b").alias("otra"),
                      "edad_a", "sexo_a", "fecha_positivo_a", "desenlace_a", "fecha_positivo_b",
                      "fecha_hora", "municipio", "lugar_id", "duracion_min", "distancia_m")
    lado_b = c.select(F.col("persona_b").alias("persona_id"), F.col("persona_a").alias("otra"),
                      "edad_b", "sexo_b", "fecha_positivo_b", "desenlace_b", "fecha_positivo_a",
                      "fecha_hora", "municipio", "lugar_id", "duracion_min", "distancia_m")
    cols = ["persona_id", "otra", "edad", "sexo", "fecha_positivo", "desenlace", "fecha_positivo_otra",
            "fecha_hora", "municipio", "lugar_id", "duracion_min", "distancia_m"]
    pp = lado_a.toDF(*cols).unionByName(lado_b.toDF(*cols)).cache()

    # ------------------------------------------------------------------ 3. INDICADORES
    print("3) Indicadores")

    # 3.1 Personas (nodos): atributos + grado (contactos distintos) + municipio principal
    w = Window.partitionBy("persona_id").orderBy(F.desc("n"))
    mun_principal = (pp.groupBy("persona_id", "municipio").agg(F.count("*").alias("n"))
                       .withColumn("r", F.row_number().over(w)).filter("r = 1")
                       .select("persona_id", "municipio"))
    personas = (pp.groupBy("persona_id")
                  .agg(F.first("edad").alias("edad"), F.first("sexo").alias("sexo"),
                       F.max("fecha_positivo").alias("fecha_positivo"),
                       F.first("desenlace").alias("desenlace"),
                       F.countDistinct("otra").alias("grado"),
                       F.count("*").alias("n_contactos"))
                  .join(mun_principal, "persona_id"))
    personas.cache()
    print(f"   Personas: {personas.count():,d}  |  confirmadas: "
          f"{personas.filter(F.col('fecha_positivo').isNotNull()).count():,d}")

    # 3.2 Aristas persona-persona agregadas (relación CONTACTO para Neo4j)
    aristas = (c.withColumn("p1", F.least("persona_a", "persona_b"))
                .withColumn("p2", F.greatest("persona_a", "persona_b"))
                .groupBy("p1", "p2")
                .agg(F.count("*").alias("n_contactos"),
                     F.sum("duracion_min").alias("minutos_total"),
                     F.min("distancia_m").alias("distancia_min"),
                     F.min("fecha_hora").alias("primer_contacto"),
                     F.max("fecha_hora").alias("ultimo_contacto"))
                .withColumnRenamed("p1", "persona_1").withColumnRenamed("p2", "persona_2"))

    # 3.3 Lugares y visitas persona-lugar (relación VISITO para Neo4j)
    lugares = (c.groupBy("lugar_id", "tipo_lugar", "municipio")
                .agg(F.count("*").alias("n_contactos"),
                     F.countDistinct("fecha").alias("dias_activo")))
    visitas = (pp.groupBy("persona_id", "lugar_id")
                 .agg(F.count("*").alias("n_contactos"),
                      F.min("fecha_hora").alias("primera_vez")))

    # 3.4 Indicadores diarios por municipio (ALCANCE) para el tablero
    contactos_dia = (c.groupBy(F.col("fecha").alias("fecha"), "municipio")
                       .agg(F.count("*").alias("contactos"),
                            F.sum(F.when(F.col("distancia_m") < 1, 1).otherwise(0)).alias("contactos_cercanos")))
    casos_dia = (personas.filter(F.col("fecha_positivo").isNotNull())
                   .groupBy(F.date_format("fecha_positivo", "yyyy-MM-dd").alias("fecha"), "municipio")
                   .agg(F.count("*").alias("casos_confirmados"),
                        F.sum(F.when(F.col("desenlace") == "HOSPITALIZADO", 1).otherwise(0)).alias("hospitalizados"),
                        F.sum(F.when(F.col("desenlace") == "FALLECIDO", 1).otherwise(0)).alias("fallecidos")))
    indicadores = (contactos_dia.join(casos_dia, ["fecha", "municipio"], "full")
                     .na.fill(0).orderBy("fecha", "municipio"))

    # 3.5 TIEMPO DE DETECCIÓN: días entre la primera exposición a un caso
    #     (contacto con alguien que dio positivo en los 14 días siguientes) y la prueba positiva propia
    expos = (pp.filter(F.col("fecha_positivo").isNotNull() & F.col("fecha_positivo_otra").isNotNull())
               .filter((F.datediff("fecha_positivo_otra", F.to_date("fecha_hora")).between(0, 14)) &
                       (F.to_date("fecha_hora") <= F.col("fecha_positivo")))
               .groupBy("persona_id", "municipio", "fecha_positivo")
               .agg(F.min(F.to_date("fecha_hora")).alias("fecha_exposicion")))
    deteccion = (expos.withColumn("dias_deteccion", F.datediff("fecha_positivo", "fecha_exposicion"))
                      .filter(F.col("dias_deteccion").between(0, 21)))
    det = deteccion.agg(F.avg("dias_deteccion"), F.expr("percentile_approx(dias_deteccion, 0.5)")).first()
    print(f"   Tiempo de detección: promedio {det[0]:.1f} días | mediana {det[1]} días "
          f"({deteccion.count():,d} casos con exposición identificada)")

    # 3.6 NODOS CRÍTICOS: top 20 por contactos distintos (grado)
    criticos = personas.orderBy(F.desc("grado")).limit(20)
    print("   Top 5 nodos críticos (más contactos distintos):")
    for r in criticos.limit(5).collect():
        print(f"     {r.persona_id}  grado={r.grado:3d}  municipio={r.municipio}  positivo={r.fecha_positivo}")

    # ------------------------------------------------------------------ 4. SIMULACIÓN
    print(f"4) Simulación de propagación ({a.oleadas} oleadas)")
    # Aristas dirigidas con probabilidad de contagio según duración y distancia
    dirigidas = pp.select(F.col("persona_id").alias("origen"), F.col("otra").alias("destino"),
                          "fecha_hora", "duracion_min", "distancia_m")
    dirigidas = dirigidas.withColumn(
        "p", F.least(F.lit(0.9), F.col("duracion_min") / 60 * 0.5 *
                     F.when(F.col("distancia_m") < 1, 1.3).otherwise(0.8))).cache()
    # Casos índice: los 20 primeros confirmados; se asume infección 5 días antes de la prueba
    infectados = (personas.filter(F.col("fecha_positivo").isNotNull())
                    .orderBy("fecha_positivo", "persona_id").limit(20)
                    .select("persona_id", F.expr("cast(date_sub(fecha_positivo, 5) as timestamp)").alias("t_inf"),
                            F.lit(0).alias("oleada"))).localCheckpoint()
    frente = infectados
    filas_sim = [(0, infectados.count())]
    for k in range(1, a.oleadas + 1):
        nuevos = (frente.join(dirigidas, frente.persona_id == dirigidas.origen)
                        .filter((F.col("fecha_hora") >= F.col("t_inf")) &
                                (F.col("fecha_hora") <= F.col("t_inf") + F.expr("INTERVAL 14 DAYS")))
                        .filter(F.rand(seed=k) < F.col("p"))                  # sorteo de contagio
                        .groupBy(F.col("destino").alias("persona_id"))
                        .agg(F.min("fecha_hora").alias("t_inf"))
                        .join(infectados.select("persona_id"), "persona_id", "left_anti")
                        .withColumn("oleada", F.lit(k))
                        .localCheckpoint())          # corta el linaje: evita que el plan crezca en cada oleada
        cuantos = nuevos.count()
        infectados = infectados.unionByName(nuevos).localCheckpoint()
        filas_sim.append((k, cuantos))
        frente = nuevos
        print(f"   Oleada {k}: {cuantos:,d} nuevos infectados")
    total_sim = infectados.count()
    coinciden = (infectados.join(personas.filter(F.col("fecha_positivo").isNotNull()), "persona_id").count())
    print(f"   Alcance simulado: {total_sim:,d} personas | de ellas {coinciden:,d} "
          f"({coinciden/total_sim:.0%}) sí fueron casos confirmados en los datos")
    acumulado, sim_rows = 0, []
    for k, v in filas_sim:
        acumulado += v
        sim_rows.append((k, v, acumulado))
    simulacion = spark.createDataFrame(sim_rows, ["oleada", "nuevos_infectados", "acumulado"])

    # ------------------------------------------------------------------ 5. ESCRIBIR
    print("5) Guardando resultados")
    salidas = {"personas": personas, "aristas_contacto": aristas, "lugares": lugares,
               "visitas": visitas, "indicadores_diarios": indicadores,
               "deteccion": deteccion, "nodos_criticos": criticos, "simulacion": simulacion,
               "simulacion_infectados": infectados}
    for nombre, df in salidas.items():
        guardar_csv(df, nombre)
        if salida_s3:
            df.write.mode("overwrite").parquet(salida_s3 + nombre)
        print(f"   {nombre:24s} -> data/curated/{nombre}.csv" + (f"  +  {salida_s3}{nombre}/" if salida_s3 else ""))

    print(f"\nTiempo total Spark: {time.time() - t0:,.1f} s")
    spark.stop()


if __name__ == "__main__":
    main()
