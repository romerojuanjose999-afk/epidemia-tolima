# Rastreo y propagación de una epidemia en el Tolima

Trabajo parcial · Bases de datos masivas · UNIMINUTO Ibagué
**Reto 5:** Trazabilidad y propagación en una epidemia
**Integrantes:** Juan José Romero Hernández · César Rodríguez

## Problema
Ante un brote, la autoridad de salud necesita saber en segundos **de dónde vino un caso** (rastreo hacia atrás) y **a quién pudo contagiar** (rastreo hacia adelante), qué personas y lugares son **nodos críticos**, y cuál es el **alcance** y el **tiempo de detección** por municipio. Los contactos llegan en flujo continuo: 526 037 en 90 días para 37 000 personas.

## Arquitectura (6 tecnologías)
```
Generador SEIR (CSV) → Kafka ─┬─> Cassandra   (rastreo por persona / lugar, 8–21 ms)
                              └─> Amazon S3   (raw: Parquet por fecha)
                                     │
                                     ▼
                                   Spark  (limpieza, indicadores, simulación) → S3 curated/
                                     ├─> Neo4j    (grafo: rastreo, PageRank, comunidades)
                                     └─> BigQuery (8 vistas) → Looker Studio (tablero)
```

## Estructura del repositorio
| Carpeta | Contenido |
|---|---|
| `generador/` | Generador sintético reproducible (semilla fija) |
| `ingesta/` | Productor Kafka y consumidores Kafka → Cassandra y Kafka → S3 |
| `procesamiento/` | Job de Spark (lee S3, limpia, calcula indicadores, simula) |
| `modelos/` | Esquema de Cassandra (CQL) y modelo de grafo de Neo4j (Cypher) |
| `grafo/` | Carga del grafo en Neo4j |
| `analitica/` | Carga a BigQuery y creación de vistas |
| `consultas/` | Consultas CQL, Cypher y SQL, y scripts que las ejecutan con tiempos |
| `data/` | `metadatos.json` (checksum del dataset). Los CSV no se suben: se regeneran |
| `docker-compose.yml` | Kafka, Kafka UI, Cassandra, Neo4j, Spark master y worker |

## Requisitos
- Docker Desktop con **al menos 4 GB** de memoria (archivo `%USERPROFILE%\.wslconfig` con `memory=4GB`)
- Python 3.10 o superior
- Cuenta de **AWS** (usuario IAM con `AmazonS3FullAccess`) y de **Google Cloud** (cuenta de servicio con *BigQuery Data Editor* y *BigQuery Job User*)

## Instalación
```powershell
git clone https://github.com/romerojuanjose999-afk/epidemia-tolima.git
cd epidemia-tolima
pip install -r requirements.txt
Copy-Item .env.example .env      # llenar las llaves de AWS, el bucket y el proyecto de Google
# guardar la llave JSON de Google en credenciales/gcp-sa.json
```
> `.env` y `credenciales/` están en `.gitignore`: **nunca** se suben.

## Ejecución paso a paso
Con 8 GB de RAM, encienda **solo los servicios de cada paso** (`docker compose stop ...` para el resto).

| # | Paso | Comando | Resultado esperado |
|---|---|---|---|
| 1 | Generar el dataset | `python generador/generar_dataset.py` | `data/dataset_epidemia_tolima.csv`, 526 037 contactos |
| 2 | Levantar Kafka y Cassandra | `docker compose up -d kafka kafka-ui cassandra` | Kafka UI en http://localhost:8090 |
| 3 | CSV → Kafka | `python ingesta/productor_kafka.py` | 526 037 eventos, unos 29 000 eventos/s |
| 4 | Kafka → Cassandra | `python ingesta/consumidor_cassandra.py` | 526 037 eventos guardados |
| | Verificar Cassandra | `python consultas/verificar_cassandra.py` | 526 037 filas; consultas en 8–21 ms |
| 5 | Kafka → S3 (Parquet) | `python ingesta/consumidor_s3.py` | 90 particiones `fecha=` en S3 |
| 6 | Spark (S3 → indicadores) | `docker compose up -d spark-master spark-worker` y luego el comando de abajo | 9 CSV en `data/curated/` y en S3 `curated/` |
| 7 | Grafo en Neo4j | `docker compose up -d neo4j`, `python grafo/cargar_neo4j.py` y `python consultas/consultas_neo4j.py` | 34 560 personas, 435 460 contactos, 7 consultas |
| 8 | BigQuery | `python analitica/cargar_bigquery.py` y `python consultas/consultas_bigquery.py` | 7 tablas y 8 vistas del tablero |

Comando del paso 6:
```powershell
docker exec -u root spark-master chmod -R 777 /opt/app/data
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.hadoop:hadoop-aws:3.3.4 --conf spark.jars.ivy=/opt/app/.ivy2 --driver-memory 768m --executor-memory 768m /opt/app/procesamiento/spark_procesamiento.py
```

## Interfaces
| Servicio | URL |
|---|---|
| Kafka UI | http://localhost:8090 |
| Neo4j Browser | http://localhost:7474 (neo4j / epidemia2025) |
| Spark master | http://localhost:8080 |
| BigQuery | consola de Google Cloud → `epidemia_tolima` |

## Resultados principales
- 3 428 casos confirmados (9,9 %); tiempo de detección promedio de **8,3 días**.
- Rastreo de un caso en **7,7 a 21,5 ms** (Cassandra) y cadenas de 3 saltos en **0,5 a 5,3 s** (Neo4j).
- Las medidas de contención redujeron los contactos un **51 %**.
- El nodo crítico número 1 por PageRank (P002604) **no estaba confirmado**.

## Problemas conocidos
| Síntoma | Solución |
|---|---|
| Docker responde `500 Internal Server Error` | Quit Docker Desktop, ejecutar `wsl --shutdown` y volver a abrirlo |
| Cassandra `WriteTimeout` | `python ingesta/consumidor_cassandra.py --concurrencia 8` |
| Spark `Permission denied: /opt/app/data/curated` | `docker exec -u root spark-master chmod -R 777 /opt/app/data` |
| Spark `Illegal Parquet type: TIMESTAMP(NANOS)` | Ya resuelto en el job (`nanosAsLong`) y en el consumidor S3 (`coerce_timestamps="us"`) |
