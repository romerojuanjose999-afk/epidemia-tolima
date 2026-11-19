# Rastreo y propagación de una epidemia en el Tolima

Trabajo parcial · Bases de datos masivas · Reto 5: Trazabilidad y propagación en una epidemia
**Integrantes:** Juan José Romero Hernández · César

## Problema
Ante un brote, la autoridad de salud necesita saber en segundos **de dónde vino un caso (rastreo hacia atrás)** y **a quién pudo contagiar (rastreo hacia adelante)**, qué personas y lugares son críticos y cómo evoluciona el brote por municipio. Los contactos llegan como un flujo continuo de cientos de miles de eventos.

## Arquitectura
```
CSV → Kafka → Cassandra (consulta por persona / lugar)
          └→ S3 (histórico) → Spark (análisis + simulación) → Neo4j (rastreo)
                                                         └→ BigQuery (tablero)
```

## Datos
- Dataset sintético: **526 037 contactos** entre 37 000 personas de los 47 municipios del Tolima, durante 90 días.
- Generado con un modelo SEIR sobre la red de contactos (`generador/generar_dataset.py`), con semilla fija: siempre produce el mismo archivo. El checksum está en `data/metadatos.json`.

## Requisitos
- Docker Desktop (Docker con 4 GB de memoria como mínimo)
- Python 3.10 o superior: `pip install -r requirements.txt`

## Instalación y ejecución
```bash
# 1. Generar el dataset → data/dataset_epidemia_tolima.csv
python generador/generar_dataset.py

# 2. Levantar Kafka, Cassandra, Neo4j y Spark
docker compose up -d

# 3. Enviar los contactos a Kafka
python ingesta/productor_kafka.py
```

## Interfaces
| Servicio | URL |
|---|---|
| Kafka UI | http://localhost:8090 |
| Neo4j | http://localhost:7474 (neo4j / epidemia2025) |
| Spark | http://localhost:8080 |

## Estado
- [x] Paso 1: dataset y generador
- [x] Paso 2: infraestructura (docker-compose)
- [x] Paso 3: productor Kafka
- [ ] Paso 4: Kafka → Cassandra
- [ ] Paso 5: histórico en S3
- [ ] Paso 6: Spark
- [ ] Paso 7: grafo en Neo4j
- [ ] Paso 8: tablero en BigQuery
