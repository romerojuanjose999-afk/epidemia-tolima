#!/usr/bin/env python3
"""
Paso 5 - Consumidor Kafka -> S3 (data lake histórico)
Lee los eventos del topic "contactos", los convierte a formato Parquet
particionado por fecha y los sube a Amazon S3:

    s3://<bucket>/raw/contactos/fecha=2025-03-03/contactos.parquet
    s3://<bucket>/raw/contactos/fecha=2025-03-04/contactos.parquet
    ...

Parquet es columnar y comprimido: el CSV de 55 MB queda en pocos MB,
y Spark solo lee las columnas y fechas que necesita.

Requisitos: archivo .env con AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
AWS_DEFAULT_REGION y S3_BUCKET (ver .env.example).

Uso (desde la carpeta del proyecto):
    python ingesta/consumidor_s3.py
    python ingesta/consumidor_s3.py --grupo escritor-s3-v2   # volver a leer TODO el topic (grupo nuevo)
"""
import argparse
import io
import json
import os
import time

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from confluent_kafka import Consumer


def cargar_env(ruta=".env"):
    """Lee el archivo .env (clave=valor) y lo pone en las variables de entorno."""
    if not os.path.exists(ruta):
        raise SystemExit("Falta el archivo .env (copie .env.example como .env y llene sus llaves de AWS)")
    for linea in open(ruta, encoding="utf-8"):
        linea = linea.strip()
        if linea and not linea.startswith("#") and "=" in linea:
            k, v = linea.split("=", 1)
            if v.strip():
                os.environ[k.strip()] = v.strip()


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--kafka", default="localhost:9092")
    p.add_argument("--topic", default="contactos")
    p.add_argument("--grupo", default="escritor-s3")
    p.add_argument("--prefijo", default="raw/contactos")
    return p.parse_args()


def leer_kafka(a):
    """Lee todos los mensajes disponibles del topic (termina tras 10 s sin mensajes)."""
    c = Consumer({
        "bootstrap.servers": a.kafka,
        "group.id": a.grupo,              # grupo distinto al de Cassandra: cada uno lee TODO el topic
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 1800000,
    })
    c.subscribe([a.topic])
    eventos, vacios = [], 0
    print(f"1) Leyendo el topic '{a.topic}' desde Kafka...")
    while vacios < (5 if eventos else 15):        # 10 s sin mensajes al final (30 s si nunca llegan)
        msgs = [m for m in c.consume(num_messages=5000, timeout=2.0) if m.error() is None]
        if not msgs:
            vacios += 1
            continue
        vacios = 0
        eventos.extend(json.loads(m.value()) for m in msgs)
        if len(eventos) % 100000 < len(msgs):
            print(f"   {len(eventos):,d} eventos leídos")
    print(f"   Total leídos: {len(eventos):,d}")
    return c, eventos


def asegurar_bucket(s3, bucket, region):
    try:
        s3.head_bucket(Bucket=bucket)
        print(f"   Bucket '{bucket}' ya existe")
    except ClientError:
        cfg = {} if region == "us-east-1" else {"CreateBucketConfiguration": {"LocationConstraint": region}}
        s3.create_bucket(Bucket=bucket, **cfg)
        print(f"   Bucket '{bucket}' creado en {region}")


def main():
    a = parse_args()
    cargar_env()
    bucket = os.environ.get("S3_BUCKET")
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    if not bucket:
        raise SystemExit("Falta S3_BUCKET en el archivo .env")

    t0 = time.time()
    consumidor, eventos = leer_kafka(a)
    if not eventos:
        raise SystemExit("No hay mensajes en Kafka. Ejecute primero: python ingesta/productor_kafka.py")

    print("2) Convirtiendo a Parquet particionado por fecha...")
    df = pd.DataFrame(eventos)
    df["fecha_hora"] = pd.to_datetime(df["fecha_hora"])
    df["fecha"] = df["fecha_hora"].dt.strftime("%Y-%m-%d")
    df = df.drop_duplicates("contacto_id")          # por si el topic tuviera mensajes repetidos

    print(f"3) Subiendo a s3://{bucket}/{a.prefijo}/ ...")
    s3 = boto3.client("s3", region_name=region)
    asegurar_bucket(s3, bucket, region)
    bytes_total, archivos = 0, 0
    for fecha, parte in df.groupby("fecha"):
        buf = io.BytesIO()
        # coerce_timestamps="us": fechas en MICROsegundos, el formato que Spark 3.x sabe leer
        # (algunas versiones de pandas guardan nanosegundos y Spark falla con "Illegal Parquet type")
        parte.drop(columns="fecha").to_parquet(buf, index=False, compression="snappy",
                                               coerce_timestamps="us", allow_truncated_timestamps=True)
        clave = f"{a.prefijo}/fecha={fecha}/contactos.parquet"
        s3.put_object(Bucket=bucket, Key=clave, Body=buf.getvalue())
        bytes_total += buf.tell()
        archivos += 1
        if archivos % 15 == 0:
            print(f"   {archivos} particiones subidas...")

    consumidor.commit(asynchronous=False)          # confirmar en Kafka solo después de subir a S3
    consumidor.close()

    seg = time.time() - t0
    print("-" * 55)
    print(f"Registros en S3     : {len(df):,d}")
    print(f"Particiones (días)  : {archivos}")
    print(f"Tamaño en Parquet   : {bytes_total/1e6:,.1f} MB  (el CSV pesa ~55 MB)")
    print(f"Tiempo total        : {seg:,.1f} s")
    print(f"Ruta                : s3://{bucket}/{a.prefijo}/")


if __name__ == "__main__":
    main()
