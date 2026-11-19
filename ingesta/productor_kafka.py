#!/usr/bin/env python3
"""
Paso 3 - Productor Kafka
Lee data/dataset_epidemia_tolima.csv y envía cada contacto como un evento JSON
al tema (topic) "contactos" de Kafka, simulando un flujo en tiempo real.

Uso (desde la carpeta del proyecto):
    python ingesta/productor_kafka.py --limite 1000      # prueba rápida
    python ingesta/productor_kafka.py                    # todo el dataset
    python ingesta/productor_kafka.py --tasa 500         # 500 eventos/segundo (para la demo en vivo)
    python ingesta/productor_kafka.py --simular          # solo lee el CSV, no envía (sin Kafka)
"""
import argparse
import csv
import json
import os
import time

CAMPOS_ENTEROS = ("edad_a", "edad_b", "duracion_min")
CAMPOS_DECIMALES = ("distancia_m",)


def parse_args():
    p = argparse.ArgumentParser(description="Envía el dataset de contactos a Kafka")
    p.add_argument("--csv", default=os.path.join("data", "dataset_epidemia_tolima.csv"))
    p.add_argument("--servidor", default="localhost:9092")
    p.add_argument("--topic", default="contactos")
    p.add_argument("--limite", type=int, default=0, help="Enviar solo N registros (0 = todos)")
    p.add_argument("--tasa", type=int, default=0, help="Eventos por segundo (0 = lo más rápido posible)")
    p.add_argument("--simular", action="store_true", help="No envía a Kafka, solo mide la lectura")
    return p.parse_args()


def limpiar(fila):
    """Convierte tipos y deja vacíos como null."""
    for c in CAMPOS_ENTEROS:
        fila[c] = int(fila[c])
    for c in CAMPOS_DECIMALES:
        fila[c] = float(fila[c])
    for c in ("fecha_positivo_a", "fecha_positivo_b"):
        fila[c] = fila[c] or None
    return fila


def crear_topic(servidor, topic):
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({"bootstrap.servers": servidor})
    if topic not in admin.list_topics(timeout=10).topics:
        f = admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])
        f[topic].result()
        print(f"Topic '{topic}' creado (3 particiones)")
    else:
        print(f"Topic '{topic}' ya existe")


def main():
    a = parse_args()
    if not os.path.exists(a.csv):
        raise SystemExit(f"No se encuentra el CSV en: {a.csv}")

    productor = None
    errores = {"n": 0}
    if not a.simular:
        from confluent_kafka import Producer
        crear_topic(a.servidor, a.topic)
        productor = Producer({
            "bootstrap.servers": a.servidor,
            "linger.ms": 50,              # agrupa mensajes en lotes
            "batch.num.messages": 10000,
            "compression.type": "lz4",
            "acks": "1",
        })

    def reporte(err, msg):
        if err is not None:
            errores["n"] += 1

    enviados = 0
    t0 = time.time()
    with open(a.csv, newline="", encoding="utf-8") as f:
        for fila in csv.DictReader(f):
            evento = limpiar(fila)
            if productor:
                while True:
                    try:
                        # clave = municipio: los contactos de un mismo municipio van a la misma partición
                        productor.produce(a.topic, key=evento["municipio"],
                                          value=json.dumps(evento, ensure_ascii=False), callback=reporte)
                        break
                    except BufferError:
                        productor.poll(0.5)   # cola llena: esperar a que Kafka reciba
                productor.poll(0)
            enviados += 1

            if a.tasa:
                espera = enviados / a.tasa - (time.time() - t0)
                if espera > 0:
                    time.sleep(espera)
            if enviados % 50000 == 0:
                seg = time.time() - t0
                print(f"  {enviados:>8,d} enviados | {enviados/seg:,.0f} eventos/s")
            if a.limite and enviados >= a.limite:
                break

    if productor:
        productor.flush(60)
    seg = time.time() - t0
    print("-" * 50)
    print(f"Total enviados : {enviados:,d}")
    print(f"Errores        : {errores['n']:,d}")
    print(f"Tiempo         : {seg:,.1f} s")
    print(f"Rendimiento    : {enviados/seg:,.0f} eventos/segundo")


if __name__ == "__main__":
    main()
