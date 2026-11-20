#!/usr/bin/env python3
"""
Paso 4 - Consumidor Kafka -> Cassandra
Lee los eventos del topic "contactos" y los guarda en las dos tablas de
Cassandra definidas en modelos/cassandra/esquema.cql.

Uso (desde la carpeta del proyecto, con docker compose arriba):
    python ingesta/consumidor_cassandra.py               # procesa todo lo que hay en Kafka y termina
    python ingesta/consumidor_cassandra.py --continuo    # se queda escuchando (para la demo en vivo)
    python ingesta/consumidor_cassandra.py --concurrencia 8   # si el PC es lento y salen timeouts
"""
import argparse
import json
import os
import time
from datetime import date, datetime

from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.io.asyncioreactor import AsyncioConnection   # funciona en Windows y Python 3.12+
from confluent_kafka import Consumer, KafkaException

ESQUEMA = os.path.join("modelos", "cassandra", "esquema.cql")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--kafka", default="localhost:9092")
    p.add_argument("--cassandra", default="127.0.0.1")
    p.add_argument("--topic", default="contactos")
    p.add_argument("--grupo", default="escritor-cassandra")
    p.add_argument("--lote", type=int, default=500, help="Mensajes por lote")
    p.add_argument("--concurrencia", type=int, default=16,
                   help="Escrituras simultáneas en Cassandra (bajar a 8 si hay timeouts)")
    p.add_argument("--continuo", action="store_true", help="No terminar cuando se acaben los mensajes")
    return p.parse_args()


def crear_esquema(sesion):
    """Ejecuta cada sentencia del archivo esquema.cql (crea keyspace y tablas si no existen)."""
    texto = "\n".join(l for l in open(ESQUEMA, encoding="utf-8") if not l.strip().startswith("--"))
    for sentencia in [s.strip() for s in texto.split(";") if s.strip()]:
        sesion.execute(sentencia)
    print("Esquema listo: keyspace 'epidemia' con 2 tablas")


def escribir(sesion, sentencia, filas, concurrencia):
    """Escribe en paralelo y REINTENTA las filas que fallen por timeout
    (Cassandra en un PC con poca memoria a veces responde lento)."""
    pendientes = filas
    for intento in range(8):
        res = execute_concurrent_with_args(sesion, sentencia, pendientes,
                                           concurrency=concurrencia, raise_on_first_error=False)
        fallidas = [f for f, (ok, _) in zip(pendientes, res) if not ok]
        if not fallidas:
            return
        espera = 2 * (intento + 1)
        print(f"    {len(fallidas)} escrituras lentas, reintentando en {espera}s...")
        time.sleep(espera)
        pendientes = fallidas
    raise RuntimeError("Cassandra no responde. Revise la memoria de Docker (docker stats).")


def a_fecha(txt):
    return date.fromisoformat(txt) if txt else None


def main():
    a = parse_args()
    cluster = Cluster([a.cassandra], connection_class=AsyncioConnection)
    s = cluster.connect()
    s.default_timeout = 60
    crear_esquema(s)

    ins_persona = s.prepare("""
        INSERT INTO epidemia.contactos_por_persona
        (persona_id, fecha_hora, contacto_id, otra_persona, edad_otra, lugar_id, tipo_lugar,
         municipio, duracion_min, distancia_m, fecha_positivo_otra)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""")
    ins_lugar = s.prepare("""
        INSERT INTO epidemia.contactos_por_lugar_dia
        (lugar_id, fecha, fecha_hora, contacto_id, persona_a, persona_b, tipo_lugar,
         municipio, duracion_min, distancia_m)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""")

    consumidor = Consumer({
        "bootstrap.servers": a.kafka,
        "group.id": a.grupo,
        "auto.offset.reset": "earliest",   # si el grupo es nuevo, empieza desde el primer mensaje
        "enable.auto.commit": False,       # confirmamos el offset SOLO después de escribir en Cassandra
        "max.poll.interval.ms": 1800000,   # tolera pausas largas (PC lento o suspendido) sin salir del grupo
        "session.timeout.ms": 60000,
    })
    consumidor.subscribe([a.topic])
    print(f"Escuchando el topic '{a.topic}'... (Ctrl+C para detener)")

    total, vacios, t0 = 0, 0, None
    try:
        while True:
            msgs = consumidor.consume(num_messages=a.lote, timeout=2.0)
            msgs = [m for m in msgs if m.error() is None]
            if not msgs:
                vacios += 1
                if not a.continuo and total > 0 and vacios >= 5:
                    break                      # 10 s sin mensajes: terminó
                continue
            vacios = 0
            t0 = t0 or time.time()

            filas_p, filas_l = [], []
            for m in msgs:
                e = json.loads(m.value())
                fh = datetime.strptime(e["fecha_hora"], "%Y-%m-%d %H:%M")
                comunes = (e["lugar_id"], e["tipo_lugar"], e["municipio"], e["duracion_min"], e["distancia_m"])
                # contacto visto desde la persona A y desde la persona B
                filas_p.append((e["persona_a"], fh, e["contacto_id"], e["persona_b"], e["edad_b"], *comunes,
                                a_fecha(e["fecha_positivo_b"])))
                filas_p.append((e["persona_b"], fh, e["contacto_id"], e["persona_a"], e["edad_a"], *comunes,
                                a_fecha(e["fecha_positivo_a"])))
                filas_l.append((e["lugar_id"], fh.date(), fh, e["contacto_id"], e["persona_a"], e["persona_b"],
                                e["tipo_lugar"], e["municipio"], e["duracion_min"], e["distancia_m"]))

            escribir(s, ins_persona, filas_p, a.concurrencia)
            escribir(s, ins_lugar, filas_l, a.concurrencia)
            try:
                consumidor.commit(asynchronous=False)
            except KafkaException as err:
                # Si Kafka reasignó las particiones (pausa larga), este lote se volverá a leer.
                # No hay duplicados: en Cassandra un INSERT con la misma clave sobrescribe (upsert).
                print(f"    Aviso: no se pudo confirmar el lote ({err.args[0].str()}); se reintentará.")

            total += len(msgs)
            if total % 50000 < len(msgs):
                seg = time.time() - t0
                print(f"  {total:>8,d} eventos guardados | {total/seg:,.0f} eventos/s")
    except KeyboardInterrupt:
        pass
    finally:
        consumidor.close()
        cluster.shutdown()

    if t0:
        seg = time.time() - t0
        print("-" * 50)
        print(f"Eventos procesados : {total:,d}")
        print(f"Filas escritas     : {total * 3:,d}  (2 por persona + 1 por lugar)")
        print(f"Tiempo             : {seg:,.1f} s")
        print(f"Rendimiento        : {total/seg:,.0f} eventos/segundo")


if __name__ == "__main__":
    main()
