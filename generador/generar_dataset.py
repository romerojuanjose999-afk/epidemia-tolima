#!/usr/bin/env python3
"""
Generador sintético reproducible - Brote epidémico en el Tolima
Trabajo parcial: Arquitecturas de datos masivos integradas (Reto 5)

Simula una población sintética de municipios del Tolima, sus rutinas diarias
(colegio, trabajo, mercado, transporte, iglesia, plaza, hospital), los contactos
cercanos que se producen en cada lugar y la propagación de un brote con un
modelo SEIR sobre la red de contactos (hogar + contactos del día).

Resultado: un solo archivo  data/dataset_epidemia_tolima.csv  (una fila por contacto
cercano, con datos de las dos personas y su estado de salud) + data/metadatos.json

Uso (desde la carpeta del proyecto):
    python generador/generar_dataset.py                    # 37 000 personas, 90 días -> 526 037 contactos
    python generador/generar_dataset.py --personas 10000   # versión pequeña (pruebas de escalabilidad)
    python generador/generar_dataset.py --personas 74000 --salida data_2x

La misma semilla produce SIEMPRE los mismos archivos (reproducible).
"""
import argparse
import hashlib
import json
import os
import time
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# 1. Parámetros
# ---------------------------------------------------------------------------
# Población aproximada de los 47 municipios del Tolima (solo para ponderar
# cuántas personas sintéticas se ubican en cada uno; no son cifras oficiales).
MUNICIPIOS = [
    ("Ibagué", 541000), ("Alpujarra", 5000), ("Alvarado", 9000), ("Ambalema", 7000),
    ("Anzoátegui", 18000), ("Armero", 12000), ("Ataco", 23000), ("Cajamarca", 20000),
    ("Carmen de Apicalá", 9000), ("Casabianca", 6000), ("Chaparral", 47000), ("Coello", 10000),
    ("Coyaima", 28000), ("Cunday", 10000), ("Dolores", 8000), ("Espinal", 76000),
    ("Falan", 9000), ("Flandes", 29000), ("Fresno", 30000), ("Guamo", 32000),
    ("Herveo", 8000), ("Honda", 24000), ("Icononzo", 11000), ("Lérida", 18000),
    ("Líbano", 40000), ("Mariquita", 33000), ("Melgar", 38000), ("Murillo", 5000),
    ("Natagaima", 22000), ("Ortega", 32000), ("Palocabildo", 9000), ("Piedras", 6000),
    ("Planadas", 30000), ("Prado", 8000), ("Purificación", 30000), ("Rioblanco", 24000),
    ("Roncesvalles", 6000), ("Rovira", 20000), ("Saldaña", 15000), ("San Antonio", 15000),
    ("San Luis", 19000), ("Santa Isabel", 6000), ("Suárez", 5000), ("Valle de San Juan", 6000),
    ("Venadillo", 15000), ("Villahermosa", 11000), ("Villarrica", 6000),
]
POLOS = ["Ibagué", "Espinal"]  # destinos de quienes viajan a trabajar/estudiar a otro municipio

FECHA_INICIO = date(2025, 3, 3)  # lunes

# Contactos cercanos registrados por persona y día en cada tipo de lugar
CONTACTOS_POR_PERSONA = {
    "COLEGIO": 0.55, "UNIVERSIDAD": 0.50, "EMPRESA": 0.45, "MERCADO": 0.70,
    "TRANSPORTE": 0.90, "IGLESIA": 0.70, "PLAZA": 0.50, "HOSPITAL": 0.60,
}
DURACION_MEDIA_MIN = {
    "COLEGIO": 120, "UNIVERSIDAD": 90, "EMPRESA": 150, "MERCADO": 25,
    "TRANSPORTE": 45, "IGLESIA": 60, "PLAZA": 30, "HOSPITAL": 40,
}
HORA_TIPICA = {
    "COLEGIO": 7, "UNIVERSIDAD": 8, "EMPRESA": 8, "MERCADO": 9,
    "TRANSPORTE": 6, "IGLESIA": 10, "PLAZA": 17, "HOSPITAL": 9,
}

# Parámetros epidemiológicos (modelo SEIR)
P_HOGAR_DIA = 0.120          # prob. diaria de contagio entre convivientes
P_CONTACTO_BASE = 0.700      # prob. de contagio por contacto cercano de 60 min
LATENCIA = (2, 5)            # días en E (expuesto, no contagia)
INFECCIOSO = (5, 10)         # días en I (contagia)
INCUBACION = (3, 7)          # días hasta síntomas
N_CASOS_INDICE = 20
# Medidas de contención (restricción de movilidad) a partir de este día
DIA_MEDIDAS = 45
FACTOR_MEDIDAS = {"ESTUDIO": .30, "TRABAJO": .70, "COMUNITARIO": .50}


def parse_args():
    p = argparse.ArgumentParser(description="Generador sintético de brote epidémico (Tolima)")
    p.add_argument("--personas", type=int, default=37000)
    p.add_argument("--dias", type=int, default=90)
    p.add_argument("--semilla", type=int, default=2025)
    p.add_argument("--salida", default="data")
    return p.parse_args()


# ---------------------------------------------------------------------------
# 2. Población
# ---------------------------------------------------------------------------
def generar_municipios():
    df = pd.DataFrame(MUNICIPIOS, columns=["nombre", "poblacion_aprox"])
    df.insert(0, "municipio_id", [f"M{i+1:02d}" for i in range(len(df))])
    df["departamento"] = "Tolima"
    return df


def edad_aleatoria(rng, n):
    # Pirámide simplificada: (edad_min, edad_max, peso)
    grupos = [(0, 4, .07), (5, 17, .21), (18, 24, .12), (25, 39, .22), (40, 59, .24), (60, 90, .14)]
    idx = rng.choice(len(grupos), size=n, p=[g[2] for g in grupos])
    lo = np.array([g[0] for g in grupos])[idx]
    hi = np.array([g[1] for g in grupos])[idx]
    return rng.integers(lo, hi + 1)


def generar_personas(rng, n, municipios):
    pesos = municipios.poblacion_aprox / municipios.poblacion_aprox.sum()
    mun = rng.choice(municipios.municipio_id.values, size=n, p=pesos.values)
    edad = edad_aleatoria(rng, n)
    sexo = rng.choice(["F", "M"], size=n, p=[.51, .49])
    es_ibague = mun == "M01"
    zona = np.where(rng.random(n) < np.where(es_ibague, .93, .55), "URBANA", "RURAL")

    ocup = np.empty(n, dtype=object)
    u = rng.random(n)
    ocup[edad < 5] = "PRIMERA_INFANCIA"
    ocup[(edad >= 5) & (edad <= 17)] = "ESTUDIANTE"
    m = (edad >= 18) & (edad <= 24)
    ocup[m & (u < .40)] = "UNIVERSITARIO"
    ocup[m & (u >= .40) & (u < .90)] = "TRABAJADOR"
    ocup[m & (u >= .90)] = "HOGAR"
    m = (edad >= 25) & (edad <= 59)
    ocup[m & (u < .75)] = "TRABAJADOR"
    ocup[m & (u >= .75)] = "HOGAR"
    m = edad >= 60
    ocup[m & (u < .20)] = "TRABAJADOR"
    ocup[m & (u >= .20)] = "ADULTO_MAYOR"

    df = pd.DataFrame({
        "persona_id": [f"P{i+1:06d}" for i in range(n)],
        "edad": edad, "sexo": sexo, "municipio_id": mun, "zona": zona, "ocupacion": ocup,
    })

    # Hogares: se agrupan personas del mismo municipio y zona en hogares de 1 a 6
    df = df.sample(frac=1, random_state=int(rng.integers(1e9))).sort_values(["municipio_id", "zona"], kind="stable")
    tam = rng.choice([1, 2, 3, 4, 5, 6], size=n, p=[.12, .22, .26, .22, .12, .06])
    hogar = np.empty(n, dtype=np.int64)
    pos, h = 0, 0
    grupos = df.groupby(["municipio_id", "zona"], sort=False).size().values
    for g in grupos:
        fin = pos + g
        while pos < fin:
            t = min(tam[h % n], fin - pos)
            hogar[pos:pos + t] = h
            pos += t
            h += 1
    df["hogar_id"] = [f"H{x+1:06d}" for x in hogar]
    return df.sort_values("persona_id").reset_index(drop=True)


def generar_lugares(rng, personas, municipios):
    filas = []
    conteo = personas.groupby(["municipio_id", "ocupacion"]).size().unstack(fill_value=0)
    for mid in municipios.municipio_id:
        c = conteo.loc[mid] if mid in conteo.index else pd.Series(dtype=int)
        est = int(c.get("ESTUDIANTE", 0))
        trab = int(c.get("TRABAJADOR", 0))
        tot = int((personas.municipio_id == mid).sum())
        cant = {
            "COLEGIO": max(1, int(np.ceil(est / 350))),
            "EMPRESA": max(2, int(np.ceil(trab / 25))),
            "MERCADO": max(1, int(np.ceil(tot / 2500))),
            "TRANSPORTE": max(1, int(np.ceil(tot / 4000))),
            "IGLESIA": max(1, int(np.ceil(tot / 2000))),
            "PLAZA": max(1, int(np.ceil(tot / 6000))),
            "HOSPITAL": max(1, int(np.ceil(tot / 15000))),
        }
        if mid == "M01":
            cant["UNIVERSIDAD"] = 4
        for tipo, k in cant.items():
            for j in range(k):
                filas.append((mid, tipo, f"{tipo.title()} {j+1}"))
    df = pd.DataFrame(filas, columns=["municipio_id", "tipo", "nombre"])
    df.insert(0, "lugar_id", [f"L{i+1:05d}" for i in range(len(df))])
    df["aforo_aprox"] = df.tipo.map({"COLEGIO": 400, "UNIVERSIDAD": 3000, "EMPRESA": 30, "MERCADO": 300,
                                     "TRANSPORTE": 200, "IGLESIA": 250, "PLAZA": 500, "HOSPITAL": 150})
    nm = municipios.set_index("municipio_id").nombre
    df["nombre"] = df.nombre + " - " + df.municipio_id.map(nm)
    return df


def asignar_rutinas(rng, personas, lugares, municipios):
    """Asigna colegio / universidad / empresa fija y marca a quienes viajan a otro municipio."""
    n = len(personas)
    rutina = np.full(n, None, dtype=object)
    viaja = np.zeros(n, dtype=bool)
    polos = municipios.set_index("nombre").loc[POLOS, "municipio_id"].tolist()
    por_mun_tipo = {k: v.lugar_id.values for k, v in lugares.groupby(["municipio_id", "tipo"])}

    mun = personas.municipio_id.values
    ocup = personas.ocupacion.values
    for i in range(n):
        o, m = ocup[i], mun[i]
        if o == "ESTUDIANTE":
            rutina[i] = rng.choice(por_mun_tipo[(m, "COLEGIO")])
        elif o == "UNIVERSITARIO":
            rutina[i] = rng.choice(por_mun_tipo[("M01", "UNIVERSIDAD")])
            viaja[i] = m != "M01"
        elif o == "TRABAJADOR":
            if m not in polos and rng.random() < .06:
                destino = polos[int(rng.random() < .3)]
                viaja[i] = True
            else:
                destino = m
            rutina[i] = rng.choice(por_mun_tipo[(destino, "EMPRESA")])
    personas["lugar_rutina_id"] = rutina
    personas["viaja_otro_municipio"] = viaja
    return personas


# ---------------------------------------------------------------------------
# 3. Simulación día a día: visitas -> contactos -> contagios
# ---------------------------------------------------------------------------
def pares_en_grupos(rng, idx_personas, lugar_cod, k_por_persona):
    """Dado un arreglo de asistentes ordenado por lugar, genera pares aleatorios
    dentro de cada lugar (vectorizado)."""
    if len(idx_personas) < 2:
        return np.empty(0, int), np.empty(0, int), np.empty(0, int)
    cambios = np.flatnonzero(np.diff(lugar_cod)) + 1
    inicios = np.r_[0, cambios]
    tam = np.diff(np.r_[inicios, len(lugar_cod)])
    lugar_grupo = lugar_cod[inicios]
    m = rng.poisson(tam * k_por_persona[lugar_grupo] / 2.0)
    m[tam < 2] = 0
    total = m.sum()
    if total == 0:
        return np.empty(0, int), np.empty(0, int), np.empty(0, int)
    g = np.repeat(np.arange(len(tam)), m)
    n_g = tam[g]
    a = rng.integers(0, n_g)
    b = (a + 1 + rng.integers(0, n_g - 1)) % n_g
    off = inicios[g]
    return idx_personas[off + a], idx_personas[off + b], lugar_grupo[g]


def simular(rng, personas, lugares, municipios, dias):
    n = len(personas)
    t0 = time.time()
    lug_ids = lugares.lugar_id.values
    lug_tipo = lugares.tipo.values
    lug_mun = lugares.municipio_id.values
    cod_lugar = {l: i for i, l in enumerate(lug_ids)}
    tipos = ["COLEGIO", "UNIVERSIDAD", "EMPRESA", "MERCADO", "TRANSPORTE", "IGLESIA", "PLAZA", "HOSPITAL"]
    k_lugar = np.array([CONTACTOS_POR_PERSONA[t] for t in lug_tipo])
    dur_lugar = np.array([DURACION_MEDIA_MIN[t] for t in lug_tipo])
    hora_lugar = np.array([HORA_TIPICA[t] for t in lug_tipo])

    mun = personas.municipio_id.values
    ocup = personas.ocupacion.values
    edad = personas.edad.values
    rutina = np.array([cod_lugar.get(x, -1) if x is not None else -1 for x in personas.lugar_rutina_id.values])
    viaja = personas.viaja_otro_municipio.values

    # Lugares comunitarios por municipio
    def lugares_de(tipo):
        d = {}
        for i in range(len(lug_ids)):
            if lug_tipo[i] == tipo:
                d.setdefault(lug_mun[i], []).append(i)
        return {k: np.array(v) for k, v in d.items()}
    comunitarios = {t: lugares_de(t) for t in ["MERCADO", "TRANSPORTE", "IGLESIA", "PLAZA", "HOSPITAL"]}
    mun_codes, mun_idx = np.unique(mun, return_inverse=True)

    def elegir_comunitario(tipo, personas_idx):
        res = np.empty(len(personas_idx), dtype=np.int64)
        mi = mun_idx[personas_idx]
        for k in np.unique(mi):
            sel = mi == k
            opciones = comunitarios[tipo][mun_codes[k]]
            res[sel] = opciones[rng.integers(0, len(opciones), sel.sum())]
        return res

    # Hogares -> lista de convivientes
    hogar_cod = pd.factorize(personas.hogar_id.values)[0]
    orden_h = np.argsort(hogar_cod, kind="stable")
    hogar_ini = np.searchsorted(hogar_cod[orden_h], np.arange(hogar_cod.max() + 2))

    # Estado SEIR
    S, E, I, R, D = 0, 1, 2, 3, 4
    estado = np.zeros(n, np.int8)
    fin_estado = np.full(n, 10**6)
    dia_infeccion = np.full(n, -1)
    sintomatico = np.zeros(n, bool)
    dia_sintomas = np.full(n, -1)
    dia_prueba_pos = np.full(n, -1)
    aislado_desde = np.full(n, 10**6)
    transmisiones = []  # (persona, infectado_por, dia, via, lugar)
    eventos = []        # (persona, dia, hora, tipo)

    def infectar(idx, fuente, dia, via, lugar):
        idx = np.asarray(idx)
        nuevos = idx[estado[idx] == S]
        nuevos, pos_unica = np.unique(nuevos, return_index=True)
        if len(nuevos) == 0:
            return
        fuente = np.asarray(fuente)[estado[idx] == S][pos_unica] if np.ndim(fuente) else np.full(len(nuevos), fuente)
        lugar = np.asarray(lugar)[estado[idx] == S][pos_unica] if np.ndim(lugar) else np.full(len(nuevos), lugar)
        estado[nuevos] = E
        dia_infeccion[nuevos] = dia
        fin_estado[nuevos] = dia + rng.integers(LATENCIA[0], LATENCIA[1] + 1, len(nuevos))
        p_sint = np.where(edad[nuevos] >= 60, .80, np.where(edad[nuevos] < 18, .40, .65))
        s = rng.random(len(nuevos)) < p_sint
        sintomatico[nuevos] = s
        ds = dia + rng.integers(INCUBACION[0], INCUBACION[1] + 1, len(nuevos))
        dia_sintomas[nuevos[s]] = ds[s]
        # Pruebas: 70 % de sintomáticos se hace prueba 1-3 días después; 12 % de asintomáticos por tamizaje
        prob_prueba = np.where(s, .70, .12)
        hace = rng.random(len(nuevos)) < prob_prueba
        dp = np.where(s, ds + rng.integers(1, 4, len(nuevos)), dia + rng.integers(3, 10, len(nuevos)))
        dia_prueba_pos[nuevos[hace]] = dp[hace]
        # Aislamiento: desde la prueba positiva (o 50 % desde los síntomas)
        aisl = np.where(hace, dp, 10**6)
        aisl = np.where(s & (rng.random(len(nuevos)) < .5), np.minimum(aisl, ds), aisl)
        aislado_desde[nuevos] = aisl
        for p, f, l in zip(nuevos, fuente, lugar):
            transmisiones.append((p, f, dia, via, l))

    # Casos índice: concentrados en Ibagué y San Antonio + 2 importados
    cand = np.flatnonzero((ocup == "TRABAJADOR") & np.isin(mun, ["M01", "M40"]))
    otros = np.flatnonzero((ocup == "TRABAJADOR") & ~np.isin(mun, ["M01", "M40"]))
    indice = np.r_[rng.choice(cand, N_CASOS_INDICE - 2, replace=False), rng.choice(otros, 2, replace=False)]
    infectar(indice, -1, 0, "CASO_INDICE", -1)

    contactos_out = []
    for d in range(dias):
        fecha = FECHA_INICIO + timedelta(days=d)
        dow = fecha.weekday()  # 0 lunes ... 6 domingo
        habil = dow < 5
        activo = (estado != D) & (aislado_desde > d)
        hosp_hoy = np.zeros(n, bool)

        # --- Visitas del día
        vp, vl = [], []
        medidas = d >= DIA_MEDIDAS
        f_est = FACTOR_MEDIDAS["ESTUDIO"] if medidas else 1.0
        f_tra = FACTOR_MEDIDAS["TRABAJO"] if medidas else 1.0
        f_com = FACTOR_MEDIDAS["COMUNITARIO"] if medidas else 1.0
        if habil:
            r = activo & (rutina >= 0) & (rng.random(n) < np.where(ocup == "TRABAJADOR", .88 * f_tra, .93 * f_est))
            vp.append(np.flatnonzero(r)); vl.append(rutina[r])
            v = np.flatnonzero(r & viaja)
            if len(v):
                vp.append(v); vl.append(elegir_comunitario("TRANSPORTE", v))
        else:
            r = activo & (ocup == "TRABAJADOR") & (rutina >= 0) & (rng.random(n) < .25 * f_tra)
            vp.append(np.flatnonzero(r)); vl.append(rutina[r])
        prob = {"MERCADO": .10 if habil else .22, "PLAZA": .04 if habil else .10,
                "IGLESIA": .35 if dow == 6 else .02, "TRANSPORTE": .02}
        for tipo, p in prob.items():
            v = np.flatnonzero(activo & (ocup != "PRIMERA_INFANCIA") & (rng.random(n) < p * f_com))
            if len(v):
                vp.append(v); vl.append(elegir_comunitario(tipo, v))
        # Hospital: consultas generales + sintomáticos que consultan
        cons = (estado != D) & (rng.random(n) < .002)
        sint_hoy = (dia_sintomas == d) & (rng.random(n) < .35)
        v = np.flatnonzero(cons | sint_hoy)
        if len(v):
            vp.append(v); vl.append(elegir_comunitario("HOSPITAL", v))

        vp = np.concatenate(vp); vl = np.concatenate(vl)
        orden = np.lexsort((vp, vl))
        vp, vl = vp[orden], vl[orden]

        # --- Contactos cercanos dentro de cada lugar
        a, b, l = pares_en_grupos(rng, vp, vl, k_lugar)
        keep = a != b
        a, b, l = a[keep], b[keep], l[keep]
        dur_c = np.maximum(5, rng.exponential(dur_lugar[l] * .5)).astype(int)
        dist = np.round(rng.uniform(.3, 2.0, len(a)), 1)
        hora_c = np.clip(hora_lugar[l] + rng.integers(0, 4, len(l)), 5, 22)
        contactos_out.append(pd.DataFrame({"a": a, "b": b, "l": l, "d": d, "h": hora_c,
                                           "mi": rng.integers(0, 60, len(a)), "dur": dur_c, "dist": dist}))

        # --- Transmisión por contactos (si alguno de los dos está infeccioso)
        p_c = 1 - (1 - P_CONTACTO_BASE) ** (dur_c / 60.0)
        p_c = p_c * np.where(dist < 1.0, 1.3, 0.8)
        for src, dst in ((a, b), (b, a)):
            m = (estado[src] == I) & (estado[dst] == S)
            exito = m & (rng.random(len(src)) < p_c)
            if exito.any():
                infectar(dst[exito], src[exito], d, "CONTACTO", l[exito])

        # --- Transmisión en el hogar
        inf = np.flatnonzero(estado == I)
        if len(inf):
            hs = hogar_cod[inf]
            for p, h in zip(inf, hs):
                miembros = orden_h[hogar_ini[h]:hogar_ini[h + 1]]
                miembros = miembros[(miembros != p) & (estado[miembros] == S)]
                if len(miembros):
                    exito = miembros[rng.random(len(miembros)) < P_HOGAR_DIA]
                    if len(exito):
                        infectar(exito, p, d, "HOGAR", -1)

        # --- Transiciones de estado al final del día
        e2i = (estado == E) & (fin_estado <= d)
        estado[e2i] = I
        fin_estado[e2i] = d + rng.integers(INFECCIOSO[0], INFECCIOSO[1] + 1, e2i.sum())
        i2r = np.flatnonzero((estado == I) & (fin_estado <= d))
        if len(i2r):
            # Hospitalización / fallecimiento según edad (solo sintomáticos)
            e = edad[i2r]
            p_h = np.where(e >= 60, .15, np.where(e >= 40, .04, .01)) * sintomatico[i2r]
            hosp = rng.random(len(i2r)) < p_h
            p_d = np.where(e >= 60, .25, .06) * hosp
            muere = rng.random(len(i2r)) < p_d
            for p, ho, mu in zip(i2r, hosp, muere):
                if ho:
                    eventos.append((p, max(dia_sintomas[p], d - 3), 14, "HOSPITALIZACION"))
                eventos.append((p, d, 18, "FALLECIDO" if mu else "RECUPERADO"))
            estado[i2r] = np.where(muere, D, R)

        if d % 10 == 0 or d == dias - 1:
            print(f"  día {d:3d} {fecha}  S={np.sum(estado==S):6d} E={np.sum(estado==E):5d} "
                  f"I={np.sum(estado==I):5d} R={np.sum(estado==R):6d} D={np.sum(estado==D):4d}  "
                  f"({time.time()-t0:.0f}s)")

    # Eventos de síntomas / pruebas
    for p in np.flatnonzero((dia_sintomas >= 0) & (dia_sintomas < dias)):
        eventos.append((p, dia_sintomas[p], 8, "INICIO_SINTOMAS"))
    for p in np.flatnonzero((dia_prueba_pos >= 0) & (dia_prueba_pos < dias)):
        eventos.append((p, dia_prueba_pos[p], 11, "PRUEBA_POSITIVA"))
    # Pruebas negativas de tamizaje en personas no infectadas en ese momento
    k = rng.poisson(.0025 * n * dias)
    pp = rng.integers(0, n, k)
    dd = rng.integers(0, dias, k)
    neg = (dia_infeccion[pp] < 0) | (dia_infeccion[pp] > dd)
    for p, d in zip(pp[neg], dd[neg]):
        eventos.append((p, d, 10, "PRUEBA_NEGATIVA"))
    eventos = [e for e in eventos if e[1] < dias]

    return pd.concat(contactos_out, ignore_index=True), eventos, transmisiones


# ---------------------------------------------------------------------------
# 4. Escritura de archivos
# ---------------------------------------------------------------------------
def ts(dias_arr, hora, minuto):
    base = np.datetime64(FECHA_INICIO.isoformat() + "T00:00:00")
    t = base + dias_arr.astype("timedelta64[D]") + hora.astype("timedelta64[h]") + minuto.astype("timedelta64[m]")
    return pd.Series(t).dt.strftime("%Y-%m-%dT%H:%M:%S")


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def construir_csv_final(r, destino):
    """Une contactos + datos de las dos personas + estado de salud en un solo CSV."""
    r = r + os.sep
    mun = pd.read_csv(r + "municipios.csv").set_index("municipio_id").nombre
    per = pd.read_csv(r + "personas.csv").set_index("persona_id")
    con = pd.read_csv(r + "contactos.csv.gz")
    eve = pd.read_csv(r + "eventos_salud.csv.gz")
    pos = eve[eve.tipo_evento == "PRUEBA_POSITIVA"].groupby("persona_id").fecha_hora.min().str[:10]
    des = pd.Series("NINGUNO", index=per.index)
    for t, v in [("RECUPERADO", "RECUPERADO"), ("HOSPITALIZACION", "HOSPITALIZADO"), ("FALLECIDO", "FALLECIDO")]:
        des[eve[eve.tipo_evento == t].persona_id.unique()] = v
    out = pd.DataFrame({"contacto_id": con.contacto_id,
                        "fecha_hora": con.fecha_hora.str.replace("T", " ").str[:16],
                        "municipio": con.municipio_id.map(mun), "tipo_lugar": con.tipo_lugar,
                        "lugar_id": con.lugar_id})
    for s in "ab":
        p = con["persona_" + s]
        out["persona_" + s] = p
        out["edad_" + s] = p.map(per.edad).values
        out["sexo_" + s] = p.map(per.sexo).values
    out["duracion_min"] = con.duracion_min
    out["distancia_m"] = con.distancia_m
    for s in "ab":
        p = con["persona_" + s]
        out["fecha_positivo_" + s] = p.map(pos).fillna("").values
        out["desenlace_" + s] = p.map(des).values
    out.to_csv(destino, index=False)
    return len(out)


def main():
    args = parse_args()
    rng = np.random.default_rng(args.semilla)
    import tempfile, shutil
    tmp = tempfile.mkdtemp(prefix="gen_epidemia_")
    out_raw = os.path.join(tmp, "raw")
    out_ver = os.path.join(tmp, "verdad")
    os.makedirs(out_raw, exist_ok=True)
    os.makedirs(out_ver, exist_ok=True)
    t0 = time.time()

    print("1) Municipios, personas y lugares...")
    municipios = generar_municipios()
    personas = generar_personas(rng, args.personas, municipios)
    lugares = generar_lugares(rng, personas, municipios)
    personas = asignar_rutinas(rng, personas, lugares, municipios)

    print(f"2) Simulando {args.dias} días...")
    contactos, eventos, transmisiones = simular(rng, personas, lugares, municipios, args.dias)

    print("3) Escribiendo archivos...")
    pid = personas.persona_id.values
    lid = lugares.lugar_id.values
    pmun = personas.municipio_id.values

    contactos_df = pd.DataFrame({
        "contacto_id": [f"C{i+1:08d}" for i in range(len(contactos))],
        "persona_a": pid[contactos.a.values],
        "persona_b": pid[contactos.b.values],
        "lugar_id": lid[contactos.l.values],
        "tipo_lugar": lugares.tipo.values[contactos.l.values],
        "municipio_id": lugares.municipio_id.values[contactos.l.values],
        "fecha_hora": ts(contactos.d.values, contactos.h.values, contactos.mi.values),
        "duracion_min": contactos.dur.values,
        "distancia_m": contactos.dist.values,
    }).sort_values(["fecha_hora", "contacto_id"])

    ev = pd.DataFrame(eventos, columns=["p", "d", "h", "tipo_evento"])
    ev["mi"] = rng.integers(0, 60, len(ev))
    ev = ev.sort_values(["d", "h", "mi", "p"]).reset_index(drop=True)
    eventos_df = pd.DataFrame({
        "evento_id": [f"E{i+1:07d}" for i in range(len(ev))],
        "persona_id": pid[ev.p.values],
        "municipio_id": pmun[ev.p.values],
        "fecha_hora": ts(ev.d.values, ev.h.values, ev.mi.values),
        "tipo_evento": ev.tipo_evento.values,
    })

    tr = pd.DataFrame(transmisiones, columns=["p", "f", "d", "via", "l"])
    verdad_df = pd.DataFrame({
        "persona_id": pid[tr.p.values],
        "infectado_por": np.where(tr.f.values >= 0, pid[np.maximum(tr.f.values, 0)], ""),
        "fecha_infeccion": [(FECHA_INICIO + timedelta(days=int(x))).isoformat() for x in tr.d.values],
        "via": tr.via.values,
        "lugar_id": np.where(tr.l.values >= 0, lid[np.maximum(tr.l.values, 0)], ""),
    })

    archivos = {
        os.path.join(out_raw, "municipios.csv"): municipios,
        os.path.join(out_raw, "personas.csv"): personas,
        os.path.join(out_raw, "lugares.csv"): lugares,
        os.path.join(out_raw, "contactos.csv.gz"): contactos_df,
        os.path.join(out_raw, "eventos_salud.csv.gz"): eventos_df,
        os.path.join(out_ver, "transmisiones_verdad.csv"): verdad_df,
    }
    meta = {"generado": datetime.now().isoformat(timespec="seconds"), "semilla": args.semilla,
            "personas": args.personas, "dias": args.dias, "fecha_inicio": FECHA_INICIO.isoformat(),
            "parametros": {"P_HOGAR_DIA": P_HOGAR_DIA, "P_CONTACTO_BASE": P_CONTACTO_BASE,
                           "N_CASOS_INDICE": N_CASOS_INDICE, "DIA_MEDIDAS": DIA_MEDIDAS,
                           "FACTOR_MEDIDAS": FACTOR_MEDIDAS,
                           "LATENCIA": LATENCIA, "INFECCIOSO": INFECCIOSO, "INCUBACION": INCUBACION,
                           "CONTACTOS_POR_PERSONA": CONTACTOS_POR_PERSONA},
            "archivos": {}}
    total = 0
    for path, df in archivos.items():
        df.to_csv(path, index=False, compression={"method": "gzip", "mtime": 0} if path.endswith(".gz") else None)
        nombre = os.path.relpath(path, tmp)
        meta["archivos"][nombre] = {"registros": len(df), "sha256": sha256(path)}
        total += len(df)
        print(f"   {nombre:35s} {len(df):>10,d} registros")

    infectados = len(verdad_df)
    meta["resumen"] = {
        "total_registros": total,
        "infectados_totales": infectados,
        "tasa_ataque": round(infectados / args.personas, 4),
        "fallecidos": int((eventos_df.tipo_evento == "FALLECIDO").sum()),
        "casos_confirmados": int((eventos_df.tipo_evento == "PRUEBA_POSITIVA").sum()),
        "R0_empirico_generacion_1_3": None,
    }
    # R empírico: promedio de contagios causados por los infectados en los primeros 20 días
    tempranos = verdad_df[pd.to_datetime(verdad_df.fecha_infeccion) < pd.Timestamp(FECHA_INICIO) + pd.Timedelta(days=20)]
    hijos = verdad_df.infectado_por.value_counts()
    meta["resumen"]["R0_empirico_generacion_1_3"] = round(float(hijos.reindex(tempranos.persona_id).fillna(0).mean()), 2)

    os.makedirs(args.salida, exist_ok=True)
    final = os.path.join(args.salida, "dataset_epidemia_tolima.csv")
    n_final = construir_csv_final(out_raw, final)
    meta["archivo_final"] = {"nombre": "dataset_epidemia_tolima.csv", "registros": n_final, "sha256": sha256(final)}
    meta.pop("archivos")
    with open(os.path.join(args.salida, "metadatos.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    shutil.rmtree(tmp, ignore_errors=True)
    print(f"\nArchivo final: {final}  ({n_final:,d} contactos)")
    print(f"TOTAL simulado: {total:,d} registros | infectados {infectados:,d} "
          f"({meta['resumen']['tasa_ataque']:.1%}) | R0 empírico ≈ {meta['resumen']['R0_empirico_generacion_1_3']} "
          f"| {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
