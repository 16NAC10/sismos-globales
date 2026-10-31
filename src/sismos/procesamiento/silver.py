"""Capa silver: validación, limpieza, deduplicación, normalización, clasificación y cruce entre fuentes."""

import logging
from typing import Any

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from sismos.config import CONFIG, cargar_regiones
from sismos.procesamiento.sesion import crear_sesion

log = logging.getLogger(__name__)

RONDAS_PROPAGACION = 5  # iteraciones de propagación de etiquetas para agrupar un mismo sismo

ESQUEMA_MENSAJE = T.StructType(
    [
        T.StructField("fuente", T.StringType()),
        T.StructField("id_fuente", T.StringType()),
        T.StructField("tiempo_utc", T.StringType()),
        T.StructField("latitud", T.DoubleType()),
        T.StructField("longitud", T.DoubleType()),
        T.StructField("profundidad_km", T.DoubleType()),
        T.StructField("magnitud", T.DoubleType()),
        T.StructField("tipo_magnitud", T.StringType()),
        T.StructField("autor_magnitud", T.StringType()),
        T.StructField("ubicacion", T.StringType()),
        T.StructField("tipo_evento", T.StringType()),
        T.StructField("nivel_alerta", T.StringType()),
        T.StructField("tsunami", T.IntegerType()),
        T.StructField("significancia", T.IntegerType()),
        T.StructField("estado", T.StringType()),
        T.StructField("actualizado_utc", T.StringType()),
        T.StructField("url", T.StringType()),
        T.StructField("ingestado_utc", T.StringType()),
    ]
)


def _texto_limpio(columna: F.Column) -> F.Column:
    recortado = F.trim(columna)
    return F.when(recortado == "", F.lit(None)).otherwise(recortado)


def decodificar_bronze(bronze: DataFrame) -> DataFrame:
    """Convierte el JSON del campo valor en columnas tipadas. Los mensajes mal formados quedan con nulos."""
    return bronze.select(
        F.from_json("valor", ESQUEMA_MENSAJE).alias("m"),
        F.col("clave"),
        F.col("valor").alias("valor_original"),
    ).select("m.*", "clave", "valor_original")


def validar_y_normalizar(mensajes: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Devuelve (aceptados, rechazados). Los rechazados incluyen el motivo para auditoría."""
    df = (
        mensajes.withColumn("fuente", F.upper(_texto_limpio(F.col("fuente"))))
        .withColumn("id_fuente", _texto_limpio(F.col("id_fuente")))
        .withColumn("tiempo", F.to_timestamp(F.col("tiempo_utc")))
        .withColumn("tipo_evento", F.lower(_texto_limpio(F.coalesce(F.col("tipo_evento"), F.lit("earthquake")))))
        .withColumn("ubicacion", _texto_limpio(F.col("ubicacion")))
        .withColumn("tipo_magnitud", _texto_limpio(F.col("tipo_magnitud")))
        .withColumn("autor_magnitud", _texto_limpio(F.col("autor_magnitud")))
        .withColumn("nivel_alerta", F.lower(_texto_limpio(F.col("nivel_alerta"))))
        .withColumn(
            "profundidad_km",
            F.when(F.col("profundidad_km").between(-5, 800), F.col("profundidad_km")),
        )
    )

    motivo = (
        F.when(F.col("fuente").isNull() | F.col("id_fuente").isNull(), F.lit("sin identificador de fuente"))
        .when(F.col("tiempo").isNull(), F.lit("fecha no válida"))
        .when(~F.col("latitud").between(-90, 90), F.lit("latitud no válida"))
        .when(~F.col("longitud").between(-180, 180), F.lit("longitud no válida"))
        .when(F.col("magnitud").isNull() | ~F.col("magnitud").between(-1, 10), F.lit("magnitud no válida"))
        .when(F.col("tipo_evento") != "earthquake", F.lit("evento no sísmico"))
    )
    df = df.withColumn("motivo_rechazo", motivo)
    return df.filter(F.col("motivo_rechazo").isNull()), df.filter(F.col("motivo_rechazo").isNotNull())


def normalizar(df: DataFrame) -> DataFrame:
    """Unifica tipos de magnitud (p. ej. mww, mwp -> Mw), convierte fechas a UTC y separa la zona del nombre."""
    return (
        df.withColumn(
            "tipo_magnitud",
            F.when(F.lower(F.col("tipo_magnitud")).startswith("mw"), F.lit("Mw")).otherwise(
                F.upper(F.col("tipo_magnitud"))
            ),
        )
        .withColumn("actualizado", F.to_timestamp(F.col("actualizado_utc")))
        .withColumn("ingestado", F.to_timestamp(F.col("ingestado_utc")))
        .withColumn(
            "zona",
            F.when(
                F.instr(F.col("ubicacion"), ",") > 0,
                F.trim(F.element_at(F.split(F.col("ubicacion"), ","), -1)),
            ).otherwise(F.col("ubicacion")),
        )
        .withColumn("evento_id", F.concat_ws(":", F.col("fuente"), F.col("id_fuente")))
    )


def deduplicar_fuente(df: DataFrame) -> DataFrame:
    """Una sola versión por evento y fuente: la más reciente según la fecha de actualización de la agencia."""
    ventana = Window.partitionBy("fuente", "id_fuente").orderBy(
        F.col("actualizado").desc_nulls_last(), F.col("ingestado").desc_nulls_last()
    )
    return df.withColumn("_orden", F.row_number().over(ventana)).filter(F.col("_orden") == 1).drop("_orden")


def _expresion_region() -> F.Column:
    casos = []
    for region in cargar_regiones():
        for lat_min, lat_max, lon_min, lon_max in region["cajas"]:
            condicion = (
                F.col("latitud").between(lat_min, lat_max) & F.col("longitud").between(lon_min, lon_max)
            )
            casos.append((condicion, region["nombre"]))
    expresion = F.lit("Sin clasificar")
    for condicion, nombre in reversed(casos):
        expresion = F.when(condicion, F.lit(nombre)).otherwise(expresion)
    return expresion


def clasificar(df: DataFrame) -> DataFrame:
    p = CONFIG
    return (
        df.withColumn("anio", F.year("tiempo"))
        .withColumn("mes", F.month("tiempo"))
        .withColumn(
            "categoria_magnitud",
            F.when(F.col("magnitud") < 3, F.lit("Menor (<3)"))
            .when(F.col("magnitud") < 5, F.lit("Leve (3-4.9)"))
            .when(F.col("magnitud") < 6, F.lit("Moderado (5-5.9)"))
            .when(F.col("magnitud") < 7, F.lit("Fuerte (6-6.9)"))
            .otherwise(F.lit("Mayor (>=7)")),
        )
        .withColumn(
            "clase_profundidad",
            F.when(F.col("profundidad_km").isNull(), F.lit("Desconocida"))
            .when(F.col("profundidad_km") <= p.profundidad_superficial_km, F.lit("Superficial"))
            .when(F.col("profundidad_km") <= p.profundidad_intermedia_km, F.lit("Intermedia"))
            .otherwise(F.lit("Profunda")),
        )
        .withColumn("region", _expresion_region())
        .withColumn("supera_umbral", F.col("magnitud") >= p.umbral_magnitud)
    )


def _distancia_km(lat1: F.Column, lon1: F.Column, lat2: F.Column, lon2: F.Column) -> F.Column:
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    h = F.sin(dlat / 2) ** 2 + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2
    return 2 * 6371.0 * F.asin(F.sqrt(h))


def _prioridad_fuente(df: DataFrame) -> DataFrame:
    expresion = F.lit(99)
    for fuente in reversed(CONFIG.fuentes):
        expresion = F.when(F.col("fuente") == fuente.nombre, F.lit(fuente.prioridad)).otherwise(expresion)
    return df.withColumn("prioridad_fuente", expresion)


def cruzar_fuentes(df: DataFrame) -> DataFrame:
    """Agrupa los registros que describen el mismo sismo en distintas fuentes y conserva el de mayor prioridad.

    Dos registros son el mismo sismo si están a menos de `ventana_cruce_segundos`, a menos de
    `distancia_cruce_km` y su magnitud difiere como máximo `diferencia_magnitud_max`.
    Se usan celdas temporales para evitar un producto cartesiano completo.
    """
    ventana = CONFIG.ventana_cruce_segundos
    base = df.select(
        "evento_id",
        "fuente",
        F.col("tiempo").cast("double").alias("t"),
        "latitud",
        "longitud",
        "magnitud",
        (F.floor(F.col("tiempo").cast("double") / ventana)).cast("long").alias("celda"),
    )
    a = base.select(
        F.col("evento_id").alias("id_a"),
        F.col("fuente").alias("fuente_a"),
        F.col("t").alias("t_a"),
        F.col("latitud").alias("lat_a"),
        F.col("longitud").alias("lon_a"),
        F.col("magnitud").alias("mag_a"),
        F.col("celda"),
    )
    b = base.select(
        F.col("evento_id").alias("id_b"),
        F.col("fuente").alias("fuente_b"),
        F.col("t").alias("t_b"),
        F.col("latitud").alias("lat_b"),
        F.col("longitud").alias("lon_b"),
        F.col("magnitud").alias("mag_b"),
        F.explode(F.array(F.col("celda") - 1, F.col("celda"), F.col("celda") + 1)).alias("celda"),
    )
    pares = (
        a.join(b, "celda")
        .where(
            (F.col("id_a") != F.col("id_b"))
            & (F.col("fuente_a") != F.col("fuente_b"))
            & (F.abs(F.col("t_a") - F.col("t_b")) <= ventana)
            & (F.abs(F.col("mag_a") - F.col("mag_b")) <= CONFIG.diferencia_magnitud_max)
            & (_distancia_km(F.col("lat_a"), F.col("lon_a"), F.col("lat_b"), F.col("lon_b")) <= CONFIG.distancia_cruce_km)
        )
        .select("id_a", "id_b")
    )
    pares = pares.union(pares.select(F.col("id_b").alias("id_a"), F.col("id_a").alias("id_b")))

    etiquetas = base.select(F.col("evento_id").alias("id"), F.col("evento_id").alias("etiqueta"))
    for _ in range(RONDAS_PROPAGACION):
        vecinos = (
            pares.join(etiquetas.select(F.col("id").alias("id_b"), F.col("etiqueta").alias("etiqueta_b")), "id_b")
            .groupBy("id_a")
            .agg(F.min("etiqueta_b").alias("minimo_vecino"))
        )
        etiquetas = (
            etiquetas.join(vecinos, etiquetas["id"] == vecinos["id_a"], "left")
            .select(
                "id",
                F.least(F.col("etiqueta"), F.coalesce(F.col("minimo_vecino"), F.col("etiqueta"))).alias("etiqueta"),
            )
        )

    agrupado = (
        df.join(etiquetas, df["evento_id"] == etiquetas["id"])
        .drop("id")
        .transform(_prioridad_fuente)
    )
    fuentes_grupo = agrupado.groupBy("etiqueta").agg(
        F.array_sort(F.collect_set("fuente")).alias("fuentes"),
        F.count(F.lit(1)).alias("n_fuentes"),
    )
    ventana_prioridad = Window.partitionBy("etiqueta").orderBy(F.col("prioridad_fuente"), F.col("evento_id"))
    canonicos = (
        agrupado.withColumn("_orden", F.row_number().over(ventana_prioridad))
        .filter(F.col("_orden") == 1)
        .drop("_orden")
    )
    return canonicos.join(fuentes_grupo, "etiqueta").drop("etiqueta", "prioridad_fuente")


def seleccionar_silver(df: DataFrame) -> DataFrame:
    return df.select(
        "evento_id",
        "fuente",
        "fuentes",
        "n_fuentes",
        "id_fuente",
        F.col("tiempo").alias("tiempo_utc"),
        "anio",
        "mes",
        "latitud",
        "longitud",
        "profundidad_km",
        "magnitud",
        "tipo_magnitud",
        "autor_magnitud",
        "categoria_magnitud",
        "clase_profundidad",
        "region",
        "zona",
        "ubicacion",
        "tipo_evento",
        "nivel_alerta",
        "tsunami",
        "significancia",
        "estado",
        "supera_umbral",
        "actualizado",
        "ingestado",
        "url",
        F.current_timestamp().alias("procesado_utc"),
    )


def construir_silver(mensajes: DataFrame) -> dict[str, DataFrame]:
    """Pipeline completo a partir de los mensajes ya decodificados."""
    aceptados, rechazados = validar_y_normalizar(mensajes)
    normalizados = normalizar(aceptados)
    sin_duplicados = deduplicar_fuente(normalizados)
    clasificados = clasificar(sin_duplicados)
    canonicos = cruzar_fuentes(clasificados)
    return {
        "silver": seleccionar_silver(canonicos),
        "rechazados": rechazados,
        "normalizados": normalizados,
        "sin_duplicados": sin_duplicados,
    }


def ejecutar_silver() -> dict[str, int]:
    """Recalcula la capa silver completa desde bronze. Es determinista y reemplaza la salida anterior."""
    spark = crear_sesion("sismos-silver")
    bronze = spark.read.parquet(str(CONFIG.ruta_bronze))
    mensajes = decodificar_bronze(bronze).cache()

    resultado = construir_silver(mensajes)
    silver = resultado["silver"].cache()
    rechazados = resultado["rechazados"].cache()

    metricas = {
        "mensajes_bronze": mensajes.count(),
        "rechazados": rechazados.count(),
        "duplicados_fuente": resultado["normalizados"].count() - resultado["sin_duplicados"].count(),
        "fusionados_entre_fuentes": resultado["sin_duplicados"].count() - silver.count(),
        "eventos_silver": silver.count(),
    }

    rechazados.write.mode("overwrite").parquet(str(CONFIG.ruta_rechazados))
    silver.write.mode("overwrite").partitionBy("anio").parquet(str(CONFIG.ruta_silver))
    log.info("Silver: %s", metricas)
    spark.stop()
    return metricas
