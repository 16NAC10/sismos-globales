"""Capa gold: agregados para el análisis de actividad sísmica, calculados con Spark sobre la capa silver."""

import logging
from datetime import datetime, timedelta, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sismos.config import CONFIG
from sismos.procesamiento.sesion import crear_sesion

log = logging.getLogger(__name__)

NIVELES_ALERTA = ["yellow", "orange", "red"]


def _guardar(df: DataFrame, nombre: str) -> int:
    ruta = CONFIG.ruta_gold / nombre
    df.coalesce(1).write.mode("overwrite").parquet(str(ruta))
    return df.count()


def eventos_por_periodo(silver: DataFrame) -> DataFrame:
    return (
        silver.groupBy("anio", "mes")
        .agg(
            F.count(F.lit(1)).alias("total_eventos"),
            F.sum(F.when(F.col("supera_umbral"), 1).otherwise(0)).alias("eventos_relevantes"),
            F.round(F.avg("magnitud"), 2).alias("magnitud_media"),
        )
        .withColumn("periodo", F.format_string("%04d-%02d", "anio", "mes"))
        .orderBy("anio", "mes")
    )


def distribucion_magnitud(silver: DataFrame) -> DataFrame:
    return (
        silver.groupBy("categoria_magnitud")
        .agg(
            F.count(F.lit(1)).alias("total_eventos"),
            F.round(F.avg("profundidad_km"), 1).alias("profundidad_media_km"),
        )
    )


def distribucion_profundidad(silver: DataFrame) -> DataFrame:
    return (
        silver.groupBy("clase_profundidad")
        .agg(
            F.count(F.lit(1)).alias("total_eventos"),
            F.round(F.avg("magnitud"), 2).alias("magnitud_media"),
        )
    )


def actividad_regional(silver: DataFrame) -> DataFrame:
    return (
        silver.groupBy("region")
        .agg(
            F.count(F.lit(1)).alias("total_eventos"),
            F.round(F.avg("magnitud"), 2).alias("magnitud_media"),
            F.max("magnitud").alias("magnitud_maxima"),
            F.sum(F.when(F.col("supera_umbral"), 1).otherwise(0)).alias("eventos_relevantes"),
        )
        .orderBy(F.col("total_eventos").desc())
    )


def relacion_magnitud_profundidad(silver: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Malla de frecuencias magnitud × profundidad y coeficiente de correlación de Pearson."""
    con_profundidad = silver.filter(F.col("profundidad_km").isNotNull())
    malla = (
        con_profundidad.withColumn("profundidad_bin_km", F.floor(F.col("profundidad_km") / 10) * 10)
        .withColumn("magnitud_bin", F.floor(F.col("magnitud") * 2) / 2)
        .groupBy("profundidad_bin_km", "magnitud_bin")
        .agg(F.count(F.lit(1)).alias("total_eventos"))
    )
    correlacion = con_profundidad.agg(
        F.corr("magnitud", "profundidad_km").alias("correlacion_pearson"),
        F.count(F.lit(1)).alias("eventos_con_profundidad"),
    )
    return malla, correlacion


def resumen_fuentes(silver: DataFrame) -> DataFrame:
    contribuidos = silver.select("evento_id", F.explode("fuentes").alias("fuente_origen"))
    por_fuente = contribuidos.groupBy("fuente_origen").agg(F.count(F.lit(1)).alias("eventos_contribuidos"))
    canonicos = silver.groupBy("fuente").agg(
        F.count(F.lit(1)).alias("eventos_canonicos"),
        F.min("tiempo_utc").alias("primer_evento_utc"),
        F.max("tiempo_utc").alias("ultimo_evento_utc"),
    )
    return (
        por_fuente.join(canonicos, por_fuente["fuente_origen"] == canonicos["fuente"], "left")
        .select(
            F.col("fuente_origen").alias("fuente"),
            "eventos_contribuidos",
            F.coalesce(F.col("eventos_canonicos"), F.lit(0)).alias("eventos_canonicos"),
            "primer_evento_utc",
            "ultimo_evento_utc",
        )
        .orderBy("fuente")
    )


def alertas_recientes(silver: DataFrame) -> DataFrame:
    """Eventos de la ventana reciente que superan el umbral o tienen nivel de alerta de USGS."""
    desde = datetime.now(timezone.utc) - timedelta(hours=CONFIG.ventana_alertas_horas)
    return (
        silver.filter(F.col("tiempo_utc") >= F.lit(desde.replace(tzinfo=None)))
        .filter(F.col("supera_umbral") | F.col("nivel_alerta").isin(NIVELES_ALERTA))
        .select(
            "evento_id",
            "fuente",
            "tiempo_utc",
            "latitud",
            "longitud",
            "magnitud",
            "profundidad_km",
            "region",
            "zona",
            "nivel_alerta",
            "supera_umbral",
            "tsunami",
        )
        .orderBy(F.col("tiempo_utc").desc())
    )


def ejecutar_gold() -> dict[str, int]:
    spark = crear_sesion("sismos-gold")
    silver = spark.read.parquet(str(CONFIG.ruta_silver)).cache()

    malla, correlacion = relacion_magnitud_profundidad(silver)
    tablas = {
        "eventos_por_periodo": eventos_por_periodo(silver),
        "distribucion_magnitud": distribucion_magnitud(silver),
        "distribucion_profundidad": distribucion_profundidad(silver),
        "actividad_regional": actividad_regional(silver),
        "malla_magnitud_profundidad": malla,
        "correlacion_magnitud_profundidad": correlacion,
        "resumen_fuentes": resumen_fuentes(silver),
        "alertas_recientes": alertas_recientes(silver),
    }
    metricas = {nombre: _guardar(df, nombre) for nombre, df in tablas.items()}
    log.info("Gold: %s", metricas)
    spark.stop()
    return metricas
