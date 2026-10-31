"""Capa bronze: consume los mensajes nuevos de Kafka y los guarda en Parquet sin transformar."""

import logging

from pyspark.sql import functions as F

from sismos.config import CONFIG
from sismos.procesamiento.sesion import crear_sesion

log = logging.getLogger(__name__)


def ingerir_bronze() -> None:
    """Usa Structured Streaming con availableNow: procesa solo lo nuevo desde el último offset guardado."""
    spark = crear_sesion("sismos-bronze", con_kafka=True)
    mensajes = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", CONFIG.kafka_bootstrap)
        .option("subscribe", CONFIG.kafka_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    registros = mensajes.select(
        F.col("key").cast("string").alias("clave"),
        F.col("value").cast("string").alias("valor"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("timestamp_kafka"),
    )
    consulta = (
        registros.writeStream.format("parquet")
        .outputMode("append")
        .option("checkpointLocation", str(CONFIG.ruta_checkpoints / "spark_bronze"))
        .option("path", str(CONFIG.ruta_bronze))
        .trigger(availableNow=True)
        .start()
    )
    consulta.awaitTermination()
    log.info("Bronze actualizado en %s", CONFIG.ruta_bronze)
    spark.stop()
