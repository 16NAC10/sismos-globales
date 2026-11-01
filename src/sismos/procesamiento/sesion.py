"""Creación de la sesión de Spark con el conector de Kafka."""

import os
import sys

from pyspark.sql import SparkSession

VERSION_SPARK = "4.0.1"
PAQUETE_KAFKA = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{VERSION_SPARK}"


def crear_sesion(nombre: str, con_kafka: bool = False) -> SparkSession:
    # Los workers de Python deben usar el mismo intérprete que el proceso principal (evita el alias de la Store en Windows).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    constructor = (
        SparkSession.builder.appName(nombre)
        .master(os.getenv("SISMOS_SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.getenv("SISMOS_SPARK_MEMORIA", "2g"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.shuffle.partitions", os.getenv("SISMOS_SHUFFLE_PARTITIONS", "64"))
        .config("spark.sql.parquet.compression.codec", "snappy")
    )
    if con_kafka:
        constructor = constructor.config("spark.jars.packages", PAQUETE_KAFKA)
    sesion = constructor.getOrCreate()
    sesion.sparkContext.setLogLevel("WARN")
    return sesion
