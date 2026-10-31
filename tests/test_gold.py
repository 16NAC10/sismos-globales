import pandas as pd
import pytest
from pyspark.sql import functions as F

from sismos.carga.mongo import _a_documentos
from sismos.procesamiento.gold import eventos_por_periodo, relacion_magnitud_profundidad, resumen_fuentes


def silver_de_prueba(spark):
    filas = [
        {"evento_id": "USGS:a", "fuente": "USGS", "fuentes": ["INGV", "USGS"], "anio": 2020, "mes": 3,
         "tiempo_utc": pd.Timestamp("2020-03-01T10:00:00"), "magnitud": 5.0, "profundidad_km": 10.0,
         "supera_umbral": False},
        {"evento_id": "EMSC:b", "fuente": "EMSC", "fuentes": ["EMSC"], "anio": 2020, "mes": 3,
         "tiempo_utc": pd.Timestamp("2020-03-05T10:00:00"), "magnitud": 6.0, "profundidad_km": 20.0,
         "supera_umbral": True},
        {"evento_id": "USGS:c", "fuente": "USGS", "fuentes": ["USGS"], "anio": 2021, "mes": 1,
         "tiempo_utc": pd.Timestamp("2021-01-02T10:00:00"), "magnitud": 7.0, "profundidad_km": 30.0,
         "supera_umbral": True},
    ]
    df = spark.createDataFrame(pd.DataFrame(filas, dtype=object))
    return df.withColumn("magnitud", F.col("magnitud").cast("double")).withColumn(
        "profundidad_km", F.col("profundidad_km").cast("double")
    )


def test_eventos_por_periodo_agrupa_por_mes(spark):
    resultado = {r["periodo"]: r["total_eventos"] for r in eventos_por_periodo(silver_de_prueba(spark)).collect()}
    assert resultado == {"2020-03": 2, "2021-01": 1}


def test_correlacion_magnitud_profundidad_con_datos_crecientes(spark):
    _, correlacion = relacion_magnitud_profundidad(silver_de_prueba(spark))
    fila = correlacion.collect()[0]
    assert fila["eventos_con_profundidad"] == 3
    assert fila["correlacion_pearson"] == pytest.approx(1.0)


def test_resumen_fuentes_cuenta_contribuciones_y_canonicos(spark):
    filas = {r["fuente"]: r for r in resumen_fuentes(silver_de_prueba(spark)).collect()}
    assert filas["USGS"]["eventos_contribuidos"] == 2
    assert filas["USGS"]["eventos_canonicos"] == 2
    assert filas["INGV"]["eventos_contribuidos"] == 1
    assert filas["INGV"]["eventos_canonicos"] == 0


def test_documentos_mongo_con_coordenadas_geojson_y_nulos():
    df = pd.DataFrame(
        {
            "evento_id": ["USGS:a"],
            "latitud": [38.0],
            "longitud": [143.5],
            "magnitud": [5.2],
            "profundidad_km": [None],
        }
    )
    documento = _a_documentos(df, geo=True)[0]
    assert documento["_id"] == "USGS:a"
    assert documento["ubicacion"] == {"type": "Point", "coordinates": [143.5, 38.0]}
    assert documento["profundidad_km"] is None
    assert "latitud" not in documento
