import pytest

pytest.importorskip("pyspark")
import pandas as pd  # noqa: E402

from sismos.procesamiento.silver import ESQUEMA_MENSAJE, construir_silver, validar_y_normalizar  # noqa: E402


def mensaje(**campos):
    base = {
        "fuente": "USGS",
        "id_fuente": "us1",
        "tiempo_utc": "2026-10-01T19:38:01.254Z",
        "latitud": 38.0,
        "longitud": 143.0,
        "profundidad_km": 20.0,
        "magnitud": 5.0,
        "tipo_magnitud": "mww",
        "autor_magnitud": "us",
        "ubicacion": "100 km E of Sendai, Japan",
        "tipo_evento": "earthquake",
        "nivel_alerta": None,
        "tsunami": 0,
        "significancia": 300,
        "estado": "reviewed",
        "actualizado_utc": "2026-10-02T00:00:00.000Z",
        "url": None,
        "ingestado_utc": "2026-10-02T01:00:00.000Z",
    }
    base.update(campos)
    return base


def df(spark, filas):
    # Arrow evita los workers de Python, que no arrancan de forma fiable en Windows.
    return spark.createDataFrame(pd.DataFrame(filas, dtype=object), schema=ESQUEMA_MENSAJE)


def test_rechaza_registros_no_validos_con_motivo(spark):
    filas = [
        mensaje(id_fuente="ok"),
        mensaje(id_fuente="lat", latitud=120.0),
        mensaje(id_fuente="mag", magnitud=None),
        mensaje(id_fuente="cantera", tipo_evento="quarry blast"),
    ]
    aceptados, rechazados = validar_y_normalizar(df(spark, filas))
    assert aceptados.count() == 1
    motivos = {r["id_fuente"]: r["motivo_rechazo"] for r in rechazados.collect()}
    assert motivos == {
        "lat": "latitud no válida",
        "mag": "magnitud no válida",
        "cantera": "evento no sísmico",
    }


def test_conserva_una_sola_version_por_fuente(spark):
    filas = [
        mensaje(id_fuente="us1", magnitud=4.8, actualizado_utc="2026-10-02T00:00:00.000Z"),
        mensaje(id_fuente="us1", magnitud=5.3, actualizado_utc="2026-10-03T00:00:00.000Z"),
    ]
    resultado = construir_silver(df(spark, filas))["silver"].collect()
    assert len(resultado) == 1
    assert resultado[0]["magnitud"] == 5.3


def test_fusiona_el_mismo_sismo_de_dos_fuentes_y_mantiene_eventos_distintos(spark):
    filas = [
        mensaje(fuente="USGS", id_fuente="us1", magnitud=5.1),
        mensaje(fuente="INGV", id_fuente="ing1", tiempo_utc="2026-10-01T19:38:06.000Z",
                latitud=38.05, longitud=143.04, magnitud=5.0, tipo_magnitud="Mw"),
        mensaje(fuente="EMSC", id_fuente="em9", latitud=-30.0, longitud=-70.0,
                ubicacion="Chile", magnitud=4.6),
    ]
    silver = construir_silver(df(spark, filas))["silver"].collect()
    assert len(silver) == 2
    fusionado = next(r for r in silver if r["n_fuentes"] == 2)
    assert fusionado["fuente"] == "USGS"
    assert sorted(fusionado["fuentes"]) == ["INGV", "USGS"]
    assert fusionado["magnitud"] == 5.1


def test_clasifica_region_profundidad_y_umbral(spark):
    silver = construir_silver(df(spark, [mensaje(magnitud=6.2, profundidad_km=30.0)]))["silver"].collect()[0]
    assert silver["region"] == "Japón"
    assert silver["clase_profundidad"] == "Superficial"
    assert silver["categoria_magnitud"] == "Fuerte (6-6.9)"
    assert silver["supera_umbral"] is True
    assert silver["zona"] == "Japan"
    assert silver["tipo_magnitud"] == "Mw"


def test_profundidad_fuera_de_rango_se_trata_como_faltante(spark):
    silver = construir_silver(df(spark, [mensaje(profundidad_km=-40.0)]))["silver"].collect()[0]
    assert silver["profundidad_km"] is None
    assert silver["clase_profundidad"] == "Desconocida"
