from datetime import datetime

from sismos.ingesta.fuentes import (
    formatear_tiempo,
    parsear_fdsn_texto,
    parsear_usgs_geojson,
    ventanas_mensuales,
)

TEXTO_INGV = (
    "#EventID|Time|Latitude|Longitude|Depth/Km|Author|Catalog|Contributor|ContributorID|MagType|Magnitude|MagAuthor|EventLocationName|EventType\n"
    "47427942|2026-10-02T16:34:41.085000|51.6023|159.816|14.8|SURVEY-INGV-A||||Mwp|6.0|--|Off east coast of Kamchatka Peninsula, Russia|earthquake\n"
    "46749241|2026-10-01T20:28:14.590000|39.5417|15.6732||SURVEY-INGV||||ML|3.0|--|Costa Calabra|earthquake\n"
)

TEXTO_EMSC = (
    "#EventID|Time|Latitude|Longitude|Depth/km|Author|Catalog|Contributor|ContributorID|MagType|Magnitude|MagAuthor|EventLocationName\n"
    "20261002_0000343|2026-10-02T23:59:28.08Z|51.6667|159.6611|17.6|EMSC|EMSC-RTS|EMSC|2068784|mb|4.6|EMSC|OFF EAST COAST OF KAMCHATKA\n"
)

GEOJSON_USGS = {
    "features": [
        {
            "id": "us6000tywu",
            "geometry": {"type": "Point", "coordinates": [166.9873, -11.8787, 10]},
            "properties": {
                "mag": 5.1,
                "magType": "mww",
                "net": "us",
                "place": "182 km SE of Lata, Solomon Islands",
                "time": 1790883481254,
                "updated": 1791898846040,
                "alert": None,
                "status": "reviewed",
                "tsunami": 0,
                "sig": 400,
                "type": "earthquake",
                "url": "https://earthquake.usgs.gov/earthquakes/eventpage/us6000tywu",
            },
        }
    ]
}


def test_parsea_fdsn_ingv_con_profundidad_vacia_y_nombres_de_columna_distintos():
    eventos = parsear_fdsn_texto(TEXTO_INGV, "INGV")
    assert len(eventos) == 2
    primero = eventos[0]
    assert primero["id_fuente"] == "47427942"
    assert primero["latitud"] == 51.6023
    assert primero["profundidad_km"] == 14.8
    assert primero["tipo_magnitud"] == "Mwp"
    assert primero["tipo_evento"] == "earthquake"
    assert primero["tiempo_utc"] == "2026-10-02T16:34:41.085Z"
    assert eventos[1]["profundidad_km"] is None


def test_parsea_fdsn_emsc_con_zona_horaria_y_sin_tipo_de_evento():
    evento = parsear_fdsn_texto(TEXTO_EMSC, "EMSC")[0]
    assert evento["tiempo_utc"] == "2026-10-02T23:59:28.080Z"
    assert evento["tipo_evento"] is None
    assert evento["fuente"] == "EMSC"


def test_texto_fdsn_vacio_no_produce_eventos():
    assert parsear_fdsn_texto("", "GFZ") == []
    assert parsear_fdsn_texto("#EventID|Time|Latitude|Longitude|Depth/km|Magnitude\n", "GFZ") == []


def test_parsea_geojson_de_usgs_con_alerta_y_tsunami():
    evento = parsear_usgs_geojson(GEOJSON_USGS)[0]
    assert evento["fuente"] == "USGS"
    assert evento["latitud"] == -11.8787
    assert evento["longitud"] == 166.9873
    assert evento["profundidad_km"] == 10
    assert evento["magnitud"] == 5.1
    assert evento["nivel_alerta"] is None
    assert evento["tsunami"] == 0
    assert evento["actualizado_utc"] == "2026-10-13T13:40:46.040Z"


def test_formatear_tiempo_acepta_epoch_en_milisegundos():
    assert formatear_tiempo("1790883481254") == "2026-10-01T19:38:01.254Z"


def test_ventanas_mensuales_cubren_el_intervalo_sin_solapes():
    inicio = datetime(2024, 1, 15)
    fin = datetime(2024, 4, 3)
    ventanas = list(ventanas_mensuales(inicio, fin))
    assert ventanas[0][0] == inicio
    assert ventanas[-1][1] == fin
    for (_, fin_previo), (inicio_siguiente, _) in zip(ventanas, ventanas[1:]):
        assert fin_previo == inicio_siguiente
    assert len(ventanas) == 4
