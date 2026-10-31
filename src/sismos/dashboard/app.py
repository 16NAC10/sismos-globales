"""Panel de análisis sísmico. Lee las colecciones de MongoDB con agregaciones y filtros."""

import datetime as dt
from typing import Any

import pandas as pd
import plotly.express as px
import pydeck as pdk
import streamlit as st
from pymongo import MongoClient

from sismos.config import CONFIG

st.set_page_config(page_title="Sismos globales", layout="wide")

MAX_PUNTOS_MAPA = 20000
MAX_PUNTOS_CALOR = 50000
MUESTRA_DISPERSION = 5000
ORDEN_MAGNITUD = ["Menor (<3)", "Leve (3-4.9)", "Moderado (5-5.9)", "Fuerte (6-6.9)", "Mayor (>=7)"]
ORDEN_PROFUNDIDAD = ["Superficial", "Intermedia", "Profunda", "Desconocida"]


@st.cache_resource
def base_datos():
    return MongoClient(CONFIG.mongo_uri, serverSelectionTimeoutMS=3000)[CONFIG.mongo_base]


@st.cache_data(ttl=300)
def limites() -> dict[str, Any]:
    db = base_datos()
    rango = next(
        db.eventos.aggregate(
            [
                {
                    "$group": {
                        "_id": None,
                        "inicio": {"$min": "$tiempo_utc"},
                        "fin": {"$max": "$tiempo_utc"},
                        "mag_min": {"$min": "$magnitud"},
                        "mag_max": {"$max": "$magnitud"},
                        "prof_min": {"$min": "$profundidad_km"},
                        "prof_max": {"$max": "$profundidad_km"},
                    }
                }
            ]
        ),
        {},
    )
    profundidad_defecto = (max(0.0, float(rango.get("prof_min") or 0)), float(rango.get("prof_max") or 800))
    return {
        "regiones": sorted(db.eventos.distinct("region")),
        "fuentes": sorted(db.eventos.distinct("fuente")),
        "profundidad_defecto": profundidad_defecto,
        **rango,
    }


def construir_filtro(inicio, fin, magnitud, profundidad, regiones, fuentes, lim) -> dict[str, Any]:
    filtro: dict[str, Any] = {
        "tiempo_utc": {"$gte": dt.datetime.combine(inicio, dt.time.min), "$lte": dt.datetime.combine(fin, dt.time.max)},
        "magnitud": {"$gte": magnitud[0], "$lte": magnitud[1]},
    }
    # Con el rango completo no se filtra, así se conservan los eventos sin profundidad conocida.
    if profundidad != lim["profundidad_defecto"]:
        filtro["profundidad_km"] = {"$gte": profundidad[0], "$lte": profundidad[1]}
    if set(regiones) != set(lim["regiones"]):
        filtro["region"] = {"$in": list(regiones)}
    if set(fuentes) != set(lim["fuentes"]):
        filtro["fuentes"] = {"$in": list(fuentes)}
    return filtro


@st.cache_data(ttl=300)
def agregados(filtro: dict[str, Any]) -> dict[str, pd.DataFrame]:
    coleccion = base_datos().eventos
    mensual = pd.DataFrame(
        list(
            coleccion.aggregate(
                [
                    {"$match": filtro},
                    {
                        "$group": {
                            "_id": {"anio": "$anio", "mes": "$mes"},
                            "total": {"$sum": 1},
                            "relevantes": {"$sum": {"$cond": ["$supera_umbral", 1, 0]}},
                            "magnitud_media": {"$avg": "$magnitud"},
                        }
                    },
                    {"$sort": {"_id.anio": 1, "_id.mes": 1}},
                ]
            )
        )
    )
    if not mensual.empty:
        mensual["periodo"] = mensual["_id"].apply(lambda i: f"{i['anio']:04d}-{i['mes']:02d}")

    def agrupar(campo: str) -> pd.DataFrame:
        return pd.DataFrame(
            list(
                coleccion.aggregate(
                    [
                        {"$match": filtro},
                        {
                            "$group": {
                                "_id": f"${campo}",
                                "total": {"$sum": 1},
                                "magnitud_media": {"$avg": "$magnitud"},
                                "magnitud_maxima": {"$max": "$magnitud"},
                            }
                        },
                        {"$sort": {"total": -1}},
                    ]
                )
            )
        )

    return {
        "mensual": mensual,
        "regional": agrupar("region"),
        "categorias": agrupar("categoria_magnitud"),
        "profundidades": agrupar("clase_profundidad"),
    }


@st.cache_data(ttl=300)
def muestra_eventos(filtro: dict[str, Any], tope: int, orden_magnitud: bool = True) -> pd.DataFrame:
    etapas: list[dict[str, Any]] = [{"$match": filtro}]
    if orden_magnitud:
        etapas.append({"$sort": {"magnitud": -1}})
    etapas += [
        {"$limit": tope},
        {
            "$project": {
                "_id": 0,
                "evento_id": "$_id",
                "tiempo_utc": 1,
                "latitud": {"$arrayElemAt": ["$ubicacion.coordinates", 1]},
                "longitud": {"$arrayElemAt": ["$ubicacion.coordinates", 0]},
                "magnitud": 1,
                "profundidad_km": 1,
                "region": 1,
                "zona": 1,
                "fuente": 1,
                "clase_profundidad": 1,
            }
        },
    ]
    return pd.DataFrame(list(base_datos().eventos.aggregate(etapas)))


@st.cache_data(ttl=300)
def tabla_gold(nombre: str) -> pd.DataFrame:
    return pd.DataFrame(list(base_datos()[f"gold_{nombre}"].find({}, {"_id": 0})))


def _ordenar(df: pd.DataFrame, orden: list[str]) -> pd.DataFrame:
    return df.set_index("_id").reindex(orden).dropna().reset_index()


st.title("Sismos globales: análisis de actividad sísmica")

try:
    lim = limites()
except Exception as error:
    st.error(f"No se puede conectar con MongoDB ({CONFIG.mongo_uri}): {error}")
    st.stop()

if not lim.get("inicio"):
    st.warning("No hay datos cargados. Ejecute `python -m sismos pipeline` tras la ingesta.")
    st.stop()

with st.sidebar:
    st.header("Filtros")
    fechas = st.date_input(
        "Fecha",
        value=(lim["inicio"].date(), lim["fin"].date()),
        min_value=lim["inicio"].date(),
        max_value=lim["fin"].date(),
    )
    if isinstance(fechas, tuple) and len(fechas) == 2:
        fecha_inicio, fecha_fin = fechas
    else:
        fecha_inicio, fecha_fin = lim["inicio"].date(), lim["fin"].date()
    magnitud = st.slider("Magnitud", 0.0, 10.0, (float(lim["mag_min"]), float(lim["mag_max"])), 0.1)
    profundidad = st.slider("Profundidad (km)", 0.0, 800.0, lim["profundidad_defecto"], 1.0)
    regiones = st.multiselect("Región", lim["regiones"], default=lim["regiones"])
    fuentes = st.multiselect("Fuente de datos", lim["fuentes"], default=lim["fuentes"])

if not regiones or not fuentes:
    st.info("Seleccione al menos una región y una fuente.")
    st.stop()

filtro = construir_filtro(fecha_inicio, fecha_fin, magnitud, profundidad, regiones, fuentes, lim)
datos = agregados(filtro)
mensual = datos["mensual"]
total = int(mensual["total"].sum()) if not mensual.empty else 0

st.caption(f"{total:,} eventos con los filtros actuales. Base de datos: '{CONFIG.mongo_base}'.")
if total == 0:
    st.info("No hay eventos con los filtros seleccionados.")
    st.stop()

pestana_resumen, pestana_mapas, pestana_tiempo, pestana_regiones, pestana_profundidad, pestana_alertas = st.tabs(
    ["Resumen", "Mapas", "Evolución temporal", "Regiones y magnitud", "Profundidad", "Alertas y fuentes"]
)

with pestana_resumen:
    columnas = st.columns(4)
    columnas[0].metric("Eventos", f"{total:,}")
    columnas[1].metric("Eventos relevantes", f"{int(mensual['relevantes'].sum()):,}")
    columnas[2].metric("Magnitud media", f"{(mensual['magnitud_media'] * mensual['total']).sum() / total:.2f}")
    columnas[3].metric("Regiones activas", f"{len(datos['regional'])}")
    izquierda, derecha = st.columns(2)
    izquierda.plotly_chart(
        px.bar(
            _ordenar(datos["categorias"], ORDEN_MAGNITUD),
            x="_id",
            y="total",
            title="Eventos por categoría de magnitud",
            labels={"_id": "Categoría", "total": "Eventos"},
        )
    )
    derecha.plotly_chart(
        px.bar(
            _ordenar(datos["profundidades"], ORDEN_PROFUNDIDAD),
            x="_id",
            y="total",
            title="Eventos por clase de profundidad",
            labels={"_id": "Clase", "total": "Eventos"},
        )
    )

with pestana_mapas:
    puntos = muestra_eventos(filtro, MAX_PUNTOS_MAPA)
    st.plotly_chart(
        px.scatter_geo(
            puntos,
            lat="latitud",
            lon="longitud",
            color="magnitud",
            size="magnitud",
            hover_name="zona",
            hover_data={"tiempo_utc": True, "profundidad_km": True, "fuente": True},
            projection="natural earth",
            color_continuous_scale="Turbo",
            title=f"Epicentros (los {len(puntos):,} de mayor magnitud)",
        )
    )
    st.subheader("Mapa de calor de actividad")
    calor = muestra_eventos(filtro, MAX_PUNTOS_CALOR)
    st.pydeck_chart(
        pdk.Deck(
            layers=[
                pdk.Layer(
                    "HeatmapLayer",
                    data=calor,
                    get_position="[longitud, latitud]",
                    get_weight="magnitud",
                    radius_pixels=30,
                    aggregation="SUM",
                )
            ],
            initial_view_state=pdk.ViewState(latitude=10, longitude=0, zoom=0.8),
            map_style=None,
        )
    )

with pestana_tiempo:
    st.plotly_chart(
        px.line(mensual, x="periodo", y="total", title="Eventos por mes", labels={"periodo": "Periodo", "total": "Eventos"})
    )
    st.plotly_chart(
        px.line(
            mensual,
            x="periodo",
            y="magnitud_media",
            title="Magnitud media por mes",
            labels={"periodo": "Periodo", "magnitud_media": "Magnitud media"},
        )
    )

with pestana_regiones:
    regional = datos["regional"].head(20)
    st.plotly_chart(
        px.bar(regional, x="_id", y="total", title="Eventos por región (top 20)", labels={"_id": "Región", "total": "Eventos"})
    )
    st.plotly_chart(
        px.bar(
            regional,
            x="_id",
            y="magnitud_maxima",
            color="magnitud_media",
            title="Magnitud máxima por región (top 20)",
            labels={"_id": "Región", "magnitud_maxima": "Magnitud máxima", "magnitud_media": "Magnitud media"},
        )
    )

with pestana_profundidad:
    muestra = muestra_eventos(filtro, MUESTRA_DISPERSION, orden_magnitud=False).dropna(subset=["profundidad_km"])
    st.plotly_chart(
        px.scatter(
            muestra,
            x="profundidad_km",
            y="magnitud",
            color="clase_profundidad",
            opacity=0.5,
            title=f"Magnitud frente a profundidad (muestra de {len(muestra):,})",
            labels={"profundidad_km": "Profundidad (km)", "magnitud": "Magnitud"},
        )
    )
    correlacion = tabla_gold("correlacion_magnitud_profundidad")
    if not correlacion.empty:
        st.metric(
            "Correlación magnitud-profundidad (histórico completo, sin filtros)",
            f"{correlacion['correlacion_pearson'].iloc[0]:.3f}",
        )

with pestana_alertas:
    st.subheader(f"Eventos recientes relevantes (últimas {CONFIG.ventana_alertas_horas} h)")
    alertas = tabla_gold("alertas_recientes")
    if alertas.empty:
        st.info("No hay eventos recientes que superen el umbral ni con alerta.")
    else:
        st.dataframe(alertas)
    st.subheader("Contribución por fuente (histórico completo)")
    st.dataframe(tabla_gold("resumen_fuentes"))
