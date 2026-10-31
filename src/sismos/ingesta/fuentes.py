"""Clientes de las APIs sísmicas. Todas usan el servicio FDSN Event salvo USGS en GeoJSON."""

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

import requests

from sismos.config import CONFIG, Fuente

log = logging.getLogger(__name__)

# Columnas del formato FDSN de texto, normalizadas a minúsculas sin separadores.
_CAMPOS_TEXTO = {
    "eventid": "id_fuente",
    "time": "tiempo",
    "latitude": "latitud",
    "longitude": "longitud",
    "depthkm": "profundidad_km",
    "magtype": "tipo_magnitud",
    "magnitude": "magnitud",
    "magauthor": "autor_magnitud",
    "eventlocationname": "ubicacion",
    "eventtype": "tipo_evento",
    "author": "autor",
}


def _a_float(valor: Any) -> float | None:
    if valor is None or str(valor).strip() == "":
        return None
    try:
        return float(valor)
    except (TypeError, ValueError):
        return None


def _a_texto(valor: Any) -> str | None:
    if valor is None:
        return None
    texto = str(valor).strip()
    return texto or None


def _normalizar_clave(clave: str) -> str:
    return "".join(c for c in clave.lower() if c.isalnum())


def formatear_tiempo(valor: str | None) -> str | None:
    """Convierte fechas FDSN (con o sin 'Z') o epoch en milisegundos a ISO 8601 UTC."""
    if valor is None:
        return None
    texto = str(valor).strip()
    if not texto:
        return None
    try:
        if texto.isdigit():
            dt = datetime.fromtimestamp(int(texto) / 1000, tz=timezone.utc)
        else:
            dt = datetime.fromisoformat(texto.replace("Z", ""))
            dt = dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
    except ValueError:
        return None
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _evento_base(fuente: str) -> dict[str, Any]:
    return {
        "fuente": fuente,
        "id_fuente": None,
        "tiempo_utc": None,
        "latitud": None,
        "longitud": None,
        "profundidad_km": None,
        "magnitud": None,
        "tipo_magnitud": None,
        "autor_magnitud": None,
        "ubicacion": None,
        "tipo_evento": None,
        "nivel_alerta": None,
        "tsunami": None,
        "significancia": None,
        "estado": None,
        "actualizado_utc": None,
        "url": None,
    }


def parsear_fdsn_texto(texto: str, fuente: str) -> list[dict[str, Any]]:
    """Parsea la salida `format=text` del servicio FDSN (separador '|')."""
    lineas = [linea for linea in texto.splitlines() if linea.strip()]
    if not lineas:
        return []
    cabecera = [_normalizar_clave(c.lstrip("#")) for c in lineas[0].split("|")]
    eventos = []
    for linea in lineas[1:]:
        valores = linea.split("|")
        fila = dict(zip(cabecera, valores))
        evento = _evento_base(fuente)
        for clave_origen, campo in _CAMPOS_TEXTO.items():
            if clave_origen in fila:
                evento[campo] = fila[clave_origen]
        if evento.get("tiempo") is not None:
            evento["tiempo_utc"] = formatear_tiempo(evento.pop("tiempo"))
        evento.pop("autor", None)
        evento["id_fuente"] = _a_texto(evento["id_fuente"])
        evento["latitud"] = _a_float(evento["latitud"])
        evento["longitud"] = _a_float(evento["longitud"])
        evento["profundidad_km"] = _a_float(evento["profundidad_km"])
        evento["magnitud"] = _a_float(evento["magnitud"])
        evento["tipo_magnitud"] = _a_texto(evento["tipo_magnitud"])
        evento["autor_magnitud"] = _a_texto(evento["autor_magnitud"])
        evento["ubicacion"] = _a_texto(evento["ubicacion"])
        evento["tipo_evento"] = _a_texto(evento["tipo_evento"])
        if evento["id_fuente"]:
            eventos.append(evento)
    return eventos


def parsear_usgs_geojson(datos: dict[str, Any]) -> list[dict[str, Any]]:
    """Parsea la respuesta GeoJSON de USGS, que incluye nivel de alerta, tsunami y significancia."""
    eventos = []
    for feature in datos.get("features", []):
        props = feature.get("properties") or {}
        coords = (feature.get("geometry") or {}).get("coordinates") or [None, None, None]
        evento = _evento_base("USGS")
        evento.update(
            {
                "id_fuente": feature.get("id"),
                "tiempo_utc": formatear_tiempo(str(props["time"])) if props.get("time") else None,
                "latitud": _a_float(coords[1]),
                "longitud": _a_float(coords[0]),
                "profundidad_km": _a_float(coords[2]) if len(coords) > 2 else None,
                "magnitud": _a_float(props.get("mag")),
                "tipo_magnitud": _a_texto(props.get("magType")),
                "autor_magnitud": _a_texto(props.get("net")),
                "ubicacion": _a_texto(props.get("place")),
                "tipo_evento": _a_texto(props.get("type")),
                "nivel_alerta": _a_texto(props.get("alert")),
                "tsunami": props.get("tsunami"),
                "significancia": props.get("sig"),
                "estado": _a_texto(props.get("status")),
                "actualizado_utc": formatear_tiempo(str(props["updated"])) if props.get("updated") else None,
                "url": _a_texto(props.get("url")),
            }
        )
        if evento["id_fuente"]:
            eventos.append(evento)
    return eventos


def ventanas_mensuales(inicio: datetime, fin: datetime) -> Iterator[tuple[datetime, datetime]]:
    """Divide un intervalo en meses para respetar el límite de resultados por consulta."""
    cursor = inicio.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while cursor < fin:
        siguiente = (cursor + timedelta(days=32)).replace(day=1)
        yield max(cursor, inicio), min(siguiente, fin)
        cursor = siguiente


class ClienteSismico:
    """Cliente HTTP con reintentos y backoff exponencial para una fuente sísmica."""

    def __init__(self, fuente: Fuente, sesion: requests.Session | None = None):
        self.fuente = fuente
        self.sesion = sesion or requests.Session()
        self.sesion.headers["User-Agent"] = "sismos-globales/1.0 (proyecto academico)"

    def _get(self, params: dict[str, Any]) -> requests.Response | None:
        for intento in range(1, CONFIG.reintentos + 1):
            try:
                respuesta = self.sesion.get(self.fuente.url, params=params, timeout=CONFIG.timeout_segundos)
                if respuesta.status_code == 204:
                    return None
                respuesta.raise_for_status()
                return respuesta
            except requests.RequestException as error:
                estado = getattr(error.response, "status_code", None)
                reintentable = estado is None or estado == 429 or estado >= 500
                if not reintentable or intento == CONFIG.reintentos:
                    log.error("%s: fallo definitivo tras %d intento(s): %s", self.fuente.nombre, intento, error)
                    raise
                espera = CONFIG.pausa_base_segundos * (2 ** (intento - 1))
                log.warning("%s: reintento %d en %.1fs (%s)", self.fuente.nombre, intento, espera, error)
                time.sleep(espera)
        return None

    def consultar(
        self,
        inicio: datetime,
        fin: datetime,
        actualizado_desde: datetime | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "starttime": inicio.strftime("%Y-%m-%dT%H:%M:%S"),
            "endtime": fin.strftime("%Y-%m-%dT%H:%M:%S"),
            "minmagnitude": CONFIG.magnitud_minima,
            "orderby": "time-asc",
        }
        if actualizado_desde is not None:
            params["updatedafter"] = actualizado_desde.strftime("%Y-%m-%dT%H:%M:%S")

        if self.fuente.formato == "usgs_geojson":
            params["format"] = "geojson"
            respuesta = self._get(params)
            if respuesta is None:
                return []
            eventos = parsear_usgs_geojson(respuesta.json())
        else:
            params["format"] = "text"
            respuesta = self._get(params)
            if respuesta is None:
                return []
            eventos = parsear_fdsn_texto(respuesta.text, self.fuente.nombre)

        if len(eventos) >= self.fuente.limite_por_consulta:
            log.warning("%s: consulta alcanzó el límite (%d); dividir la ventana",
                        self.fuente.nombre, len(eventos))
        log.info("%s: %d eventos entre %s y %s", self.fuente.nombre, len(eventos), params["starttime"], params["endtime"])
        return eventos
