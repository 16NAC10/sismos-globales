"""Carga de configuración desde YAML con sobrescritura por variables de entorno."""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

RAIZ = Path(__file__).resolve().parents[2]
CONFIG_DIR = RAIZ / "config"


def _leer_yaml(nombre: str) -> dict[str, Any]:
    with open(CONFIG_DIR / nombre, encoding="utf-8") as f:
        return yaml.safe_load(f)


@dataclass(frozen=True)
class Fuente:
    nombre: str
    formato: str
    url: str
    prioridad: int
    limite_por_consulta: int


@dataclass(frozen=True)
class Configuracion:
    magnitud_minima: float
    anios_historico: int
    intervalo_minutos: int
    solapamiento_horas: int
    reintentos: int
    pausa_base_segundos: float
    timeout_segundos: int
    kafka_bootstrap: str
    kafka_topic: str
    mongo_uri: str
    mongo_base: str
    ruta_bronze: Path
    ruta_silver: Path
    ruta_gold: Path
    ruta_rechazados: Path
    ruta_checkpoints: Path
    ventana_cruce_segundos: int
    distancia_cruce_km: float
    diferencia_magnitud_max: float
    umbral_magnitud: float
    profundidad_superficial_km: float
    profundidad_intermedia_km: float
    ventana_alertas_horas: int
    fuentes: tuple[Fuente, ...]


def _ruta(valor: str) -> Path:
    ruta = Path(valor)
    return ruta if ruta.is_absolute() else RAIZ / ruta


def cargar_configuracion() -> Configuracion:
    general = _leer_yaml("configuracion.yaml")
    fuentes = _leer_yaml("fuentes.yaml")["fuentes"]
    ing, kafka, mongo = general["ingesta"], general["kafka"], general["mongo"]
    rutas, proc = general["rutas"], general["procesamiento"]

    return Configuracion(
        magnitud_minima=float(ing["magnitud_minima"]),
        anios_historico=int(ing["anios_historico"]),
        intervalo_minutos=int(ing["intervalo_minutos"]),
        solapamiento_horas=int(ing["solapamiento_horas"]),
        reintentos=int(ing["reintentos"]),
        pausa_base_segundos=float(ing["pausa_base_segundos"]),
        timeout_segundos=int(ing["timeout_segundos"]),
        kafka_bootstrap=os.getenv("SISMOS_KAFKA_BOOTSTRAP", kafka["bootstrap_servers"]),
        kafka_topic=kafka["topic"],
        mongo_uri=os.getenv("SISMOS_MONGO_URI", mongo["uri"]),
        mongo_base=mongo["base_datos"],
        ruta_bronze=_ruta(rutas["bronze"]),
        ruta_silver=_ruta(rutas["silver"]),
        ruta_gold=_ruta(rutas["gold"]),
        ruta_rechazados=_ruta(rutas["rechazados"]),
        ruta_checkpoints=_ruta(rutas["checkpoints"]),
        ventana_cruce_segundos=int(proc["ventana_cruce_segundos"]),
        distancia_cruce_km=float(proc["distancia_cruce_km"]),
        diferencia_magnitud_max=float(proc["diferencia_magnitud_max"]),
        umbral_magnitud=float(proc["umbral_magnitud"]),
        profundidad_superficial_km=float(proc["profundidad_superficial_km"]),
        profundidad_intermedia_km=float(proc["profundidad_intermedia_km"]),
        ventana_alertas_horas=int(proc["ventana_alertas_horas"]),
        fuentes=tuple(Fuente(**f) for f in fuentes),
    )


def cargar_regiones() -> list[dict[str, Any]]:
    return _leer_yaml("regiones.yaml")["regiones"]


CONFIG = cargar_configuracion()
