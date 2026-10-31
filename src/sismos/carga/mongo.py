"""Carga de la capa silver y de los agregados gold en MongoDB para consultas rápidas."""

import logging
from typing import Any

import pandas as pd
import pyarrow.dataset as ds
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.database import Database

from sismos.config import CONFIG

log = logging.getLogger(__name__)

TAMANO_LOTE = 5000


def _leer_parquet(ruta) -> pd.DataFrame:
    return ds.dataset(str(ruta), format="parquet", partitioning="hive").to_table().to_pandas()


def _a_documentos(df: pd.DataFrame, geo: bool = False) -> list[dict[str, Any]]:
    df = df.astype(object).where(df.notna(), None)
    documentos = df.to_dict(orient="records")
    if geo:
        for doc in documentos:
            lat, lon = doc.pop("latitud", None), doc.pop("longitud", None)
            doc["ubicacion"] = (
                {"type": "Point", "coordinates": [lon, lat]} if lat is not None and lon is not None else None
            )
            doc["_id"] = doc.pop("evento_id")
    return documentos


def _reemplazar(db: Database, nombre: str, documentos: list[dict[str, Any]]) -> int:
    """Escribe en una colección temporal y la intercambia al final: las consultas nunca ven una carga parcial."""
    temporal = f"{nombre}_tmp"
    db[temporal].drop()
    for inicio in range(0, len(documentos), TAMANO_LOTE):
        db[temporal].insert_many(documentos[inicio : inicio + TAMANO_LOTE], ordered=False)
    db[temporal].rename(nombre, dropTarget=True)
    return len(documentos)


def _indices_eventos(db: Database) -> None:
    eventos = db["eventos"]
    eventos.create_index([("ubicacion", "2dsphere")])
    eventos.create_index([("tiempo_utc", DESCENDING)])
    eventos.create_index([("magnitud", ASCENDING)])
    eventos.create_index([("profundidad_km", ASCENDING)])
    eventos.create_index([("region", ASCENDING), ("tiempo_utc", DESCENDING)])
    eventos.create_index([("fuentes", ASCENDING)])


def cargar_mongo() -> dict[str, int]:
    cliente = MongoClient(CONFIG.mongo_uri, serverSelectionTimeoutMS=5000)
    db = cliente[CONFIG.mongo_base]
    metricas: dict[str, int] = {}

    silver = _leer_parquet(CONFIG.ruta_silver)
    metricas["eventos"] = _reemplazar(db, "eventos", _a_documentos(silver, geo=True))
    _indices_eventos(db)

    for carpeta in sorted(CONFIG.ruta_gold.iterdir()):
        if not carpeta.is_dir():
            continue
        nombre = f"gold_{carpeta.name}"
        metricas[nombre] = _reemplazar(db, nombre, _a_documentos(_leer_parquet(carpeta)))

    cliente.close()
    log.info("Mongo: %s", metricas)
    return metricas
