"""Publica en Kafka los eventos sísmicos de cada fuente, en modo incremental y de backfill histórico."""

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from confluent_kafka import Producer

from sismos.config import CONFIG, Fuente
from sismos.ingesta.fuentes import ClienteSismico, ventanas_mensuales

log = logging.getLogger(__name__)


def _ahora() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _leer_checkpoint(ruta: Path) -> dict[str, Any]:
    if not ruta.exists():
        return {}
    return json.loads(ruta.read_text(encoding="utf-8"))


def _escribir_checkpoint(ruta: Path, datos: dict[str, Any]) -> None:
    ruta.parent.mkdir(parents=True, exist_ok=True)
    ruta.write_text(json.dumps(datos, ensure_ascii=False, indent=2), encoding="utf-8")


class PublicadorKafka:
    """Envía eventos al topic configurado. La clave es fuente:id_fuente para mantener el orden por evento."""

    def __init__(self, bootstrap: str = CONFIG.kafka_bootstrap, topic: str = CONFIG.kafka_topic):
        self.topic = topic
        self.errores = 0
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": 50,
            }
        )

    def _callback(self, error, _mensaje) -> None:
        if error is not None:
            self.errores += 1
            log.error("Error al publicar en Kafka: %s", error)

    def publicar(self, eventos: list[dict[str, Any]]) -> int:
        ingestado = _ahora().isoformat().replace("+00:00", "Z")
        for evento in eventos:
            evento["ingestado_utc"] = ingestado
            clave = f"{evento['fuente']}:{evento['id_fuente']}".encode("utf-8")
            while True:
                try:
                    self.producer.produce(
                        self.topic,
                        key=clave,
                        value=json.dumps(evento, ensure_ascii=False).encode("utf-8"),
                        on_delivery=self._callback,
                    )
                    break
                except BufferError:
                    self.producer.poll(0.5)
            self.producer.poll(0)
        return len(eventos)

    def vaciar(self) -> None:
        pendientes = self.producer.flush(30)
        if pendientes or self.errores:
            raise RuntimeError(f"Kafka: {pendientes} mensajes sin confirmar, {self.errores} errores")

    def cerrar(self) -> None:
        self.vaciar()


def ingesta_incremental(fuentes: tuple[Fuente, ...] = CONFIG.fuentes) -> dict[str, int]:
    """Consulta cada fuente desde su último checkpoint, con solapamiento para captar revisiones."""
    ahora = _ahora()
    resumen: dict[str, int] = {}
    checkpoints: list[Path] = []
    publicador = PublicadorKafka()
    try:
        for fuente in fuentes:
            ruta = CONFIG.ruta_checkpoints / f"incremental_{fuente.nombre}.json"
            ultimo = _leer_checkpoint(ruta).get("hasta_utc")
            desde = (
                datetime.fromisoformat(ultimo.replace("Z", "+00:00"))
                if ultimo
                else ahora - timedelta(days=2)
            )
            inicio = desde - timedelta(hours=CONFIG.solapamiento_horas)
            cliente = ClienteSismico(fuente)
            try:
                eventos = cliente.consultar(
                    inicio,
                    ahora,
                    actualizado_desde=desde if fuente.formato == "usgs_geojson" else None,
                )
            except Exception as error:
                log.error("%s: ingesta omitida en este ciclo (%s)", fuente.nombre, error)
                resumen[fuente.nombre] = -1
                continue
            resumen[fuente.nombre] = publicador.publicar(eventos)
            checkpoints.append(ruta)
    finally:
        publicador.cerrar()
    for ruta in checkpoints:
        _escribir_checkpoint(ruta, {"hasta_utc": ahora.isoformat().replace("+00:00", "Z")})
    log.info("Ingesta incremental: %s", resumen)
    return resumen


def backfill(anios: int = CONFIG.anios_historico, fuentes: tuple[Fuente, ...] = CONFIG.fuentes) -> dict[str, int]:
    """Carga histórica por meses. Es reanudable: los meses ya completados se registran en un checkpoint."""
    fin = _ahora()
    inicio = fin - timedelta(days=int(anios * 365.25))
    resumen: dict[str, int] = {}
    publicador = PublicadorKafka()
    try:
        for fuente in fuentes:
            ruta = CONFIG.ruta_checkpoints / f"backfill_{fuente.nombre}.json"
            completados = set(_leer_checkpoint(ruta).get("meses", []))
            cliente = ClienteSismico(fuente)
            total = 0
            for ventana_inicio, ventana_fin in ventanas_mensuales(inicio, fin):
                clave = ventana_inicio.strftime("%Y-%m")
                if clave in completados:
                    continue
                try:
                    eventos = cliente.consultar(ventana_inicio, ventana_fin)
                except Exception as error:
                    log.error("%s %s: ventana omitida (%s)", fuente.nombre, clave, error)
                    continue
                total += publicador.publicar(eventos)
                completados.add(clave)
                time.sleep(0.2)
            publicador.vaciar()
            _escribir_checkpoint(ruta, {"meses": sorted(completados)})
            resumen[fuente.nombre] = total
            log.info("Backfill %s completado: %d eventos publicados", fuente.nombre, total)
    finally:
        publicador.cerrar()
    return resumen
