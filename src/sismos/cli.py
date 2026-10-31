"""Punto de entrada: `python -m sismos <comando>`."""

import argparse
import logging
import subprocess
import sys
import time
from pathlib import Path

from sismos.config import CONFIG

log = logging.getLogger("sismos")


def _etapa_pipeline() -> None:
    from sismos.carga.mongo import cargar_mongo
    from sismos.procesamiento.bronze import ingerir_bronze
    from sismos.procesamiento.gold import ejecutar_gold
    from sismos.procesamiento.silver import ejecutar_silver

    ingerir_bronze()
    ejecutar_silver()
    ejecutar_gold()
    cargar_mongo()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(prog="sismos", description="Sistema de eventos sísmicos globales")
    sub = parser.add_subparsers(dest="comando", required=True)

    sub.add_parser("ingesta", help="Publica en Kafka los eventos nuevos de todas las fuentes")
    backfill = sub.add_parser("backfill", help="Carga histórica por meses (reanudable)")
    backfill.add_argument("--anios", type=int, default=CONFIG.anios_historico)
    sub.add_parser("bronze", help="Kafka -> Parquet bronze (solo mensajes nuevos)")
    sub.add_parser("silver", help="Bronze -> silver: limpieza, deduplicación y clasificación con Spark")
    sub.add_parser("gold", help="Silver -> agregados de análisis con Spark")
    sub.add_parser("mongo", help="Silver y gold -> MongoDB")
    sub.add_parser("pipeline", help="Ejecuta bronze, silver, gold y carga en MongoDB")
    ciclo = sub.add_parser("ciclo", help="Ingesta y pipeline de forma periódica")
    ciclo.add_argument("--intervalo", type=int, default=CONFIG.intervalo_minutos, help="minutos entre ciclos")
    sub.add_parser("dashboard", help="Abre el panel de análisis (Streamlit)")

    args = parser.parse_args()

    if args.comando == "ingesta":
        from sismos.ingesta.productor import ingesta_incremental

        print(ingesta_incremental())
    elif args.comando == "backfill":
        from sismos.ingesta.productor import backfill

        print(backfill(args.anios))
    elif args.comando == "bronze":
        from sismos.procesamiento.bronze import ingerir_bronze

        ingerir_bronze()
    elif args.comando == "silver":
        from sismos.procesamiento.silver import ejecutar_silver

        print(ejecutar_silver())
    elif args.comando == "gold":
        from sismos.procesamiento.gold import ejecutar_gold

        print(ejecutar_gold())
    elif args.comando == "mongo":
        from sismos.carga.mongo import cargar_mongo

        print(cargar_mongo())
    elif args.comando == "pipeline":
        _etapa_pipeline()
    elif args.comando == "ciclo":
        from sismos.ingesta.productor import ingesta_incremental

        while True:
            try:
                ingesta_incremental()
                _etapa_pipeline()
            except Exception:
                log.exception("Ciclo fallido; se reintentará en el siguiente intervalo")
            time.sleep(args.intervalo * 60)
    elif args.comando == "dashboard":
        ruta = Path(__file__).parent / "dashboard" / "app.py"
        sys.exit(subprocess.call([sys.executable, "-m", "streamlit", "run", str(ruta)]))


if __name__ == "__main__":
    main()
