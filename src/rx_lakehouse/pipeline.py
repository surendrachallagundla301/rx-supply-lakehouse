"""Entry point used by the Databricks job tasks and by local runs.

    python -m rx_lakehouse.pipeline --layer all
    python -m rx_lakehouse.pipeline --layer silver
"""

from __future__ import annotations

import argparse
import json
import logging
import time

from . import bronze, gold, quality, silver
from .config import Settings
from .io import get_spark

LAYERS = {"bronze": bronze.run, "silver": silver.run, "gold": gold.run}
log = logging.getLogger("rx_lakehouse")


def run(layer: str = "all") -> dict:
    settings = Settings.load()
    spark = get_spark()
    report: dict = {"settings": settings.__dict__}
    targets = list(LAYERS) if layer == "all" else [layer] if layer in LAYERS else []
    for name in targets:
        t0 = time.perf_counter()
        report[name] = LAYERS[name](spark, settings)
        log.info("%s done in %.1fs: %s", name, time.perf_counter() - t0, report[name])
    if layer in ("all", "quality"):
        report["quality"] = [r.__dict__ for r in quality.run(spark, settings)]
    return report


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--layer", default="all", choices=["all", "quality", *LAYERS])
    print(json.dumps(run(p.parse_args().layer), indent=2, default=str))


if __name__ == "__main__":
    main()
