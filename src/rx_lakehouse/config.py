"""Runtime settings.

On Databricks the pipeline writes Delta tables into Unity Catalog (`catalog.schema.table`).
Locally (tests, laptop runs) it writes Parquet folders under `LAKEHOUSE_ROOT`, because the
Delta Lake jars are not bundled with the PySpark wheel.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def on_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


@dataclass(frozen=True)
class Settings:
    catalog: str
    schema: str
    raw_path: str
    local_root: str
    table_format: str

    @classmethod
    def load(cls) -> Settings:
        default_format = "delta" if on_databricks() else "parquet"
        return cls(
            catalog=os.getenv("LAKEHOUSE_CATALOG", "rx_dev"),
            schema=os.getenv("LAKEHOUSE_SCHEMA", "supply"),
            raw_path=os.getenv("LAKEHOUSE_RAW_PATH", "data/raw"),
            local_root=os.getenv("LAKEHOUSE_ROOT", "data/lakehouse"),
            table_format=os.getenv("LAKEHOUSE_FORMAT", default_format),
        )

    @property
    def uses_catalog(self) -> bool:
        return self.table_format == "delta" and on_databricks()
