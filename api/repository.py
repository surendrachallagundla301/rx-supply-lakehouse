"""Query the gold layer.

Locally, DuckDB reads the gold Parquet folders the pipeline wrote. In Azure the same SQL runs
on a Databricks SQL warehouse (set DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN).
The rest of the API only sees `Repository.query(sql, params)`.
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Any, Protocol

GOLD_TABLES = ["dim_date", "dim_product", "dim_customer", "dim_distribution_center",
               "fact_order_line", "agg_daily_kpis", "agg_stockout_risk"]


class Repository(Protocol):
    def query(self, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]: ...


class DuckDBRepository:
    def __init__(self, root: str):
        import duckdb

        self.con = duckdb.connect()
        for t in GOLD_TABLES:
            path = os.path.join(root, "gold", t)
            self.con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{path}/**/*.parquet')")

    def query(self, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        cur = self.con.cursor()
        cur.execute(sql, params or [])
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]


class DatabricksRepository:
    """Databricks SQL warehouse via databricks-sql-connector (installed only in the Azure image)."""

    def __init__(self) -> None:
        from databricks import sql as dbsql

        self.catalog = os.getenv("LAKEHOUSE_CATALOG", "rx_dev")
        self.schema = os.getenv("LAKEHOUSE_SCHEMA", "supply")
        self.conn = dbsql.connect(server_hostname=os.environ["DATABRICKS_HOST"],
                                  http_path=os.environ["DATABRICKS_HTTP_PATH"],
                                  access_token=os.environ["DATABRICKS_TOKEN"])

    def query(self, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        from .sql_guard import qualify_for_databricks

        with self.conn.cursor() as cur:
            cur.execute(qualify_for_databricks(sql, self.catalog, self.schema), params or [])
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]


@lru_cache(maxsize=1)
def get_repository() -> Repository:
    if os.getenv("DATABRICKS_HOST"):
        return DatabricksRepository()
    return DuckDBRepository(os.getenv("LAKEHOUSE_ROOT", "data/lakehouse"))
