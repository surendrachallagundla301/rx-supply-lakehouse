"""Data-quality gates that run after gold is built. A failed check fails the job, so bad data
never reaches dashboards. Results are returned so the job can log them."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Settings
from .io import read_table


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str


def unique(df: DataFrame, cols: list[str], name: str) -> CheckResult:
    dupes = df.groupBy(*cols).count().where("count > 1").count()
    return CheckResult(name, dupes == 0, f"{dupes} duplicate keys on {cols}")


def no_orphans(fact: DataFrame, dim: DataFrame, key: str, name: str) -> CheckResult:
    orphans = fact.join(dim.select(key), key, "left_anti").count()
    return CheckResult(name, orphans == 0, f"{orphans} fact rows without a {key}")


def between(df: DataFrame, col: str, lo: float, hi: float, name: str) -> CheckResult:
    bad = df.where((F.col(col) < lo) | (F.col(col) > hi)).count()
    return CheckResult(name, bad == 0, f"{bad} rows with {col} outside [{lo}, {hi}]")


def max_quarantine_ratio(valid: DataFrame, quarantined: DataFrame, limit: float) -> CheckResult:
    v, q = valid.count(), quarantined.count()
    ratio = q / max(1, v + q)
    detail = f"{ratio:.2%} of order lines quarantined (limit {limit:.0%})"
    return CheckResult("quarantine_ratio", ratio <= limit, detail)


def run(spark: SparkSession, settings: Settings, quarantine_limit: float = 0.02) -> list[CheckResult]:
    g = lambda n: read_table(spark, settings, "gold", n)  # noqa: E731
    fact = g("fact_order_line")
    results = [
        unique(fact, ["order_id", "line_no"], "fact_pk_unique"),
        unique(g("dim_product"), ["product_key"], "dim_product_pk_unique"),
        no_orphans(fact, g("dim_product"), "product_key", "fact_product_fk"),
        no_orphans(fact, g("dim_customer"), "customer_key", "fact_customer_fk"),
        no_orphans(fact, g("dim_distribution_center"), "dc_key", "fact_dc_fk"),
        no_orphans(fact.withColumnRenamed("order_date_key", "date_key"), g("dim_date"), "date_key", "fact_date_fk"),
        between(g("agg_daily_kpis"), "unit_fill_rate", 0.0, 1.0, "fill_rate_range"),
        max_quarantine_ratio(read_table(spark, settings, "silver", "order_lines"),
                             read_table(spark, settings, "silver", "order_lines_quarantine"), quarantine_limit),
    ]
    failed = [r for r in results if not r.passed]
    if failed:
        raise AssertionError("Data-quality checks failed: " + "; ".join(f"{r.name}: {r.detail}" for r in failed))
    return results
