"""Silver: typed, de-duplicated, validated records. Rows that fail a rule go to quarantine
with the reason, instead of being dropped silently."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .config import Settings
from .io import read_table, write_table

NDC_PATTERN = r"^\d{5}-\d{4}-\d{2}$"  # 5-4-2 labeler-product-package format


def safe_date(col: str) -> Column:
    """Bad date strings become NULL (and then fail a rule) instead of crashing the job."""
    return F.expr(f"try_cast({col} AS DATE)")


def safe_ts(col: str) -> Column:
    return F.expr(f"try_cast({col} AS TIMESTAMP)")


def clean_products(df: DataFrame) -> DataFrame:
    return (df.select(
                F.trim("ndc").alias("ndc"),
                F.trim("product_name").alias("product_name"),
                "therapeutic_class", "manufacturer",
                F.col("unit_cost").cast("decimal(12,2)").alias("unit_cost"),
                F.col("is_controlled").cast("boolean").alias("is_controlled"),
                F.col("requires_cold_chain").cast("boolean").alias("requires_cold_chain"))
              .where(F.col("ndc").rlike(NDC_PATTERN))
              .dropDuplicates(["ndc"]))


def clean_customers(df: DataFrame) -> DataFrame:
    return (df.select("customer_id", "customer_name", "customer_type", "state", "home_dc_id",
                      safe_date("onboarded_date").alias("onboarded_date"))
              .dropDuplicates(["customer_id"]))


def clean_distribution_centers(df: DataFrame) -> DataFrame:
    return df.select("dc_id", "city", "state").dropDuplicates(["dc_id"])


def clean_inventory(df: DataFrame) -> DataFrame:
    return (df.select(safe_date("snapshot_date").alias("snapshot_date"), "dc_id", "ndc",
                      F.col("on_hand_units").cast("int").alias("on_hand_units"))
              .dropDuplicates(["snapshot_date", "dc_id", "ndc"]))


def typed_order_lines(df: DataFrame) -> DataFrame:
    return df.select(
        "order_id", "line_no", "customer_id", "dc_id", F.trim("ndc").alias("ndc"),
        safe_ts("order_ts").alias("order_ts"),
        "qty_ordered", "qty_shipped",
        F.col("unit_price").cast("decimal(12,2)").alias("unit_price"),
        safe_date("promised_date").alias("promised_date"),
        safe_date("delivered_date").alias("delivered_date"),
        "_ingested_at",
    )


def dedupe_latest(df: DataFrame, keys: list[str], order_col: str = "_ingested_at") -> DataFrame:
    """Keep one row per business key: the most recently ingested copy wins."""
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def validate_order_lines(lines: DataFrame, products: DataFrame, customers: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split order lines into (valid, quarantined). Each quarantined row lists every rule it broke."""
    known_ndc = products.select(F.col("ndc").alias("_p_ndc"))
    known_cust = customers.select(F.col("customer_id").alias("_c_id"))
    joined = (lines.join(known_ndc, lines.ndc == known_ndc._p_ndc, "left")
                   .join(known_cust, lines.customer_id == known_cust._c_id, "left"))

    rules: dict[str, Column] = {
        "malformed_ndc": ~F.coalesce(F.col("ndc").rlike(NDC_PATTERN), F.lit(False)),
        "unknown_ndc": F.col("ndc").rlike(NDC_PATTERN) & F.col("_p_ndc").isNull(),
        "unknown_customer": F.col("_c_id").isNull(),
        "non_positive_qty": F.coalesce(F.col("qty_ordered") <= 0, F.lit(True)),
        "shipped_exceeds_ordered": (F.col("qty_ordered") > 0) & (F.col("qty_shipped") > F.col("qty_ordered")),
        "missing_order_ts": F.col("order_ts").isNull(),
    }
    reasons = F.array_compact(F.array(*[F.when(cond, F.lit(name)) for name, cond in rules.items()]))
    flagged = joined.withColumn("dq_reasons", reasons).drop("_p_ndc", "_c_id")

    valid = flagged.where(F.size("dq_reasons") == 0).drop("dq_reasons")
    quarantined = flagged.where(F.size("dq_reasons") > 0).withColumn("quarantined_at", F.current_timestamp())
    return valid, quarantined


def run(spark: SparkSession, settings: Settings) -> dict[str, int]:
    products = clean_products(read_table(spark, settings, "bronze", "products"))
    customers = clean_customers(read_table(spark, settings, "bronze", "customers"))
    dcs = clean_distribution_centers(read_table(spark, settings, "bronze", "distribution_centers"))
    inventory = clean_inventory(read_table(spark, settings, "bronze", "inventory_snapshots"))

    lines = dedupe_latest(typed_order_lines(read_table(spark, settings, "bronze", "order_lines")),
                          ["order_id", "line_no"])
    valid, quarantined = validate_order_lines(lines, products, customers)

    outputs = {"products": products, "customers": customers, "distribution_centers": dcs,
               "inventory_snapshots": inventory, "order_lines": valid,
               "order_lines_quarantine": quarantined}
    for name, df in outputs.items():
        write_table(df, settings, "silver", name)
    return {name: read_table(spark, settings, "silver", name).count() for name in outputs}

