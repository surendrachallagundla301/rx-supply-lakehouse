"""Gold: a star schema for BI plus a few ready-made KPI tables.

    dim_date ─┐
 dim_product ─┼── fact_order_line ── agg_daily_kpis
dim_customer ─┤                  └── agg_stockout_risk (joined with inventory)
      dim_dc ─┘

Surrogate keys are xxhash64 of the natural key, so they are stable across full rebuilds and
the fact table can be built without a lookup against the dimension tables.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Settings
from .io import read_table, write_table

DEMAND_WINDOW_DAYS = 28
LOW_SUPPLY_DAYS = 7


def _sk(*cols: str):
    return F.xxhash64(*[F.col(c) for c in cols])


def date_key(col):
    return F.date_format(col, "yyyyMMdd").cast("int")


def build_dim_date(lines: DataFrame) -> DataFrame:
    bounds = lines.select(F.min(F.to_date("order_ts")).alias("lo"),
                          F.date_add(F.max("promised_date"), 14).alias("hi"))
    days = bounds.select(F.explode(F.sequence("lo", "hi")).alias("date"))
    return days.select(
        date_key(F.col("date")).alias("date_key"),
        "date",
        F.date_format("date", "EEEE").alias("day_name"),
        F.dayofweek("date").alias("day_of_week"),
        F.date_trunc("week", "date").cast("date").alias("week_start"),
        F.month("date").alias("month"),
        F.quarter("date").alias("quarter"),
        F.year("date").alias("year"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )


def build_dim_product(products: DataFrame) -> DataFrame:
    return products.select(_sk("ndc").alias("product_key"), "*")


def build_dim_customer(customers: DataFrame) -> DataFrame:
    return customers.select(_sk("customer_id").alias("customer_key"), "*")


def build_dim_dc(dcs: DataFrame) -> DataFrame:
    return dcs.select(_sk("dc_id").alias("dc_key"), "*")


def build_fact_order_line(lines: DataFrame) -> DataFrame:
    on_time = F.col("delivered_date").isNotNull() & (F.col("delivered_date") <= F.col("promised_date"))
    return lines.select(
        "order_id", "line_no",
        date_key(F.to_date("order_ts")).alias("order_date_key"),
        _sk("ndc").alias("product_key"),
        _sk("customer_id").alias("customer_key"),
        _sk("dc_id").alias("dc_key"),
        "order_ts", "promised_date", "delivered_date",
        "qty_ordered", "qty_shipped",
        (F.col("qty_ordered") - F.col("qty_shipped")).alias("qty_short"),
        "unit_price",
        (F.col("qty_shipped") * F.col("unit_price")).cast("decimal(14,2)").alias("shipped_amount"),
        (F.col("qty_shipped") == F.col("qty_ordered")).alias("is_line_filled"),
        on_time.alias("is_on_time"),
        F.greatest(F.lit(0), F.datediff("delivered_date", "promised_date")).alias("days_late"),
    )


def build_daily_kpis(fact: DataFrame, dim_dc: DataFrame) -> DataFrame:
    shipped = F.col("qty_shipped") > 0
    return (fact.join(dim_dc.select("dc_key", "dc_id"), "dc_key")
                .withColumn("order_date", F.to_date("order_ts"))
                .groupBy("order_date", "dc_id")
                .agg(F.count("*").alias("order_lines"),
                     F.countDistinct("order_id").alias("orders"),
                     F.sum("qty_ordered").alias("units_ordered"),
                     F.sum("qty_shipped").alias("units_shipped"),
                     F.sum("shipped_amount").alias("revenue"),
                     F.avg(F.col("is_line_filled").cast("int")).alias("line_fill_rate"),
                     (F.sum(F.when(shipped & F.col("is_on_time"), 1).otherwise(0))
                      / F.sum(F.when(shipped, 1).otherwise(0))).alias("on_time_rate"))
                .withColumn("unit_fill_rate", F.col("units_shipped") / F.col("units_ordered"))
                .orderBy("order_date", "dc_id"))


def build_stockout_risk(fact: DataFrame, inventory: DataFrame, dim_product: DataFrame,
                        dim_dc: DataFrame) -> DataFrame:
    """Days of supply per DC and NDC: latest on-hand units divided by recent average daily demand."""
    latest_date = inventory.agg(F.max("snapshot_date")).first()[0]
    on_hand = inventory.where(F.col("snapshot_date") == F.lit(latest_date))

    demand = (fact.join(dim_product.select("product_key", "ndc"), "product_key")
                  .join(dim_dc.select("dc_key", "dc_id"), "dc_key")
                  .where(F.to_date("order_ts") > F.date_sub(F.lit(latest_date), DEMAND_WINDOW_DAYS))
                  .groupBy("dc_id", "ndc")
                  .agg((F.sum("qty_ordered") / F.lit(DEMAND_WINDOW_DAYS)).alias("avg_daily_demand")))

    return (on_hand.join(demand, ["dc_id", "ndc"], "inner")
                   .join(dim_product.select("ndc", "product_name", "therapeutic_class",
                                            "requires_cold_chain"), "ndc")
                   .withColumn("days_of_supply",
                               F.round(F.col("on_hand_units") / F.col("avg_daily_demand"), 1))
                   .withColumn("risk_level",
                               F.when(F.col("days_of_supply") < 3, "HIGH")
                                .when(F.col("days_of_supply") < LOW_SUPPLY_DAYS, "MEDIUM")
                                .otherwise("LOW"))
                   .select("snapshot_date", "dc_id", "ndc", "product_name", "therapeutic_class",
                           "requires_cold_chain", "on_hand_units",
                           F.round("avg_daily_demand", 2).alias("avg_daily_demand"),
                           "days_of_supply", "risk_level"))


def run(spark: SparkSession, settings: Settings) -> dict[str, int]:
    s = {n: read_table(spark, settings, "silver", n)
         for n in ["products", "customers", "distribution_centers", "inventory_snapshots", "order_lines"]}

    dim_product = build_dim_product(s["products"])
    dim_dc = build_dim_dc(s["distribution_centers"])
    fact = build_fact_order_line(s["order_lines"])
    outputs = {
        "dim_date": build_dim_date(s["order_lines"]),
        "dim_product": dim_product,
        "dim_customer": build_dim_customer(s["customers"]),
        "dim_distribution_center": dim_dc,
        "fact_order_line": fact,
        "agg_daily_kpis": build_daily_kpis(fact, dim_dc),
        "agg_stockout_risk": build_stockout_risk(fact, s["inventory_snapshots"], dim_product, dim_dc),
    }
    for name, df in outputs.items():
        write_table(df, settings, "gold", name,
                    partition_by=["order_date_key"] if name == "fact_order_line" and settings.uses_catalog else None)
    return {name: read_table(spark, settings, "gold", name).count() for name in outputs}
