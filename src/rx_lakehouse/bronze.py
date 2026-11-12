"""Bronze: land raw files as-is, adding lineage columns. No cleaning happens here."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .config import Settings
from .io import write_table

ORDER_LINE_SCHEMA = T.StructType([
    T.StructField("order_id", T.StringType()),
    T.StructField("line_no", T.IntegerType()),
    T.StructField("customer_id", T.StringType()),
    T.StructField("dc_id", T.StringType()),
    T.StructField("ndc", T.StringType()),
    T.StructField("order_ts", T.StringType()),
    T.StructField("qty_ordered", T.IntegerType()),
    T.StructField("qty_shipped", T.IntegerType()),
    T.StructField("unit_price", T.DoubleType()),
    T.StructField("promised_date", T.StringType()),
    T.StructField("delivered_date", T.StringType()),
])

CSV_SOURCES = ["products", "customers", "distribution_centers", "inventory_snapshots"]


def _with_lineage(df: DataFrame) -> DataFrame:
    return (df.withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source_file", F.col("_metadata.file_path")))


def read_raw(spark: SparkSession, raw_path: str) -> dict[str, DataFrame]:
    frames = {
        name: _with_lineage(spark.read.option("header", True).csv(f"{raw_path}/{name}.csv"))
        for name in CSV_SOURCES
    }
    # Explicit schema for the JSON feed: a producer adding a field must not silently change types.
    frames["order_lines"] = _with_lineage(
        spark.read.schema(ORDER_LINE_SCHEMA).json(f"{raw_path}/order_lines.jsonl"))
    return frames


def run(spark: SparkSession, settings: Settings) -> dict[str, int]:
    counts = {}
    for name, df in read_raw(spark, settings.raw_path).items():
        write_table(df, settings, "bronze", name)
        counts[name] = df.count()
    return counts
