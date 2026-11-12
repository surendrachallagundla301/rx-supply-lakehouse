"""Reading and writing lakehouse tables in one place, so layers never care about storage."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from .config import Settings


def get_spark(app_name: str = "rx-lakehouse") -> SparkSession:
    active = SparkSession.getActiveSession()
    if active is not None:  # Databricks notebooks and jobs already have one
        return active
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def table_name(settings: Settings, layer: str, name: str) -> str:
    return f"{settings.catalog}.{settings.schema}.{layer}_{name}"


def _local_path(settings: Settings, layer: str, name: str) -> str:
    return f"{settings.local_root}/{layer}/{name}"


def write_table(df: DataFrame, settings: Settings, layer: str, name: str,
                partition_by: list[str] | None = None) -> None:
    writer = df.write.mode("overwrite").format(settings.table_format)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    if settings.uses_catalog:
        writer.option("overwriteSchema", "true").saveAsTable(table_name(settings, layer, name))
    else:
        writer.save(_local_path(settings, layer, name))


def read_table(spark: SparkSession, settings: Settings, layer: str, name: str) -> DataFrame:
    if settings.uses_catalog:
        return spark.table(table_name(settings, layer, name))
    return spark.read.format(settings.table_format).load(_local_path(settings, layer, name))
