# Databricks notebook source
# MAGIC %md
# MAGIC # Supply performance analysis
# MAGIC Questions from the operations team, answered from the gold layer:
# MAGIC 1. Are fill rate and on-time delivery stable across distribution centers?
# MAGIC 2. Where is revenue concentrated, and what does that mean for stockout priorities?
# MAGIC 3. Does service level differ by customer type?
# MAGIC
# MAGIC Runs on Databricks (Unity Catalog tables) or locally with PySpark (Parquet).

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

from rx_lakehouse.config import Settings
from rx_lakehouse.io import get_spark, read_table

spark = get_spark()
s = Settings.load()
gold = lambda name: read_table(spark, s, "gold", name)  # noqa: E731

kpis = gold("agg_daily_kpis")
fact = gold("fact_order_line")
product = gold("dim_product")
customer = gold("dim_customer")

# COMMAND ----------

# MAGIC %md ## 1. Fill rate and on-time delivery by DC

# COMMAND ----------

by_dc = (kpis.groupBy("dc_id")
             .agg((F.sum("units_shipped") / F.sum("units_ordered")).alias("unit_fill_rate"),
                  (F.sum(F.col("on_time_rate") * F.col("order_lines")) / F.sum("order_lines")).alias("on_time_rate"))
             .orderBy("unit_fill_rate"))
display(by_dc)  # noqa: F821  (display is provided by Databricks)

# COMMAND ----------

# Weekly trend for the weakest DC: is the gap constant, or a specific period?
weakest = by_dc.first()["dc_id"]
weekly = (kpis.where(F.col("dc_id") == weakest)
              .groupBy(F.date_trunc("week", "order_date").alias("week"))
              .agg((F.sum("units_shipped") / F.sum("units_ordered")).alias("unit_fill_rate"),
                   (F.sum(F.col("on_time_rate") * F.col("order_lines")) / F.sum("order_lines")).alias("on_time_rate"))
              .orderBy("week"))
display(weekly)  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC **Finding.** DC02's overall fill rate (96.0%) trails the other DCs (97.7–98.0%), but the gap is
# MAGIC not constant: it comes from a two-week window in early July, when fill rate fell to 87.0% and
# MAGIC on-time delivery to 57.0% (vs. 97.8% and 80.6% the rest of the period). The anomaly endpoint
# MAGIC (`/api/anomalies`, 3σ below a trailing 28-day mean) flags 5 days in that window. Recommendation:
# MAGIC alert on this rule daily rather than reviewing monthly averages, which hide a two-week event.

# COMMAND ----------

# MAGIC %md ## 2. Revenue concentration

# COMMAND ----------

by_class = (fact.join(product, "product_key")
                .groupBy("therapeutic_class")
                .agg(F.sum("shipped_amount").alias("revenue"), F.countDistinct("ndc").alias("skus"))
                .withColumn("revenue_share", F.col("revenue") / F.sum("revenue").over(Window.partitionBy()))
                .orderBy(F.desc("revenue")))
display(by_class)  # noqa: F821

units = fact.groupBy("product_key").agg(F.sum("qty_ordered").alias("units"))
ranked = units.withColumn("rank", F.row_number().over(Window.orderBy(F.desc("units"))))
n = units.count()
top20 = ranked.where(F.col("rank") <= n * 0.2).agg(F.sum("units")).first()[0] / units.agg(F.sum("units")).first()[0]
print(f"Top 20% of products by volume account for {top20:.0%} of units ordered")

# COMMAND ----------

# MAGIC %md
# MAGIC **Finding.** Oncology and Immunology are 25% of SKUs but 84% of revenue (48% and 36%), and most
# MAGIC of those products are cold-chain. By volume, the top 20% of products make up 69% of units.
# MAGIC Recommendation: rank stockout alerts by revenue at risk (days of supply × daily revenue), not by
# MAGIC units, so a low-volume oncology vial outranks a high-volume generic.

# COMMAND ----------

# MAGIC %md ## 3. Service level by customer type

# COMMAND ----------

by_type = (fact.join(customer, "customer_key")
               .groupBy("customer_type")
               .agg(F.avg(F.col("is_on_time").cast("int")).alias("on_time_rate"),
                    (F.sum("qty_shipped") / F.sum("qty_ordered")).alias("unit_fill_rate"),
                    F.count("*").alias("lines"))
               .orderBy("on_time_rate"))
display(by_type)  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC **Finding.** No meaningful difference: on-time delivery is 78.0–78.6% and fill rate 97.3–97.9%
# MAGIC across hospitals, clinics, retail and specialty pharmacies. Service problems in this period are
# MAGIC driven by location (DC02), not by customer segment, so segment-level SLAs would not have caught them.
