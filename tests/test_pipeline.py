"""End-to-end: generate -> bronze -> silver -> gold -> quality gates on a 14-day dataset."""

import pytest

from rx_lakehouse import quality
from rx_lakehouse.config import Settings
from rx_lakehouse.io import read_table


def test_all_quality_gates_pass(lakehouse):
    _, report = lakehouse
    assert report["quality"] and all(r["passed"] for r in report["quality"])


def test_row_counts_reconcile(lakehouse):
    _, report = lakehouse
    silver, gold = report["silver"], report["gold"]
    # Every valid silver line lands in the fact table exactly once.
    assert gold["fact_order_line"] == silver["order_lines"]
    # Bronze minus duplicates equals valid plus quarantined.
    assert report["bronze"]["order_lines"] >= silver["order_lines"] + silver["order_lines_quarantine"]
    assert silver["order_lines_quarantine"] > 0  # the generator injects bad rows on purpose


def test_kpis_match_fact_table(spark, lakehouse):
    s = Settings.load()
    fact = read_table(spark, s, "gold", "fact_order_line")
    kpis = read_table(spark, s, "gold", "agg_daily_kpis")
    assert kpis.agg({"units_shipped": "sum"}).first()[0] == fact.agg({"qty_shipped": "sum"}).first()[0]


def test_quality_gate_fails_on_duplicates(spark, lakehouse):
    s = Settings.load()
    fact = read_table(spark, s, "gold", "fact_order_line")
    result = quality.unique(fact.unionByName(fact.limit(1)), ["order_id", "line_no"], "dup_test")
    assert not result.passed


@pytest.mark.parametrize("level", ["HIGH", "MEDIUM", "LOW"])
def test_risk_levels_are_consistent(spark, lakehouse, level):
    s = Settings.load()
    risk = read_table(spark, s, "gold", "agg_stockout_risk").where(f"risk_level = '{level}'")
    outside = {"HIGH": "days_of_supply >= 3",
               "MEDIUM": "days_of_supply < 3 OR days_of_supply >= 7",
               "LOW": "days_of_supply < 7"}[level]
    assert risk.where(outside).count() == 0
