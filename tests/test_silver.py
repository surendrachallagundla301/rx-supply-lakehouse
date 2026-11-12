from datetime import datetime

from rx_lakehouse.silver import dedupe_latest, typed_order_lines, validate_order_lines

LINE_COLS = ["order_id", "line_no", "customer_id", "dc_id", "ndc", "order_ts", "qty_ordered",
             "qty_shipped", "unit_price", "promised_date", "delivered_date", "_ingested_at"]


def _lines(spark, rows):
    return typed_order_lines(spark.createDataFrame(rows, LINE_COLS))


def _row(order_id="SO1", line_no=1, customer="C1", ndc="12345-6789-01", qty=5, shipped=5,
         ts="2026-06-01T10:00:00+00:00", ingested=datetime(2026, 6, 2)):
    return (order_id, line_no, customer, "DC01", ndc, ts, qty, shipped, 9.99,
            "2026-06-02", "2026-06-02", ingested)


def test_dedupe_keeps_latest_copy(spark):
    df = _lines(spark, [_row(shipped=3, ingested=datetime(2026, 6, 2)),
                        _row(shipped=5, ingested=datetime(2026, 6, 3))])
    out = dedupe_latest(df, ["order_id", "line_no"]).collect()
    assert len(out) == 1 and out[0].qty_shipped == 5


def test_validation_quarantines_with_reasons(spark):
    products = spark.createDataFrame([("12345-6789-01",)], ["ndc"])
    customers = spark.createDataFrame([("C1",)], ["customer_id"])
    lines = _lines(spark, [
        _row("SO1"),                                    # valid
        _row("SO2", ndc="123456789"),                   # malformed NDC
        _row("SO3", ndc="99999-0000-00"),               # well-formed but unknown
        _row("SO4", customer="C404"),                   # unknown customer
        _row("SO5", qty=-2, shipped=0),                 # negative quantity
        _row("SO6", qty=2, shipped=3),                  # shipped more than ordered
        _row("SO7", ts="not-a-timestamp"),              # unparseable timestamp
    ])
    valid, quarantined = validate_order_lines(lines, products, customers)
    assert [r.order_id for r in valid.collect()] == ["SO1"]
    reasons = {r.order_id: r.dq_reasons for r in quarantined.collect()}
    assert reasons == {
        "SO2": ["malformed_ndc"],
        "SO3": ["unknown_ndc"],
        "SO4": ["unknown_customer"],
        "SO5": ["non_positive_qty"],
        "SO6": ["shipped_exceeds_ordered"],
        "SO7": ["missing_order_ts"],
    }
