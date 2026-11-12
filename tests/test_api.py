import json

import pytest
from fastapi.testclient import TestClient

from api.ask import ask
from api.repository import DuckDBRepository
from api.sql_guard import UnsafeSQL, check_and_limit, qualify_for_databricks


@pytest.fixture(scope="module")
def client(lakehouse):
    root, _ = lakehouse
    from api.main import app, repo

    r = DuckDBRepository(str(root / "lakehouse"))
    app.dependency_overrides[repo] = lambda: r
    yield TestClient(app), r
    app.dependency_overrides.clear()


def test_summary_and_daily(client):
    c, _ = client
    s = c.get("/api/kpis/summary").json()
    assert 0.8 < s["unit_fill_rate"] <= 1 and s["orders"] > 0
    daily = c.get("/api/kpis/daily", params={"dc_id": "DC01"}).json()
    assert len(daily) == 14 and all(0 <= d["unit_fill_rate"] <= 1 for d in daily)


def test_request_event_headers(client):
    c, _ = client
    resp = c.get("/api/health", headers={"x-request-id": "abc123"})
    assert resp.headers["x-request-id"] == "abc123" and "app;dur=" in resp.headers["server-timing"]


def test_stockout_and_validation(client):
    c, _ = client
    assert c.get("/api/stockout-risk", params={"level": "BOGUS"}).status_code == 422
    rows = c.get("/api/stockout-risk", params={"level": "LOW", "limit": 5}).json()
    assert len(rows) <= 5 and all(r["risk_level"] == "LOW" for r in rows)


def test_ask_rules_fallback(client):
    c, _ = client
    out = c.post("/api/ask", json={"question": "Which DC has the lowest fill rate?"}).json()
    assert out["source"] == "rules" and len(out["rows"]) == 5


@pytest.mark.parametrize("sql", [
    "DELETE FROM fact_order_line",
    "SELECT * FROM fact_order_line; DROP TABLE dim_product",
    "SELECT * FROM information_schema.tables",
    "SELECT * FROM read_parquet('/etc/passwd')",
    "SELECT * FROM read_csv('secrets.csv')",
])
def test_guard_blocks_unsafe_sql(sql):
    with pytest.raises(UnsafeSQL):
        check_and_limit(sql)


def test_guard_adds_limit_and_allows_ctes():
    out = check_and_limit("WITH x AS (SELECT dc_id FROM agg_daily_kpis) SELECT * FROM x")
    assert "LIMIT 200" in out


def test_databricks_qualification():
    out = qualify_for_databricks("SELECT dc_id FROM agg_daily_kpis k JOIN dim_product p ON 1=1", "rx_prod", "supply")
    assert "rx_prod.supply.gold_agg_daily_kpis" in out and "rx_prod.supply.gold_dim_product" in out


def test_ask_llm_path_and_injection_fallback(client):
    _, r = client
    calls = []

    def good_llm(system, user):
        calls.append(user)
        if len(calls) == 1:
            return json.dumps({"sql": "SELECT dc_id, AVG(on_time_rate) r FROM agg_daily_kpis GROUP BY 1",
                               "chart": "bar"}), {"prompt_tokens": 300, "completion_tokens": 40}
        return "DC03 is best at 81%.", {"prompt_tokens": 200, "completion_tokens": 12}

    out = ask("on-time rate by DC", r, good_llm)
    assert out.source == "llm" and out.usage["prompt_tokens"] == 500 and len(out.rows) == 5

    def malicious_llm(system, user):
        return json.dumps({"sql": "DROP TABLE fact_order_line"}), {}

    out = ask("on-time rate by DC", r, malicious_llm)
    assert out.source == "rules"  # unsafe model SQL is never executed


def test_anomalies_endpoint(client):
    c, _ = client
    assert c.get("/api/anomalies", params={"metric": "revenue"}).status_code == 422
    rows = c.get("/api/anomalies", params={"z": 1.5, "min_drop": 0}).json()
    assert all(r["z_score"] < -1.5 for r in rows)
