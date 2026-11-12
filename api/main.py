"""KPI API over the gold layer. Run: uvicorn api.main:app --reload"""

from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import date
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .ask import ask as run_ask
from .ask import default_llm
from .repository import Repository, get_repository

logging.basicConfig(level=logging.INFO, format="%(message)s")
events = logging.getLogger("rx_api.events")

app = FastAPI(title="Rx Supply Lakehouse API", version="0.1.0")
app.add_middleware(CORSMiddleware, allow_origins=["http://localhost:5173"], allow_methods=["*"],
                   allow_headers=["*"])


@app.middleware("http")
async def request_events(request: Request, call_next):
    """One structured event per request: the raw material for latency and usage dashboards."""
    request_id = request.headers.get("x-request-id", uuid.uuid4().hex[:12])
    t0 = time.perf_counter()
    response = await call_next(request)
    ms = round((time.perf_counter() - t0) * 1000, 1)
    response.headers["x-request-id"] = request_id
    response.headers["server-timing"] = f"app;dur={ms}"
    if request.url.path.startswith("/api"):
        events.info(json.dumps({"event": "api_request", "request_id": request_id, "method": request.method,
                                "route": request.url.path, "status": response.status_code, "duration_ms": ms}))
    return response


def repo() -> Repository:
    return get_repository()


def _dc_filter(dc_id: str | None, params: list) -> str:
    if dc_id:
        params.append(dc_id)
        return "WHERE dc_id = ?"
    return ""


@app.get("/api/health")
def health(r: Repository = Depends(repo)):
    latest = r.query("SELECT MAX(order_date) AS latest FROM agg_daily_kpis")[0]["latest"]
    return {"status": "ok", "data_through": latest}


@app.get("/api/kpis/summary")
def summary(dc_id: str | None = None, r: Repository = Depends(repo)):
    params: list = []
    where = _dc_filter(dc_id, params)
    row = r.query(f"""
        SELECT SUM(orders) AS orders, SUM(order_lines) AS order_lines,
               SUM(units_shipped)::DOUBLE / SUM(units_ordered) AS unit_fill_rate,
               SUM(on_time_rate * order_lines) / SUM(order_lines) AS on_time_rate,
               SUM(revenue)::DOUBLE AS revenue, MIN(order_date) AS start_date, MAX(order_date) AS end_date
        FROM agg_daily_kpis {where}""", params)[0]
    at_risk = r.query(f"SELECT COUNT(*) AS n FROM agg_stockout_risk {where} "
                      f"{'AND' if where else 'WHERE'} risk_level = 'HIGH'", params)[0]["n"]
    return {**row, "high_risk_items": at_risk}


@app.get("/api/kpis/daily")
def daily(dc_id: str | None = None, start: date | None = None, end: date | None = None,
          r: Repository = Depends(repo)):
    clauses, params = [], []
    if dc_id:
        clauses.append("dc_id = ?")
        params.append(dc_id)
    if start:
        clauses.append("order_date >= ?")
        params.append(start)
    if end:
        clauses.append("order_date <= ?")
        params.append(end)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return r.query(f"""
        SELECT order_date, SUM(orders) AS orders, SUM(units_shipped)::DOUBLE / SUM(units_ordered) AS unit_fill_rate,
               SUM(on_time_rate * order_lines) / SUM(order_lines) AS on_time_rate, SUM(revenue)::DOUBLE AS revenue
        FROM agg_daily_kpis {where} GROUP BY order_date ORDER BY order_date""", params)


@app.get("/api/kpis/by-dc")
def by_dc(r: Repository = Depends(repo)):
    return r.query("""
        SELECT k.dc_id, d.city, d.state, SUM(units_shipped)::DOUBLE / SUM(units_ordered) AS unit_fill_rate,
               SUM(on_time_rate * order_lines) / SUM(order_lines) AS on_time_rate, SUM(revenue)::DOUBLE AS revenue
        FROM agg_daily_kpis k JOIN dim_distribution_center d USING (dc_id)
        GROUP BY ALL ORDER BY k.dc_id""")


@app.get("/api/anomalies")
def anomalies(metric: str = Query("unit_fill_rate", pattern="^(unit_fill_rate|on_time_rate)$"),
              z: float = Query(3.0, ge=1.5, le=6), min_drop: float = Query(0.03, ge=0, le=1),
              r: Repository = Depends(repo)):
    """Days where a DC's metric fell more than `z` standard deviations below its trailing 28-day mean.

    Guards against noise: needs 14 days of history, a drop of at least `min_drop` (absolute), and
    the day must have a normal order volume (weekend days with few lines swing too much)."""
    return r.query(f"""
        WITH s AS (
          SELECT order_date, dc_id, order_lines, {metric} AS value,
                 AVG({metric}) OVER w AS trailing_mean, STDDEV_SAMP({metric}) OVER w AS trailing_std,
                 COUNT(*) OVER w AS history_days, AVG(order_lines) OVER w AS avg_lines
          FROM agg_daily_kpis
          WINDOW w AS (PARTITION BY dc_id ORDER BY order_date ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING))
        SELECT order_date, dc_id, value, trailing_mean, (value - trailing_mean) / trailing_std AS z_score
        FROM s
        WHERE history_days >= 14 AND trailing_std > 0 AND order_lines >= 0.6 * avg_lines
          AND trailing_mean - value >= ? AND (value - trailing_mean) / trailing_std < -?
        ORDER BY order_date, dc_id""", [min_drop, z])


@app.get("/api/stockout-risk")
def stockout_risk(level: str = Query("HIGH", pattern="^(HIGH|MEDIUM|LOW)$"), dc_id: str | None = None,
                  limit: int = Query(50, ge=1, le=500), r: Repository = Depends(repo)):
    params: list = [level]
    extra = ""
    if dc_id:
        extra = "AND dc_id = ?"
        params.append(dc_id)
    params.append(limit)
    return r.query(f"""SELECT * FROM agg_stockout_risk WHERE risk_level = ? {extra}
                       ORDER BY days_of_supply LIMIT ?""", params)


class AskRequest(BaseModel):
    question: str = Field(min_length=3, max_length=500)


@app.post("/api/ask")
def ask(body: AskRequest, r: Repository = Depends(repo)):
    try:
        result = run_ask(body.question, r, default_llm())
    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    events.info(json.dumps({"event": "ask", "source": result.source, "rows": len(result.rows), **result.usage}))
    return result.__dict__


# Serve the built React app from the same container in production.
_web = Path(__file__).resolve().parent.parent / "web" / "dist"
if _web.exists():
    app.mount("/assets", StaticFiles(directory=_web / "assets"), name="assets")

    @app.get("/{path:path}", include_in_schema=False)
    def spa(path: str):
        return FileResponse(_web / "index.html")
