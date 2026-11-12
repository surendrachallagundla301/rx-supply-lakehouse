"""Natural-language questions over the gold layer.

Flow: question -> LLM writes one SQL query (JSON) -> sql_guard checks it -> run it -> LLM writes a
two-sentence answer grounded in the returned rows. Token use is capped per call, and if no model is
configured or the model output is unusable, a small set of rule-based intents still answers.
"""

from __future__ import annotations

import json
import logging
import os
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from .repository import Repository
from .sql_guard import UnsafeSQL, check_and_limit

log = logging.getLogger("rx_api.ask")

SCHEMA_DOC = """
Tables (DuckDB SQL):
- agg_daily_kpis(order_date DATE, dc_id, order_lines, orders, units_ordered, units_shipped, revenue,
  line_fill_rate, on_time_rate, unit_fill_rate)   -- one row per day per distribution center
- agg_stockout_risk(snapshot_date, dc_id, ndc, product_name, therapeutic_class, requires_cold_chain,
  on_hand_units, avg_daily_demand, days_of_supply, risk_level 'HIGH'|'MEDIUM'|'LOW')
- fact_order_line(order_id, line_no, order_date_key INT yyyymmdd, product_key, customer_key, dc_key, order_ts,
  promised_date, delivered_date, qty_ordered, qty_shipped, qty_short, unit_price, shipped_amount,
  is_line_filled BOOL, is_on_time BOOL, days_late)
- dim_product(product_key, ndc, product_name, therapeutic_class, manufacturer, unit_cost, is_controlled,
  requires_cold_chain)
- dim_customer(customer_key, customer_id, customer_name, customer_type, state, home_dc_id, onboarded_date)
- dim_distribution_center(dc_key, dc_id, city, state)
- dim_date(date_key, date, day_name, day_of_week, week_start, month, quarter, year, is_weekend)
Rates are fractions between 0 and 1. Revenue is in USD.
""".strip()

SQL_PROMPT = """You write one DuckDB SQL SELECT query that answers a supply-chain analytics question.
{schema}
Rules: use only these tables; never modify data; prefer the agg_ tables when they answer the question;
return at most 50 rows. Reply with JSON only: {{"sql": "...", "chart": "line"|"bar"|"table"}}."""

ANSWER_PROMPT = """Answer the user's question in at most two sentences using only these query results.
Quote the actual numbers; format rates as percentages. If the rows do not answer it, say so.
Question: {question}
Rows (JSON, possibly truncated): {rows}"""

MAX_OUTPUT_TOKENS = 400
MAX_ROWS_IN_PROMPT = 30


@dataclass
class Answer:
    question: str
    sql: str
    rows: list[dict[str, Any]]
    answer: str
    chart: str = "table"
    source: str = "llm"  # "llm" or "rules"
    usage: dict[str, int] = field(default_factory=dict)


LLMCall = Callable[[str, str], tuple[str, dict[str, int]]]  # (system, user) -> (text, usage)


def default_llm() -> LLMCall | None:
    """Azure OpenAI when configured, otherwise OpenAI, otherwise None (rules only)."""
    if os.getenv("AZURE_OPENAI_ENDPOINT"):
        from openai import AzureOpenAI

        client = AzureOpenAI(azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
                             api_key=os.environ["AZURE_OPENAI_API_KEY"],
                             api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-10-21"))
        model = os.environ["AZURE_OPENAI_DEPLOYMENT"]
    elif os.getenv("OPENAI_API_KEY"):
        from openai import OpenAI

        client = OpenAI()
        model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    else:
        return None

    def call(system: str, user: str) -> tuple[str, dict[str, int]]:
        resp = client.chat.completions.create(
            model=model, temperature=0, max_tokens=MAX_OUTPUT_TOKENS,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}])
        u = resp.usage
        return resp.choices[0].message.content or "", {
            "prompt_tokens": u.prompt_tokens, "completion_tokens": u.completion_tokens} if u else {}

    return call


# --- rule-based fallback -------------------------------------------------------------------------

RULES: list[tuple[re.Pattern, str, str]] = [
    (re.compile(r"fill rate.*(by|per|each) (dc|distribution center)|which dc", re.I),
     "SELECT dc_id, SUM(units_shipped) / SUM(units_ordered) AS unit_fill_rate FROM agg_daily_kpis "
     "GROUP BY dc_id ORDER BY unit_fill_rate", "bar"),
    (re.compile(r"on[- ]time", re.I),
     "SELECT dc_id, AVG(on_time_rate) AS on_time_rate FROM agg_daily_kpis GROUP BY dc_id ORDER BY on_time_rate",
     "bar"),
    (re.compile(r"stock ?out|days of supply|at risk|low (stock|inventory)", re.I),
     "SELECT dc_id, ndc, product_name, therapeutic_class, days_of_supply, risk_level FROM agg_stockout_risk "
     "WHERE risk_level <> 'LOW' ORDER BY days_of_supply", "table"),
    (re.compile(r"revenue.*(class|therapeutic)|(class|therapeutic).*revenue", re.I),
     "SELECT p.therapeutic_class, SUM(f.shipped_amount) AS revenue FROM fact_order_line f "
     "JOIN dim_product p USING (product_key) GROUP BY 1 ORDER BY 2 DESC", "bar"),
    (re.compile(r"revenue|sales", re.I),
     "SELECT order_date, SUM(revenue) AS revenue FROM agg_daily_kpis GROUP BY 1 ORDER BY 1", "line"),
]


def _rules(question: str) -> tuple[str, str] | None:
    for pattern, sql, chart in RULES:
        if pattern.search(question):
            return sql, chart
    return None


def _describe(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "No rows matched."
    first = rows[0]
    parts = [f"{k}={v:.2%}" if isinstance(v, float) and 0 <= v <= 1 and "rate" in k else f"{k}={v}"
             for k, v in first.items()]
    return f"{len(rows)} rows. First: " + ", ".join(parts)


# --- main entry ----------------------------------------------------------------------------------

def _parse_json(text: str) -> dict[str, Any]:
    m = re.search(r"\{.*\}", text, re.S)
    if not m:
        raise ValueError("model did not return JSON")
    return json.loads(m.group(0))


def ask(question: str, repo: Repository, llm: LLMCall | None = None) -> Answer:
    question = question.strip()[:500]
    usage: dict[str, int] = {}
    if llm is not None:
        try:
            text, u1 = llm(SQL_PROMPT.format(schema=SCHEMA_DOC), question)
            plan = _parse_json(text)
            sql = check_and_limit(plan["sql"])
            rows = repo.query(sql)
            preview = json.dumps(rows[:MAX_ROWS_IN_PROMPT], default=str)[:6000]
            answer, u2 = llm("You are a concise supply-chain analyst.",
                             ANSWER_PROMPT.format(question=question, rows=preview))
            usage = {k: u1.get(k, 0) + u2.get(k, 0) for k in set(u1) | set(u2)}
            return Answer(question, sql, rows, answer.strip(), plan.get("chart", "table"), "llm", usage)
        except (UnsafeSQL, ValueError, KeyError, json.JSONDecodeError) as e:
            log.warning("LLM path rejected, falling back to rules: %s", e)
        except Exception as e:  # network, auth, rate limit, SQL runtime error
            log.warning("LLM path failed, falling back to rules: %s", e)

    matched = _rules(question)
    if matched is None:
        return Answer(question, "", [], "I can answer questions about fill rate, on-time delivery, "
                      "stockout risk and revenue. Try: 'Which DC has the lowest fill rate?'", "table", "rules")
    sql, chart = matched
    sql = check_and_limit(sql)
    rows = repo.query(sql)
    return Answer(question, sql, rows, _describe(rows), chart, "rules", usage)
