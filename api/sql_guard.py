"""Guardrails for model-written SQL. The model's output is treated as untrusted input:
it must parse, be a single read-only SELECT, touch only gold tables, and get a row limit."""

from __future__ import annotations

import sqlglot
from sqlglot import exp

from .repository import GOLD_TABLES

MAX_ROWS = 200
FORBIDDEN = (exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Create, exp.Alter, exp.Merge, exp.Command)


class UnsafeSQL(ValueError):
    pass


def check_and_limit(sql: str, dialect: str = "duckdb") -> str:
    try:
        statements = [s for s in sqlglot.parse(sql, read=dialect) if s is not None]
    except sqlglot.errors.ParseError as e:
        raise UnsafeSQL(f"SQL did not parse: {e}") from e
    if len(statements) != 1:
        raise UnsafeSQL("Exactly one statement is allowed")
    tree = statements[0]
    if not isinstance(tree, (exp.Select, exp.Union)) and not tree.find(exp.Select):
        raise UnsafeSQL("Only SELECT queries are allowed")
    if any(tree.find(node) for node in FORBIDDEN):
        raise UnsafeSQL("Write or DDL statements are not allowed")

    cte_names = {cte.alias_or_name for cte in tree.find_all(exp.CTE)}
    tables = {t.name for t in tree.find_all(exp.Table)} - cte_names
    unknown = tables - set(GOLD_TABLES)
    if unknown:
        raise UnsafeSQL(f"Tables not allowed: {sorted(unknown)}")
    for fn in tree.find_all(exp.Func):
        name = (fn.name if isinstance(fn, exp.Anonymous) else fn.sql_name()).lower()
        if name.startswith(("read_", "glob", "load")) or isinstance(fn, exp.ReadCSV):
            raise UnsafeSQL(f"Function not allowed: {name}")

    limit = tree.args.get("limit")
    if limit is None or int(limit.expression.this) > MAX_ROWS:
        tree = tree.limit(MAX_ROWS)
    return tree.sql(dialect=dialect)


def qualify_for_databricks(sql: str, catalog: str, schema: str) -> str:
    """Rewrite bare gold table names (duckdb views) to Unity Catalog names: catalog.schema.gold_<t>."""
    tree = sqlglot.parse_one(sql, read="duckdb")
    cte_names = {cte.alias_or_name for cte in tree.find_all(exp.CTE)}
    for t in tree.find_all(exp.Table):
        if t.name in GOLD_TABLES and t.name not in cte_names:
            t.set("this", exp.to_identifier(f"gold_{t.name}"))
            t.set("db", exp.to_identifier(schema))
            t.set("catalog", exp.to_identifier(catalog))
    return tree.sql(dialect="databricks")
