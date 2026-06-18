"""Data-quality gate for the sales analytics pipeline.

Triggered after the Glue job succeeds. Runs a small suite of Athena checks
(row count, nulls, freshness) against the curated table and raises if any fail,
so the pipeline stops before bad data reaches the BI layer.

The actual check SQL lives in sql/checks/ — this handler is the runner.
"""
import os
import time

import boto3

athena = boto3.client("athena")

DATABASE = os.environ["DATABASE"]
WORKGROUP = os.environ["WORKGROUP"]


def _run_query(sql: str) -> list[list[str]]:
    """Execute a query in the project workgroup and return result rows."""
    start = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        WorkGroup=WORKGROUP,
    )
    qid = start["QueryExecutionId"]

    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)

    if state != "SUCCEEDED":
        reason = status["QueryExecution"]["Status"].get("StateChangeReason", "")
        raise RuntimeError(f"Query {qid} {state}: {reason}")

    rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    return [[c.get("VarCharValue", "") for c in r["Data"]] for r in rows]


# Each check: (name, sql, predicate on the first data value). Predicate returns
# True when the data is HEALTHY. Wire these to sql/checks/*.sql as the suite grows.
CHECKS = [
    (
        "row_count_positive",
        "SELECT COUNT(*) FROM sales_curated",
        lambda v: int(v) > 0,
    ),
    (
        "no_null_revenue",
        "SELECT COUNT(*) FROM sales_curated WHERE revenue IS NULL",
        lambda v: int(v) == 0,
    ),
    (
        "freshness_within_2_days",
        "SELECT date_diff('day', MAX(sale_date), current_date) FROM sales_curated",
        lambda v: int(v) <= 2,
    ),
]


def handler(event, context):
    failures = []
    for name, sql, ok in CHECKS:
        rows = _run_query(sql)
        value = rows[1][0] if len(rows) > 1 else None  # row[0] is the header
        if value is None or not ok(value):
            failures.append({"check": name, "value": value})

    if failures:
        raise RuntimeError(f"Data-quality checks failed: {failures}")

    return {"status": "passed", "checks": len(CHECKS)}
