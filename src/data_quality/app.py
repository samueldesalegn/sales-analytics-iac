"""Data-quality gate for the sales analytics pipeline.

Generic runner: discovers every ``checks/*.sql`` shipped with this function,
executes each against Athena, and fails (raises) if any check reports
``passed = false``. Adding a new check is a new .sql file — no code change.

Each check query must return a single row with two columns:
    passed  BOOLEAN   -- true when the data is healthy
    detail  VARCHAR   -- a count or value to show when it isn't

Triggered after the Glue job succeeds, so bad data stops here before the
BI layer ever sees it.
"""
import os
import time
from pathlib import Path

import boto3

athena = boto3.client("athena")

DATABASE = os.environ["DATABASE"]
WORKGROUP = os.environ["WORKGROUP"]
CHECKS_DIR = Path(__file__).parent / "checks"


def _run_query(sql: str) -> list[dict]:
    """Run a query in the project workgroup and return result rows as dicts."""
    qid = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        WorkGroup=WORKGROUP,
    )["QueryExecutionId"]

    while True:
        execution = athena.get_query_execution(QueryExecutionId=qid)
        state = execution["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)

    if state != "SUCCEEDED":
        reason = execution["QueryExecution"]["Status"].get("StateChangeReason", "")
        raise RuntimeError(f"Athena query {qid} {state}: {reason}")

    rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    if len(rows) < 2:  # row[0] is the header, row[1+] are data
        return []
    header = [c.get("VarCharValue", "") for c in rows[0]["Data"]]
    return [
        dict(zip(header, [c.get("VarCharValue", "") for c in r["Data"]]))
        for r in rows[1:]
    ]


def handler(event, context):
    check_files = sorted(CHECKS_DIR.glob("*.sql"))
    if not check_files:
        raise RuntimeError(f"No check files found in {CHECKS_DIR}")

    results, failures = [], []
    for path in check_files:
        name = path.stem
        rows = _run_query(path.read_text())
        row = rows[0] if rows else {}
        passed = str(row.get("passed", "")).lower() == "true"
        detail = row.get("detail", "")
        results.append({"check": name, "passed": passed, "detail": detail})
        if not passed:
            failures.append({"check": name, "detail": detail})

    if failures:
        raise RuntimeError(f"Data-quality checks failed: {failures}")

    return {"status": "passed", "checks": len(results), "results": results}
