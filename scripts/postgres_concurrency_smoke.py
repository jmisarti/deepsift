"""Stress PostgreSQL pool checkout, rollback, and reuse with concurrent tasks."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import time
import uuid


def run(schema: str, target_url_env: str, tasks: int, workers: int) -> dict:
    target_url = (os.getenv(target_url_env) or "").strip()
    if not target_url:
        raise RuntimeError(f"Set {target_url_env} before running the smoke test.")
    if schema.strip().lower() == "public":
        raise RuntimeError("Refusing to run the concurrency smoke test against public.")

    workers = max(2, int(workers))
    tasks = max(workers, int(tasks))
    marker = f"postgres_concurrency_smoke_{uuid.uuid4().hex}"
    os.environ.update(
        {
            "CRM_DB_BACKEND": "postgres",
            "CRM_POSTGRES_URL": target_url,
            "CRM_POSTGRES_SCHEMA": schema,
            "CRM_POSTGRES_POOL_ENABLED": "true",
            "CRM_POSTGRES_POOL_MIN_SIZE": "2",
            "CRM_POSTGRES_POOL_MAX_SIZE": str(workers),
            "CRM_POSTGRES_POOL_TIMEOUT_SECONDS": "20",
        }
    )

    from database import close_postgres_pools, open_database_connection

    def exercise(task_number: int) -> dict:
        connection = open_database_connection("unused.db")
        try:
            property_count = int(
                connection.execute("SELECT COUNT(*) AS count FROM properties").fetchone()["count"] or 0
            )
            cursor = connection.execute(
                """
                INSERT INTO app_errors (source, route, status_code, error_message, details)
                VALUES (?, ?, ?, ?, ?)
                """,
                (marker, "postgres_concurrency_smoke", 200, f"task-{task_number}", "rollback-only"),
            )
            inserted_id = int(cursor.lastrowid or 0)
            time.sleep(0.02)
            connection.rollback()
            return {
                "task": task_number,
                "property_count": property_count,
                "generated_id": inserted_id,
            }
        finally:
            connection.close()

    started = time.monotonic()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            results = list(executor.map(exercise, range(tasks)))

        verifier = open_database_connection("unused.db")
        try:
            persisted = int(
                verifier.execute(
                    "SELECT COUNT(*) AS count FROM app_errors WHERE source = ?",
                    (marker,),
                ).fetchone()["count"]
                or 0
            )
            verifier.rollback()
        finally:
            verifier.close()
    finally:
        close_postgres_pools()

    if persisted:
        raise RuntimeError(f"{persisted} rollback-only concurrency rows unexpectedly persisted.")
    property_counts = {result["property_count"] for result in results}
    if len(property_counts) != 1:
        raise RuntimeError(f"Concurrent readers returned inconsistent counts: {sorted(property_counts)}")
    if any(result["generated_id"] <= 0 for result in results):
        raise RuntimeError("At least one concurrent generated-id check failed.")
    return {
        "ok": True,
        "schema": schema,
        "tasks": tasks,
        "workers": workers,
        "property_count": property_counts.pop(),
        "unique_generated_ids": len({result["generated_id"] for result in results}),
        "rollback_verified": True,
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema", default="deepsift_stage")
    parser.add_argument("--target-url-env", default="POSTGRES_MIGRATION_URL")
    parser.add_argument("--tasks", type=int, default=32)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()
    try:
        print(
            json.dumps(
                run(args.schema, args.target_url_env, args.tasks, args.workers),
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except Exception as exc:
        print(
            json.dumps(
                {"ok": False, "error_type": type(exc).__name__, "error": str(exc)},
                indent=2,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
