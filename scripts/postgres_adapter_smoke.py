"""Exercise the database adapter against a PostgreSQL staging schema."""

from __future__ import annotations

import argparse
import json
import os
import uuid


def run(schema: str, target_url_env: str) -> dict:
    target_url = (os.getenv(target_url_env) or "").strip()
    if not target_url:
        raise RuntimeError(f"Set {target_url_env} before running the smoke test.")
    os.environ["CRM_DB_BACKEND"] = "postgres"
    os.environ.setdefault("CRM_POSTGRES_POOL_ENABLED", "false")

    from database import install_postgres_compatibility, open_database_connection

    connection = open_database_connection(
        "unused.db",
        postgres_url=target_url,
        postgres_schema=schema,
    )
    source_marker = f"postgres_adapter_smoke_{uuid.uuid4().hex}"
    try:
        install_postgres_compatibility(connection)
        inventory = connection.execute(
            "SELECT COUNT(*) AS count FROM properties WHERE id > ?",
            (0,),
        ).fetchone()
        date_values = connection.execute(
            "SELECT datetime('now', '-30 days') AS cutoff, date(?) AS supplied_date",
            ("2026-08-26 12:34:56",),
        ).fetchone()
        like_value = connection.execute(
            "SELECT CASE WHEN 'EmailClicked' LIKE ? THEN 1 ELSE 0 END AS matched",
            ("%emailclicked%",),
        ).fetchone()
        aggregate = connection.execute(
            """
            SELECT GROUP_CONCAT(value, ', ') AS joined
            FROM (
                SELECT 'Alpha' AS value
                UNION ALL
                SELECT 'Beta' AS value
            ) valueset
            """
        ).fetchone()
        insert_cursor = connection.execute(
            """
            INSERT INTO app_errors (source, route, status_code, error_message, details)
            VALUES (?, ?, ?, ?, ?)
            """,
            (source_marker, "postgres_adapter_smoke", 200, "rollback-only", "rollback-only"),
        )
        inserted_id = int(insert_cursor.lastrowid or 0)
        inserted = connection.execute(
            "SELECT id, source FROM app_errors WHERE id = ?",
            (inserted_id,),
        ).fetchone()
        if inserted_id <= 0 or not inserted or inserted["source"] != source_marker:
            raise RuntimeError("Generated-id compatibility check failed.")
        connection.rollback()
        persisted = connection.execute(
            "SELECT COUNT(*) AS count FROM app_errors WHERE source = ?",
            (source_marker,),
        ).fetchone()
        if int(persisted["count"] or 0) != 0:
            raise RuntimeError("Rollback-only smoke row unexpectedly persisted.")
        connection.rollback()
        return {
            "ok": True,
            "schema": schema,
            "property_count": int(inventory["count"] or 0),
            "datetime_cutoff": date_values["cutoff"],
            "date_value": date_values["supplied_date"],
            "case_insensitive_like": bool(like_value["matched"]),
            "group_concat": aggregate["joined"],
            "generated_id": inserted_id,
            "rollback_verified": True,
            "row_mapping": dict(inserted),
        }
    finally:
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema", default="deepsift_stage")
    parser.add_argument("--target-url-env", default="POSTGRES_MIGRATION_URL")
    args = parser.parse_args()
    try:
        print(json.dumps(run(args.schema, args.target_url_env), indent=2, sort_keys=True))
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
