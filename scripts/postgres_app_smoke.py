"""Exercise DeepSift application queries against a PostgreSQL staging schema.

This runner is intentionally isolated from provider integrations. It disables all
background workers before importing the application and never accepts the public
schema as a target.
"""

from __future__ import annotations

import argparse
import html
import importlib
import json
import os
import re
import sys
import uuid
from pathlib import Path


def configure_environment(schema: str, target_url_env: str) -> None:
    target_url = (os.getenv(target_url_env) or "").strip()
    if not target_url:
        raise RuntimeError(f"Set {target_url_env} before running the smoke test.")
    if schema.strip().lower() == "public":
        raise RuntimeError("Refusing to run the staging smoke test against public.")

    os.environ.update(
        {
            "CRM_DB_BACKEND": "postgres",
            "CRM_POSTGRES_URL": target_url,
            "CRM_POSTGRES_SCHEMA": schema,
            "CRM_POSTGRES_POOL_ENABLED": "false",
            "RUN_BACKGROUND_WORKERS": "false",
            "APP_AUTH_ENABLED": "false",
            "DATABASE_MAINTENANCE_WORKER_ENABLED": "false",
            "REISIFT_NEW_RECORDS_AUTO_REFRESH_ENABLED": "false",
            "REISIFT_PHONE_STATUS_DELTA_ENABLED": "false",
            "EMAIL_VALIDATION_QUEUE_WORKER_ENABLED": "false",
            "EMAILOCTOPUS_SYNC_QUEUE_WORKER_ENABLED": "false",
            "CALL_RECORDING_WORKER_ENABLED": "false",
            "SMS_ANALYSIS_WORKER_ENABLED": "false",
            "AGENT_REFRESH_WORKER_ENABLED": "false",
        }
    )


def load_application(code_dir: Path):
    code_dir = code_dir.resolve()
    if not (code_dir / "app.py").is_file():
        raise RuntimeError(f"app.py is missing from {code_dir}")
    sys.path.insert(0, str(code_dir))
    return importlib.import_module("app")


def response_summary(response) -> dict:
    payload = response.get_json(silent=True)
    if isinstance(payload, list):
        payload_summary = {"kind": "list", "count": len(payload)}
    elif isinstance(payload, dict):
        payload_summary = {
            "kind": "object",
            "keys": sorted(str(key) for key in payload.keys()),
        }
    else:
        payload_summary = {"kind": type(payload).__name__}
    summary = {
        "status_code": int(response.status_code),
        "is_json": bool(response.is_json),
        "payload": payload_summary,
    }
    if response.status_code >= 400:
        body = response.get_data(as_text=True)
        summary["body_prefix"] = body[:500]
        summary["body_suffix"] = body[-1500:]
        error_match = re.search(r'class="error-banner">(.*?)</div>', body, flags=re.DOTALL)
        if error_match:
            summary["error_banner"] = html.unescape(re.sub(r"<[^>]+>", "", error_match.group(1))).strip()
    return summary


def run(
    code_dir: Path,
    schema: str,
    target_url_env: str,
    run_migrations: bool,
    run_startup: bool,
    render_operational_pages: bool,
) -> dict:
    configure_environment(schema, target_url_env)
    application = load_application(code_dir)
    application.ENSURE_DB_READY = True

    result = {
        "ok": False,
        "schema": schema,
        "workers_enabled": bool(application.RUN_BACKGROUND_WORKERS),
        "migration_executed": bool(run_migrations),
        "startup_executed": bool(run_startup),
    }
    if run_startup:
        application.ENSURE_DB_READY = False
        application.ensure_db(force=True)
    else:
        application.ENSURE_DB_READY = True

    db = application.open_sqlite_connection()
    try:
        if run_migrations:
            application.migrate_db(db)
            db.commit()

        inventory = {
            "properties": int(db.execute("SELECT COUNT(*) AS count FROM properties").fetchone()["count"] or 0),
            "people": int(db.execute("SELECT COUNT(*) AS count FROM people").fetchone()["count"] or 0),
            "new_records": int(
                db.execute(
                    "SELECT COUNT(*) AS count FROM reisift_new_records WHERE segment = ?",
                    (application.REISIFT_NEW_RECORDS_SEGMENT,),
                ).fetchone()["count"]
                or 0
            ),
            "deep_prospecting": int(
                db.execute(
                    "SELECT COUNT(*) AS count FROM reisift_new_records WHERE segment = ?",
                    (application.REISIFT_DEEP_PROSPECTING_SEGMENT,),
                ).fetchone()["count"]
                or 0
            ),
        }
        new_record_filters = application.get_new_record_filter_options(
            db,
            segment=application.REISIFT_NEW_RECORDS_SEGMENT,
        )
        deep_prospecting_filters = application.get_new_record_filter_options(
            db,
            segment=application.REISIFT_DEEP_PROSPECTING_SEGMENT,
        )
        clicks = application.get_email_click_engagement_rows(db, {"min_clicks": 1})
        advisor = application.build_agent_advisor_snapshot(db, window_hours=72)

        marker = uuid.uuid4().hex
        phone_marker = f"202555{int(marker[:4], 16) % 10000:04d}"
        email_marker = f"postgres-smoke-{marker}@example.invalid"
        original_context_lookup = application.get_cached_contact_context
        application.get_cached_contact_context = lambda *_args, **_kwargs: None
        try:
            application.upsert_cached_contact_context(
                db,
                phone_marker,
                classification="unknown",
                source="postgres_app_smoke",
                confidence=0.4,
            )
            application.upsert_cached_contact_context(
                db,
                phone_marker,
                classification="owner",
                source="postgres_app_smoke",
                confidence=0.9,
            )
        finally:
            application.get_cached_contact_context = original_context_lookup
        contact_context = original_context_lookup(db, phone_marker)
        application.enqueue_emailoctopus_sync(
            db,
            "subscribe",
            email_marker,
            source="postgres_app_smoke",
            priority=10,
        )
        application.enqueue_emailoctopus_sync(
            db,
            "subscribe",
            email_marker,
            source="postgres_app_smoke",
            priority=3,
        )
        queued_email = db.execute(
            "SELECT priority FROM emailoctopus_sync_queue WHERE normalized_email = ?",
            (email_marker,),
        ).fetchone()
        if not contact_context or float(contact_context["confidence"] or 0) != 0.9:
            raise RuntimeError("External contact upsert did not preserve the highest confidence.")
        if not queued_email or int(queued_email["priority"] or 0) != 3:
            raise RuntimeError("EmailOctopus queue upsert did not preserve the lowest priority value.")

        error_source = f"postgres_app_smoke_{marker}"
        try:
            db.execute("SELECT deliberately_missing_column FROM app_errors LIMIT 1")
        except Exception:
            pass
        application.log_app_error(
            db,
            error_source,
            "rollback-only failed-transaction recovery",
            route="postgres_app_smoke",
            status_code=500,
        )
        recovered_error = db.execute(
            "SELECT COUNT(*) AS count FROM app_errors WHERE source = ?",
            (error_source,),
        ).fetchone()
        if int(recovered_error["count"] or 0) != 1:
            raise RuntimeError("App error logging did not recover the failed PostgreSQL transaction.")
        db.rollback()
        persisted_contact = db.execute(
            "SELECT COUNT(*) AS count FROM external_contact_context WHERE phone_norm = ?",
            (phone_marker,),
        ).fetchone()
        persisted_email = db.execute(
            "SELECT COUNT(*) AS count FROM emailoctopus_sync_queue WHERE normalized_email = ?",
            (email_marker,),
        ).fetchone()
        persisted_error = db.execute(
            "SELECT COUNT(*) AS count FROM app_errors WHERE source = ?",
            (error_source,),
        ).fetchone()
        if (
            int(persisted_contact["count"] or 0)
            or int(persisted_email["count"] or 0)
            or int(persisted_error["count"] or 0)
        ):
            raise RuntimeError("Rollback-only application smoke rows unexpectedly persisted.")
        db.rollback()

        result.update(
            {
                "inventory": inventory,
                "filter_option_counts": {
                    "new_records": {key: len(value) for key, value in new_record_filters.items()},
                    "deep_prospecting": {key: len(value) for key, value in deep_prospecting_filters.items()},
                },
                "email_click_rows": len(clicks),
                "advisor_sections": sorted(advisor.keys()),
                "write_paths": {
                    "external_contact_upsert": True,
                    "emailoctopus_queue_upsert": True,
                    "failed_transaction_error_logging": True,
                    "rollback_verified": True,
                },
            }
        )
    finally:
        db.close()

    with application.app.test_client() as client:
        route_results = {
            "healthz": response_summary(client.get("/healthz")),
            "app_errors": response_summary(client.get("/api/app-errors?limit=2")),
            "provider_alerts": response_summary(client.get("/api/provider-alerts/recent?limit=2")),
        }
        if render_operational_pages:
            operational_paths = {
                "new_records": "/new-records?page=1&per_page=5",
                "deep_prospecting": "/deep-prospecting?page=1&per_page=5",
                "sms_queue": "/sms-queue",
                "sms_schedule": "/sms-queue/schedule",
                "email_clicks": "/email-clicks?min_clicks=1",
                "customer_lookup": "/customer-lookup",
                "dashboard": "/dashboard",
                "follow_ups": "/follow-ups",
                "submitted_leads": "/submitted-leads",
                "anonymous_leads": "/anon",
                "referrals": "/referral",
                "driving_for_dollars": "/d4d",
                "agents": "/agents",
                "buyers": "/buyers",
                "settings": "/settings",
            }
            route_results.update(
                {
                    route_name: response_summary(client.get(path))
                    for route_name, path in operational_paths.items()
                }
            )
    result["routes"] = route_results
    failed_routes = {
        route_name: route_result["status_code"]
        for route_name, route_result in route_results.items()
        if route_result["status_code"] != 200
    }
    if failed_routes:
        error_db = application.open_sqlite_connection()
        try:
            recent_errors = error_db.execute(
                """
                SELECT source, route, status_code, error_message, created_at
                FROM app_errors
                ORDER BY id DESC
                LIMIT 20
                """
            ).fetchall()
            error_db.rollback()
        finally:
            error_db.close()
        result["failed_routes"] = failed_routes
        result["recent_app_errors"] = [dict(row) for row in recent_errors]
    result["ok"] = not failed_routes
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--code-dir", type=Path, default=Path.cwd())
    parser.add_argument("--schema", default="deepsift_stage")
    parser.add_argument("--target-url-env", default="POSTGRES_MIGRATION_URL")
    parser.add_argument(
        "--run-migrations",
        action="store_true",
        help="Run DeepSift's idempotent schema migrations in the staging schema first.",
    )
    parser.add_argument(
        "--run-startup",
        action="store_true",
        help="Run DeepSift's complete ensure_db startup preparation in staging first.",
    )
    parser.add_argument(
        "--render-operational-pages",
        action="store_true",
        help="Render the primary local-data pages after the query checks.",
    )
    args = parser.parse_args()
    if args.run_migrations and args.run_startup:
        parser.error("Choose either --run-migrations or --run-startup, not both.")
    try:
        result = run(
            args.code_dir,
            args.schema,
            args.target_url_env,
            args.run_migrations,
            args.run_startup,
            args.render_operational_pages,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result.get("ok") else 1
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
