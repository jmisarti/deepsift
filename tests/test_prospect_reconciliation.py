import sqlite3
import unittest
from unittest import mock

import app


class ProspectReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self.db.executescript(
            """
            CREATE TABLE properties (
                id INTEGER PRIMARY KEY,
                owner_person_id INTEGER,
                resident_person_id INTEGER,
                status TEXT
            );
            CREATE TABLE touchpoints (
                id INTEGER PRIMARY KEY,
                person_id INTEGER,
                channel_type TEXT,
                value TEXT
            );
            CREATE TABLE person_relationships (
                id INTEGER PRIMARY KEY,
                subject_person_id INTEGER,
                related_person_id INTEGER
            );
            CREATE TABLE reisift_new_records (
                id INTEGER PRIMARY KEY,
                property_uuid TEXT UNIQUE,
                segment TEXT,
                status TEXT,
                local_property_id INTEGER,
                automation_eligible INTEGER DEFAULT 1,
                is_active INTEGER DEFAULT 1,
                last_synced_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE email_campaign_syncs (
                id INTEGER PRIMARY KEY,
                normalized_email TEXT NOT NULL UNIQUE,
                touchpoint_id INTEGER,
                person_id INTEGER,
                property_id INTEGER,
                source TEXT,
                provider_list_id TEXT,
                provider_contact_id TEXT,
                sync_status TEXT,
                validation_status TEXT,
                last_error TEXT,
                payload_json TEXT,
                synced_at TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE emailoctopus_sync_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_key TEXT NOT NULL UNIQUE,
                action TEXT NOT NULL,
                normalized_email TEXT NOT NULL,
                touchpoint_id INTEGER,
                person_id INTEGER,
                property_id INTEGER,
                source TEXT,
                provider_list_id TEXT,
                provider_contact_id TEXT,
                contact_status TEXT,
                validation_status TEXT,
                tags_json TEXT,
                fields_json TEXT,
                tags_remove_json TEXT,
                payload_json TEXT,
                queue_status TEXT NOT NULL DEFAULT 'Queued',
                priority INTEGER NOT NULL DEFAULT 10,
                attempts INTEGER NOT NULL DEFAULT 0,
                run_after TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_error TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                processed_at TEXT
            );
            CREATE TABLE website_lead_submissions (
                id INTEGER PRIMARY KEY,
                latest_email TEXT,
                local_property_id INTEGER,
                status TEXT
            );
            """
        )

    def tearDown(self):
        self.db.close()

    def test_bulk_eligibility_preserves_gmail_identity_on_another_active_property(self):
        self.db.executemany(
            "INSERT INTO properties (id, owner_person_id, status) VALUES (?, ?, ?)",
            [(1, 11, "New Record"), (2, 22, "New Record")],
        )
        self.db.executemany(
            "INSERT INTO touchpoints (id, person_id, channel_type, value) VALUES (?, ?, 'Email', ?)",
            [
                (1, 11, "john.smith+first@gmail.com"),
                (2, 22, "johnsmith+second@googlemail.com"),
            ],
        )
        self.db.executemany(
            "INSERT INTO reisift_new_records (id, property_uuid, segment, status, local_property_id, is_active) VALUES (?, ?, 'new_records', 'New Record', ?, 1)",
            [(1, "uuid-1", 1), (2, "uuid-2", 2)],
        )
        self.db.execute(
            "INSERT INTO email_campaign_syncs (id, normalized_email, property_id, sync_status) VALUES (1, 'johnsmith@gmail.com', 1, 'synced')"
        )

        related = app._bulk_email_identity_related_property_ids(self.db, ["johnsmith@gmail.com"])
        context = app._build_emailoctopus_eligibility_context(
            self.db,
            ["johnsmith@gmail.com"],
            related_by_email=related,
        )

        self.assertEqual(related["johnsmith@gmail.com"], {1, 2})
        self.assertTrue(
            app._email_identity_is_eligible_from_context(
                context,
                "john.smith+campaign@gmail.com",
                exclude_property_ids={1},
            )
        )

    def test_bulk_exit_queue_deduplicates_and_skips_email_active_elsewhere(self):
        self.db.executemany(
            "INSERT INTO properties (id, owner_person_id, status) VALUES (?, ?, ?)",
            [(1, 11, "New Record"), (2, 22, "New Record")],
        )
        self.db.executemany(
            "INSERT INTO touchpoints (id, person_id, channel_type, value) VALUES (?, ?, 'Email', ?)",
            [
                (1, 11, "exit-only@example.com"),
                (2, 11, "shared@example.com"),
                (3, 22, "shared@example.com"),
            ],
        )
        self.db.executemany(
            "INSERT INTO reisift_new_records (id, property_uuid, segment, status, local_property_id, is_active) VALUES (?, ?, 'new_records', 'New Record', ?, ?)",
            [(1, "uuid-1", 1, 0), (2, "uuid-2", 2, 1)],
        )
        self.db.executemany(
            """
            INSERT INTO email_campaign_syncs
                (id, normalized_email, touchpoint_id, person_id, property_id, sync_status)
            VALUES (?, ?, ?, ?, 1, 'synced')
            """,
            [(1, "exit-only@example.com", 1, 11), (2, "shared@example.com", 2, 11)],
        )

        settings = {"emailoctopus_api_key": "test-key", "emailoctopus_list_id": "test-list"}
        with mock.patch.object(app, "get_anonymous_email_marketing_settings", return_value=settings), mock.patch.object(
            app,
            "upsert_anonymous_email_campaign_registry",
            return_value=True,
        ):
            result = app.enqueue_emailoctopus_unsubscribes_for_property_exits(self.db, [1])

        queued = self.db.execute(
            "SELECT normalized_email FROM emailoctopus_sync_queue ORDER BY normalized_email"
        ).fetchall()
        self.assertEqual(result["candidate_emails"], 2)
        self.assertEqual(result["queued"], 1)
        self.assertEqual(result["skipped_active_elsewhere"], 1)
        self.assertEqual([row["normalized_email"] for row in queued], ["exit-only@example.com"])

    def test_deactivation_is_chunked_beyond_sqlite_parameter_limit(self):
        rows = []
        for index in range(1, 1002):
            property_uuid = f"00000000-0000-4000-8000-{index:012d}"
            self.db.execute(
                """
                INSERT INTO reisift_new_records
                    (id, property_uuid, segment, status, local_property_id, is_active)
                VALUES (?, ?, 'new_records', 'New Record', ?, 1)
                """,
                (index, property_uuid, index),
            )
            rows.append({"property_uuid": property_uuid, "local_property_id": index})

        result = app._deactivate_reisift_prospect_rows(self.db, rows, "new_records")
        remaining = self.db.execute(
            "SELECT COUNT(*) AS count FROM reisift_new_records WHERE is_active = 1"
        ).fetchone()["count"]

        self.assertEqual(result["deactivated"], 1001)
        self.assertEqual(len(result["local_property_ids"]), 1001)
        self.assertEqual(remaining, 0)

    def test_reisift_search_uses_large_pages_without_truncating_total_results(self):
        requests_seen = []

        class FakeResponse:
            status_code = 200
            reason = "OK"
            url = "https://example.test/property/"

            def __init__(self, payload):
                self._payload = payload

            def raise_for_status(self):
                return None

            def json(self):
                return self._payload

        def fake_post(url, headers, json, timeout):
            requests_seen.append(json)
            offset = int(json["offset"])
            page_size = min(int(json["limit"]), 1500 - offset)
            return FakeResponse(
                {
                    "count": 1500,
                    "results": [{"uuid": f"row-{offset + index}"} for index in range(page_size)],
                }
            )

        with mock.patch.object(app.requests, "post", side_effect=fake_post):
            rows, total = app.reisift_search_property_rows_by_query(
                "token",
                {"must": {"any_property_status": ["New Record"]}},
                max_rows=50000,
            )

        self.assertEqual(total, 1500)
        self.assertEqual(len(rows), 1500)
        self.assertEqual([request["offset"] for request in requests_seen], [0, 1000])
        self.assertEqual([request["limit"] for request in requests_seen], [1000, 1000])
        self.assertEqual(
            requests_seen[0]["query"],
            {"must": {"any_property_status": ["New Record"]}},
        )

    def test_reisift_search_splits_zip_query_above_api_window(self):
        requests_seen = []

        class FakeResponse:
            status_code = 200
            reason = "OK"
            url = "https://example.test/property/"

            def __init__(self, payload):
                self._payload = payload

            def raise_for_status(self):
                return None

            def json(self):
                return self._payload

        def fake_post(url, headers, json, timeout):
            requests_seen.append(json)
            zip_values = json["query"]["must"]["any_zip5"]
            offset = int(json["offset"])
            if len(zip_values) == 2:
                total = 10001
                uuid_group = "9000"
            elif zip_values == ["07001"]:
                total = 5001
                uuid_group = "8001"
            else:
                total = 5000
                uuid_group = "8002"
            page_size = max(0, min(int(json["limit"]), total - offset))
            return FakeResponse(
                {
                    "count": total,
                    "results": [
                        {"uuid": f"00000000-0000-4000-{uuid_group}-{offset + index:012d}"}
                        for index in range(page_size)
                    ],
                }
            )

        query = {
            "must": {
                "any_property_status": ["New Record"],
                "any_zip5": ["07001", "07002"],
            }
        }
        with mock.patch.object(app.requests, "post", side_effect=fake_post):
            rows, total = app.reisift_search_property_rows_by_query(
                "token",
                query,
                max_rows=50000,
            )

        self.assertEqual(total, 10001)
        self.assertEqual(len(rows), 10001)
        self.assertEqual(requests_seen[0]["query"], query)
        self.assertTrue(all(request["offset"] + request["limit"] <= 10000 for request in requests_seen))
        partition_zip_sets = {
            tuple(request["query"]["must"]["any_zip5"])
            for request in requests_seen[1:]
        }
        self.assertEqual(partition_zip_sets, {("07001",), ("07002",)})

    def test_emailoctopus_queue_completion_uses_boolean_case_parameter(self):
        class RecordingDb:
            def __init__(self):
                self.params = None
                self.sql = ""

            def execute(self, sql, params=()):
                self.sql = sql
                self.params = params
                return self

        db = RecordingDb()
        app._mark_emailoctopus_queue_item(db, 42, "Completed", processed=True)

        self.assertIs(db.params[2], True)
        self.assertIn("CAST(CURRENT_TIMESTAMP AS TEXT)", db.sql)

    def test_stale_emailoctopus_inflight_rows_are_requeued(self):
        old_stamp = app.format_db_time(app.datetime.utcnow() - app.timedelta(minutes=30))
        recent_stamp = app.format_db_time(app.datetime.utcnow() - app.timedelta(minutes=2))
        self.db.executemany(
            """
            INSERT INTO emailoctopus_sync_queue
                (queue_key, action, normalized_email, queue_status, updated_at)
            VALUES (?, 'unsubscribe', ?, 'InFlight', ?)
            """,
            [
                ("old", "old@example.com", old_stamp),
                ("recent", "recent@example.com", recent_stamp),
            ],
        )

        recovered = app._requeue_stale_emailoctopus_inflight(self.db, stale_minutes=10)
        statuses = {
            row["normalized_email"]: row["queue_status"]
            for row in self.db.execute(
                "SELECT normalized_email, queue_status FROM emailoctopus_sync_queue"
            ).fetchall()
        }

        self.assertEqual(recovered, 1)
        self.assertEqual(statuses["old@example.com"], "Retry")
        self.assertEqual(statuses["recent@example.com"], "InFlight")

    def test_lis_pendens_related_lists_share_foreclosure_bucketing(self):
        list_names = [
            "Pre-Foreclosures - Lis Pendens",
            "Notice of Lis-Pendens",
            "LisPendens",
            "PreForeclosure",
            "ForeclosureComplaint",
            "Pre-Foreclosures - Notice of Default",
        ]
        sms_rule = {
            "bucket": "Foreclosure",
            "list_contains": "foreclosure,lis pendens,preforeclosure",
        }

        for list_name in list_names:
            with self.subTest(list_name=list_name):
                self.assertEqual(app._emailoctopus_category_from_terms([list_name]), "foreclosure")
                self.assertEqual(app._sms_bucket_from_payload({"lists": [list_name]}), "Foreclosure")
                self.assertTrue(app._sms_rule_source_matches(sms_rule, [list_name]))

    def test_sheriff_sale_keeps_precedence_over_foreclosure(self):
        list_names = ["Pre-Foreclosures - Lis Pendens", "Sheriff Sale"]

        self.assertEqual(app._emailoctopus_category_from_terms(list_names), "sheriff sale")
        self.assertEqual(app._sms_bucket_from_payload({"lists": list_names}), "Sheriff Sale")

    def test_foreclosure_keeps_precedence_over_probate(self):
        list_names = ["Probate", "Pre-Foreclosures - Lis Pendens"]

        self.assertEqual(app._emailoctopus_category_from_terms(list_names), "foreclosure")
        self.assertEqual(app._sms_bucket_from_payload({"lists": list_names}), "Foreclosure")


if __name__ == "__main__":
    unittest.main()
