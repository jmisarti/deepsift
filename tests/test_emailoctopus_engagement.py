import csv
import os
import sqlite3
import tempfile
import unittest

import app


class EmailOctopusEngagementTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self.db.executescript(
            """
            CREATE TABLE people (
                id INTEGER PRIMARY KEY,
                first_name TEXT,
                middle_name TEXT,
                last_name TEXT,
                primary_phone TEXT,
                primary_email TEXT
            );
            CREATE TABLE addresses (
                id INTEGER PRIMARY KEY,
                street TEXT,
                city TEXT,
                state TEXT,
                postal_code TEXT
            );
            CREATE TABLE properties (
                id INTEGER PRIMARY KEY,
                property_address_id INTEGER,
                reisift_property_uuid TEXT
            );
            CREATE TABLE touchpoints (
                id INTEGER PRIMARY KEY,
                person_id INTEGER,
                channel_type TEXT,
                value TEXT
            );
            CREATE TABLE emailoctopus_engagement_daily (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                property_id INTEGER NOT NULL,
                person_id INTEGER,
                contact_key TEXT NOT NULL,
                contact_email TEXT,
                contact_id TEXT,
                event_action TEXT NOT NULL,
                event_date TEXT NOT NULL,
                event_count INTEGER NOT NULL DEFAULT 0,
                first_occurred_at TEXT NOT NULL,
                last_occurred_at TEXT NOT NULL,
                campaign_ids_json TEXT NOT NULL DEFAULT '[]',
                last_event_key TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(property_id, contact_key, event_action, event_date)
            );
            CREATE TABLE emailoctopus_engagement_event_keys (
                event_key TEXT PRIMARY KEY,
                engagement_id INTEGER,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE emailoctopus_webhook_events (
                id INTEGER PRIMARY KEY,
                event_key TEXT,
                event_action TEXT,
                contact_id TEXT,
                contact_email TEXT,
                campaign_id TEXT,
                occurred_at TEXT,
                property_id INTEGER,
                person_id INTEGER
            );
            CREATE TABLE app_settings (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TEXT
            );
            CREATE TABLE email_campaign_syncs (
                id INTEGER PRIMARY KEY,
                normalized_email TEXT,
                provider TEXT,
                provider_list_id TEXT,
                provider_contact_id TEXT,
                person_id INTEGER,
                property_id INTEGER,
                touchpoint_id INTEGER,
                updated_at TEXT
            );
            CREATE TABLE reisift_new_records (
                id INTEGER PRIMARY KEY,
                local_property_id INTEGER,
                property_uuid TEXT
            );
            """
        )
        self.db.execute(
            "INSERT INTO people (id, first_name, last_name, primary_email) VALUES (1, 'Jane', 'Owner', 'jane@example.com')"
        )
        self.db.execute(
            "INSERT INTO addresses (id, street, city, state, postal_code) VALUES (1, '1 Main St', 'Newark', 'NJ', '07102')"
        )
        self.db.execute(
            "INSERT INTO properties (id, property_address_id, reisift_property_uuid) VALUES (1, 1, 'property-uuid')"
        )

    def tearDown(self):
        self.db.close()

    def _record(self, event_key, occurred_at, campaign_id="campaign-a"):
        return app.record_emailoctopus_engagement(
            self.db,
            {
                "event_key": event_key,
                "event_action": "clicked",
                "contact_id": "contact-1",
                "contact_email": "jane@example.com",
                "campaign_id": campaign_id,
                "occurred_at": occurred_at,
            },
            {"property_id": 1, "person_id": 1},
        )

    def test_rollup_is_idempotent_and_preserves_daily_click_history(self):
        self.assertTrue(self._record("event-1", "2026-09-15T13:00:00Z").get("engagement_id"))
        self.assertTrue(self._record("event-2", "2026-09-15T15:00:00Z", "campaign-b").get("engagement_id"))
        self.assertEqual(self._record("event-1", "2026-09-15T13:00:00Z").get("skipped"), "already_recorded")
        self.assertTrue(self._record("event-3", "2026-09-16T09:00:00Z").get("engagement_id"))

        rows = self.db.execute(
            "SELECT event_date, event_count, campaign_ids_json FROM emailoctopus_engagement_daily ORDER BY event_date"
        ).fetchall()
        self.assertEqual([(row["event_date"], row["event_count"]) for row in rows], [("2026-09-15", 2), ("2026-09-16", 1)])
        self.assertIn("campaign-b", rows[0]["campaign_ids_json"])

        report = app.get_email_click_engagement_rows(self.db, {"min_clicks": "2"})
        self.assertEqual(len(report), 1)
        self.assertEqual(report[0]["clicks"], 3)
        self.assertEqual(report[0]["email"], "jane@example.com")

    def test_one_time_backfill_marks_completion_after_retained_events(self):
        self.db.execute(
            """
            INSERT INTO emailoctopus_webhook_events (
                id, event_key, event_action, contact_id, contact_email, campaign_id,
                occurred_at, property_id, person_id
            ) VALUES (1, 'historic-click', 'clicked', 'contact-1', 'jane@example.com', 'campaign-a',
                      '2026-09-15T13:00:00Z', 1, 1)
            """
        )

        result = app.ensure_emailoctopus_engagement_history_backfilled(self.db)
        self.assertEqual(result["recorded"], 1)
        self.assertEqual(app.get_setting(self.db, "emailoctopus_engagement_history_backfill_v1"), "complete")
        self.assertEqual(app.ensure_emailoctopus_engagement_history_backfilled(self.db)["skipped"], "already_complete")

    def test_historical_export_matches_contact_and_address_without_double_counting(self):
        self.db.execute(
            """
            INSERT INTO email_campaign_syncs (
                id, normalized_email, provider, provider_contact_id, person_id, property_id, updated_at
            ) VALUES (1, 'jane@example.com', 'emailoctopus', 'contact-1', 1, 1, '2026-09-15 13:00:00')
            """
        )
        handle, path = tempfile.mkstemp(suffix=".csv")
        os.close(handle)
        try:
            with open(path, "w", encoding="utf-8", newline="") as target:
                writer = csv.DictWriter(
                    target,
                    fieldnames=["Identifier", "Email address", "address", "city", "state", "Tags", "Created", "Last changed"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "Identifier": "contact-1",
                        "Email address": "jane@example.com",
                        "address": "1 Main St",
                        "city": "Newark",
                        "state": "NJ",
                        "Tags": "EmailClicked,General",
                        "Created": "2026-09-01T13:00:00+00:00",
                        "Last changed": "2026-09-02T13:00:00+00:00",
                    }
                )
            result = app.import_emailoctopus_clicked_export(self.db, path)
            self.assertEqual(result["imported"], 1)
            self.assertEqual(result["unmatched"], 0)
            self.assertEqual(result["conflicted"], 0)
            self.assertEqual(app.import_emailoctopus_clicked_export(self.db, path)["already_present"], 1)
        finally:
            os.unlink(path)


if __name__ == "__main__":
    unittest.main()
