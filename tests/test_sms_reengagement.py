import datetime
import sqlite3
import sys
import types
import unittest
from unittest import mock

# The lightweight Codex test runtime omits requests; these database-only tests
# never call an HTTP path, so a minimal import shim keeps them hermetic.
try:
    import requests  # noqa: F401
except ModuleNotFoundError:
    sys.modules["requests"] = types.ModuleType("requests")

import app


class SmsReengagementTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self.db.executescript(
            """
            CREATE TABLE app_settings (key TEXT PRIMARY KEY, value TEXT, updated_at TEXT);
            CREATE TABLE addresses (
                id INTEGER PRIMARY KEY,
                street TEXT,
                city TEXT,
                state TEXT,
                postal_code TEXT
            );
            CREATE TABLE people (
                id INTEGER PRIMARY KEY,
                first_name TEXT,
                last_name TEXT
            );
            CREATE TABLE properties (
                id INTEGER PRIMARY KEY,
                status TEXT,
                owner_person_id INTEGER,
                resident_person_id INTEGER,
                reisift_property_uuid TEXT,
                property_address_id INTEGER
            );
            CREATE TABLE touchpoints (
                id INTEGER PRIMARY KEY,
                person_id INTEGER
            );
            CREATE TABLE sms_automation_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_key TEXT UNIQUE,
                property_id INTEGER,
                person_id INTEGER,
                touchpoint_id INTEGER,
                phone_number TEXT,
                from_number TEXT,
                contact_role TEXT,
                bucket TEXT,
                rule_key TEXT,
                sequence_name TEXT,
                step_order INTEGER,
                message_body TEXT,
                rendered_variables_json TEXT,
                source_info_json TEXT,
                status TEXT,
                scheduled_for TEXT,
                sent_at TEXT,
                communication_id INTEGER,
                created_at TEXT
            );
            CREATE TABLE communications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                property_id INTEGER,
                channel TEXT,
                direction TEXT,
                sent_at TEXT,
                created_at TEXT
            );
            """
        )

    def tearDown(self):
        self.db.close()

    def test_verified_phone_status_only_accepts_explicit_confirmation(self):
        self.assertTrue(app.is_verified_phone_status_text("Correct"))
        self.assertTrue(app.is_verified_phone_status_text(" verified "))
        self.assertFalse(app.is_verified_phone_status_text("Unknown"))
        self.assertFalse(app.is_verified_phone_status_text("Dead"))
        self.assertFalse(app.is_verified_phone_status_text(""))

    def _insert_original_sequence(self, rendered_variables_json=None):
        rendered_variables_json = rendered_variables_json or '{"first_name":"Jane","property_address":"1 Main St"}'
        values = []
        for step in range(1, 5):
            suffix = "" if step == 1 else f":fu:{step}"
            values.append(
                (
                    f"nr1:1:10:20:2015550100:general:owner{suffix}",
                    1,
                    10,
                    20,
                    "2015550100",
                    "9733975960",
                    "owner",
                    "General New Record",
                    "general:owner",
                    "General New Record - Owner",
                    step,
                    "Initial message",
                    rendered_variables_json,
                    "{}",
                    "Sent",
                    f"2026-09-0{step} 14:00:00",
                )
            )
        self.db.executemany(
            """
            INSERT INTO sms_automation_queue
                (queue_key, property_id, person_id, touchpoint_id, phone_number, from_number, contact_role,
                 bucket, rule_key, sequence_name, step_order, message_body, rendered_variables_json,
                 source_info_json, status, sent_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            values,
        )

    def test_reengagement_creates_once_after_completed_unanswered_sequence(self):
        self._insert_original_sequence()
        with mock.patch.object(app, "sms_automation_property_suppression_reason", return_value=""), mock.patch.object(
            app, "sms_automation_touchpoint_suppression_reason", return_value=""
        ), mock.patch.object(app, "stale_reisift_new_record_touchpoint_reason", return_value=""):
            result = app.generate_sms_automation_reengagement_drafts(
                self.db, now_utc=datetime.datetime(2026, 9, 22, 14, 0, 0)
            )
            self.assertEqual(result["created"], 1)
            row = self.db.execute(
                "SELECT * FROM sms_automation_queue WHERE queue_key LIKE '%:re14'"
            ).fetchone()
            self.assertEqual(row["status"], "Draft")
            self.assertEqual(row["step_order"], app.SMS_AUTOMATION_REENGAGEMENT_INITIAL_STEP_ORDER)
            self.assertIn("checking back", row["message_body"].lower())

            duplicate = app.generate_sms_automation_reengagement_drafts(
                self.db, now_utc=datetime.datetime(2026, 9, 22, 14, 0, 0)
            )
            self.assertEqual(duplicate["created"], 0)
            self.assertEqual(duplicate["skip_reasons"].get("already_created"), 1)

    def test_reengagement_rebuilds_tokens_when_historical_queue_json_is_empty(self):
        self.db.execute(
            "INSERT INTO addresses (id, street, city, state, postal_code) VALUES (1, '1 Main St', 'Newark', 'NJ', '07102')"
        )
        self.db.execute(
            "INSERT INTO people (id, first_name, last_name) VALUES (10, 'jane', 'doe')"
        )
        self.db.execute(
            "INSERT INTO properties (id, status, owner_person_id, property_address_id) VALUES (1, 'New Record', 10, 1)"
        )
        self.db.execute("INSERT INTO touchpoints (id, person_id) VALUES (20, 10)")
        self._insert_original_sequence(rendered_variables_json="{}")
        original = self.db.execute(
            "SELECT * FROM sms_automation_queue WHERE step_order = 1 LIMIT 1"
        ).fetchone()
        self.assertIn("Hi Jane,", app.sms_automation_initial_message_for_approval(self.db, original))
        self.assertIn("1 Main St", app._sms_automation_followup_message(self.db, original, 2))
        with mock.patch.object(app, "sms_automation_property_suppression_reason", return_value=""), mock.patch.object(
            app, "sms_automation_touchpoint_suppression_reason", return_value=""
        ), mock.patch.object(app, "stale_reisift_new_record_touchpoint_reason", return_value=""):
            result = app.generate_sms_automation_reengagement_drafts(
                self.db, now_utc=datetime.datetime(2026, 9, 22, 14, 0, 0)
            )
        self.assertEqual(result["created"], 1)
        row = self.db.execute(
            "SELECT message_body, rendered_variables_json FROM sms_automation_queue WHERE queue_key LIKE '%:re14'"
        ).fetchone()
        self.assertIn("Hi Jane,", row["message_body"])
        self.assertIn("1 Main St", row["message_body"])
        self.assertIn('"first_name": "Jane"', row["rendered_variables_json"])

    def test_reengagement_followups_use_three_business_day_cadence(self):
        self._insert_original_sequence()
        with mock.patch.object(app, "sms_automation_property_suppression_reason", return_value=""), mock.patch.object(
            app, "sms_automation_touchpoint_suppression_reason", return_value=""
        ), mock.patch.object(app, "stale_reisift_new_record_touchpoint_reason", return_value=""), mock.patch.object(
            app, "sms_automation_inbound_reply_suppression_reason", return_value=""
        ):
            app.generate_sms_automation_reengagement_drafts(
                self.db, now_utc=datetime.datetime(2026, 9, 22, 14, 0, 0)
            )
            day14 = self.db.execute(
                "SELECT * FROM sms_automation_queue WHERE queue_key LIKE '%:re14'"
            ).fetchone()
            self.db.execute(
                "UPDATE sms_automation_queue SET status = 'Sent', sent_at = ? WHERE id = ?",
                ("2026-09-10 14:00:00", day14["id"]),
            )
            sent_row = self.db.execute("SELECT * FROM sms_automation_queue WHERE id = ?", (day14["id"],)).fetchone()
            result = app.ensure_sms_automation_followups_for_sent_row(
                self.db, sent_row, sent_at="2026-09-10 14:00:00"
            )
            self.assertEqual(result["created"], 3)
            rows = self.db.execute(
                "SELECT step_order, scheduled_for FROM sms_automation_queue WHERE queue_key LIKE '%:re14:fu:%' ORDER BY step_order"
            ).fetchall()
            self.assertEqual([row["step_order"] for row in rows], [11, 12, 13])
            dates = [
                app.parse_db_time(row["scheduled_for"]).replace(tzinfo=datetime.timezone.utc).astimezone(app.EST_TZ).date()
                for row in rows
            ]
            self.assertEqual(dates, [datetime.date(2026, 9, 15), datetime.date(2026, 9, 18), datetime.date(2026, 9, 23)])


if __name__ == "__main__":
    unittest.main()
