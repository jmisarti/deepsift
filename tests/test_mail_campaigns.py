import sys
import types
import sqlite3
import unittest

# These matcher tests are database and network independent.
try:
    import requests  # noqa: F401
except ModuleNotFoundError:
    sys.modules["requests"] = types.ModuleType("requests")

from app import _normalize_openletterconnect_status, get_mail_issue_rows, mail_campaign_matched_rules


def cached_row(*, county, lists, out_of_state=0, vacant=0, llc=0):
    return {
        "county": county,
        "property_lists_json": lists,
        "owner_out_of_state": out_of_state,
        "is_vacant": vacant,
        "is_llc_owner": llc,
    }


class MailCampaignAudienceTests(unittest.TestCase):
    def test_matches_only_the_approved_essex_judgment_lien_rule(self):
        rows = mail_campaign_matched_rules(
            cached_row(county="Essex", lists='["Judgment Lien"]')
        )
        self.assertEqual([row["key"] for row in rows], ["essex_judgment_lien"])

    def test_foreclosure_list_supplies_lis_pendens_signal(self):
        rows = mail_campaign_matched_rules(
            cached_row(county="Union County", lists='["Foreclosure", "Senior"]')
        )
        self.assertEqual([row["key"] for row in rows], ["union_lis_pendens_senior"])

    def test_matches_essex_free_clear_vacant_from_cached_vacancy(self):
        rows = mail_campaign_matched_rules(
            cached_row(county="Essex", lists='["Free & Clear"]', vacant=1)
        )
        self.assertEqual([row["key"] for row in rows], ["essex_free_clear_vacant"])

    def test_rejects_other_counties_and_llc_owners(self):
        wrong_county = mail_campaign_matched_rules(
            cached_row(county="Bergen", lists='["Judgment Lien"]')
        )
        llc_owner = mail_campaign_matched_rules(
            cached_row(county="Essex", lists='["Judgment Lien"]', llc=1)
        )
        self.assertEqual(wrong_county, [])
        self.assertEqual(llc_owner, [])

    def test_nixie_is_a_bad_address_mail_status(self):
        self.assertEqual(_normalize_openletterconnect_status("Nixie"), "Bad Address")
        self.assertEqual(_normalize_openletterconnect_status("Return to sender"), "Bad Address")

    def test_mail_issues_combines_provider_return_and_unmailable_address(self):
        db = sqlite3.connect(":memory:")
        db.row_factory = sqlite3.Row
        db.executescript(
            """
            CREATE TABLE addresses (id INTEGER PRIMARY KEY, street TEXT, city TEXT, state TEXT, postal_code TEXT, is_verified_deliverable INTEGER);
            CREATE TABLE people (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT);
            CREATE TABLE properties (id INTEGER PRIMARY KEY, status TEXT, owner_person_id INTEGER, property_address_id INTEGER);
            CREATE TABLE person_addresses (id INTEGER PRIMARY KEY, person_id INTEGER, address_id INTEGER, label TEXT, is_default_mailing INTEGER);
            CREATE TABLE mail_orders (
                id INTEGER PRIMARY KEY, property_id INTEGER, person_id INTEGER, status TEXT, external_order_id TEXT,
                status_updated_at TEXT, bad_address_at TEXT, created_at TEXT
            );
            INSERT INTO addresses VALUES (1, '7 Wells Pl', 'Middlesex', 'NJ', '08846', 1);
            INSERT INTO addresses VALUES (2, '10 Bad Rd', 'Middlesex', 'NJ', '08846', 0);
            INSERT INTO people VALUES (1, 'Rose', 'Lee');
            INSERT INTO properties VALUES (1, 'New Record', 1, 1);
            INSERT INTO person_addresses VALUES (1, 1, 2, 'Golden Tax Mailing Address', 1);
            INSERT INTO mail_orders VALUES (1, 1, 1, 'Nixie', 'order-1', '2026-09-28 12:00:00', NULL, '2026-09-28 11:00:00');
            """
        )
        try:
            rows = get_mail_issue_rows(db)
        finally:
            db.close()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["issue_label"], "Provider return + Marked not mailable")
        self.assertEqual(rows[0]["mailing_address"], "10 Bad Rd, Middlesex, NJ 08846")
