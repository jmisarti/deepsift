import sys
import types
import unittest

# These matcher tests are database and network independent.
try:
    import requests  # noqa: F401
except ModuleNotFoundError:
    sys.modules["requests"] = types.ModuleType("requests")

from app import mail_campaign_matched_rules


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
