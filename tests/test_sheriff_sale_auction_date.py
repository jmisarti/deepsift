import sqlite3
from unittest import mock

import app


def test_normalize_reisift_auction_date_accepts_reisift_formats():
    assert app.normalize_reisift_auction_date("2026-10-16") == "2026-10-16"
    assert app.normalize_reisift_auction_date("10/16/2026") == "2026-10-16"
    assert app.normalize_reisift_auction_date("2026-10-16T09:30:00Z") == "2026-10-16"
    assert app.normalize_reisift_auction_date(None) == ""


def test_reisift_auction_date_overrides_legacy_message_date():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute(
        """
        CREATE TABLE property_source_info (
            property_id INTEGER PRIMARY KEY,
            property_uuid TEXT,
            source_info_bucket TEXT,
            sheriff_sale_date TEXT,
            source_info_raw TEXT,
            source_info_json TEXT,
            last_checked_at TEXT
        )
        """
    )
    property_uuid = "123e4567-e89b-12d3-a456-426614174000"
    with (
        mock.patch.object(app, "fetch_reisift_property_payload", return_value={"auction_date": "2026-10-16"}),
        mock.patch.object(app, "fetch_reisift_property_messages", return_value=[{"text": "Source Info; Sale: 10/01/2026"}]),
    ):
        result = app.refresh_property_source_info_from_reisift(db, "token", 1, property_uuid)

    assert result["sheriff_sale_date"] == "2026-10-16"
    assert result["auction_date_source"] == "reisift.auction_date"
    assert result["source_info_raw"] == "ReiSift auction_date: 2026-10-16"
    db.close()
