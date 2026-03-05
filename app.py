
import csv
import io
import json
import os
import random
import re
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from html import unescape
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests
from flask import Flask, g, jsonify, redirect, render_template, request, url_for

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "crm.db"
SCHEMA_PATH = BASE_DIR / "schema.sql"

app = Flask(__name__)
app.config["SECRET_KEY"] = "dev"

SMRTPHONE_SEND_URL = "https://phone.smrt.studio/sms/send"
SMRTPHONE_FROM_NUMBER = "19088679098"
SKIPSHERPA_BASE_URL = "https://skipsherpa.com"
SKIPSHERPA_API_KEY = os.getenv("SKIPSHERPA_API_KEY", "66f5fc68-2c42-413c-8e5e-5eb124ef1c16")
try:
    EST_TZ = ZoneInfo("America/New_York")
except ZoneInfoNotFoundError:
    EST_TZ = timezone(timedelta(hours=-5))
BULK_SMS_WORKER_STARTED = False

RELATION_TYPE_MAP = {
    "son": "Child",
    "sons": "Child",
    "daughter": "Child",
    "daughters": "Child",
    "child": "Child",
    "children": "Child",
    "brother": "Sibling",
    "brothers": "Sibling",
    "sister": "Sibling",
    "sisters": "Sibling",
    "sibling": "Sibling",
    "siblings": "Sibling",
    "mother": "Parent",
    "father": "Parent",
    "parents": "Parent",
    "niece": "Niece/Nephew",
    "nieces": "Niece/Nephew",
    "nephew": "Niece/Nephew",
    "nephews": "Niece/Nephew",
    "grandson": "Grandchild",
    "grandsons": "Grandchild",
    "granddaughter": "Grandchild",
    "granddaughters": "Grandchild",
    "grandchildren": "Grandchild",
    "relative": "Relative",
    "relatives": "Relative",
    "friend": "Associate",
    "friends": "Associate",
}


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(_error):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def ensure_column(db, table_name, column_name, definition):
    cols = [r["name"] for r in db.execute(f"PRAGMA table_info({table_name})").fetchall()]
    if column_name not in cols:
        db.execute(f"ALTER TABLE {table_name} ADD COLUMN {definition}")


def migrate_db(db):
    with SCHEMA_PATH.open("r", encoding="utf-8") as schema_file:
        db.executescript(schema_file.read())

    ensure_column(db, "people", "outreach_status", "outreach_status TEXT NOT NULL DEFAULT 'No Contact'")
    ensure_column(db, "communications", "from_number", "from_number TEXT")
    ensure_column(db, "communications", "to_number", "to_number TEXT")
    ensure_column(db, "communications", "is_read", "is_read INTEGER NOT NULL DEFAULT 1")
    ensure_column(db, "people", "age", "age INTEGER")
    ensure_column(db, "people", "deceased", "deceased INTEGER NOT NULL DEFAULT 0")
    ensure_column(db, "people", "birth_year", "birth_year TEXT")
    ensure_column(db, "people", "deceased_date", "deceased_date TEXT")
    ensure_column(db, "people", "bankruptcy", "bankruptcy INTEGER NOT NULL DEFAULT 0")
    ensure_column(db, "people", "employer", "employer TEXT")


def init_db():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    migrate_db(db)

    has_properties = db.execute("SELECT COUNT(*) AS c FROM properties").fetchone()["c"]
    if not has_properties:
        seed_database(db)

    db.commit()
    db.close()


def ensure_db():
    if not DB_PATH.exists():
        init_db()
        return

    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    migrate_db(db)
    db.commit()
    db.close()


def create_address(db, street, city, state, postal_code):
    cur = db.execute(
        """
        INSERT INTO addresses (street, city, state, postal_code)
        VALUES (?, ?, ?, ?)
        """,
        (street.strip(), city.strip(), state.strip(), postal_code.strip()),
    )
    return cur.lastrowid


def create_person(db, first_name, last_name, phone="", email="", notes=""):
    cur = db.execute(
        """
        INSERT INTO people (first_name, last_name, primary_phone, primary_email, notes)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            first_name.strip() or "Unknown",
            last_name.strip() or "Person",
            phone.strip(),
            email.strip(),
            notes.strip(),
        ),
    )
    return cur.lastrowid


def person_name(person_row):
    return f"{person_row['first_name']} {person_row['last_name']}".strip()


def seed_database(db):
    owner_id = create_person(
        db,
        first_name="Jill",
        last_name="Smith",
        phone="(973) 555-0102",
        email="jill.smith@examplemail.com",
        notes="Primary owner target. Out-of-state mailer.",
    )
    resident_id = create_person(
        db,
        first_name="Jill",
        last_name="Smith",
        phone="(408) 555-0109",
        email="jill.s@samplemail.com",
        notes="Mailing/resident profile.",
    )

    property_addr_id = create_address(db, "123 Main Street", "West Orange", "NJ", "07052")
    create_address(db, "13 Tiger Dr", "San Jose", "CA", "95134")

    db.execute(
        """
        INSERT INTO properties (property_address_id, owner_person_id, resident_person_id, status, notes)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            property_addr_id,
            owner_id,
            resident_id,
            "Active Prospect",
            "Initial seeded record for deep-prospecting workflow.",
        ),
    )

    relative_id = create_person(
        db,
        first_name="Maya",
        last_name="Smith",
        phone="(510) 555-0131",
        email="maya.smith@examplemail.com",
        notes="Possible sister.",
    )

    db.execute(
        """
        INSERT INTO person_relationships (subject_person_id, related_person_id, relationship_type, note)
        VALUES (?, ?, ?, ?)
        """,
        (owner_id, relative_id, "Relative", "Found through public records."),
    )

    db.execute(
        """
        INSERT INTO touchpoints (person_id, channel_type, channel_label, value, status, note, last_attempted)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            owner_id,
            "Phone",
            "Primary mobile",
            "(973) 555-0102",
            "No Answer",
            "Left voicemail twice.",
            datetime.utcnow().isoformat(timespec="seconds"),
        ),
    )


def build_property_network(db, property_row):
    base_ids = [
        x
        for x in [property_row["owner_person_id"], property_row["resident_person_id"]]
        if x is not None
    ]
    network_ids = set(base_ids)

    if base_ids:
        placeholders = ",".join(["?"] * len(base_ids))
        relationship_rows = db.execute(
            f"""
            SELECT * FROM person_relationships
            WHERE subject_person_id IN ({placeholders}) OR related_person_id IN ({placeholders})
            """,
            tuple(base_ids + base_ids),
        ).fetchall()
        for rel in relationship_rows:
            network_ids.add(rel["subject_person_id"])
            network_ids.add(rel["related_person_id"])
    else:
        relationship_rows = []

    return sorted(network_ids), relationship_rows


def web_lookup(query, limit=5):
    if not query:
        return []

    url = "https://api.duckduckgo.com/"
    params = {
        "q": query,
        "format": "json",
        "no_html": 1,
        "skip_disambig": 1,
    }
    response = requests.get(url, params=params, timeout=8)
    response.raise_for_status()
    payload = response.json()

    results = []
    if payload.get("AbstractText"):
        results.append(
            {
                "title": payload.get("Heading") or query,
                "snippet": payload["AbstractText"],
                "url": payload.get("AbstractURL") or "",
            }
        )

    for topic in payload.get("RelatedTopics", []):
        if len(results) >= limit:
            break
        if isinstance(topic, dict) and topic.get("Text"):
            results.append(
                {
                    "title": topic.get("FirstURL", "Result").split("/")[-1],
                    "snippet": topic["Text"],
                    "url": topic.get("FirstURL") or "",
                }
            )
        elif isinstance(topic, dict) and topic.get("Topics"):
            for nested in topic["Topics"]:
                if len(results) >= limit:
                    break
                if nested.get("Text"):
                    results.append(
                        {
                            "title": nested.get("FirstURL", "Result").split("/")[-1],
                            "snippet": nested["Text"],
                            "url": nested.get("FirstURL") or "",
                        }
                    )

    return results[:limit]


def normalize_phone(value):
    digits = re.sub(r"\D", "", value or "")
    if len(digits) >= 10:
        return digits[-10:]
    return digits


def normalize_social_profile_url(platform, handle, url):
    raw_url = (url or "").strip()
    if raw_url:
        if raw_url.startswith(("http://", "https://")):
            return raw_url
        return f"https://{raw_url}"

    p = (platform or "").strip().lower()
    h = (handle or "").strip()
    if not h:
        return ""
    h = h.lstrip("@")

    if "linkedin" in p:
        if h.startswith("in/") or h.startswith("company/"):
            return f"https://www.linkedin.com/{h}"
        return f"https://www.linkedin.com/in/{h}"
    if p in {"x", "twitter"}:
        return f"https://x.com/{h}"
    if "facebook" in p:
        return f"https://www.facebook.com/{h}"
    if "instagram" in p:
        return f"https://www.instagram.com/{h}"
    if "tiktok" in p:
        return f"https://www.tiktok.com/@{h}"
    if "youtube" in p:
        return f"https://www.youtube.com/@{h}"
    return ""


def extract_sms_id_from_payload(payload):
    if isinstance(payload, dict):
        for k in ("smsId", "sms_id", "id", "messageId", "message_id"):
            val = payload.get(k)
            if isinstance(val, str) and val.strip():
                return val.strip()
        for v in payload.values():
            nested = extract_sms_id_from_payload(v)
            if nested:
                return nested
    elif isinstance(payload, list):
        for item in payload:
            nested = extract_sms_id_from_payload(item)
            if nested:
                return nested
    return ""


def send_smrtphone_sms(to_number, message_body):
    api_key = os.getenv("SMRTPHONE_API_KEY", "").strip()
    if not api_key:
        raise ValueError("SMRTPHONE_API_KEY is not set")

    headers = {
        "X-Auth-smrtPhone": api_key,
    }
    payload = {
        "from": SMRTPHONE_FROM_NUMBER,
        "to": to_number,
        "message": message_body,
    }
    response = requests.post(SMRTPHONE_SEND_URL, data=payload, headers=headers, timeout=20)

    raw = {}
    try:
        raw = response.json()
    except ValueError:
        raw = {"raw_text": response.text}

    if not response.ok:
        raise ValueError(f"SmrtPhone send failed ({response.status_code}): {raw}")

    sms_id = extract_sms_id_from_payload(raw)
    if not sms_id and isinstance(raw, dict):
        raw_text = str(raw.get("raw_text") or "")
        m = re.search(r"(SM[a-fA-F0-9]{16,}|smsId[:=]\s*([A-Za-z0-9\-_]+))", raw_text)
        if m:
            sms_id = (m.group(2) or m.group(1) or "").replace("smsId=", "").replace("smsId:", "").strip()
    status = "Queued"
    if isinstance(raw, dict) and raw.get("status"):
        status = str(raw["status"]).strip().title()
    elif sms_id:
        status = "Sent"

    return {"sms_id": sms_id, "status": status, "raw": raw}


def call_skipsherpa_person_lookup(first_name, last_name, street, city="", state="", zipcode=""):
    if not SKIPSHERPA_API_KEY:
        raise ValueError("SKIPSHERPA_API_KEY is not set")

    payload = {
        "person_lookups": [
            {
                "first_name": (first_name or "").strip() or None,
                "last_name": (last_name or "").strip() or None,
                "mailing_addresses": [
                    {
                        "street": (street or "").strip(),
                        "city": (city or "").strip() or None,
                        "state": (state or "").strip() or None,
                        "zipcode": (zipcode or "").strip() or None,
                    }
                ],
            }
        ]
    }
    response = requests.put(
        f"{SKIPSHERPA_BASE_URL}/api/beta6/person",
        headers={"API-Key": SKIPSHERPA_API_KEY, "Content-Type": "application/json"},
        json=payload,
        timeout=45,
    )
    try:
        data = response.json()
    except ValueError:
        data = {"raw_text": response.text}
    if not response.ok:
        if response.status_code == 404 and isinstance(data, dict) and "person_results" in data:
            return {"request": payload, "response": data}
        raise ValueError(f"SkipSherpa person lookup failed ({response.status_code}): {data}")
    return {"request": payload, "response": data}


def touchpoint_exists(db, person_id, channel_type, value):
    if not value:
        return False
    rows = db.execute(
        "SELECT value FROM touchpoints WHERE person_id = ? AND lower(channel_type) = lower(?)",
        (person_id, channel_type),
    ).fetchall()
    if channel_type.lower() == "phone":
        target = normalize_phone(value)
        return any(normalize_phone(r["value"]) == target for r in rows)
    target = value.strip().lower()
    return any((r["value"] or "").strip().lower() == target for r in rows)


def import_skipsherpa_person_result(db, property_id, person_id, lookup_response):
    results = lookup_response.get("person_results") or []
    person_results = []
    if isinstance(results, list):
        for r in results:
            person_results.extend(r.get("persons") or [])

    phones_added = 0
    emails_added = 0
    addresses_added = 0
    relatives_added = 0

    for p in person_results:
        age_val = p.get("age")
        deceased_val = bool(p.get("deceased")) if p.get("deceased") is not None else False
        dob_my = (p.get("date_of_birth_month_year") or "").strip()
        birth_year = ""
        if dob_my and "-" in dob_my:
            birth_year = dob_my.split("-")[-1].strip()
        bankruptcy_val = bool(p.get("bankruptcy")) if p.get("bankruptcy") is not None else False
        employers = [e.get("name", "").strip() for e in (p.get("employers") or []) if (e.get("name") or "").strip()]
        employer_text = ", ".join(sorted(set(employers))) if employers else ""
        deceased_date = (
            (p.get("date_of_death") or p.get("deceased_date") or "").strip()
            if isinstance(p, dict)
            else ""
        )
        db.execute(
            """
            UPDATE people
            SET age = COALESCE(?, age),
                deceased = ?,
                birth_year = COALESCE(NULLIF(?, ''), birth_year),
                deceased_date = COALESCE(NULLIF(?, ''), deceased_date),
                bankruptcy = ?,
                employer = COALESCE(NULLIF(?, ''), employer)
            WHERE id = ?
            """,
            (
                age_val,
                1 if deceased_val else 0,
                birth_year,
                deceased_date,
                1 if bankruptcy_val else 0,
                employer_text,
                person_id,
            ),
        )

        for ph in p.get("phone_numbers") or []:
            number = (ph.get("e164_format") or ph.get("local_format") or "").strip()
            if not number or touchpoint_exists(db, person_id, "Phone", number):
                continue
            ptype = (ph.get("type") or "").strip().lower()
            label = {"mobile": "Mobile", "landline": "Landline", "voip": "VoIP"}.get(ptype, "Unknown")
            status = "Unknown"
            note_meta = {
                "source": "SkipSherpa",
                "carrier": ph.get("carrier"),
                "last_seen": ph.get("last_seen"),
                "dnc_statuses": ph.get("dnc_statuses") or [],
                "raw_phone": ph,
            }
            db.execute(
                """
                INSERT INTO touchpoints (person_id, channel_type, channel_label, value, status, note, last_attempted)
                VALUES (?, 'Phone', ?, ?, ?, ?, ?)
                """,
                (person_id, label, number, status, json.dumps(note_meta), ""),
            )
            phones_added += 1

        for em in p.get("emails") or []:
            email = (em.get("email_address") or "").strip()
            if not email or touchpoint_exists(db, person_id, "Email", email):
                continue
            db.execute(
                """
                INSERT INTO touchpoints (person_id, channel_type, channel_label, value, status, note, last_attempted)
                VALUES (?, 'Email', 'Email', ?, 'Unknown', ?, ?)
                """,
                (person_id, email, "Imported from SkipSherpa person lookup", ""),
            )
            emails_added += 1

        for a in p.get("addresses") or []:
            us = a.get("us_address") or {}
            street = (us.get("street") or "").strip()
            city = (us.get("city") or "").strip()
            state = (us.get("state") or "").strip()
            zipcode = (us.get("zipcode") or "").strip()
            if not (street and city and state and zipcode):
                continue
            exists = db.execute(
                """
                SELECT pa.id
                FROM person_addresses pa
                JOIN addresses a ON a.id = pa.address_id
                WHERE pa.person_id = ? AND lower(a.street)=lower(?) AND lower(a.city)=lower(?) AND lower(a.state)=lower(?) AND a.postal_code=?
                LIMIT 1
                """,
                (person_id, street, city, state, zipcode),
            ).fetchone()
            if exists:
                continue
            addr_id = create_address(db, street, city, state, zipcode)
            db.execute(
                "INSERT INTO person_addresses (person_id, address_id, label) VALUES (?, ?, ?)",
                (person_id, addr_id, "SkipSherpa Address"),
            )
            addresses_added += 1

        for rel in p.get("relatives") or []:
            full_name = (rel.get("name") or "").strip()
            if not full_name:
                continue
            rel_type = (rel.get("relation_type") or "Relative").strip() or "Relative"
            related_id = find_or_create_person_by_full_name(
                db, full_name, notes=f"Imported from SkipSherpa for property {property_id}"
            )
            exists_rel = db.execute(
                """
                SELECT id FROM person_relationships
                WHERE subject_person_id = ? AND related_person_id = ? AND relationship_type = ?
                LIMIT 1
                """,
                (person_id, related_id, rel_type),
            ).fetchone()
            if exists_rel:
                continue
            db.execute(
                """
                INSERT INTO person_relationships (subject_person_id, related_person_id, relationship_type, note)
                VALUES (?, ?, ?, ?)
                """,
                (
                    person_id,
                    related_id,
                    rel_type,
                    f"Imported from SkipSherpa person lookup | payload={json.dumps(rel)}",
                ),
            )
            relatives_added += 1

    db.execute(
        """
        INSERT INTO activity_log (property_id, person_id, activity_type, outcome, note)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            property_id,
            person_id,
            "Skip Trace",
            f"SkipSherpa matched {len(person_results)} person records",
            f"Phones+{phones_added}, Emails+{emails_added}, Addresses+{addresses_added}, Relatives+{relatives_added}",
        ),
    )
    add_person_note(
        db,
        person_id,
        "SkipSherpa",
        "Skip trace completed.",
        {
            "summary": {
                "matched_people": len(person_results),
                "phones_added": phones_added,
                "emails_added": emails_added,
                "addresses_added": addresses_added,
                "relatives_added": relatives_added,
            },
            "response": lookup_response,
        },
    )
    return {
        "matched_people": len(person_results),
        "phones_added": phones_added,
        "emails_added": emails_added,
        "addresses_added": addresses_added,
        "relatives_added": relatives_added,
    }


def find_person_id_by_phone(db, phone_value):
    target = normalize_phone(phone_value)
    if not target:
        return None
    rows = db.execute(
        "SELECT person_id, value FROM touchpoints WHERE lower(channel_type) = 'phone'"
    ).fetchall()
    for row in rows:
        if normalize_phone(row["value"]) == target:
            return row["person_id"]
    return None


def find_property_id_for_person(db, person_id):
    if not person_id:
        return None
    row = db.execute(
        """
        SELECT id FROM properties
        WHERE owner_person_id = ? OR resident_person_id = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (person_id, person_id),
    ).fetchone()
    if row:
        return row["id"]
    rel = db.execute(
        """
        SELECT p.id
        FROM person_relationships r
        JOIN properties p ON p.owner_person_id = r.subject_person_id
        WHERE r.related_person_id = ?
        ORDER BY p.created_at DESC
        LIMIT 1
        """,
        (person_id,),
    ).fetchone()
    return rel["id"] if rel else None


def find_person_id_by_recent_outbound_to_number(db, property_id, inbound_from_number):
    normalized = normalize_phone(inbound_from_number)
    if not normalized:
        return None
    rows = db.execute(
        """
        SELECT person_id, to_number
        FROM communications
        WHERE property_id = ?
          AND upper(channel) = 'SMS'
          AND lower(direction) = 'outbound'
          AND person_id IS NOT NULL
        ORDER BY sent_at DESC, id DESC
        LIMIT 200
        """,
        (property_id,),
    ).fetchall()
    for r in rows:
        if normalize_phone(r["to_number"]) == normalized:
            return r["person_id"]
    return None


def parse_db_time(value):
    if not value:
        return None
    text = str(value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(text[:19], fmt)
        except ValueError:
            continue
    return None


def bulk_window_next_open(now_est):
    start = now_est.replace(hour=10, minute=0, second=0, microsecond=0)
    end = now_est.replace(hour=16, minute=30, second=0, microsecond=0)
    if now_est < start:
        return start
    if now_est > end:
        nxt = now_est + timedelta(days=1)
        return nxt.replace(hour=10, minute=0, second=0, microsecond=0)
    return now_est


def bulk_window_adjust(dt_est):
    start = dt_est.replace(hour=10, minute=0, second=0, microsecond=0)
    end = dt_est.replace(hour=16, minute=30, second=0, microsecond=0)
    if dt_est < start:
        return start
    if dt_est > end:
        nxt = dt_est + timedelta(days=1)
        return nxt.replace(hour=10, minute=0, second=0, microsecond=0)
    return dt_est


def format_db_time(dt_obj):
    return dt_obj.strftime("%Y-%m-%d %H:%M:%S")


def infer_touchpoint_update_from_status(status_text):
    s = (status_text or "").strip().lower()
    if not s:
        return {}

    update = {}
    if "landline" in s:
        update["channel_label"] = "Landline"
    if any(x in s for x in ["undeliverable", "not deliver", "no route", "failed", "failure"]):
        update["status"] = "Undeliverable"
    if any(x in s for x in ["no longer in service", "not in service", "disconnected", "deactivated"]):
        update["status"] = "Not in service"
    if "invalid" in s:
        update["status"] = "Incorrect"
    return update


def apply_touchpoint_status_inference(db, phone_number, status_text):
    normalized = normalize_phone(phone_number)
    if not normalized:
        return
    update = infer_touchpoint_update_from_status(status_text)
    if not update:
        return

    rows = db.execute(
        "SELECT id, value, channel_label, status FROM touchpoints WHERE lower(channel_type) = 'phone'"
    ).fetchall()
    for row in rows:
        if normalize_phone(row["value"]) != normalized:
            continue
        channel_label = update.get("channel_label", row["channel_label"])
        status = update.get("status", row["status"])
        db.execute(
            "UPDATE touchpoints SET channel_label = ?, status = ? WHERE id = ?",
            (channel_label, status, row["id"]),
        )


def find_recent_inbound_match(db, property_id, person_id, from_number, to_number, message, seconds=120):
    threshold = datetime.utcnow() - timedelta(seconds=seconds)
    return db.execute(
        """
        SELECT id, external_id
        FROM communications
        WHERE property_id = ?
          AND COALESCE(person_id, -1) = COALESCE(?, -1)
          AND upper(channel) = 'SMS'
          AND lower(direction) = 'inbound'
          AND COALESCE(from_number, '') = COALESCE(?, '')
          AND COALESCE(to_number, '') = COALESCE(?, '')
          AND COALESCE(body, '') = COALESCE(?, '')
          AND sent_at >= ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (property_id, person_id, from_number, to_number, message, format_db_time(threshold)),
    ).fetchone()


def log_smrtphone_webhook_event(
    db,
    event_type,
    payload,
    processing_status="received",
    sms_id="",
    from_number="",
    to_number="",
    communication_id=None,
    error_text="",
):
    try:
        db.execute(
            """
            INSERT INTO smrtphone_webhook_events
            (event_type, processing_status, sms_id, from_number, to_number, communication_id, error_text, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                (event_type or "").strip(),
                (processing_status or "").strip(),
                (sms_id or "").strip(),
                (from_number or "").strip(),
                (to_number or "").strip(),
                communication_id,
                (error_text or "").strip(),
                json.dumps(payload or {}),
            ),
        )
    except Exception:
        pass


def add_person_note(db, person_id, source, note_body, payload=None):
    if not person_id:
        return
    db.execute(
        """
        INSERT INTO person_notes (person_id, source, note_body, payload_json)
        VALUES (?, ?, ?, ?)
        """,
        (
            person_id,
            (source or "").strip() or "System",
            (note_body or "").strip(),
            json.dumps(payload, indent=2) if payload is not None else None,
        ),
    )


def update_person_outreach_status_for_sms(db, person_id, event_type):
    if not person_id:
        return
    row = db.execute("SELECT outreach_status FROM people WHERE id = ?", (person_id,)).fetchone()
    if not row:
        return

    current = (row["outreach_status"] or "").strip()
    if event_type == "outbound_success":
        if current in {"", "No Contact", "Outreach Attempted"}:
            db.execute(
                "UPDATE people SET outreach_status = ? WHERE id = ?",
                ("Contact No Response", person_id),
            )
    elif event_type == "inbound_received":
        if current != "Follow Up":
            db.execute(
                "UPDATE people SET outreach_status = ? WHERE id = ?",
                ("Follow Up", person_id),
            )


def start_bulk_sms_worker():
    global BULK_SMS_WORKER_STARTED
    if BULK_SMS_WORKER_STARTED:
        return
    BULK_SMS_WORKER_STARTED = True

    def worker():
        while True:
            try:
                run_bulk_sms_tick()
            except Exception:
                pass
            time.sleep(20)

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()


def run_bulk_sms_tick():
    ensure_db()
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    try:
        active_jobs = db.execute(
            "SELECT * FROM bulk_sms_jobs WHERE status = 'Active' ORDER BY id ASC"
        ).fetchall()
        now_utc = datetime.utcnow()

        for job in active_jobs:
            item = db.execute(
                """
                SELECT * FROM bulk_sms_job_items
                WHERE job_id = ? AND status IN ('Pending', 'Retry')
                  AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                ORDER BY id ASC
                LIMIT 1
                """,
                (job["id"], format_db_time(now_utc)),
            ).fetchone()

            if not item:
                remaining = db.execute(
                    "SELECT COUNT(*) AS c FROM bulk_sms_job_items WHERE job_id = ? AND status IN ('Pending', 'Retry')",
                    (job["id"],),
                ).fetchone()["c"]
                if remaining == 0:
                    db.execute(
                        "UPDATE bulk_sms_jobs SET status = 'Completed', finished_at = ? WHERE id = ?",
                        (format_db_time(now_utc), job["id"]),
                    )
                continue

            now_est = datetime.now(EST_TZ)
            open_est = bulk_window_next_open(now_est)
            if open_est > now_est:
                item_next = open_est.astimezone(timezone.utc).replace(tzinfo=None)
                db.execute(
                    "UPDATE bulk_sms_job_items SET next_attempt_at = ? WHERE id = ?",
                    (format_db_time(item_next), item["id"]),
                )
                continue

            to_number = item["to_number"]
            message = job["message"]
            try:
                sent = send_smrtphone_sms(to_number, message)
                status = sent.get("status") or "Sent"
                cur = db.execute(
                    """
                    INSERT INTO communications (property_id, person_id, channel, direction, from_number, to_number, body, status, is_read, sent_at, external_id)
                    VALUES (?, ?, 'SMS', 'Outbound', ?, ?, ?, ?, 1, ?, ?)
                    """,
                    (
                        item["property_id"],
                        item["person_id"],
                        SMRTPHONE_FROM_NUMBER,
                        to_number,
                        message,
                        status,
                        format_db_time(now_utc),
                        sent.get("sms_id"),
                    ),
                )
                update_person_outreach_status_for_sms(db, item["person_id"], "outbound_success")
                db.execute(
                    """
                    UPDATE bulk_sms_job_items
                    SET status = 'Sent', attempts = attempts + 1, sent_comm_id = ?, external_id = ?, last_error = ''
                    WHERE id = ?
                    """,
                    (cur.lastrowid, sent.get("sms_id"), item["id"]),
                )
            except Exception as exc:
                db.execute(
                    """
                    UPDATE bulk_sms_job_items
                    SET status = 'Retry', attempts = attempts + 1, last_error = ?
                    WHERE id = ?
                    """,
                    (str(exc), item["id"]),
                )

            min_i = max(2, int(job["min_interval_minutes"] or 2))
            max_i = max(min_i, int(job["max_interval_minutes"] or 5))
            gap = random.randint(min_i, max_i)
            next_est = bulk_window_adjust(datetime.now(EST_TZ) + timedelta(minutes=gap))
            next_utc = next_est.astimezone(timezone.utc).replace(tzinfo=None)
            db.execute(
                """
                UPDATE bulk_sms_job_items
                SET next_attempt_at = ?
                WHERE job_id = ? AND status IN ('Pending', 'Retry') AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                """,
                (format_db_time(next_utc), job["id"], format_db_time(now_utc)),
            )

        db.commit()
    finally:
        db.close()

def fetch_obituary_text(url):
    headers = {"User-Agent": "Mozilla/5.0 (compatible; DeepProspectCRM/1.0)"}
    response = requests.get(url, timeout=12, headers=headers)
    response.raise_for_status()

    page = response.text
    page = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", page)
    page = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", page)
    text = re.sub(r"(?is)<[^>]+>", " ", page)
    text = unescape(text)
    text = re.sub(r"\s+", " ", text).strip()

    anchor = re.search(
        r"(Obituary|It is with deep sorrow|passed away|survived by).{0,4000}",
        text,
        re.IGNORECASE,
    )
    if anchor:
        return anchor.group(0)
    return text[:4000]


def clean_name(candidate):
    cleaned = re.sub(r"\b(the late|late|beloved|dear|cherished)\b", "", candidate, flags=re.I)
    cleaned = re.sub(r"[^A-Za-z'\-\s]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,;")
    words = cleaned.split()
    if len(words) < 2:
        return ""
    if words[0].lower() in {"her", "his", "their", "and"}:
        return ""
    return " ".join(words[:4])


def split_candidate_names(block):
    compact = re.sub(r"\s+", " ", block).strip(" .;")
    compact = compact.replace(";", ",")
    chunks = re.split(r",|\band\b", compact, flags=re.I)
    names = []
    for chunk in chunks:
        name = clean_name(chunk)
        if name:
            names.append(name)
    return list(dict.fromkeys(names))


def parse_obituary_relationships(text):
    parsed = {
        "subject_status": "Unknown",
        "surviving_relatives": [],
        "preceded_relatives": [],
        "notes": [],
    }

    lower = text.lower()
    if re.search(r"\b(widow|widower)\b", lower) or re.search(
        r"preceded in death by (his|her) (husband|wife)", lower
    ):
        parsed["subject_status"] = "Widowed"
    elif re.search(r"survived by (his|her) (husband|wife|spouse)", lower):
        parsed["subject_status"] = "Married"

    spouse_match = re.search(
        r"survived by (?:his|her) (husband|wife|spouse)\s+([A-Z][A-Za-z'\-]+(?:\s+[A-Z][A-Za-z'\-]+){0,3})",
        text,
    )
    if spouse_match:
        parsed["surviving_relatives"].append(
            {
                "name": spouse_match.group(1).strip(),
                "relationship_type": "Spouse",
                "relative_status": "Living",
                "marital_status": "Married",
                "spouse_name": "",
            }
        )

    sentence_parts = re.split(r"(?<=[\.!?])\s+", text)
    kin_terms = "|".join(sorted(RELATION_TYPE_MAP.keys(), key=len, reverse=True))

    for sentence in sentence_parts:
        s_lower = sentence.lower()
        is_surviving = any(
            token in s_lower
            for token in ["survived by", "leaves behind", "also survived by", "is survived by"]
        )
        is_preceded = any(token in s_lower for token in ["preceded in death by", "predeceased by"])

        if not is_surviving and not is_preceded:
            continue

        for match in re.finditer(
            rf"(?:his|her|their)\s+({kin_terms})\s*(?:,|:)?\s*([^.;]+)",
            sentence,
            re.IGNORECASE,
        ):
            raw_type = match.group(1).lower()
            rel_type = RELATION_TYPE_MAP.get(raw_type, "Relative")
            raw_names = match.group(2)

            married_pairs = re.findall(
                r"([A-Z][A-Za-z'\-]+(?:\s+[A-Z][A-Za-z'\-]+){0,2})\s*\(([^)]+)\)",
                raw_names,
            )
            consumed = set()

            for primary, spouse in married_pairs:
                p_name = clean_name(primary)
                s_name = clean_name(spouse)
                if p_name:
                    parsed_target = parsed["surviving_relatives"] if is_surviving else parsed["preceded_relatives"]
                    parsed_target.append(
                        {
                            "name": p_name,
                            "relationship_type": rel_type,
                            "relative_status": "Living" if is_surviving else "Deceased",
                            "marital_status": "Married" if s_name else "Unknown",
                            "spouse_name": s_name,
                        }
                    )
                    consumed.add(p_name)
                if s_name and is_surviving:
                    parsed["surviving_relatives"].append(
                        {
                            "name": s_name,
                            "relationship_type": "In-Law",
                            "relative_status": "Living",
                            "marital_status": "Married",
                            "spouse_name": p_name,
                        }
                    )
                    consumed.add(s_name)

            for name in split_candidate_names(raw_names):
                if name in consumed:
                    continue
                parsed_target = parsed["surviving_relatives"] if is_surviving else parsed["preceded_relatives"]
                parsed_target.append(
                    {
                        "name": name,
                        "relationship_type": rel_type,
                        "relative_status": "Living" if is_surviving else "Deceased",
                        "marital_status": "Unknown",
                        "spouse_name": "",
                    }
                )

    def dedupe(items):
        seen = set()
        out = []
        for item in items:
            key = (item["name"].lower(), item["relationship_type"], item["relative_status"])
            if item["name"] and key not in seen:
                out.append(item)
                seen.add(key)
        return out

    parsed["surviving_relatives"] = dedupe(parsed["surviving_relatives"])
    parsed["preceded_relatives"] = dedupe(parsed["preceded_relatives"])
    return parsed


def find_or_create_person_by_full_name(db, full_name, notes=""):
    parts = [p for p in full_name.split() if p]
    if not parts:
        return None

    first_name = parts[0]
    last_name = " ".join(parts[1:]) if len(parts) > 1 else "Unknown"

    row = db.execute(
        """
        SELECT id FROM people
        WHERE lower(first_name) = lower(?) AND lower(last_name) = lower(?)
        LIMIT 1
        """,
        (first_name, last_name),
    ).fetchone()
    if row:
        if notes:
            db.execute(
                "UPDATE people SET notes = trim(coalesce(notes,'') || ' ' || ?) WHERE id = ?",
                (notes, row["id"]),
            )
        return row["id"]

    return create_person(db, first_name, last_name, notes=notes)



def extract_obituary_context(raw_text, name, width=260):
    if not raw_text:
        return ""
    if not name:
        return re.sub(r"\s+", " ", raw_text[:width]).strip()

    idx = raw_text.lower().find(name.lower())
    if idx < 0:
        return re.sub(r"\s+", " ", raw_text[:width]).strip()

    start = max(0, idx - (width // 2))
    end = min(len(raw_text), idx + len(name) + (width // 2))
    snippet = raw_text[start:end]
    return re.sub(r"\s+", " ", snippet).strip()
def import_obituary_into_property(db, property_id, subject_person_id, source_url):
    raw_text = fetch_obituary_text(source_url)
    extracted = parse_obituary_relationships(raw_text)

    db.execute(
        """
        INSERT INTO obituary_sources (property_id, subject_person_id, source_url, raw_text, extracted_json)
        VALUES (?, ?, ?, ?, ?)
        """,
        (property_id, subject_person_id, source_url, raw_text, json.dumps(extracted)),
    )

    inserted_relationships = 0
    created_people = 0

    for item in extracted["surviving_relatives"] + extracted["preceded_relatives"]:
        count_before = db.execute("SELECT COUNT(*) AS c FROM people").fetchone()["c"]
        relative_id = find_or_create_person_by_full_name(
            db,
            item["name"],
            notes=f"Imported from obituary: {source_url}",
        )
        count_after = db.execute("SELECT COUNT(*) AS c FROM people").fetchone()["c"]
        if count_after > count_before:
            created_people += 1

        context = extract_obituary_context(raw_text, item['name'])
        note = f"Obituary import ({item['relative_status']}, marital: {item['marital_status']}) | Source: {context}"
        db.execute(
            """
            INSERT INTO person_relationships (subject_person_id, related_person_id, relationship_type, note)
            VALUES (?, ?, ?, ?)
            """,
            (subject_person_id, relative_id, item["relationship_type"], note),
        )
        inserted_relationships += 1

        spouse_name = item.get("spouse_name", "").strip()
        if spouse_name:
            spouse_id = find_or_create_person_by_full_name(
                db,
                spouse_name,
                notes=f"Imported spouse from obituary: {source_url}",
            )
            db.execute(
                """
                INSERT INTO person_relationships (subject_person_id, related_person_id, relationship_type, note)
                VALUES (?, ?, ?, ?)
                """,
                (relative_id, spouse_id, "Spouse", "Inferred from obituary structure"),
            )

    db.execute(
        """
        INSERT INTO activity_log (property_id, person_id, activity_type, outcome, note)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            property_id,
            subject_person_id,
            "Obituary Import",
            f"Imported {inserted_relationships} relationships",
            f"URL: {source_url} | Subject status: {extracted['subject_status']}",
        ),
    )
    add_person_note(
        db,
        subject_person_id,
        "Obituary Import",
        f"Imported obituary from {source_url}",
        {"source_url": source_url, "summary": extracted},
    )

    return {
        "subject_status": extracted["subject_status"],
        "created_people": created_people,
        "inserted_relationships": inserted_relationships,
        "surviving_relatives": extracted["surviving_relatives"],
        "preceded_relatives": extracted["preceded_relatives"],
    }


def call_openai_obituary_agent(obituary_text, subject_name=""):
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise ValueError("OPENAI_API_KEY is not set")

    model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
    schema = {
        "name": "obituary_relationships",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "subject_name": {"type": "string"},
                "subject_status": {
                    "type": "string",
                    "enum": ["Married", "Widowed", "Single", "Unknown"],
                },
                "relations": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "name": {"type": "string"},
                            "relationship": {"type": "string"},
                            "status": {"type": "string", "enum": ["Living", "Deceased", "Unknown"]},
                            "marital_status": {
                                "type": "string",
                                "enum": ["Married", "Widowed", "Single", "Unknown"],
                            },
                            "spouse_name": {"type": "string"},
                            "note": {"type": "string"},
                        },
                        "required": ["name", "relationship", "status", "marital_status", "spouse_name", "note"],
                    },
                },
            },
            "required": ["subject_name", "subject_status", "relations"],
        },
        "strict": True,
    }

    prompt = (
        "Extract every named individual in this obituary and map each person to the subject. "
        "Return only schema-valid JSON. Treat phrases like 'preceded in death by' as Deceased. "
        "If a relative is listed with spouse wording, map in-law or spouse explicitly. "
        "Do not merge distinct names."
    )

    payload = {
        "model": model,
        "input": [
            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": "You are a high-precision obituary relationship extractor for CRM enrichment.",
                    }
                ],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": f"Subject: {subject_name or 'Unknown'}\n\n{prompt}\n\nObituary text:\n{obituary_text}",
                    }
                ],
            },
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": schema["name"],
                "schema": schema["schema"],
                "strict": True,
            }
        },
    }

    response = requests.post(
        "https://api.openai.com/v1/responses",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()

    output_text = data.get("output_text", "").strip()
    if output_text:
        return json.loads(output_text)

    for item in data.get("output", []):
        for part in item.get("content", []):
            if part.get("type") in {"output_text", "text"} and part.get("text"):
                return json.loads(part["text"])

    raise ValueError("No JSON output returned by obituary agent")


def normalize_ai_obituary_result(ai_result):
    subject_status = ai_result.get("subject_status", "Unknown")
    relations = ai_result.get("relations", [])

    normalized = {
        "subject_status": subject_status,
        "surviving_relatives": [],
        "preceded_relatives": [],
    }

    for rel in relations:
        name = (rel.get("name") or "").strip()
        if not name:
            continue

        status = (rel.get("status") or "Unknown").strip()
        item = {
            "name": name,
            "relationship_type": (rel.get("relationship") or "Relative").strip() or "Relative",
            "relative_status": status,
            "marital_status": (rel.get("marital_status") or "Unknown").strip(),
            "spouse_name": (rel.get("spouse_name") or "").strip(),
            "note": (rel.get("note") or "AI obituary extraction").strip(),
        }
        if status == "Deceased":
            normalized["preceded_relatives"].append(item)
        else:
            normalized["surviving_relatives"].append(item)

    return normalized


def import_normalized_obituary(db, property_id, subject_person_id, source_url, raw_text, normalized):
    db.execute(
        """
        INSERT INTO obituary_sources (property_id, subject_person_id, source_url, raw_text, extracted_json)
        VALUES (?, ?, ?, ?, ?)
        """,
        (property_id, subject_person_id, source_url, raw_text, json.dumps(normalized)),
    )

    inserted_relationships = 0
    created_people = 0

    for item in normalized["surviving_relatives"] + normalized["preceded_relatives"]:
        count_before = db.execute("SELECT COUNT(*) AS c FROM people").fetchone()["c"]
        relative_id = find_or_create_person_by_full_name(
            db,
            item["name"],
            notes=f"Imported from obituary: {source_url}",
        )
        count_after = db.execute("SELECT COUNT(*) AS c FROM people").fetchone()["c"]
        if count_after > count_before:
            created_people += 1

        context = extract_obituary_context(raw_text, item['name'])
        note = f"Obituary import ({item['relative_status']}, marital: {item['marital_status']}) | {item.get('note', '')} | Source: {context}".strip()
        db.execute(
            """
            INSERT INTO person_relationships (subject_person_id, related_person_id, relationship_type, note)
            VALUES (?, ?, ?, ?)
            """,
            (subject_person_id, relative_id, item["relationship_type"], note),
        )
        inserted_relationships += 1

        spouse_name = item.get("spouse_name", "").strip()
        if spouse_name:
            spouse_id = find_or_create_person_by_full_name(
                db,
                spouse_name,
                notes=f"Imported spouse from obituary: {source_url}",
            )
            db.execute(
                """
                INSERT INTO person_relationships (subject_person_id, related_person_id, relationship_type, note)
                VALUES (?, ?, ?, ?)
                """,
                (relative_id, spouse_id, "Spouse", "Inferred from obituary extraction"),
            )

    db.execute(
        """
        INSERT INTO activity_log (property_id, person_id, activity_type, outcome, note)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            property_id,
            subject_person_id,
            "Obituary Import",
            f"Imported {inserted_relationships} relationships",
            f"URL: {source_url} | Subject status: {normalized['subject_status']}",
        ),
    )
    add_person_note(
        db,
        subject_person_id,
        "Obituary Import",
        f"Imported obituary from {source_url}",
        {
            "source_url": source_url,
            "normalized": normalized,
            "raw_excerpt": raw_text[:1200],
        },
    )

    return {
        "subject_status": normalized["subject_status"],
        "created_people": created_people,
        "inserted_relationships": inserted_relationships,
        "surviving_relatives": normalized["surviving_relatives"],
        "preceded_relatives": normalized["preceded_relatives"],
    }


def import_obituary_ai_into_property(db, property_id, subject_person_id, source_url, subject_name=""):
    raw_text = fetch_obituary_text(source_url)
    ai_result = call_openai_obituary_agent(raw_text, subject_name=subject_name)
    normalized = normalize_ai_obituary_result(ai_result)
    return import_normalized_obituary(db, property_id, subject_person_id, source_url, raw_text, normalized)
@app.route("/")
def dashboard():
    ensure_db()
    db = get_db()

    status_filter = request.args.get("status", "").strip()
    q = request.args.get("q", "").strip()

    where = []
    params = []
    if status_filter:
        where.append("p.status = ?")
        params.append(status_filter)
    if q:
        where.append("lower(a.street || ' ' || a.city || ' ' || a.state || ' ' || a.postal_code) LIKE ?")
        params.append(f"%{q.lower()}%")

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    properties = db.execute(
        f"""
        SELECT p.id,
               p.status,
               p.notes,
               p.created_at,
               a.street,
               a.city,
               a.state,
               a.postal_code,
               op.first_name AS owner_first,
               op.last_name AS owner_last
        FROM properties p
        JOIN addresses a ON a.id = p.property_address_id
        LEFT JOIN people op ON op.id = p.owner_person_id
        {where_sql}
        ORDER BY p.created_at DESC
        """,
        tuple(params),
    ).fetchall()

    counts = {
        "properties": db.execute("SELECT COUNT(*) AS c FROM properties").fetchone()["c"],
        "people": db.execute("SELECT COUNT(*) AS c FROM people").fetchone()["c"],
        "touchpoints": db.execute("SELECT COUNT(*) AS c FROM touchpoints").fetchone()["c"],
        "relationships": db.execute("SELECT COUNT(*) AS c FROM person_relationships").fetchone()["c"],
    }

    return render_template(
        "dashboard.html",
        properties=properties,
        counts=counts,
        status_filter=status_filter,
        q=q,
    )


@app.route("/properties", methods=["POST"])
def create_property_route():
    ensure_db()
    db = get_db()

    property_addr_id = create_address(
        db,
        request.form.get("property_street", ""),
        request.form.get("property_city", ""),
        request.form.get("property_state", ""),
        request.form.get("property_zip", ""),
    )

    owner_id = create_person(
        db,
        request.form.get("owner_first_name", "Unknown"),
        request.form.get("owner_last_name", "Owner"),
        phone=request.form.get("owner_phone", ""),
        email=request.form.get("owner_email", ""),
        notes=request.form.get("owner_notes", ""),
    )

    cur = db.execute(
        """
        INSERT INTO properties (property_address_id, owner_person_id, resident_person_id, status, notes)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            property_addr_id,
            owner_id,
            None,
            request.form.get("property_status", "Untouched"),
            request.form.get("property_notes", ""),
        ),
    )
    db.commit()
    return redirect(url_for("property_detail", property_id=cur.lastrowid))


@app.route("/templates/phone-email-template.csv")
def phone_email_template():
    template = "channel_type,channel_label,value,status,note\nPhone,Mobile,(555)123-4567,Correct,Sample owner phone\nEmail,Email,owner@example.com,Correct,Sample owner email\n"
    return app.response_class(template, mimetype="text/csv", headers={"Content-Disposition": "attachment; filename=phone-email-template.csv"})


@app.route("/property/<int:property_id>/upload-contacts", methods=["POST"])
def upload_owner_contacts(property_id):
    ensure_db()
    db = get_db()

    prop = db.execute("SELECT owner_person_id FROM properties WHERE id = ?", (property_id,)).fetchone()
    owner_id = prop["owner_person_id"] if prop else None
    if not owner_id:
        return redirect(url_for("property_detail", property_id=property_id))

    file = request.files.get("contacts_file")
    if not file:
        return redirect(url_for("property_detail", property_id=property_id))

    content = file.stream.read().decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(content))

    for row in reader:
        channel_type = (row.get("channel_type") or "").strip()
        value = (row.get("value") or "").strip()
        if channel_type not in {"Phone", "Email"} or not value:
            continue

        db.execute(
            """
            INSERT INTO touchpoints (person_id, channel_type, channel_label, value, status, note, last_attempted)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                owner_id,
                channel_type,
                (row.get("channel_label") or "").strip(),
                value,
                (row.get("status") or "Correct").strip(),
                (row.get("note") or "").strip(),
                "",
            ),
        )

    db.commit()
    return redirect(url_for("property_detail", property_id=property_id))

@app.route("/property/<int:property_id>")
def property_detail(property_id):
    ensure_db()
    db = get_db()

    prop = db.execute(
        """
        SELECT p.*,
               a.street,
               a.city,
               a.state,
               a.postal_code,
               op.first_name AS owner_first,
               op.last_name AS owner_last
        FROM properties p
        JOIN addresses a ON a.id = p.property_address_id
        LEFT JOIN people op ON op.id = p.owner_person_id
        WHERE p.id = ?
        """,
        (property_id,),
    ).fetchone()

    if prop is None:
        return "Property not found", 404

    owner = None
    owner_touchpoints = []
    owner_relationships = []
    owner_primary_phone = ""
    owner_phone_options = []

    if prop["owner_person_id"]:
        owner = db.execute("SELECT * FROM people WHERE id = ?", (prop["owner_person_id"],)).fetchone()
        owner_touchpoints = db.execute(
            """
            SELECT * FROM touchpoints
            WHERE person_id = ? AND lower(channel_type) IN ('phone', 'email')
            ORDER BY created_at DESC
            """,
            (prop["owner_person_id"],),
        ).fetchall()
        owner_relationships = db.execute(
            """
            SELECT r.relationship_type, r.note,
                   p.id AS person_id, p.first_name, p.last_name, p.outreach_status
            FROM person_relationships r
            JOIN people p ON p.id = r.related_person_id
            WHERE r.subject_person_id = ?
            ORDER BY p.last_name, p.first_name
            """,
            (prop["owner_person_id"],),
        ).fetchall()
        for tp in owner_touchpoints:
            if (tp["channel_type"] or "").lower() == "phone" and (tp["value"] or "").strip():
                owner_phone_options.append(tp["value"].strip())
                owner_primary_phone = tp["value"].strip()
                break
        for tp in owner_touchpoints:
            if (tp["channel_type"] or "").lower() == "phone" and (tp["value"] or "").strip():
                val = tp["value"].strip()
                if val not in owner_phone_options:
                    owner_phone_options.append(val)

    network_ids, _ = build_property_network(db, prop)

    touchpoints = []
    socials = []
    if network_ids:
        placeholders = ",".join(["?"] * len(network_ids))
        touchpoints = db.execute(
            f"SELECT * FROM touchpoints WHERE person_id IN ({placeholders}) ORDER BY created_at DESC",
            tuple(network_ids),
        ).fetchall()
        socials = db.execute(
            f"SELECT * FROM social_accounts WHERE person_id IN ({placeholders}) ORDER BY created_at DESC",
            tuple(network_ids),
        ).fetchall()

    activities = db.execute(
        "SELECT * FROM activity_log WHERE property_id = ? ORDER BY created_at DESC",
        (property_id,),
    ).fetchall()

    obituary_imports = db.execute(
        """
        SELECT o.*, p.first_name, p.last_name
        FROM obituary_sources o
        JOIN people p ON p.id = o.subject_person_id
        WHERE o.property_id = ?
        ORDER BY o.created_at DESC
        """,
        (property_id,),
    ).fetchall()

    communications = db.execute(
        """
        SELECT c.*
        FROM communications c
        WHERE c.property_id = ?
          AND (? IS NULL OR c.person_id = ?)
        ORDER BY c.sent_at ASC, c.id ASC
        """,
        (property_id, prop["owner_person_id"], prop["owner_person_id"]),
    ).fetchall()

    all_people = db.execute("SELECT * FROM people ORDER BY last_name, first_name").fetchall()
    related_people_options = db.execute(
        "SELECT * FROM people WHERE id != ? ORDER BY last_name, first_name",
        (prop["owner_person_id"] or -1,),
    ).fetchall()

    sms_rollup = {"sent": 0, "received": 0}
    verified_contacts = []
    if network_ids:
        placeholders = ",".join(["?"] * len(network_ids))
        sent = db.execute(
            f"""
            SELECT COUNT(*) AS c
            FROM communications
            WHERE property_id = ?
              AND upper(channel) = 'SMS'
              AND lower(direction) = 'outbound'
              AND person_id IN ({placeholders})
            """,
            tuple([property_id] + network_ids),
        ).fetchone()["c"]
        received = db.execute(
            f"""
            SELECT COUNT(*) AS c
            FROM communications
            WHERE property_id = ?
              AND upper(channel) = 'SMS'
              AND lower(direction) = 'inbound'
              AND person_id IN ({placeholders})
            """,
            tuple([property_id] + network_ids),
        ).fetchone()["c"]
        sms_rollup = {"sent": sent, "received": received}

        people_rows = db.execute(
            f"SELECT id, first_name, last_name FROM people WHERE id IN ({placeholders}) ORDER BY last_name, first_name",
            tuple(network_ids),
        ).fetchall()
        for p in people_rows:
            correct_phone = db.execute(
                """
                SELECT COUNT(*) AS c
                FROM touchpoints
                WHERE person_id = ? AND lower(channel_type) = 'phone' AND lower(status) = 'correct'
                """,
                (p["id"],),
            ).fetchone()["c"]
            verified_social = db.execute(
                """
                SELECT COUNT(*) AS c
                FROM social_accounts
                WHERE person_id = ? AND lower(status) = 'verified'
                """,
                (p["id"],),
            ).fetchone()["c"]
            if correct_phone > 0 or verified_social > 0:
                verified_contacts.append(
                    {
                        "person_id": p["id"],
                        "full_name": f"{p['first_name']} {p['last_name']}".strip(),
                        "confirmed_phone_count": correct_phone,
                        "verified_social_count": verified_social,
                    }
                )

    return render_template(
        "property_detail.html",
        prop=prop,
        owner=owner,
        owner_touchpoints=owner_touchpoints,
        owner_primary_phone=owner_primary_phone,
        owner_phone_options=owner_phone_options,
        owner_relationships=owner_relationships,
        all_people=all_people,
        related_people_options=related_people_options,
        sms_rollup=sms_rollup,
        verified_contacts=verified_contacts,
        touchpoints=touchpoints,
        socials=socials,
        activities=activities,
        obituary_imports=obituary_imports,
        communications=communications,
        smrt_from_number=SMRTPHONE_FROM_NUMBER,
        social_url=normalize_social_profile_url,
        person_name=person_name,
    )

@app.route("/people", methods=["POST"])
def create_person_route():
    ensure_db()
    db = get_db()
    person_id = create_person(
        db,
        request.form.get("first_name", "Unknown"),
        request.form.get("last_name", "Person"),
        phone=request.form.get("phone", ""),
        email=request.form.get("email", ""),
        notes=request.form.get("notes", ""),
    )
    db.commit()

    redirect_property = request.form.get("property_id")
    if redirect_property:
        return redirect(url_for("property_detail", property_id=redirect_property))
    return redirect(url_for("person_detail", person_id=person_id))


@app.route("/person/<int:person_id>")
def person_detail(person_id):
    ensure_db()
    db = get_db()

    person = db.execute("SELECT * FROM people WHERE id = ?", (person_id,)).fetchone()
    if person is None:
        return "Person not found", 404

    touchpoints = db.execute(
        "SELECT * FROM touchpoints WHERE person_id = ? ORDER BY created_at DESC", (person_id,)
    ).fetchall()
    socials = db.execute(
        "SELECT * FROM social_accounts WHERE person_id = ? ORDER BY created_at DESC", (person_id,)
    ).fetchall()
    person_primary_phone = ""
    person_phone_options = []
    for tp in touchpoints:
        if (tp["channel_type"] or "").lower() == "phone" and (tp["value"] or "").strip():
            person_phone_options.append(tp["value"].strip())
            person_primary_phone = tp["value"].strip()
            break
    for tp in touchpoints:
        if (tp["channel_type"] or "").lower() == "phone" and (tp["value"] or "").strip():
            val = tp["value"].strip()
            if val not in person_phone_options:
                person_phone_options.append(val)

    relationships = db.execute(
        """
        SELECT r.*, p1.first_name AS s_first, p1.last_name AS s_last,
               p2.first_name AS r_first, p2.last_name AS r_last
        FROM person_relationships r
        JOIN people p1 ON p1.id = r.subject_person_id
        JOIN people p2 ON p2.id = r.related_person_id
        WHERE r.subject_person_id = ? OR r.related_person_id = ?
        ORDER BY r.created_at DESC
        """,
        (person_id, person_id),
    ).fetchall()

    addresses = db.execute(
        """
        SELECT pa.label, a.street, a.city, a.state, a.postal_code
        FROM person_addresses pa
        JOIN addresses a ON a.id = pa.address_id
        WHERE pa.person_id = ?
        ORDER BY pa.created_at DESC
        """,
        (person_id,),
    ).fetchall()


    property_context = None
    property_id = request.args.get("property_id", "").strip()
    if property_id.isdigit():
        ctx = db.execute(
            """
            SELECT p.id, a.street, a.city, a.state, a.postal_code,
                   op.first_name AS owner_first, op.last_name AS owner_last
            FROM properties p
            JOIN addresses a ON a.id = p.property_address_id
            LEFT JOIN people op ON op.id = p.owner_person_id
            WHERE p.id = ?
            """,
            (int(property_id),),
        ).fetchone()
        if ctx:
            property_context = ctx
    default_property_id = property_context["id"] if property_context else None
    if default_property_id is None:
        direct_prop = db.execute(
            "SELECT id FROM properties WHERE owner_person_id = ? OR resident_person_id = ? ORDER BY created_at DESC LIMIT 1",
            (person_id, person_id),
        ).fetchone()
        if direct_prop:
            default_property_id = direct_prop["id"]

    communications = db.execute(
        """
        SELECT c.*
        FROM communications c
        WHERE c.person_id = ?
        ORDER BY c.sent_at ASC, c.id ASC
        """,
        (person_id,),
    ).fetchall()
    person_notes = db.execute(
        "SELECT * FROM person_notes WHERE person_id = ? ORDER BY created_at DESC, id DESC",
        (person_id,),
    ).fetchall()

    return render_template(
        "person_detail.html",
        person=person,
        touchpoints=touchpoints,
        person_primary_phone=person_primary_phone,
        person_phone_options=person_phone_options,
        socials=socials,
        relationships=relationships,
        addresses=addresses,
        person_notes=person_notes,
        communications=communications,
        property_context=property_context,
        default_property_id=default_property_id,
        smrt_from_number=SMRTPHONE_FROM_NUMBER,
        social_url=normalize_social_profile_url,
    )


@app.route("/api/people/<int:person_id>/notes", methods=["POST"])
def create_person_note_api(person_id):
    ensure_db()
    db = get_db()
    payload = request.get_json(force=True)
    note_body = (payload.get("note_body") or "").strip()
    if not note_body:
        return jsonify({"error": "note_body is required"}), 400
    source = (payload.get("source") or "Manual").strip()
    payload_json = payload.get("payload")
    add_person_note(db, person_id, source, note_body, payload_json)
    db.commit()
    row = db.execute(
        "SELECT * FROM person_notes WHERE person_id = ? ORDER BY id DESC LIMIT 1",
        (person_id,),
    ).fetchone()
    return jsonify(dict(row)), 201

@app.route("/person/<int:person_id>/addresses", methods=["POST"])
def add_person_address(person_id):
    ensure_db()
    db = get_db()

    street = request.form.get("street", "").strip()
    city = request.form.get("city", "").strip()
    state = request.form.get("state", "").strip()
    postal = request.form.get("postal_code", "").strip()
    if not (street and city and state and postal):
        return redirect(url_for("person_detail", person_id=person_id))

    addr_id = create_address(db, street, city, state, postal)
    db.execute(
        "INSERT INTO person_addresses (person_id, address_id, label) VALUES (?, ?, ?)",
        (person_id, addr_id, request.form.get("label", "Related Address")),
    )
    db.commit()
    return redirect(url_for("person_detail", person_id=person_id))

@app.route("/person/<int:person_id>/touchpoints", methods=["POST"])
def add_person_touchpoint(person_id):
    ensure_db()
    db = get_db()
    db.execute(
        """
        INSERT INTO touchpoints (person_id, channel_type, channel_label, value, status, note, last_attempted)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            person_id,
            request.form.get("channel_type", "Phone"),
            request.form.get("channel_label", ""),
            request.form.get("value", ""),
            request.form.get("status", "Pending"),
            request.form.get("note", ""),
            request.form.get("last_attempted", ""),
        ),
    )
    db.commit()
    return redirect(url_for("person_detail", person_id=person_id))


@app.route("/person/<int:person_id>/socials", methods=["POST"])
def add_person_social(person_id):
    ensure_db()
    db = get_db()
    platform = request.form.get("platform", "LinkedIn")
    handle = request.form.get("handle", "")
    url = normalize_social_profile_url(platform, handle, request.form.get("url", ""))
    db.execute(
        """
        INSERT INTO social_accounts (person_id, platform, handle, url, status, note)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            person_id,
            platform,
            handle,
            url,
            request.form.get("status", "Unverified"),
            request.form.get("note", ""),
        ),
    )
    db.commit()
    return redirect(url_for("person_detail", person_id=person_id))


@app.route("/property/<int:property_id>/touchpoints", methods=["POST"])
def add_touchpoint(property_id):
    ensure_db()
    db = get_db()

    person_id = request.form.get("person_id")
    if not person_id:
        prop = db.execute("SELECT owner_person_id FROM properties WHERE id = ?", (property_id,)).fetchone()
        person_id = prop["owner_person_id"] if prop else None

    if not person_id:
        return redirect(url_for("property_detail", property_id=property_id))

    db.execute(
        """
        INSERT INTO touchpoints (person_id, channel_type, channel_label, value, status, note, last_attempted)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            person_id,
            request.form.get("channel_type", "Phone"),
            request.form.get("channel_label", ""),
            request.form.get("value", ""),
            request.form.get("status", "Pending"),
            request.form.get("note", ""),
            request.form.get("last_attempted", ""),
        ),
    )
    db.commit()
    return redirect(url_for("property_detail", property_id=property_id))



@app.route("/property/<int:property_id>/socials", methods=["POST"])
def add_social(property_id):
    ensure_db()
    db = get_db()

    person_id = request.form.get("person_id")
    if not person_id:
        prop = db.execute("SELECT owner_person_id FROM properties WHERE id = ?", (property_id,)).fetchone()
        person_id = prop["owner_person_id"] if prop else None

    if not person_id:
        return redirect(url_for("property_detail", property_id=property_id))

    platform = request.form.get("platform", "LinkedIn")
    handle = request.form.get("handle", "")
    url = normalize_social_profile_url(platform, handle, request.form.get("url", ""))
    db.execute(
        """
        INSERT INTO social_accounts (person_id, platform, handle, url, status, note)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            person_id,
            platform,
            handle,
            url,
            request.form.get("status", "Unverified"),
            request.form.get("note", ""),
        ),
    )
    db.commit()
    return redirect(url_for("property_detail", property_id=property_id))



@app.route("/property/<int:property_id>/relationships", methods=["POST"])
def add_relationship(property_id):
    ensure_db()
    db = get_db()
    db.execute(
        """
        INSERT INTO person_relationships (subject_person_id, related_person_id, relationship_type, note)
        VALUES (?, ?, ?, ?)
        """,
        (
            request.form.get("subject_person_id"),
            request.form.get("related_person_id"),
            request.form.get("relationship_type", "Associate"),
            request.form.get("note", ""),
        ),
    )
    db.commit()
    return redirect(url_for("property_detail", property_id=property_id))


@app.route("/property/<int:property_id>/relationships/new", methods=["POST"])
def add_new_relationship(property_id):
    ensure_db()
    db = get_db()

    prop = db.execute("SELECT owner_person_id FROM properties WHERE id = ?", (property_id,)).fetchone()
    owner_id = prop["owner_person_id"] if prop else None
    if not owner_id:
        return redirect(url_for("property_detail", property_id=property_id))

    first_name = (request.form.get("related_first_name") or "").strip()
    last_name = (request.form.get("related_last_name") or "").strip()
    if not first_name or not last_name:
        return redirect(url_for("property_detail", property_id=property_id))

    related_id = create_person(
        db,
        first_name,
        last_name,
        phone=(request.form.get("related_phone") or "").strip(),
        email=(request.form.get("related_email") or "").strip(),
        notes=(request.form.get("related_notes") or "").strip(),
    )

    related_status = (request.form.get("related_status") or "").strip()
    if related_status:
        db.execute("UPDATE people SET outreach_status = ? WHERE id = ?", (related_status, related_id))

    street = (request.form.get("related_street") or "").strip()
    city = (request.form.get("related_city") or "").strip()
    state = (request.form.get("related_state") or "").strip()
    postal = (request.form.get("related_zip") or "").strip()
    if street and city and state and postal:
        addr_id = create_address(db, street, city, state, postal)
        db.execute(
            "INSERT INTO person_addresses (person_id, address_id, label) VALUES (?, ?, ?)",
            (related_id, addr_id, "Related Address"),
        )

    db.execute(
        """
        INSERT INTO person_relationships (subject_person_id, related_person_id, relationship_type, note)
        VALUES (?, ?, ?, ?)
        """,
        (
            owner_id,
            related_id,
            (request.form.get("relationship_type") or "Relative").strip(),
            (request.form.get("note") or "").strip(),
        ),
    )
    db.commit()
    return redirect(url_for("property_detail", property_id=property_id))

@app.route("/property/<int:property_id>/activities", methods=["POST"])
def add_activity(property_id):
    ensure_db()
    db = get_db()
    db.execute(
        """
        INSERT INTO activity_log (property_id, person_id, activity_type, outcome, note)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            property_id,
            request.form.get("person_id") or None,
            request.form.get("activity_type", "Call"),
            request.form.get("outcome", "No answer"),
            request.form.get("note", ""),
        ),
    )
    db.commit()
    return redirect(url_for("property_detail", property_id=property_id))


@app.route("/property/<int:property_id>/obituary-import", methods=["POST"])
def import_obituary_route(property_id):
    ensure_db()
    db = get_db()
    url = request.form.get("obituary_url", "").strip()
    subject_person_id = request.form.get("subject_person_id", "").strip()

    if not subject_person_id:
        prop = db.execute("SELECT owner_person_id FROM properties WHERE id = ?", (property_id,)).fetchone()
        subject_person_id = str(prop["owner_person_id"]) if prop and prop["owner_person_id"] else ""

    if not url or not subject_person_id:
        return jsonify({"error": "owner not found or obituary URL missing"}), 400

    try:
        summary = import_obituary_into_property(db, property_id, int(subject_person_id), url)
        db.commit()
    except Exception as exc:
        db.rollback()
        return jsonify({"error": str(exc)}), 400

    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify(summary), 201
    return redirect(url_for("property_detail", property_id=property_id))

@app.route("/property/<int:property_id>/obituary-import-ai", methods=["POST"])
def import_obituary_ai_route(property_id):
    ensure_db()
    db = get_db()
    url = request.form.get("obituary_url", "").strip()
    subject_person_id = request.form.get("subject_person_id", "").strip()

    if not subject_person_id:
        prop = db.execute("SELECT owner_person_id FROM properties WHERE id = ?", (property_id,)).fetchone()
        subject_person_id = str(prop["owner_person_id"]) if prop and prop["owner_person_id"] else ""

    if not url or not subject_person_id:
        return jsonify({"error": "owner not found or obituary URL missing"}), 400

    person = db.execute("SELECT first_name, last_name FROM people WHERE id = ?", (subject_person_id,)).fetchone()
    subject_name = ""
    if person:
        subject_name = f"{person['first_name']} {person['last_name']}".strip()

    try:
        summary = import_obituary_ai_into_property(
            db,
            property_id,
            int(subject_person_id),
            url,
            subject_name=subject_name,
        )
        db.commit()
    except Exception as exc:
        db.rollback()
        return jsonify({"error": str(exc)}), 400

    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify(summary), 201
    return redirect(url_for("property_detail", property_id=property_id))

@app.route("/api/people/<int:person_id>/touchpoints", methods=["POST"])
def add_person_touchpoint_api(person_id):
    ensure_db()
    db = get_db()
    payload = request.get_json(force=True)

    channel_type = payload.get("channel_type", "Phone")
    channel_label = payload.get("channel_label", "")
    value = (payload.get("value") or "").strip()
    if not value:
        return jsonify({"error": "value is required"}), 400

    cur = db.execute(
        """
        INSERT INTO touchpoints (person_id, channel_type, channel_label, value, status, note, last_attempted)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (person_id, channel_type, channel_label, value, payload.get("status", "Unknown"), payload.get("note", ""), ""),
    )
    db.commit()
    row = db.execute("SELECT * FROM touchpoints WHERE id = ?", (cur.lastrowid,)).fetchone()
    return jsonify(dict(row)), 201


@app.route("/api/touchpoints/<int:touchpoint_id>/status", methods=["PATCH"])
def update_touchpoint_status(touchpoint_id):
    ensure_db()
    db = get_db()
    payload = request.get_json(force=True)
    status = (payload.get("status") or "").strip()
    if not status:
        return jsonify({"error": "status is required"}), 400

    db.execute("UPDATE touchpoints SET status = ? WHERE id = ?", (status, touchpoint_id))
    db.commit()
    return jsonify({"ok": True})


@app.route("/api/touchpoints/<int:touchpoint_id>/label", methods=["PATCH"])
def update_touchpoint_label(touchpoint_id):
    ensure_db()
    db = get_db()
    payload = request.get_json(force=True)
    channel_label = (payload.get("channel_label") or "").strip()
    if not channel_label:
        return jsonify({"error": "channel_label is required"}), 400

    db.execute("UPDATE touchpoints SET channel_label = ? WHERE id = ?", (channel_label, touchpoint_id))
    db.commit()
    return jsonify({"ok": True})


@app.route("/api/people/<int:person_id>/socials", methods=["POST"])
def add_person_social_api(person_id):
    ensure_db()
    db = get_db()
    payload = request.get_json(force=True)

    platform = payload.get("platform", "LinkedIn")
    handle = payload.get("handle", "")
    url = normalize_social_profile_url(platform, handle, payload.get("url", ""))
    cur = db.execute(
        """
        INSERT INTO social_accounts (person_id, platform, handle, url, status, note)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            person_id,
            platform,
            handle,
            url,
            payload.get("status", "Unknown"),
            payload.get("note", ""),
        ),
    )
    db.commit()
    row = db.execute("SELECT * FROM social_accounts WHERE id = ?", (cur.lastrowid,)).fetchone()
    return jsonify(dict(row)), 201


@app.route("/api/socials/<int:social_id>/status", methods=["PATCH"])
def update_social_status(social_id):
    ensure_db()
    db = get_db()
    payload = request.get_json(force=True)
    status = (payload.get("status") or "").strip()
    if not status:
        return jsonify({"error": "status is required"}), 400

    db.execute("UPDATE social_accounts SET status = ? WHERE id = ?", (status, social_id))
    db.commit()
    return jsonify({"ok": True})


@app.route("/api/touchpoints/<int:touchpoint_id>", methods=["DELETE"])
def delete_touchpoint(touchpoint_id):
    ensure_db()
    db = get_db()
    db.execute("DELETE FROM touchpoints WHERE id = ?", (touchpoint_id,))
    db.commit()
    return jsonify({"ok": True})


@app.route("/api/socials/<int:social_id>", methods=["DELETE"])
def delete_social(social_id):
    ensure_db()
    db = get_db()
    db.execute("DELETE FROM social_accounts WHERE id = ?", (social_id,))
    db.commit()
    return jsonify({"ok": True})

@app.route("/api/people/<int:person_id>/status", methods=["PATCH"])
def update_person_status(person_id):
    ensure_db()
    db = get_db()
    payload = request.get_json(force=True)
    status = (payload.get("status") or "").strip()
    if not status:
        return jsonify({"error": "status is required"}), 400

    db.execute("UPDATE people SET outreach_status = ? WHERE id = ?", (status, person_id))
    db.commit()
    return jsonify({"ok": True})


@app.route("/api/people/<int:person_id>/name", methods=["PATCH"])
def update_person_name(person_id):
    ensure_db()
    db = get_db()
    payload = request.get_json(force=True)
    first_name = (payload.get("first_name") or "").strip()
    last_name = (payload.get("last_name") or "").strip()
    if not first_name or not last_name:
        return jsonify({"error": "first_name and last_name are required"}), 400

    db.execute(
        "UPDATE people SET first_name = ?, last_name = ? WHERE id = ?",
        (first_name, last_name, person_id),
    )
    db.commit()
    return jsonify({"ok": True})


@app.route("/api/properties/<int:property_id>/status", methods=["PATCH"])
def update_property_status(property_id):
    ensure_db()
    db = get_db()
    payload = request.get_json(force=True)
    status = (payload.get("status") or "").strip()
    if not status:
        return jsonify({"error": "status is required"}), 400

    db.execute("UPDATE properties SET status = ? WHERE id = ?", (status, property_id))
    db.commit()
    return jsonify({"ok": True})


@app.route("/api/properties/<int:property_id>/notes", methods=["PATCH"])
def update_property_notes(property_id):
    ensure_db()
    db = get_db()
    payload = request.get_json(force=True)
    notes = (payload.get("notes") or "").strip()

    db.execute("UPDATE properties SET notes = ? WHERE id = ?", (notes, property_id))
    db.commit()
    return jsonify({"ok": True})


@app.route("/api/properties/<int:property_id>/communications", methods=["GET"])
def list_property_communications(property_id):
    ensure_db()
    db = get_db()

    channel = (request.args.get("channel") or "").strip().upper()
    person_id = (request.args.get("person_id") or "").strip()
    person_id_int = int(person_id) if person_id.isdigit() else None

    where = ["property_id = ?"]
    params = [property_id]
    if person_id_int is not None:
        where.append("person_id = ?")
        params.append(person_id_int)
    if channel in {"SMS", "EMAIL"}:
        where.append("upper(channel) = ?")
        params.append(channel)

    rows = db.execute(
        f"""
        SELECT * FROM communications
        WHERE {' AND '.join(where)}
        ORDER BY sent_at ASC, id ASC
        """,
        tuple(params),
    ).fetchall()
    return jsonify([dict(row) for row in rows])


@app.route("/api/properties/<int:property_id>/communications", methods=["POST"])
def create_property_communication(property_id):
    ensure_db()
    db = get_db()
    payload = request.get_json(force=True)

    body = (payload.get("body") or "").strip()
    if not body:
        return jsonify({"error": "body is required"}), 400

    channel = (payload.get("channel") or "SMS").strip().upper()
    if channel not in {"SMS", "EMAIL"}:
        channel = "SMS"

    direction = (payload.get("direction") or "Outbound").strip().title()
    if direction not in {"Inbound", "Outbound"}:
        direction = "Outbound"

    status = (payload.get("status") or "Queued").strip()
    person_id = payload.get("person_id")
    person_id = int(person_id) if str(person_id).isdigit() else None
    sent_at = (payload.get("sent_at") or datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")).strip()
    to_number = (payload.get("to_number") or "").strip()
    from_number = (payload.get("from_number") or SMRTPHONE_FROM_NUMBER).strip()
    external_id = (payload.get("external_id") or "").strip()

    if channel == "SMS" and direction == "Outbound":
        if not to_number:
            return jsonify({"error": "to_number is required for SMS"}), 400
        try:
            send_result = send_smrtphone_sms(to_number, body)
            if send_result.get("status"):
                status = send_result["status"]
            external_id = send_result.get("sms_id") or external_id
            update_person_outreach_status_for_sms(db, person_id, "outbound_success")
        except ValueError as exc:
            status = "Failed"
            cur = db.execute(
                """
                INSERT INTO communications (property_id, person_id, channel, direction, from_number, to_number, body, status, is_read, sent_at, external_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (property_id, person_id, channel, direction, from_number, to_number, body, status, 1, sent_at, external_id),
            )
            db.commit()
            row = db.execute("SELECT * FROM communications WHERE id = ?", (cur.lastrowid,)).fetchone()
            return jsonify({"error": str(exc), "communication": dict(row)}), 502

    cur = db.execute(
        """
        INSERT INTO communications (property_id, person_id, channel, direction, from_number, to_number, body, status, is_read, sent_at, external_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            property_id,
            person_id,
            channel,
            direction,
            from_number,
            to_number,
            body,
            status,
            1,
            sent_at,
            external_id,
        ),
    )
    db.commit()
    row = db.execute("SELECT * FROM communications WHERE id = ?", (cur.lastrowid,)).fetchone()
    return jsonify(dict(row)), 201


@app.route("/api/people/<int:person_id>/communications", methods=["GET"])
def list_person_communications(person_id):
    ensure_db()
    db = get_db()

    channel = (request.args.get("channel") or "").strip().upper()
    where = ["person_id = ?"]
    params = [person_id]
    if channel in {"SMS", "EMAIL"}:
        where.append("upper(channel) = ?")
        params.append(channel)

    rows = db.execute(
        f"""
        SELECT * FROM communications
        WHERE {' AND '.join(where)}
        ORDER BY sent_at ASC, id ASC
        """,
        tuple(params),
    ).fetchall()
    return jsonify([dict(row) for row in rows])


@app.route("/api/people/<int:person_id>/communications", methods=["POST"])
def create_person_communication(person_id):
    ensure_db()
    db = get_db()
    payload = request.get_json(force=True)

    body = (payload.get("body") or "").strip()
    if not body:
        return jsonify({"error": "body is required"}), 400

    property_id = payload.get("property_id")
    if not str(property_id).isdigit():
        return jsonify({"error": "property_id is required"}), 400
    property_id = int(property_id)

    channel = (payload.get("channel") or "SMS").strip().upper()
    if channel not in {"SMS", "EMAIL"}:
        channel = "SMS"

    direction = (payload.get("direction") or "Outbound").strip().title()
    if direction not in {"Inbound", "Outbound"}:
        direction = "Outbound"

    status = (payload.get("status") or "Queued").strip()
    sent_at = (payload.get("sent_at") or datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")).strip()
    to_number = (payload.get("to_number") or "").strip()
    from_number = (payload.get("from_number") or SMRTPHONE_FROM_NUMBER).strip()
    external_id = (payload.get("external_id") or "").strip()

    if channel == "SMS" and direction == "Outbound":
        if not to_number:
            return jsonify({"error": "to_number is required for SMS"}), 400
        try:
            send_result = send_smrtphone_sms(to_number, body)
            if send_result.get("status"):
                status = send_result["status"]
            external_id = send_result.get("sms_id") or external_id
            update_person_outreach_status_for_sms(db, person_id, "outbound_success")
        except ValueError as exc:
            status = "Failed"
            cur = db.execute(
                """
                INSERT INTO communications (property_id, person_id, channel, direction, from_number, to_number, body, status, is_read, sent_at, external_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (property_id, person_id, channel, direction, from_number, to_number, body, status, 1, sent_at, external_id),
            )
            db.commit()
            row = db.execute("SELECT * FROM communications WHERE id = ?", (cur.lastrowid,)).fetchone()
            return jsonify({"error": str(exc), "communication": dict(row)}), 502

    cur = db.execute(
        """
        INSERT INTO communications (property_id, person_id, channel, direction, from_number, to_number, body, status, is_read, sent_at, external_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            property_id,
            person_id,
            channel,
            direction,
            from_number,
            to_number,
            body,
            status,
            1,
            sent_at,
            external_id,
        ),
    )
    db.commit()
    row = db.execute("SELECT * FROM communications WHERE id = ?", (cur.lastrowid,)).fetchone()
    return jsonify(dict(row)), 201


@app.route("/api/properties/<int:property_id>/bulk-sms/start", methods=["POST"])
def start_property_bulk_sms(property_id):
    ensure_db()
    db = get_db()
    payload = request.get_json(force=True)

    message = (payload.get("message") or "").strip()
    if not message:
        return jsonify({"error": "message is required"}), 400

    min_interval = int(payload.get("min_interval_minutes") or 2)
    max_interval = int(payload.get("max_interval_minutes") or 5)
    min_interval = max(2, min_interval)
    max_interval = max(min_interval, max_interval)

    prop = db.execute("SELECT * FROM properties WHERE id = ?", (property_id,)).fetchone()
    if not prop:
        return jsonify({"error": "property not found"}), 404

    network_ids, _ = build_property_network(db, prop)
    if not network_ids:
        return jsonify({"error": "No contacts available for this property"}), 400
    placeholders = ",".join(["?"] * len(network_ids))
    rows = db.execute(
        f"""
        SELECT id, person_id, value, channel_label
        FROM touchpoints
        WHERE person_id IN ({placeholders})
          AND lower(channel_type) = 'phone'
          AND lower(COALESCE(channel_label, 'unknown')) IN ('mobile', 'unknown')
        ORDER BY created_at ASC
        """,
        tuple(network_ids),
    ).fetchall()

    dedup = set()
    already_queued = set()
    active_rows = db.execute(
        """
        SELECT i.to_number
        FROM bulk_sms_job_items i
        JOIN bulk_sms_jobs j ON j.id = i.job_id
        WHERE j.property_id = ?
          AND j.status = 'Active'
          AND i.status IN ('Pending', 'Retry', 'Sent')
        """,
        (property_id,),
    ).fetchall()
    for r in active_rows:
        norm = normalize_phone(r["to_number"])
        if norm:
            already_queued.add(norm)

    targets = []
    for r in rows:
        normalized = normalize_phone(r["value"])
        if not normalized or normalized in dedup or normalized in already_queued:
            continue
        dedup.add(normalized)
        targets.append(r)

    if not targets:
        return jsonify({"error": "No mobile/unknown phone targets under this property"}), 400

    now_est = bulk_window_next_open(datetime.now(EST_TZ))
    first_utc = now_est.astimezone(timezone.utc).replace(tzinfo=None)
    cur = db.execute(
        """
        INSERT INTO bulk_sms_jobs (property_id, message, min_interval_minutes, max_interval_minutes, status)
        VALUES (?, ?, ?, ?, 'Active')
        """,
        (property_id, message, min_interval, max_interval),
    )
    job_id = cur.lastrowid

    for t in targets:
        db.execute(
            """
            INSERT INTO bulk_sms_job_items (job_id, property_id, person_id, to_number, channel_label, status, next_attempt_at)
            VALUES (?, ?, ?, ?, ?, 'Pending', ?)
            """,
            (
                job_id,
                property_id,
                t["person_id"],
                t["value"],
                t["channel_label"],
                format_db_time(first_utc),
            ),
        )

    db.commit()
    start_bulk_sms_worker()
    return jsonify(
        {
            "ok": True,
            "job_id": job_id,
            "target_count": len(targets),
            "first_send_at_utc": format_db_time(first_utc),
        }
    ), 201


@app.route("/api/properties/<int:property_id>/people/<int:person_id>/skip-trace", methods=["POST"])
def skip_trace_person_route(property_id, person_id):
    ensure_db()
    db = get_db()

    person = db.execute("SELECT * FROM people WHERE id = ?", (person_id,)).fetchone()
    prop = db.execute(
        """
        SELECT p.id, a.street, a.city, a.state, a.postal_code
        FROM properties p
        JOIN addresses a ON a.id = p.property_address_id
        WHERE p.id = ?
        """,
        (property_id,),
    ).fetchone()
    if not person or not prop:
        return jsonify({"error": "person or property not found"}), 404

    payload = request.get_json(silent=True) or {}
    first_name = (payload.get("first_name") or person["first_name"] or "").strip()
    last_name = (payload.get("last_name") or person["last_name"] or "").strip()
    if not first_name or not last_name:
        return jsonify({"error": "first_name and last_name are required"}), 400

    try:
        lookup_pkg = call_skipsherpa_person_lookup(
            first_name=first_name,
            last_name=last_name,
            street=prop["street"],
            city=prop["city"],
            state=prop["state"],
            zipcode=prop["postal_code"],
        )
        lookup = lookup_pkg.get("response") if isinstance(lookup_pkg, dict) else {}
        req_payload = lookup_pkg.get("request") if isinstance(lookup_pkg, dict) else {}
        summary = import_skipsherpa_person_result(db, property_id, person_id, lookup)
        db.execute(
            """
            INSERT INTO skiptrace_runs (provider, property_id, person_id, request_json, response_json, summary_json)
            VALUES ('SkipSherpa', ?, ?, ?, ?, ?)
            """,
            (
                property_id,
                person_id,
                json.dumps(req_payload),
                json.dumps(lookup),
                json.dumps(summary),
            ),
        )
        db.commit()
        return jsonify({"ok": True, "summary": summary, "raw": lookup}), 200
    except Exception as exc:
        db.rollback()
        try:
            add_person_note(
                db,
                person_id,
                "SkipSherpa",
                f"Skip trace failed: {str(exc)}",
                {"error": str(exc)},
            )
            db.commit()
        except Exception:
            db.rollback()
        return jsonify({"error": str(exc)}), 400

@app.route("/api/properties", methods=["GET", "POST"])
def properties_api():
    ensure_db()
    db = get_db()

    if request.method == "GET":
        rows = db.execute(
            """
            SELECT p.id, p.status, p.notes, a.street, a.city, a.state, a.postal_code,
                   op.first_name AS owner_first, op.last_name AS owner_last,
                   rp.first_name AS resident_first, rp.last_name AS resident_last
            FROM properties p
            JOIN addresses a ON a.id = p.property_address_id
            LEFT JOIN people op ON op.id = p.owner_person_id
            LEFT JOIN people rp ON rp.id = p.resident_person_id
            ORDER BY p.created_at DESC
            """
        ).fetchall()
        return jsonify([dict(row) for row in rows])

    payload = request.get_json(force=True)

    property_addr_id = create_address(
        db,
        payload["address"]["street"],
        payload["address"]["city"],
        payload["address"]["state"],
        payload["address"]["postal_code"],
    )
    owner = payload["owner"]
    owner_id = create_person(
        db,
        owner["first_name"],
        owner["last_name"],
        phone=owner.get("phone", ""),
        email=owner.get("email", ""),
        notes=owner.get("notes", ""),
    )

    resident_id = None
    if payload.get("resident"):
        resident = payload["resident"]
        resident_id = create_person(
            db,
            resident["first_name"],
            resident["last_name"],
            phone=resident.get("phone", ""),
            email=resident.get("email", ""),
            notes=resident.get("notes", ""),
        )

    cur = db.execute(
        """
        INSERT INTO properties (property_address_id, owner_person_id, resident_person_id, status, notes)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            property_addr_id,
            owner_id,
            resident_id,
            payload.get("status", "Untouched"),
            payload.get("notes", ""),
        ),
    )
    db.commit()
    return jsonify({"property_id": cur.lastrowid}), 201


@app.route("/api/properties/<int:property_id>")
def property_api(property_id):
    ensure_db()
    db = get_db()

    prop = db.execute("SELECT * FROM properties WHERE id = ?", (property_id,)).fetchone()
    if prop is None:
        return jsonify({"error": "not found"}), 404

    network_ids, relationships = build_property_network(db, prop)

    result = {
        "property": dict(prop),
        "relationships": [dict(r) for r in relationships],
        "network_people": [],
        "touchpoints": [],
        "social_accounts": [],
    }

    if network_ids:
        placeholders = ",".join(["?"] * len(network_ids))
        result["network_people"] = [
            dict(r)
            for r in db.execute(
                f"SELECT * FROM people WHERE id IN ({placeholders})", tuple(network_ids)
            ).fetchall()
        ]
        result["touchpoints"] = [
            dict(r)
            for r in db.execute(
                f"SELECT * FROM touchpoints WHERE person_id IN ({placeholders})", tuple(network_ids)
            ).fetchall()
        ]
        result["social_accounts"] = [
            dict(r)
            for r in db.execute(
                f"SELECT * FROM social_accounts WHERE person_id IN ({placeholders})", tuple(network_ids)
            ).fetchall()
        ]

    return jsonify(result)


@app.route("/api/people/<int:person_id>/touchpoints-legacy", methods=["POST"])
def touchpoint_api(person_id):
    ensure_db()
    db = get_db()
    payload = request.get_json(force=True)

    cur = db.execute(
        """
        INSERT INTO touchpoints (person_id, channel_type, channel_label, value, status, note, last_attempted)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            person_id,
            payload.get("channel_type", "Phone"),
            payload.get("channel_label", ""),
            payload.get("value", ""),
            payload.get("status", "Pending"),
            payload.get("note", ""),
            payload.get("last_attempted", ""),
        ),
    )
    db.commit()
    return jsonify({"touchpoint_id": cur.lastrowid}), 201


@app.route("/api/people/<int:person_id>/network")
def person_network_api(person_id):
    ensure_db()
    db = get_db()

    rows = db.execute(
        """
        SELECT r.*, p1.first_name AS subject_first, p1.last_name AS subject_last,
               p2.first_name AS related_first, p2.last_name AS related_last
        FROM person_relationships r
        JOIN people p1 ON p1.id = r.subject_person_id
        JOIN people p2 ON p2.id = r.related_person_id
        WHERE subject_person_id = ? OR related_person_id = ?
        ORDER BY r.created_at DESC
        """,
        (person_id, person_id),
    ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/web-search")
def web_search_api():
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify({"error": "q parameter is required"}), 400

    try:
        results = web_lookup(query)
        return jsonify({"query": query, "results": results})
    except requests.RequestException as exc:
        return jsonify({"query": query, "results": [], "error": str(exc)}), 502


@app.route("/api/obituary/extract", methods=["POST"])
def obituary_extract_api():
    payload = request.get_json(force=True)
    url = (payload.get("url") or "").strip()
    if not url:
        return jsonify({"error": "url is required"}), 400

    try:
        raw_text = fetch_obituary_text(url)
        parsed = parse_obituary_relationships(raw_text)
        return jsonify({"url": url, "parsed": parsed})
    except requests.RequestException as exc:
        return jsonify({"url": url, "error": str(exc)}), 502


@app.route("/api/obituary/ai-extract", methods=["POST"])
def obituary_ai_extract_api():
    payload = request.get_json(force=True)
    url = (payload.get("url") or "").strip()
    subject_name = (payload.get("subject_name") or "").strip()
    raw_text = (payload.get("raw_text") or "").strip()

    if not url and not raw_text:
        return jsonify({"error": "url or raw_text is required"}), 400

    try:
        obituary_text = raw_text or fetch_obituary_text(url)
        ai_result = call_openai_obituary_agent(obituary_text, subject_name=subject_name)
        normalized = normalize_ai_obituary_result(ai_result)
        return jsonify({
            "url": url,
            "subject_name": ai_result.get("subject_name", subject_name),
            "parsed": normalized,
            "raw": ai_result,
        })
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except requests.RequestException as exc:
        return jsonify({"error": str(exc)}), 502

@app.route("/api/properties/<int:property_id>/import-obituary", methods=["POST"])
def obituary_import_api(property_id):
    ensure_db()
    db = get_db()

    payload = request.get_json(force=True)
    url = (payload.get("url") or "").strip()
    subject_person_id = payload.get("subject_person_id")

    if not url or not subject_person_id:
        return jsonify({"error": "url and subject_person_id are required"}), 400

    try:
        summary = import_obituary_into_property(db, property_id, int(subject_person_id), url)
        db.commit()
        return jsonify(summary), 201
    except requests.RequestException as exc:
        db.rollback()
        return jsonify({"error": str(exc)}), 502


@app.route("/api/properties/<int:property_id>/import-obituary-ai", methods=["POST"])
def obituary_ai_import_api(property_id):
    ensure_db()
    db = get_db()

    payload = request.get_json(force=True)
    url = (payload.get("url") or "").strip()
    subject_person_id = payload.get("subject_person_id")
    subject_name = (payload.get("subject_name") or "").strip()

    if not url or not subject_person_id:
        return jsonify({"error": "url and subject_person_id are required"}), 400

    if not subject_name:
        person = db.execute("SELECT first_name, last_name FROM people WHERE id = ?", (subject_person_id,)).fetchone()
        if person:
            subject_name = f"{person['first_name']} {person['last_name']}".strip()

    try:
        summary = import_obituary_ai_into_property(
            db,
            property_id,
            int(subject_person_id),
            url,
            subject_name=subject_name,
        )
        db.commit()
        return jsonify(summary), 201
    except ValueError as exc:
        db.rollback()
        return jsonify({"error": str(exc)}), 400
    except requests.RequestException as exc:
        db.rollback()
        return jsonify({"error": str(exc)}), 502

@app.route("/api/properties/<int:property_id>/prospecting-snapshot")
def prospecting_snapshot_api(property_id):
    ensure_db()
    db = get_db()

    prop = db.execute(
        """
        SELECT p.id, p.status, p.notes,
               a.street, a.city, a.state, a.postal_code,
               op.first_name AS owner_first, op.last_name AS owner_last,
               rp.first_name AS resident_first, rp.last_name AS resident_last
        FROM properties p
        JOIN addresses a ON a.id = p.property_address_id
        LEFT JOIN people op ON op.id = p.owner_person_id
        LEFT JOIN people rp ON rp.id = p.resident_person_id
        WHERE p.id = ?
        """,
        (property_id,),
    ).fetchone()

    if prop is None:
        return jsonify({"error": "not found"}), 404

    target_name = " ".join([x for x in [prop["owner_first"], prop["owner_last"]] if x]).strip()
    search_query = f"{target_name} {prop['city']} {prop['state']} property owner"

    results = []
    error = None
    try:
        results = web_lookup(search_query)
    except requests.RequestException as exc:
        error = str(exc)

    return jsonify(
        {
            "property": dict(prop),
            "suggested_query": search_query,
            "web_results": results,
            "error": error,
        }
    )


@app.route("/webhooks/smrtphone/inbound", methods=["POST"])
def smrtphone_inbound_webhook():
    ensure_db()
    db = get_db()

    payload = request.get_json(silent=True) or request.form.to_dict() or {}
    sms_id = str(payload.get("smsId") or payload.get("sms_id") or payload.get("id") or "").strip()
    from_number = str(payload.get("from") or payload.get("from_number") or "").strip()
    to_number = str(payload.get("to") or payload.get("to_number") or "").strip()
    message = str(payload.get("message") or payload.get("body") or payload.get("text") or "").strip()
    status = str(payload.get("status") or "Received").strip().title()

    if not message:
        log_smrtphone_webhook_event(
            db,
            "inbound",
            payload,
            processing_status="error",
            sms_id=sms_id,
            from_number=from_number,
            to_number=to_number,
            error_text="message/body/text is required",
        )
        db.commit()
        return jsonify({"error": "message/body/text is required"}), 400

    person_id = find_person_id_by_phone(db, from_number)
    property_id = None
    if str(payload.get("property_id", "")).isdigit():
        property_id = int(payload["property_id"])
    elif person_id:
        property_id = find_property_id_for_person(db, person_id)
    if not property_id:
        log_smrtphone_webhook_event(
            db,
            "inbound",
            payload,
            processing_status="error",
            sms_id=sms_id,
            from_number=from_number,
            to_number=to_number,
            error_text="Unable to resolve property_id for inbound message",
        )
        db.commit()
        return jsonify({"error": "Unable to resolve property_id for inbound message"}), 400
    if not person_id:
        person_id = find_person_id_by_recent_outbound_to_number(db, property_id, from_number)

    existing = None
    if sms_id:
        existing = db.execute("SELECT id FROM communications WHERE external_id = ?", (sms_id,)).fetchone()
    if existing:
        update_person_outreach_status_for_sms(db, person_id, "inbound_received")
        log_smrtphone_webhook_event(
            db,
            "inbound",
            payload,
            processing_status="deduped",
            sms_id=sms_id,
            from_number=from_number,
            to_number=to_number,
            communication_id=existing["id"],
        )
        db.commit()
        return jsonify({"ok": True, "deduped": True, "communication_id": existing["id"]})

    # Handle provider behavior where one callback arrives without smsId and a second with smsId.
    recent = find_recent_inbound_match(
        db, property_id, person_id, from_number, to_number, message, seconds=150
    )
    if recent:
        update_person_outreach_status_for_sms(db, person_id, "inbound_received")
        if sms_id and not (recent["external_id"] or "").strip():
            db.execute(
                "UPDATE communications SET external_id = ?, status = ? WHERE id = ?",
                (sms_id, status, recent["id"]),
            )
        log_smrtphone_webhook_event(
            db,
            "inbound",
            payload,
            processing_status="deduped",
            sms_id=sms_id,
            from_number=from_number,
            to_number=to_number,
            communication_id=recent["id"],
        )
        db.commit()
        return jsonify({"ok": True, "deduped": True, "communication_id": recent["id"]})

    cur = db.execute(
        """
        INSERT INTO communications (property_id, person_id, channel, direction, from_number, to_number, body, status, is_read, sent_at, external_id)
        VALUES (?, ?, 'SMS', 'Inbound', ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            property_id,
            person_id,
            from_number,
            to_number,
            message,
            status,
            0,
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            sms_id,
        ),
    )
    update_person_outreach_status_for_sms(db, person_id, "inbound_received")
    log_smrtphone_webhook_event(
        db,
        "inbound",
        payload,
        processing_status="stored",
        sms_id=sms_id,
        from_number=from_number,
        to_number=to_number,
        communication_id=cur.lastrowid,
    )
    db.commit()
    return jsonify({"ok": True, "communication_id": cur.lastrowid}), 201


@app.route("/webhooks/smrtphone/status", methods=["POST"])
def smrtphone_status_webhook():
    ensure_db()
    db = get_db()

    payload = request.get_json(silent=True) or request.form.to_dict() or {}
    sms_id = str(payload.get("smsId") or payload.get("sms_id") or payload.get("id") or "").strip()
    status = str(payload.get("status") or "").strip().title()
    to_number = str(payload.get("to") or payload.get("to_number") or "").strip()
    from_number = str(payload.get("from") or payload.get("from_number") or "").strip()

    if not sms_id:
        log_smrtphone_webhook_event(
            db,
            "status",
            payload,
            processing_status="error",
            error_text="smsId is required",
            from_number=from_number,
            to_number=to_number,
        )
        db.commit()
        return jsonify({"ok": True, "ignored": True, "reason": "missing smsId"}), 200
    if not status:
        log_smrtphone_webhook_event(
            db,
            "status",
            payload,
            processing_status="error",
            sms_id=sms_id,
            from_number=from_number,
            to_number=to_number,
            error_text="status is required",
        )
        db.commit()
        return jsonify({"ok": True, "ignored": True, "reason": "missing status"}), 200

    row = db.execute("SELECT id FROM communications WHERE external_id = ?", (sms_id,)).fetchone()
    if row:
        db.execute(
            "UPDATE communications SET status = ?, to_number = COALESCE(NULLIF(?, ''), to_number), from_number = COALESCE(NULLIF(?, ''), from_number) WHERE id = ?",
            (status, to_number, from_number, row["id"]),
        )
        if to_number:
            apply_touchpoint_status_inference(db, to_number, status)
        log_smrtphone_webhook_event(
            db,
            "status",
            payload,
            processing_status="updated",
            sms_id=sms_id,
            from_number=from_number,
            to_number=to_number,
            communication_id=row["id"],
        )
        db.commit()
        return jsonify({"ok": True, "communication_id": row["id"]})

    # Backfill case: sent row exists without external_id, then provider status arrives with smsId.
    body = str(payload.get("message") or payload.get("body") or "").strip()
    recent_outbound = db.execute(
        """
        SELECT id
        FROM communications
        WHERE upper(channel) = 'SMS'
          AND lower(direction) = 'outbound'
          AND COALESCE(external_id, '') = ''
          AND COALESCE(to_number, '') = COALESCE(?, '')
          AND (? = '' OR COALESCE(body, '') = ?)
          AND sent_at >= ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (to_number, body, body, format_db_time(datetime.utcnow() - timedelta(minutes=30))),
    ).fetchone()
    if recent_outbound:
        db.execute(
            """
            UPDATE communications
            SET external_id = ?, status = ?, to_number = COALESCE(NULLIF(?, ''), to_number), from_number = COALESCE(NULLIF(?, ''), from_number)
            WHERE id = ?
            """,
            (sms_id, status, to_number, from_number, recent_outbound["id"]),
        )
        if to_number:
            apply_touchpoint_status_inference(db, to_number, status)
        log_smrtphone_webhook_event(
            db,
            "status",
            payload,
            processing_status="backfilled",
            sms_id=sms_id,
            from_number=from_number,
            to_number=to_number,
            communication_id=recent_outbound["id"],
        )
        db.commit()
        return jsonify({"ok": True, "communication_id": recent_outbound["id"], "backfilled": True})

    if not str(payload.get("property_id", "")).isdigit():
        log_smrtphone_webhook_event(
            db,
            "status",
            payload,
            processing_status="orphan",
            sms_id=sms_id,
            from_number=from_number,
            to_number=to_number,
            error_text="Unknown smsId; property_id required to create status record",
        )
        db.commit()
        return jsonify({"ok": True, "ignored": True, "reason": "unknown smsId"}), 200

    property_id = int(payload["property_id"])
    person_id = None
    if str(payload.get("person_id", "")).isdigit():
        person_id = int(payload["person_id"])
    elif to_number:
        person_id = find_person_id_by_phone(db, to_number)

    cur = db.execute(
        """
        INSERT INTO communications (property_id, person_id, channel, direction, from_number, to_number, body, status, is_read, sent_at, external_id)
        VALUES (?, ?, 'SMS', 'Outbound', ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            property_id,
            person_id,
            from_number,
            to_number,
            body,
            status,
            1,
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            sms_id,
        ),
    )
    if to_number:
        apply_touchpoint_status_inference(db, to_number, status)
    log_smrtphone_webhook_event(
        db,
        "status",
        payload,
        processing_status="stored",
        sms_id=sms_id,
        from_number=from_number,
        to_number=to_number,
        communication_id=cur.lastrowid,
    )
    db.commit()
    return jsonify({"ok": True, "communication_id": cur.lastrowid}), 201


@app.route("/api/notifications/sms-unread", methods=["GET"])
def unread_sms_notifications():
    ensure_db()
    db = get_db()
    rows = db.execute(
        """
        SELECT id, property_id, person_id, from_number, to_number, status, body, sent_at
        FROM communications
        WHERE upper(channel) = 'SMS' AND lower(direction) = 'inbound' AND COALESCE(is_read, 1) = 0
        ORDER BY sent_at DESC, id DESC
        LIMIT 50
        """
    ).fetchall()
    items = [dict(r) for r in rows]
    return jsonify({"count": len(items), "items": items})


@app.route("/api/notifications/sms-unread/<int:communication_id>/read", methods=["POST"])
def mark_sms_notification_read(communication_id):
    ensure_db()
    db = get_db()
    db.execute("UPDATE communications SET is_read = 1 WHERE id = ?", (communication_id,))
    db.commit()
    return jsonify({"ok": True})


@app.route("/api/webhooks/smrtphone/events", methods=["GET"])
def smrtphone_webhook_events_api():
    ensure_db()
    db = get_db()
    limit = request.args.get("limit", "50").strip()
    try:
        n = max(1, min(500, int(limit)))
    except ValueError:
        n = 50

    rows = db.execute(
        """
        SELECT *
        FROM smrtphone_webhook_events
        ORDER BY id DESC
        LIMIT ?
        """,
        (n,),
    ).fetchall()
    return jsonify([dict(r) for r in rows])


if __name__ == "__main__":
    debug_mode = True
    ensure_db()
    if (not debug_mode) or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        start_bulk_sms_worker()
    app.run(debug=debug_mode)


















