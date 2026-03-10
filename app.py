
import csv
import base64
import hmac
import hashlib
import io
import json
import imaplib
import mimetypes
import os
import random
import re
import secrets
import sqlite3
import socket
import smtplib
import ssl
import threading
import time
import traceback
from urllib.parse import quote, quote_plus, urlsplit
from datetime import datetime, timedelta, timezone
from email import message_from_bytes
from email.header import decode_header, make_header
from email.message import EmailMessage
from email.utils import make_msgid
from html import unescape
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests
from flask import Flask, g, jsonify, redirect, render_template, request, session, url_for
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.security import check_password_hash
from werkzeug.exceptions import HTTPException
try:
    from google.oauth2 import service_account
    from google.auth.transport.requests import Request as GoogleAuthRequest
except Exception:
    service_account = None
    GoogleAuthRequest = None

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.getenv("CRM_DB_PATH", str(BASE_DIR / "crm.db"))).expanduser()
SCHEMA_PATH = BASE_DIR / "schema.sql"


def load_env_file(path: Path):
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        os.environ.setdefault(key, value)


load_env_file(BASE_DIR / ".env")


def open_sqlite_connection():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def env_flag(name, default=False):
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
app.config["SECRET_KEY"] = (
    os.getenv("FLASK_SECRET_KEY", "").strip()
    or os.getenv("SECRET_KEY", "").strip()
    or os.urandom(32).hex()
)
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = env_flag("SESSION_COOKIE_SECURE", False)

SMRTPHONE_SEND_URL = os.getenv("SMRTPHONE_SEND_URL", "https://phone.smrt.studio/sms/send").strip()
SMRTPHONE_FROM_NUMBER = os.getenv("SMRTPHONE_FROM_NUMBER", "19088679098").strip() or "19088679098"
SKIPSHERPA_BASE_URL = "https://skipsherpa.com"
SKIPSHERPA_API_KEY = os.getenv("SKIPSHERPA_API_KEY", "").strip()
REISIFT_BASE_URL = os.getenv("REISIFT_BASE_URL", "https://apiv2.reisift.io")
REISIFT_MAP_BASE_URL = os.getenv("REISIFT_MAP_BASE_URL", "https://map.reisift.io")
REISIFT_UI_VERSION = "2022.02.01.7"
REISIFT_FOLLOWUPS_EXCLUDE_TAG = os.getenv("REISIFT_FOLLOWUPS_EXCLUDE_TAG", "3cf5a950-ac8f-47b0-87e1-bcea2604e2e1").strip()
REISIFT_DISABLE_MAP_LOOKUP = env_flag("REISIFT_DISABLE_MAP_LOOKUP", True)
OPENLETTERCONNECT_BASE_URL = os.getenv("OPENLETTERCONNECT_BASE_URL", "https://api.openletterconnect.com/api/v1")
OPENLETTERCONNECT_TEMPLATE_ID = int(os.getenv("OPENLETTERCONNECT_TEMPLATE_ID", "9256"))
CALL_RECORDING_WORKER_ENABLED = env_flag("CALL_RECORDING_WORKER_ENABLED", True)
CALL_RECORDING_POLL_SECONDS = max(int((os.getenv("CALL_RECORDING_POLL_SECONDS") or "60").strip() or "60"), 15)
SMS_ANALYSIS_WORKER_ENABLED = env_flag("SMS_ANALYSIS_WORKER_ENABLED", True)
SMS_ANALYSIS_POLL_SECONDS = max(int((os.getenv("SMS_ANALYSIS_POLL_SECONDS") or "180").strip() or "180"), 30)
CALL_ANALYSIS_MAX_RETRIES = max(int((os.getenv("CALL_ANALYSIS_MAX_RETRIES") or "3").strip() or "3"), 1)
APP_AUTH_ENABLED = env_flag("APP_AUTH_ENABLED", False)
APP_AUTH_USERNAME = os.getenv("APP_AUTH_USERNAME", "").strip()
APP_AUTH_PASSWORD = os.getenv("APP_AUTH_PASSWORD", "")
APP_AUTH_PASSWORD_HASH = os.getenv("APP_AUTH_PASSWORD_HASH", "").strip()
RUN_BACKGROUND_WORKERS = env_flag("RUN_BACKGROUND_WORKERS", True)
REISIFT_TASK_WRITE_ENABLED = env_flag("REISIFT_TASK_WRITE_ENABLED", False)
GOOGLE_SHEETS_SPREADSHEET_ID = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip()
GOOGLE_SHEETS_WORKSHEET_NAME = os.getenv("GOOGLE_SHEETS_WORKSHEET_NAME", "List of Customers").strip() or "List of Customers"
GOOGLE_SHEETS_POLL_SECONDS = max(int((os.getenv("GOOGLE_SHEETS_POLL_SECONDS") or "300").strip() or "300"), 60)
GOOGLE_SHEETS_POLL_MODE = (os.getenv("GOOGLE_SHEETS_POLL_MODE") or "latest_only").strip().lower()
CLEVER_LEADS_EVENT_SOURCE = "clever_leads_ingest"
try:
    EST_TZ = ZoneInfo("America/New_York")
except ZoneInfoNotFoundError:
    EST_TZ = timezone(timedelta(hours=-5))
BULK_SMS_WORKER_STARTED = False
EMAIL_POLL_WORKER_STARTED = False
CLEVER_LEADS_WORKER_STARTED = False
REFERRAL_MARKET_WORKER_STARTED = False
CALL_RECORDING_WORKER_STARTED = False
SMS_ANALYSIS_WORKER_STARTED = False
NJ_COUNTIES = [
    "Atlantic",
    "Bergen",
    "Burlington",
    "Camden",
    "Cape May",
    "Cumberland",
    "Essex",
    "Gloucester",
    "Hudson",
    "Hunterdon",
    "Mercer",
    "Middlesex",
    "Monmouth",
    "Morris",
    "Ocean",
    "Passaic",
    "Salem",
    "Somerset",
    "Sussex",
    "Union",
    "Warren",
]

AGENT_DEFINITIONS = [
    {
        "key": "conversation_closer",
        "name": "Conversation Closer Agent",
        "objective": "Own outbound/inbound SMS, email, and direct-mail orchestration with human-like intent to secure a live phone call with acquisitions.",
        "inputs": "People, touchpoints, communications, campaign rules, and property context.",
        "outputs": "Suggested or automated responses, sentiment labels, next-step intents, and escalation recommendations.",
        "status": "Planned",
    },
    {
        "key": "underwriting",
        "name": "Underwriting Agent",
        "objective": "Estimate listing value, cash offer ranges, and confidence bands from available property data and comparables.",
        "inputs": "Property profile, historical sales, condition notes, and comp sources.",
        "outputs": "Value range, offer range, assumptions, and confidence scores.",
        "status": "Planned",
    },
    {
        "key": "lead_intake",
        "name": "Lead Intake Agent",
        "objective": "Collect internet leads, clean/normalize data, deduplicate, and push qualified records into ReiSift and Deep Dive.",
        "inputs": "Webhooks, sheet feeds, scraped/public data, import files.",
        "outputs": "Normalized lead packets, source-attribution notes, and ingestion outcomes.",
        "status": "In Progress",
    },
    {
        "key": "lead_monitor",
        "name": "Lead Monitor Agent",
        "objective": "Track lead lifecycle and identify stalled steps: missing offers, pending deadlines, and follow-up gaps.",
        "inputs": "ReiSift statuses, communication activity, tasks, and timelines.",
        "outputs": "Alerts, action queue, and recommended owner/relative escalation.",
        "status": "In Progress",
    },
    {
        "key": "advisor_synthesizer",
        "name": "Advisor Synthesizer Agent",
        "objective": "Consolidate call/SMS analysis and lead-management actions into ranked recommendations with on-demand Q&A.",
        "inputs": "Agent signals, call jobs, lead action queue, and recent run history.",
        "outputs": "Executive summary, top priorities, risks, and question-driven guidance.",
        "status": "In Progress",
    },
    {
        "key": "referral_orchestrator",
        "name": "Referral Orchestrator Agent",
        "objective": "Monitor Clever referral leads, ensure realtor outreach progress, and trigger next routing when engagement is missing.",
        "inputs": "Referral queue, realtor routing queue, communication outcomes, and on-market status.",
        "outputs": "Auto-send queue decisions, escalation prompts, and referral health summaries.",
        "status": "In Progress",
    },
]
REFERRAL_STATUSES = ["Untouched", "Referred", "Under Contract", "Dead", "Other"]
LEAD_ACTION_STATUSES = ["Pending", "Approved", "Completed", "Dismissed"]
LEAD_ACTION_PRIORITIES = ["Low", "Medium", "High"]
LEAD_STOP_STATUSES = {"not interested", "dnc", "opt out", "dead"}
LEAD_NEGATIVE_INTENT_TOKENS = [
    "not interested",
    "stop",
    "remove",
    "do not contact",
    "wrong person",
]
ACTIVE_LEAD_STATUSES = {
    "new lead",
    "new_lead",
    "ghosting lead",
    "hot lead",
    "warm lead",
    "cold lead",
    "no contact new lead",
    "nurture new lead",
}
NO_ANSWER_OUTCOMES = {"missed", "no_answer", "failed", "busy", "no answer"}

REISIFT_PLAYBOOK_PRIORITY = {
    "new lead": {
        "ScheduleAppointment": "High",
        "FollowUpTouchDue": "High",
        "EscalateToRelativeOutreach": "High",
    },
    "no contact new lead": {
        "ScheduleAppointment": "High",
        "FollowUpTouchDue": "High",
        "EscalateToRelativeOutreach": "High",
    },
    "hot lead": {
        "ScheduleAppointment": "High",
        "FollowUpTouchDue": "High",
        "EscalateToRelativeOutreach": "High",
    },
    "warm lead": {
        "ScheduleAppointment": "High",
        "FollowUpTouchDue": "Medium",
        "EscalateToRelativeOutreach": "High",
    },
    "cold lead": {
        "ScheduleAppointment": "Medium",
        "FollowUpTouchDue": "Low",
        "EscalateToRelativeOutreach": "Medium",
    },
    "ghosting lead": {
        "ScheduleAppointment": "Medium",
        "FollowUpTouchDue": "Medium",
        "EscalateToRelativeOutreach": "High",
    },
    "nurture new lead": {
        "ScheduleAppointment": "Medium",
        "FollowUpTouchDue": "Low",
        "EscalateToRelativeOutreach": "Medium",
    },
    "refer lead": {
        "ScheduleAppointment": "Low",
        "FollowUpTouchDue": "Low",
        "EscalateToRelativeOutreach": "Low",
    },
    "listed": {
        "ScheduleAppointment": "Low",
        "FollowUpTouchDue": "Low",
        "EscalateToRelativeOutreach": "Low",
    },
    "dead lead": {
        "ScheduleAppointment": "Low",
        "FollowUpTouchDue": "Low",
        "EscalateToRelativeOutreach": "Low",
    },
    "not interested": {
        "ScheduleAppointment": "Low",
        "FollowUpTouchDue": "Low",
        "EscalateToRelativeOutreach": "Low",
    },
    "opt-out": {
        "ScheduleAppointment": "Low",
        "FollowUpTouchDue": "Low",
        "EscalateToRelativeOutreach": "Low",
    },
}


def format_phone_display(value):
    raw = (value or "").strip()
    digits = re.sub(r"\D+", "", raw)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) == 10:
        return f"({digits[0:3]}){digits[3:6]}-{digits[6:10]}"
    return raw


app.jinja_env.filters["fmt_phone"] = format_phone_display


def parse_json_object(raw, default=None):
    if isinstance(raw, dict):
        return raw
    if raw is None:
        return {} if default is None else default
    text = str(raw).strip()
    if not text:
        return {} if default is None else default
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else ({} if default is None else default)
    except Exception:
        return {} if default is None else default


def parse_flexible_datetime(value):
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(text[:19], fmt)
        except ValueError:
            continue
    try:
        normalized = text.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is not None:
            return dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except ValueError:
        return None


def format_long_date(value):
    dt = parse_flexible_datetime(value)
    if dt is None:
        return value or "-"
    return f"{dt.strftime('%A, %B')} {dt.day}, {dt.year}"


def format_est_datetime(value):
    dt = parse_flexible_datetime(value)
    if dt is None:
        return value or "-"
    dt_utc = dt.replace(tzinfo=timezone.utc)
    dt_est = dt_utc.astimezone(EST_TZ)
    return f"{dt_est.strftime('%A, %B')} {dt_est.day}, {dt_est.year} {dt_est.strftime('%I:%M %p')} ET"


app.jinja_env.filters["fmt_long_date"] = format_long_date
app.jinja_env.filters["fmt_est_datetime"] = format_est_datetime


def linkify_note(value):
    text = (value or "").strip()
    if not text:
        return ""
    safe = (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )
    safe = re.sub(
        r"([A-Za-z][A-Za-z0-9\.\-'\s]{1,80})\s+\((/person/\d+(?:\?property_id=\d+)?)\)",
        r'<a href="\2">\1</a>',
        safe,
    )
    safe = re.sub(
        r"(?<![\w\"'=])(https?://[^\s<]+)",
        r'<a href="\1" target="_blank">\1</a>',
        safe,
    )
    safe = re.sub(
        r"(?<![\w\"'=])(/person/\d+(?:\?property_id=\d+)?)",
        r'<a href="\1">\1</a>',
        safe,
    )
    return safe


app.jinja_env.filters["linkify_note"] = linkify_note


def app_auth_is_configured():
    if not APP_AUTH_ENABLED:
        return False
    if not APP_AUTH_USERNAME:
        return False
    return bool(APP_AUTH_PASSWORD_HASH or APP_AUTH_PASSWORD)


def verify_app_login(username, password):
    if not app_auth_is_configured():
        return False
    if not hmac.compare_digest((username or "").strip(), APP_AUTH_USERNAME):
        return False
    candidate = password or ""
    if APP_AUTH_PASSWORD_HASH:
        try:
            return check_password_hash(APP_AUTH_PASSWORD_HASH, candidate)
        except Exception:
            return False
    return hmac.compare_digest(candidate, APP_AUTH_PASSWORD)


def is_safe_next_path(value):
    target = (value or "").strip()
    if not target:
        return False
    if target.startswith("//"):
        return False
    parsed = urlsplit(target)
    if parsed.scheme or parsed.netloc:
        return False
    return target.startswith("/")


@app.context_processor
def inject_auth_state():
    return {
        "auth_enabled": APP_AUTH_ENABLED,
        "auth_user": session.get("auth_user", ""),
    }


@app.before_request
def require_login_if_enabled():
    if RUN_BACKGROUND_WORKERS:
        # Start idempotent worker threads when running under Gunicorn.
        start_bulk_sms_worker()
        start_email_poll_worker()
        start_clever_leads_worker()
        start_referral_on_market_worker()
        start_call_recording_worker()
        start_sms_analysis_worker()
    if not APP_AUTH_ENABLED:
        return None
    endpoint = request.endpoint or ""
    if endpoint in {"login", "logout", "static"}:
        return None
    if request.path.startswith("/static/") or request.path == "/healthz":
        return None
    # Third-party callbacks must stay unauthenticated.
    if request.path.startswith("/webhooks/"):
        return None
    # External integrations authenticate via API key header.
    if request.path.startswith("/api/integrations/"):
        return None
    if session.get("auth_ok"):
        return None
    if request.path.startswith("/api/"):
        return jsonify({"error": "Authentication required"}), 401
    next_target = request.full_path if request.query_string else request.path
    return redirect(url_for("login", next=next_target))


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
        g.db = open_sqlite_connection()
    return g.db


def execute_with_retry(db, sql, params=(), retries=8, base_delay=0.2):
    for attempt in range(retries):
        try:
            return db.execute(sql, params)
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or attempt >= retries - 1:
                raise
            time.sleep(base_delay * (attempt + 1))


def commit_with_retry(db, retries=8, base_delay=0.2):
    for attempt in range(retries):
        try:
            db.commit()
            return
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or attempt >= retries - 1:
                raise
            time.sleep(base_delay * (attempt + 1))


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
    ensure_column(db, "people", "middle_name", "middle_name TEXT")
    ensure_column(db, "communications", "from_number", "from_number TEXT")
    ensure_column(db, "communications", "to_number", "to_number TEXT")
    ensure_column(db, "communications", "is_read", "is_read INTEGER NOT NULL DEFAULT 1")
    ensure_column(db, "communications", "gmail_msgid", "gmail_msgid TEXT")
    ensure_column(db, "communications", "gmail_thread_id", "gmail_thread_id TEXT")
    ensure_column(db, "communications", "in_reply_to", "in_reply_to TEXT")
    ensure_column(db, "communications", "open_tracking_token", "open_tracking_token TEXT")
    ensure_column(db, "communications", "opened_at", "opened_at TEXT")
    ensure_column(db, "communications", "open_count", "open_count INTEGER NOT NULL DEFAULT 0")
    ensure_column(db, "people", "age", "age INTEGER")
    ensure_column(db, "people", "deceased", "deceased INTEGER NOT NULL DEFAULT 0")
    ensure_column(db, "people", "birth_year", "birth_year TEXT")
    ensure_column(db, "people", "deceased_date", "deceased_date TEXT")
    ensure_column(db, "people", "bankruptcy", "bankruptcy INTEGER NOT NULL DEFAULT 0")
    ensure_column(db, "people", "employer", "employer TEXT")
    ensure_column(db, "person_relationships", "relationship_order", "relationship_order INTEGER NOT NULL DEFAULT 0")
    ensure_column(db, "properties", "attom_last_sold_date", "attom_last_sold_date TEXT")
    ensure_column(db, "properties", "attom_last_sold_price", "attom_last_sold_price REAL")
    ensure_column(db, "reisift_referrals", "referral_status", "referral_status TEXT NOT NULL DEFAULT 'Untouched'")
    ensure_column(db, "reisift_referrals", "winning_realtor_id", "winning_realtor_id INTEGER")
    ensure_column(db, "reisift_referrals", "referral_notes", "referral_notes TEXT")
    ensure_column(db, "reisift_referrals", "county", "county TEXT")
    ensure_column(db, "reisift_referrals", "on_market_status", "on_market_status TEXT NOT NULL DEFAULT 'Unknown'")
    ensure_column(db, "reisift_followups", "events_json", "events_json TEXT")
    ensure_column(db, "reisift_followups", "tasks_json", "tasks_json TEXT")
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS app_errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            route TEXT,
            status_code INTEGER,
            error_message TEXT NOT NULL,
            details TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS obituary_expansion_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            property_id INTEGER NOT NULL,
            root_subject_person_id INTEGER NOT NULL,
            root_source_url TEXT NOT NULL,
            max_depth INTEGER NOT NULL DEFAULT 1,
            expand_deceased INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'Running',
            summary_json TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            completed_at TEXT
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS obituary_expansion_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            property_id INTEGER NOT NULL,
            subject_person_id INTEGER NOT NULL,
            related_person_id INTEGER,
            depth INTEGER NOT NULL DEFAULT 1,
            person_name TEXT,
            relationship_type TEXT,
            relative_status TEXT,
            confidence REAL,
            processing_status TEXT NOT NULL DEFAULT 'Imported',
            source_url TEXT,
            note TEXT,
            details_json TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS integration_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            event_key TEXT,
            payload_json TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source, event_key)
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS referral_auto_send_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            property_uuid TEXT NOT NULL,
            realtor_id INTEGER,
            county TEXT,
            queue_status TEXT NOT NULL DEFAULT 'Queued',
            note TEXT,
            payload_json TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS clever_lead_webhook_notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_key TEXT,
            note_body TEXT NOT NULL,
            payload_json TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS lead_monitor_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL DEFAULT 'manual',
            property_id INTEGER,
            status TEXT NOT NULL DEFAULT 'Running',
            summary_json TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            completed_at TEXT
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS lead_management_actions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dedupe_key TEXT NOT NULL UNIQUE,
            property_id INTEGER NOT NULL,
            person_id INTEGER,
            action_type TEXT NOT NULL,
            priority TEXT NOT NULL DEFAULT 'Medium',
            status TEXT NOT NULL DEFAULT 'Pending',
            reason TEXT,
            payload_json TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    db.execute("CREATE INDEX IF NOT EXISTS idx_lma_property_status ON lead_management_actions(property_id, status)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_lma_status_priority ON lead_management_actions(status, priority)")
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS call_recording_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            call_sid TEXT NOT NULL UNIQUE,
            from_number TEXT,
            to_number TEXT,
            property_id INTEGER,
            person_id INTEGER,
            recording_url TEXT,
            fetch_status TEXT NOT NULL DEFAULT 'Queued',
            analysis_status TEXT NOT NULL DEFAULT 'Pending',
            transcript_text TEXT,
            summary_text TEXT,
            payload_json TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    ensure_column(db, "call_recording_jobs", "analysis_json", "analysis_json TEXT")
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS agent_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_type TEXT NOT NULL,
            source_key TEXT NOT NULL UNIQUE,
            property_id INTEGER,
            person_id INTEGER,
            channel TEXT,
            intent TEXT,
            sentiment TEXT,
            confidence REAL,
            recommended_next_step TEXT,
            summary_text TEXT,
            is_spam INTEGER NOT NULL DEFAULT 0,
            do_not_contact INTEGER NOT NULL DEFAULT 0,
            routing_status TEXT NOT NULL DEFAULT 'new',
            payload_json TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    db.execute("CREATE INDEX IF NOT EXISTS idx_agent_signals_routing ON agent_signals(routing_status, source_type)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_agent_signals_prop_person ON agent_signals(property_id, person_id)")
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS sms_thread_state (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            thread_key TEXT NOT NULL UNIQUE,
            property_id INTEGER,
            person_id INTEGER,
            counterpart_number TEXT,
            last_message_at TEXT,
            message_count INTEGER NOT NULL DEFAULT 0,
            transcript_hash TEXT,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS agent_advisor_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            log_type TEXT NOT NULL,
            question TEXT,
            response_text TEXT,
            response_json TEXT,
            snapshot_json TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    db.execute("CREATE INDEX IF NOT EXISTS idx_agent_advisor_logs_type ON agent_advisor_logs(log_type, id DESC)")


def log_app_error(db, source, error_message, details="", route="", status_code=None):
    try:
        db.execute(
            """
            INSERT INTO app_errors (source, route, status_code, error_message, details)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                (source or "app")[:80],
                (route or "")[:255],
                status_code,
                str(error_message or "")[:2000],
                str(details or "")[:20000],
            ),
        )
    except Exception:
        pass


def init_db():
    db = open_sqlite_connection()
    migrate_db(db)
    normalize_people_name_data(db)
    ensure_default_sequence_campaign(db)

    has_properties = db.execute("SELECT COUNT(*) AS c FROM properties").fetchone()["c"]
    if not has_properties:
        seed_database(db)

    db.commit()
    db.close()


def ensure_db():
    if not DB_PATH.exists():
        init_db()
        return

    db = open_sqlite_connection()
    migrate_db(db)
    normalize_people_name_data(db)
    ensure_default_sequence_campaign(db)
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


def create_person(db, first_name, last_name, middle_name="", phone="", email="", notes=""):
    cur = db.execute(
        """
        INSERT INTO people (first_name, middle_name, last_name, primary_phone, primary_email, notes)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            first_name.strip() or "Unknown",
            middle_name.strip(),
            last_name.strip() or "Person",
            phone.strip(),
            email.strip(),
            notes.strip(),
        ),
    )
    return cur.lastrowid


def person_name(person_row):
    parts = [
        (person_row["first_name"] or "").strip(),
        (person_row["middle_name"] or "").strip(),
        (person_row["last_name"] or "").strip(),
    ]
    return " ".join([p for p in parts if p]).strip()


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


def extract_first_string_by_keys(payload, keys):
    keyset = {str(k).strip().lower() for k in (keys or []) if str(k).strip()}
    if not keyset:
        return ""
    queue = [payload]
    while queue:
        current = queue.pop(0)
        if isinstance(current, dict):
            for k, v in current.items():
                if str(k).strip().lower() in keyset:
                    if isinstance(v, str) and v.strip():
                        return v.strip()
                    if v is not None and not isinstance(v, (dict, list)):
                        text = str(v).strip()
                        if text:
                            return text
            for v in current.values():
                if isinstance(v, (dict, list)):
                    queue.append(v)
        elif isinstance(current, list):
            for item in current:
                if isinstance(item, (dict, list)):
                    queue.append(item)
    return ""


def send_smrtphone_sms(to_number, message_body, from_number=None):
    api_key = os.getenv("SMRTPHONE_API_KEY", "").strip()
    if not api_key:
        raise ValueError("SMRTPHONE_API_KEY is not set")

    headers = {
        "X-Auth-smrtPhone": api_key,
    }
    payload = {
        "from": (from_number or SMRTPHONE_FROM_NUMBER).strip(),
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


def fetch_smrtphone_recording_url(call_sid):
    api_key = os.getenv("SMRTPHONE_API_KEY", "").strip()
    if not api_key:
        raise ValueError("SMRTPHONE_API_KEY is not set")
    clean_sid = (call_sid or "").strip()
    if not clean_sid:
        raise ValueError("call_sid is required")
    headers = {"X-Auth-smrtPhone": api_key}
    response = requests.get(
        "https://phone.smrt.studio/api/getRecordingUrl",
        headers=headers,
        params={"call_sid": clean_sid},
        timeout=20,
    )
    try:
        body = response.json()
    except ValueError:
        body = {"raw_text": response.text}
    if not response.ok:
        raise ValueError(f"SmrtPhone recording lookup failed ({response.status_code}): {body}")
    recording_url = ""
    if isinstance(body, dict):
        for key in ("recording_url", "recordingUrl", "url", "recording"):
            val = body.get(key)
            if isinstance(val, str) and val.strip():
                recording_url = val.strip()
                break
        if not recording_url:
            data = body.get("data")
            if isinstance(data, dict):
                for key in ("recording_url", "recordingUrl", "url", "recording"):
                    val = data.get(key)
                    if isinstance(val, str) and val.strip():
                        recording_url = val.strip()
                        break
    return {"call_sid": clean_sid, "recording_url": recording_url, "raw": body}


def transcribe_recording_with_openai(audio_bytes, filename="call_recording.mp3", mime_type="audio/mpeg"):
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise ValueError("OPENAI_API_KEY is not set")
    if not audio_bytes:
        raise ValueError("audio_bytes is empty")
    model = os.getenv("OPENAI_TRANSCRIBE_MODEL", "gpt-4o-mini-transcribe").strip() or "gpt-4o-mini-transcribe"
    files = {
        "file": (filename, io.BytesIO(audio_bytes), mime_type or "application/octet-stream"),
    }
    data = {"model": model}
    response = requests.post(
        "https://api.openai.com/v1/audio/transcriptions",
        headers={"Authorization": f"Bearer {api_key}"},
        data=data,
        files=files,
        timeout=120,
    )
    response.raise_for_status()
    payload = response.json()
    transcript = str(payload.get("text") or "").strip()
    if not transcript:
        raise ValueError("No transcript text returned")
    return {"transcript": transcript, "raw": payload}


def analyze_call_transcript_with_openai(transcript_text, property_id=None, person_id=None):
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise ValueError("OPENAI_API_KEY is not set")
    transcript = (transcript_text or "").strip()
    if not transcript:
        raise ValueError("transcript_text is required")
    model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini").strip() or "gpt-4.1-mini"
    schema = {
        "name": "call_analysis",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "summary": {"type": "string"},
                "caller_intent": {"type": "string"},
                "seller_interest": {
                    "type": "string",
                    "enum": ["High", "Medium", "Low", "Not Interested", "Unknown"],
                },
                "sentiment": {
                    "type": "string",
                    "enum": ["Positive", "Neutral", "Negative", "Mixed", "Unknown"],
                },
                "key_facts": {"type": "array", "items": {"type": "string"}},
                "objections": {"type": "array", "items": {"type": "string"}},
                "next_best_step": {
                    "type": "string",
                    "enum": [
                        "Schedule Appointment",
                        "Follow Up",
                        "Escalate To Relative",
                        "Deep Dive",
                        "Mark Not Interested",
                        "Mark Opt Out",
                        "No Action",
                    ],
                },
                "next_step_reason": {"type": "string"},
                "caller_improvement": {"type": "array", "items": {"type": "string"}},
                "confidence": {"type": "number"},
            },
            "required": [
                "summary",
                "caller_intent",
                "seller_interest",
                "sentiment",
                "key_facts",
                "objections",
                "next_best_step",
                "next_step_reason",
                "caller_improvement",
                "confidence",
            ],
        },
        "strict": True,
    }
    user_prompt = (
        "Analyze this call transcript for lead management.\n"
        f"Property ID: {property_id or '-'} | Person ID: {person_id or '-'}\n\n"
        "Return concise, factual JSON only. Focus on intent, outcome, and practical next step.\n\n"
        f"Transcript:\n{transcript}"
    )
    response = requests.post(
        "https://api.openai.com/v1/responses",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": model,
            "input": [
                {
                    "role": "system",
                    "content": [
                        {
                            "type": "input_text",
                            "text": "You are a call QA and lead-management analyst for a real estate CRM.",
                        }
                    ],
                },
                {"role": "user", "content": [{"type": "input_text", "text": user_prompt}]},
            ],
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": schema["name"],
                    "schema": schema["schema"],
                    "strict": True,
                }
            },
        },
        timeout=60,
    )
    response.raise_for_status()
    data = response.json()
    output_text = (data.get("output_text") or "").strip()
    if output_text:
        return json.loads(output_text)
    for item in data.get("output", []):
        for part in item.get("content", []):
            if part.get("type") in {"output_text", "text"} and part.get("text"):
                return json.loads(part["text"])
    raise ValueError("No JSON output returned by call analyzer")


def call_skipsherpa_person_lookup(first_name, last_name, street, city="", state="", zipcode="", middle_name=""):
    if not SKIPSHERPA_API_KEY:
        raise ValueError("SKIPSHERPA_API_KEY is not set")
    first_name, middle_name, last_name = normalize_first_middle_last(first_name, middle_name, last_name)

    payload = {
        "person_lookups": [
            {
                    "first_name": (first_name or "").strip() or None,
                    "middle_name": (middle_name or "").strip() or None,
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


def call_skipsherpa_property_lookup(street, city="", state="", zipcode=""):
    if not SKIPSHERPA_API_KEY:
        raise ValueError("SKIPSHERPA_API_KEY is not set")

    payload = {
        "property_lookups": [
            {
                "property_address_lookup": {
                    "street": (street or "").strip(),
                    "city": (city or "").strip() or None,
                    "state": (state or "").strip() or None,
                    "zipcode": (zipcode or "").strip() or None,
                }
            }
        ]
    }
    response = requests.put(
        f"{SKIPSHERPA_BASE_URL}/api/beta6/properties",
        headers={"API-Key": SKIPSHERPA_API_KEY, "Content-Type": "application/json"},
        json=payload,
        timeout=60,
    )
    try:
        data = response.json()
    except ValueError:
        data = {"raw_text": response.text}
    if not response.ok:
        raise ValueError(f"SkipSherpa property lookup failed ({response.status_code}): {data}")
    return {"request": payload, "response": data}


def openletterconnect_headers():
    api_key = os.getenv("OPENLETTERCONNECT_API_KEY", "").strip()
    if not api_key:
        raise ValueError("OPENLETTERCONNECT_API_KEY is not set")
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }


def format_phone_pretty(value):
    digits = re.sub(r"\D+", "", value or "")
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) == 10:
        return f"({digits[0:3]}) {digits[3:6]}-{digits[6:10]}"
    return (value or "").strip()


def fetch_openletterconnect_template(template_id):
    response = requests.get(
        f"{OPENLETTERCONNECT_BASE_URL}/templates/{template_id}",
        headers=openletterconnect_headers(),
        timeout=30,
    )
    try:
        payload = response.json()
    except ValueError:
        payload = {"raw_text": response.text}
    if not response.ok:
        raise ValueError(f"OpenLetterConnect template fetch failed ({response.status_code}): {payload}")
    return payload


def fetch_openletterconnect_products():
    response = requests.get(
        f"{OPENLETTERCONNECT_BASE_URL}/products",
        headers=openletterconnect_headers(),
        timeout=30,
    )
    try:
        payload = response.json()
    except ValueError:
        payload = {"raw_text": response.text}
    if not response.ok:
        raise ValueError(f"OpenLetterConnect products fetch failed ({response.status_code}): {payload}")
    data = payload.get("data") if isinstance(payload, dict) else payload
    if isinstance(data, dict):
        rows = data.get("rows")
        return rows if isinstance(rows, list) else []
    if isinstance(data, list):
        return data
    return []


def resolve_openletterconnect_product_id(template_data, preferred_postage_type="", preferred_envelope_type=""):
    base_product = template_data.get("product") if isinstance(template_data, dict) else None
    if not isinstance(base_product, dict):
        return None
    default_id = base_product.get("id")

    postage_pref = (preferred_postage_type or "").strip().lower()
    envelope_pref = (preferred_envelope_type or "").strip().lower()
    if not postage_pref and not envelope_pref:
        return default_id

    base_product_type = (base_product.get("productType") or "").strip().lower()
    base_paper_type = (base_product.get("paperType") or "").strip().lower()
    base_paper_size = (base_product.get("paperSize") or "").strip().lower()
    base_delivery_type = (base_product.get("deliveryType") or "").strip().lower()

    products = fetch_openletterconnect_products()
    scoped = []
    for p in products:
        if (p.get("productType") or "").strip().lower() != base_product_type:
            continue
        if (p.get("paperType") or "").strip().lower() != base_paper_type:
            continue
        if (p.get("paperSize") or "").strip().lower() != base_paper_size:
            continue
        scoped.append(p)
    if not scoped:
        return default_id

    # Prefer exact postage+envelope match.
    for p in scoped:
        p_postage = ((p.get("postageType") or p.get("deliveryType") or "")).strip().lower()
        p_envelope = (p.get("envelopeType") or "").strip().lower()
        postage_ok = (not postage_pref) or (p_postage == postage_pref) or (postage_pref in p_postage)
        envelope_ok = (not envelope_pref) or (p_envelope == envelope_pref) or (envelope_pref in p_envelope)
        if postage_ok and envelope_ok:
            return p.get("id") or default_id

    # Fallback to postage-only match.
    if postage_pref:
        for p in scoped:
            p_postage = ((p.get("postageType") or p.get("deliveryType") or "")).strip().lower()
            if p_postage == postage_pref or postage_pref in p_postage:
                return p.get("id") or default_id

    # Fallback to envelope-only match.
    if envelope_pref:
        for p in scoped:
            p_envelope = (p.get("envelopeType") or "").strip().lower()
            if p_envelope == envelope_pref or envelope_pref in p_envelope:
                return p.get("id") or default_id

    # Final fallback to template/default product id.
    for p in scoped:
        if (p.get("deliveryType") or "").strip().lower() == base_delivery_type:
            return p.get("id") or default_id
    return default_id


def get_template_product_options(template_id):
    template = fetch_openletterconnect_template(template_id)
    template_data = template.get("data") if isinstance(template, dict) and isinstance(template.get("data"), dict) else template
    if not isinstance(template_data, dict):
        return {"postage_options": [], "envelope_options": []}
    base_product = template_data.get("product") if isinstance(template_data.get("product"), dict) else {}
    base_product_type = (base_product.get("productType") or "").strip().lower()
    base_paper_type = (base_product.get("paperType") or "").strip().lower()
    base_paper_size = (base_product.get("paperSize") or "").strip().lower()

    products = fetch_openletterconnect_products()
    postage_options = set()
    envelope_options = set()
    for p in products:
        if (p.get("productType") or "").strip().lower() != base_product_type:
            continue
        if (p.get("paperType") or "").strip().lower() != base_paper_type:
            continue
        if (p.get("paperSize") or "").strip().lower() != base_paper_size:
            continue
        postage_val = (p.get("postageType") or p.get("deliveryType") or "").strip()
        envelope_val = (p.get("envelopeType") or "").strip()
        if postage_val:
            postage_options.add(postage_val)
        if envelope_val:
            envelope_options.add(envelope_val)
    return {
        "postage_options": sorted(postage_options),
        "envelope_options": sorted(envelope_options),
    }


def build_openletterconnect_order_payload(db, contacts, property_row, template_id=OPENLETTERCONNECT_TEMPLATE_ID, mode="property-relative-mail"):
    template = fetch_openletterconnect_template(template_id)
    template_data = template.get("data") if isinstance(template, dict) and isinstance(template.get("data"), dict) else template
    if not isinstance(template_data, dict):
        raise ValueError(f"Template payload for {template_id} is invalid")

    dm = get_direct_mail_settings(db)
    sender_first = dm["sender_first_name"] or os.getenv("DM_SENDER_FIRST_NAME", "").strip()
    sender_last = dm["sender_last_name"] or os.getenv("DM_SENDER_LAST_NAME", "").strip()
    sender_company = dm["sender_company_name"] or os.getenv("DM_SENDER_COMPANY_NAME", "").strip()
    sender_address1 = dm["sender_address1"] or os.getenv("DM_SENDER_ADDRESS1", "").strip()
    sender_address2 = dm.get("sender_address2", "") or os.getenv("DM_SENDER_ADDRESS2", "").strip()
    sender_city = dm["sender_city"] or os.getenv("DM_SENDER_CITY", "").strip()
    sender_state = dm["sender_state"] or os.getenv("DM_SENDER_STATE", "").strip()
    sender_zip = dm["sender_zip"] or os.getenv("DM_SENDER_ZIP", "").strip()
    sender_phone = format_phone_pretty(dm["sender_phone"] or os.getenv("DM_SENDER_PHONE", "").strip())
    sender_email = dm["sender_email"] or os.getenv("DM_SENDER_EMAIL", "").strip()
    sender_website = dm["sender_website"] or os.getenv("DM_SENDER_WEBSITE", "").strip()
    postage_type = dm["postage_type"] or os.getenv("DM_POSTAGE_TYPE", "").strip()
    envelope_type = dm["envelope_type"] or os.getenv("DM_ENVELOPE_TYPE", "").strip()
    product_id = resolve_openletterconnect_product_id(
        template_data,
        preferred_postage_type=postage_type,
        preferred_envelope_type=envelope_type,
    )
    if not product_id:
        raise ValueError(f"Could not resolve product id for template {template_id}")

    contacts_for_payload = []
    for c in contacts or []:
        if not isinstance(c, dict):
            continue
        row = {
            "firstName": (c.get("firstName") or "").strip() or None,
            "lastName": (c.get("lastName") or "").strip() or None,
            "address1": (c.get("address1") or "").strip() or None,
            "address2": (c.get("address2") or "").strip() or None,
            "companyName": (c.get("companyName") or "").strip() or None,
            "city": (c.get("city") or "").strip() or None,
            "state": (c.get("state") or "").strip() or None,
            "zip": (c.get("zip") or "").strip() or None,
            "country": (c.get("country") or "US").strip() or "US",
            "email": (c.get("email") or "").strip() or None,
            "phone": (c.get("phone") or "").strip() or None,
            "propertyAddress": (c.get("propertyAddress") or "").strip() or None,
            "propertyCity": (c.get("propertyCity") or "").strip() or None,
            "propertyState": (c.get("propertyState") or "").strip() or None,
            "propertyZip": (c.get("propertyZip") or "").strip() or None,
            "campaign_phone": ((c.get("campaign_phone") or "").strip() or sender_phone or ""),
            "meta_data": c.get("meta_data") if isinstance(c.get("meta_data"), dict) else None,
        }
        row_filtered = {}
        for k, v in row.items():
            if k == "campaign_phone":
                row_filtered[k] = v if v is not None else ""
                continue
            if v is None:
                continue
            if isinstance(v, str) and v.strip() == "":
                continue
            row_filtered[k] = v
        row = row_filtered
        contacts_for_payload.append(row)

    payload = {
        "name": f"{mode} - property {property_row['id']}",
        "templateId": template_id,
        "productId": product_id,
        "contacts": contacts_for_payload,
    }

    if any([sender_first, sender_last, sender_company, sender_address1, sender_address2, sender_city, sender_state, sender_zip, sender_phone, sender_email, sender_website]):
        account_name = " ".join([x for x in [sender_first, sender_last] if (x or "").strip()]).strip()
        return_addr = {
            "firstName": sender_first,
            "lastName": sender_last,
            "address1": sender_address1,
            "address2": sender_address2 or None,
            "city": sender_city,
            "state": sender_state,
            "zip": sender_zip,
            "websiteUrl": sender_website or None,
            "phoneNo": sender_phone or None,
            "email": sender_email or None,
            "propertyAddress": (property_row["street"] or "").strip() or None,
            "propertyCity": (property_row["city"] or "").strip() or None,
            "propertyState": (property_row["state"] or "").strip() or None,
            "propertyZip": (property_row["postal_code"] or "").strip() or None,
            "meta_data": {
                "account_id": (f"account of {account_name}" if account_name else None),
            },
        }
        return_addr = {
            k: v
            for k, v in return_addr.items()
            if v is not None and (not isinstance(v, str) or v.strip() != "")
        }
        payload["returnAddress"] = return_addr
        payload["returnAddressSettings"] = {
            "firstName": bool(sender_first),
            "lastName": bool(sender_last),
            "companyName": bool(sender_company),
            "fullAddress": True,
        }
    return {
        "payload": payload,
        "template": template_data,
    }


def _extract_url_entries(obj, path=""):
    entries = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{path}.{k}" if path else str(k)
            entries.extend(_extract_url_entries(v, p))
        return entries
    if isinstance(obj, list):
        for idx, v in enumerate(obj):
            p = f"{path}[{idx}]"
            entries.extend(_extract_url_entries(v, p))
        return entries
    if isinstance(obj, str):
        s = obj.strip()
        if s.startswith(("http://", "https://", "data:image/", "data:application/pdf", "/")):
            entries.append({"path": path, "url": s})
        else:
            # Base64 payload fallback (common proof encodings).
            if len(s) > 120 and re.fullmatch(r"[A-Za-z0-9+/=\r\n]+", s):
                entries.append({"path": path, "url": s})
    return entries


def _normalize_proof_url(raw):
    if not isinstance(raw, str):
        return ""
    s = raw.strip()
    if not s:
        return ""
    if s.startswith(("http://", "https://", "data:image/", "data:application/pdf")):
        return s
    if s.startswith("/"):
        try:
            base = OPENLETTERCONNECT_BASE_URL.rstrip("/")
            # keep scheme+host from configured API base
            m = re.match(r"^(https?://[^/]+)", base, flags=re.IGNORECASE)
            if m:
                return m.group(1) + s
        except Exception:
            return s
        return s
    if len(s) > 120 and re.fullmatch(r"[A-Za-z0-9+/=\r\n]+", s):
        # best effort: infer media type from content prefix
        compact = re.sub(r"\s+", "", s)
        if compact.startswith("JVBER"):
            return f"data:application/pdf;base64,{compact}"
        return f"data:image/png;base64,{compact}"
    return ""


def view_openletterconnect_proofs(order_payload):
    contacts = order_payload.get("contacts") if isinstance(order_payload, dict) else []
    if not isinstance(contacts, list) or not contacts:
        return []

    proofs = []

    template_id = order_payload.get("templateId")
    if not template_id:
        for idx, contact in enumerate(contacts):
            proofs.append(
                {
                    "index": idx,
                    "contact": contact,
                    "ok": False,
                    "error": "Proof failed (missing templateId)",
                    "response": {"message": "templateId is required"},
                }
            )
        return proofs

    ra = order_payload.get("returnAddress") or {}
    return_contact = {
        "firstName": (ra.get("firstName") or "").strip(),
        "lastName": (ra.get("lastName") or "").strip(),
        "address1": (ra.get("address1") or "").strip(),
        "address2": (ra.get("address2") or "").strip(),
        "city": (ra.get("city") or "").strip(),
        "state": (ra.get("state") or "").strip(),
        "zip": (ra.get("zip") or "").strip(),
    }
    return_contact = {k: v for k, v in return_contact.items() if v}

    for idx, contact in enumerate(contacts):
        proof_payload = {
            "templateId": template_id,
            "contact": contact,
        }
        if return_contact:
            proof_payload["returnContact"] = return_contact
        response = requests.post(
            f"{OPENLETTERCONNECT_BASE_URL}/orders/view-proof",
            headers=openletterconnect_headers(),
            json=proof_payload,
            timeout=60,
        )
        try:
            data = response.json()
        except ValueError:
            data = {"raw_text": response.text}

        if not response.ok:
            proofs.append(
                {
                    "index": idx,
                    "contact": contact,
                    "ok": False,
                    "error": f"Proof failed ({response.status_code})",
                    "response": data,
                    "request_payload": proof_payload,
                }
            )
            continue

        pblob = data.get("data") if isinstance(data, dict) else data
        entries = _extract_url_entries(pblob)
        image_urls = []
        envelope_urls = []
        letter_urls = []
        pdf_urls = []
        debug_entries = []
        for entry in entries:
            raw = entry.get("url")
            url = _normalize_proof_url(raw)
            if not url:
                continue
            path_l = (entry.get("path") or "").lower()
            debug_entries.append({"path": entry.get("path"), "url": (url[:180] + "...") if len(url) > 180 else url})
            if re.search(r"\.pdf(\?|$)", url, flags=re.IGNORECASE):
                pdf_urls.append(url)
            elif url.startswith("data:application/pdf"):
                pdf_urls.append(url)
            elif "envelope" in path_l:
                envelope_urls.append(url)
                image_urls.append(url)
            elif any(token in path_l for token in ("letter", "front", "back", "postcard", "proof", "image")):
                letter_urls.append(url)
                image_urls.append(url)
            else:
                # Signed CDN links may not include image extensions; keep them for rendering.
                image_urls.append(url)
        # De-duplicate while preserving order.
        image_urls = list(dict.fromkeys(image_urls))
        envelope_urls = list(dict.fromkeys(envelope_urls))
        letter_urls = list(dict.fromkeys(letter_urls))
        pdf_urls = list(dict.fromkeys(pdf_urls))

        proofs.append(
            {
                "index": idx,
                "contact": contact,
                "ok": True,
                "image_urls": image_urls,
                "envelope_urls": envelope_urls,
                "letter_urls": letter_urls,
                "pdf_urls": pdf_urls,
                "debug_entries": debug_entries[:25],
                "response": pblob,
                "request_payload": proof_payload,
            }
        )
    return proofs


def place_openletterconnect_order(db, contacts, property_row, template_id=OPENLETTERCONNECT_TEMPLATE_ID, mode="property-relative-mail"):
    built = build_openletterconnect_order_payload(
        db,
        contacts,
        property_row,
        template_id=template_id,
        mode=mode,
    )
    payload = built["payload"]
    response = requests.post(
        f"{OPENLETTERCONNECT_BASE_URL}/orders",
        headers=openletterconnect_headers(),
        json=payload,
        timeout=45,
    )
    try:
        data = response.json()
    except ValueError:
        data = {"raw_text": response.text}
    if not response.ok:
        raise ValueError(f"OpenLetterConnect order failed ({response.status_code}): {data}")
    return {"request": payload, "response": data, "template": built.get("template")}


def _format_person_contact_for_mail(db, property_row, person_id):
    person = db.execute(
        "SELECT id, first_name, last_name, primary_phone, primary_email, deceased FROM people WHERE id = ?",
        (person_id,),
    ).fetchone()
    if not person:
        return None
    if int(person["deceased"] or 0) == 1:
        return None

    addr = db.execute(
        """
        SELECT a.street, a.city, a.state, a.postal_code
        FROM person_addresses pa
        JOIN addresses a ON a.id = pa.address_id
        WHERE pa.person_id = ?
        ORDER BY pa.id DESC
        LIMIT 1
        """,
        (person_id,),
    ).fetchone()

    # Fallback for the primary owner if a dedicated mailing address is unavailable.
    if not addr and person_id == property_row["owner_person_id"]:
        addr = {
            "street": property_row["street"],
            "city": property_row["city"],
            "state": property_row["state"],
            "postal_code": property_row["postal_code"],
        }

    if not addr:
        return None

    email = (person["primary_email"] or "").strip()
    phone = (person["primary_phone"] or "").strip()
    if not email or not phone:
        rows = db.execute(
            """
            SELECT channel_type, value
            FROM touchpoints
            WHERE person_id = ? AND lower(channel_type) IN ('phone', 'email')
            ORDER BY id DESC
            """,
            (person_id,),
        ).fetchall()
        for r in rows:
            ctype = (r["channel_type"] or "").strip().lower()
            value = (r["value"] or "").strip()
            if not value:
                continue
            if ctype == "email" and not email:
                email = value
            if ctype == "phone" and not phone:
                phone = value

    owner_name = ""
    if property_row["owner_person_id"]:
        owner = db.execute(
            "SELECT first_name, last_name FROM people WHERE id = ?",
            (property_row["owner_person_id"],),
        ).fetchone()
        if owner:
            owner_name = " ".join(
                x for x in [(owner["first_name"] or "").strip(), (owner["last_name"] or "").strip()] if x
            ).strip()

    return {
        "firstName": person["first_name"],
        "lastName": person["last_name"],
        "address1": addr["street"],
        "address2": None,
        "companyName": None,
        "city": addr["city"],
        "state": addr["state"],
        "zip": addr["postal_code"],
        "country": "US",
        "email": email or None,
        "phone": phone or None,
        "propertyAddress": property_row["street"],
        "propertyCity": property_row["city"],
        "propertyState": property_row["state"],
        "propertyZip": property_row["postal_code"],
        "propertyOwnerName": owner_name or None,
        "meta_data": {
            "property_id": property_row["id"],
            "person_id": person["id"],
            "subject_property": {
                "street": property_row["street"],
                "city": property_row["city"],
                "state": property_row["state"],
                "postal_code": property_row["postal_code"],
            },
        },
    }


def _person_full_name_from_payload(person_payload):
    if not isinstance(person_payload, dict):
        return ""
    name = (person_payload.get("name") or "").strip()
    if name:
        return name
    person_name = person_payload.get("person_name") or {}
    first = (person_name.get("first_name") or "").strip()
    middle = (person_name.get("middle_name") or "").strip()
    last = (person_name.get("last_name") or "").strip()
    return " ".join(x for x in [first, middle, last] if x).strip()


def import_skipsherpa_property_result(db, property_id, lookup_pkg):
    response = (lookup_pkg or {}).get("response") or {}
    property_results = response.get("property_results") or []
    if not property_results:
        raise ValueError("No property_results returned from SkipSherpa property lookup")

    property_payload = None
    for item in property_results:
        if item.get("property"):
            property_payload = item["property"]
            break
    if not property_payload:
        raise ValueError("SkipSherpa property payload missing")

    attom = ((property_payload.get("address") or {}).get("attom") or {})
    last_sold_date = (attom.get("attom_last_sold_date") or "").strip() if isinstance(attom, dict) else ""
    last_sold_price = attom.get("attom_last_sold_price") if isinstance(attom, dict) else None

    db.execute(
        """
        UPDATE properties
        SET attom_last_sold_date = COALESCE(NULLIF(?, ''), attom_last_sold_date),
            attom_last_sold_price = COALESCE(?, attom_last_sold_price)
        WHERE id = ?
        """,
        (last_sold_date, last_sold_price, property_id),
    )

    prop = db.execute("SELECT owner_person_id FROM properties WHERE id = ?", (property_id,)).fetchone()
    primary_owner_id = prop["owner_person_id"] if prop else None
    owners_created = 0
    co_owner_links = 0

    for idx, owner_entry in enumerate(property_payload.get("owners") or []):
        person_payload = owner_entry.get("person") or {}
        person_name = person_payload.get("person_name") or {}
        first = (person_name.get("first_name") or "").strip()
        last = (person_name.get("last_name") or "").strip()
        full_name = _person_full_name_from_payload(person_payload)
        if not (first and last) and full_name:
            parts = [p for p in full_name.split() if p]
            if parts:
                first = parts[0]
                last = parts[-1] if len(parts) > 1 else "Unknown"
        if not (first and last):
            continue
        owner_person_id = find_or_create_person_by_name_parts(
            db,
            first,
            last,
            notes=f"Imported from SkipSherpa property lookup for property {property_id}",
        )
        if full_name:
            db.execute(
                "UPDATE people SET notes = trim(coalesce(notes,'') || ' ' || ?) WHERE id = ?",
                (f"Full name from SkipSherpa property payload: {full_name}.", owner_person_id),
            )
            add_person_note(
                db,
                owner_person_id,
                "SkipSherpa Property",
                f"Owner imported with normalized name mapping: '{full_name}' -> first='{first}', last='{last}'.",
                {"full_name": full_name, "first_name": first, "last_name": last, "property_id": property_id},
            )
        rel_age = person_payload.get("age")
        rel_deceased_raw = person_payload.get("deceased")
        rel_deceased = bool(rel_deceased_raw) if rel_deceased_raw is not None else False
        rel_dob_my = (person_payload.get("date_of_birth_month_year") or "").strip()
        rel_birth_year = rel_dob_my.split("-")[-1].strip() if rel_dob_my and "-" in rel_dob_my else ""
        db.execute(
            """
            UPDATE people
            SET age = COALESCE(?, age),
                deceased = ?,
                birth_year = COALESCE(NULLIF(?, ''), birth_year)
            WHERE id = ?
            """,
            (rel_age, 1 if rel_deceased else 0, rel_birth_year, owner_person_id),
        )

        # Persist owner addresses from property payload into person_addresses.
        for addr_payload in person_payload.get("addresses") or []:
            us = addr_payload.get("us_address") or {}
            street = (us.get("street") or "").strip()
            city = (us.get("city") or "").strip()
            state = (us.get("state") or "").strip()
            zipcode = (us.get("zipcode") or "").strip()
            if not (street and city and state and zipcode):
                continue
            exists_addr = db.execute(
                """
                SELECT pa.id
                FROM person_addresses pa
                JOIN addresses a ON a.id = pa.address_id
                WHERE pa.person_id = ?
                  AND lower(a.street)=lower(?)
                  AND lower(a.city)=lower(?)
                  AND lower(a.state)=lower(?)
                  AND a.postal_code=?
                LIMIT 1
                """,
                (owner_person_id, street, city, state, zipcode),
            ).fetchone()
            if exists_addr:
                continue
            addr_id = create_address(db, street, city, state, zipcode)
            db.execute(
                "INSERT INTO person_addresses (person_id, address_id, label) VALUES (?, ?, ?)",
                (owner_person_id, addr_id, "SkipSherpa Owner Address"),
            )

        # Persist owner emails/phones into touchpoints so mail/SMS workflows can use person data directly.
        for em in person_payload.get("emails") or []:
            if isinstance(em, dict):
                email = (em.get("email_address") or "").strip()
            else:
                email = str(em or "").strip()
            if not email or touchpoint_exists(db, owner_person_id, "Email", email):
                continue
            db.execute(
                """
                INSERT INTO touchpoints (person_id, channel_type, channel_label, value, status, note, last_attempted)
                VALUES (?, 'Email', 'Email', ?, 'Unknown', ?, ?)
                """,
                (owner_person_id, email, "Imported from SkipSherpa property owner payload", ""),
            )

        for ph in person_payload.get("phone_numbers") or []:
            number = (ph.get("e164_format") or ph.get("local_format") or "").strip()
            if not number or touchpoint_exists(db, owner_person_id, "Phone", number):
                continue
            ptype = (ph.get("type") or "").strip().lower()
            label = {"mobile": "Mobile", "landline": "Landline", "voip": "VoIP"}.get(ptype, "Unknown")
            db.execute(
                """
                INSERT INTO touchpoints (person_id, channel_type, channel_label, value, status, note, last_attempted)
                VALUES (?, 'Phone', ?, ?, 'Unknown', ?, ?)
                """,
                (
                    owner_person_id,
                    label,
                    number,
                    "Imported from SkipSherpa property owner payload",
                    "",
                ),
            )

        if primary_owner_id is None:
            db.execute("UPDATE properties SET owner_person_id = ? WHERE id = ?", (owner_person_id, property_id))
            primary_owner_id = owner_person_id
        elif owner_person_id != primary_owner_id:
            db.execute("UPDATE people SET outreach_status = ? WHERE id = ?", ("Co-Owner", owner_person_id))
            exists_rel = db.execute(
                """
                SELECT id FROM person_relationships
                WHERE subject_person_id = ? AND related_person_id = ? AND relationship_type = ?
                LIMIT 1
                """,
                (primary_owner_id, owner_person_id, "Co-Owner"),
            ).fetchone()
            if not exists_rel:
                db.execute(
                    """
                    INSERT INTO person_relationships (subject_person_id, related_person_id, relationship_type, note)
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        primary_owner_id,
                        owner_person_id,
                        "Co-Owner",
                        "Imported from SkipSherpa property owners payload",
                    ),
                )
                co_owner_links += 1
        owners_created += 1

    db.execute(
        """
        INSERT INTO activity_log (property_id, person_id, activity_type, outcome, note)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            property_id,
            primary_owner_id,
            "Skip Trace Property",
            f"Owners processed: {owners_created}, Co-owner links: {co_owner_links}",
            json.dumps(lookup_pkg),
        ),
    )

    return {
        "owners_processed": owners_created,
        "co_owner_links": co_owner_links,
        "attom_last_sold_date": last_sold_date,
        "attom_last_sold_price": last_sold_price,
    }


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


def _touchpoint_key(channel_type, value):
    ctype = (channel_type or "").strip().lower()
    raw = (value or "").strip()
    if ctype == "phone":
        return (ctype, normalize_phone(raw))
    return (ctype, raw.lower())


def _status_rank(status):
    s = (status or "").strip().lower()
    ranks = {
        "unknown": 0,
        "pending": 0,
        "unverified": 0,
        "queued": 1,
        "no answer": 1,
        "contact no response": 1,
        "outreach attempted": 1,
        "follow up": 2,
        "correct": 3,
        "verified": 3,
        "delivered": 3,
        "success": 3,
        "active": 3,
    }
    return ranks.get(s, 1)


def _relationship_rank(relationship_type):
    t = (relationship_type or "").strip().lower()
    if t == "relative":
        return 3
    if t == "pfamily member":
        return 2
    if t == "unknown":
        return 1
    return 0


def dedupe_person_touchpoints(db, person_id):
    rows = db.execute(
        """
        SELECT id, channel_type, channel_label, value, status, note, last_attempted
        FROM touchpoints
        WHERE person_id = ?
        ORDER BY id ASC
        """,
        (person_id,),
    ).fetchall()
    by_key = {}
    for row in rows:
        key = _touchpoint_key(row["channel_type"], row["value"])
        by_key.setdefault(key, []).append(row)
    for _key, group in by_key.items():
        if len(group) <= 1:
            continue
        keep = sorted(group, key=lambda r: (-_status_rank(r["status"]), r["id"]))[0]
        merged_label = (keep["channel_label"] or "").strip()
        merged_status = keep["status"]
        merged_note = (keep["note"] or "").strip()
        merged_last_attempted = (keep["last_attempted"] or "").strip()
        for row in group:
            if row["id"] == keep["id"]:
                continue
            row_label = (row["channel_label"] or "").strip()
            if not merged_label and row_label:
                merged_label = row_label
            if _status_rank(row["status"]) > _status_rank(merged_status):
                merged_status = row["status"]
            row_note = (row["note"] or "").strip()
            if row_note and row_note not in merged_note:
                merged_note = (merged_note + ("\n\n" if merged_note else "") + row_note).strip()
            row_attempted = (row["last_attempted"] or "").strip()
            if row_attempted and (not merged_last_attempted or row_attempted > merged_last_attempted):
                merged_last_attempted = row_attempted
            db.execute("DELETE FROM touchpoints WHERE id = ?", (row["id"],))
        db.execute(
            """
            UPDATE touchpoints
            SET channel_label = ?, status = ?, note = ?, last_attempted = ?
            WHERE id = ?
            """,
            (merged_label, merged_status, merged_note, merged_last_attempted, keep["id"]),
        )


def dedupe_person_addresses(db, person_id):
    rows = db.execute(
        """
        SELECT pa.id AS person_address_id, pa.address_id, pa.label, a.street, a.city, a.state, a.postal_code
        FROM person_addresses pa
        JOIN addresses a ON a.id = pa.address_id
        WHERE pa.person_id = ?
        ORDER BY pa.id ASC
        """,
        (person_id,),
    ).fetchall()
    by_key = {}
    for row in rows:
        key = (
            (row["street"] or "").strip().lower(),
            (row["city"] or "").strip().lower(),
            (row["state"] or "").strip().lower(),
            (row["postal_code"] or "").strip(),
        )
        by_key.setdefault(key, []).append(row)
    for _key, group in by_key.items():
        if len(group) <= 1:
            continue
        keep = group[0]
        keep_label = (keep["label"] or "").strip()
        for row in group[1:]:
            if not keep_label and (row["label"] or "").strip():
                keep_label = (row["label"] or "").strip()
            db.execute("DELETE FROM person_addresses WHERE id = ?", (row["person_address_id"],))
        db.execute("UPDATE person_addresses SET label = ? WHERE id = ?", (keep_label, keep["person_address_id"]))


def dedupe_person_relationship_rows(db, subject_person_id):
    rows = db.execute(
        """
        SELECT id, related_person_id, relationship_type
        FROM person_relationships
        WHERE subject_person_id = ?
        ORDER BY id ASC
        """,
        (subject_person_id,),
    ).fetchall()
    by_related = {}
    for row in rows:
        by_related.setdefault(row["related_person_id"], []).append(row)
    for related_id, group in by_related.items():
        if len(group) <= 1:
            continue
        keep = sorted(group, key=lambda r: (-_relationship_rank(r["relationship_type"]), r["id"]))[0]
        for row in group:
            if row["id"] == keep["id"]:
                continue
            db.execute("DELETE FROM person_relationships WHERE id = ?", (row["id"],))
        wanted = keep["relationship_type"] or "Unknown"
        if wanted and wanted != keep["relationship_type"]:
            db.execute(
                "UPDATE person_relationships SET relationship_type = ? WHERE id = ?",
                (wanted, keep["id"]),
            )


def cleanup_person_contact_and_relationship_duplicates(db, person_id):
    dedupe_person_touchpoints(db, person_id)
    dedupe_person_addresses(db, person_id)
    dedupe_person_relationship_rows(db, person_id)


def person_merge_score(db, person_id):
    row = db.execute(
        """
        SELECT id, first_name, last_name, primary_phone, primary_email
        FROM people
        WHERE id = ?
        """,
        (person_id,),
    ).fetchone()
    if not row:
        return -1
    phone_count = db.execute(
        "SELECT COUNT(*) AS c FROM touchpoints WHERE person_id = ? AND lower(channel_type) = 'phone'",
        (person_id,),
    ).fetchone()["c"]
    email_count = db.execute(
        "SELECT COUNT(*) AS c FROM touchpoints WHERE person_id = ? AND lower(channel_type) = 'email'",
        (person_id,),
    ).fetchone()["c"]
    addr_count = db.execute(
        "SELECT COUNT(*) AS c FROM person_addresses WHERE person_id = ?",
        (person_id,),
    ).fetchone()["c"]
    social_count = db.execute(
        "SELECT COUNT(*) AS c FROM social_accounts WHERE person_id = ?",
        (person_id,),
    ).fetchone()["c"]
    comm_count = db.execute(
        "SELECT COUNT(*) AS c FROM communications WHERE person_id = ?",
        (person_id,),
    ).fetchone()["c"]
    inbound_count = db.execute(
        "SELECT COUNT(*) AS c FROM communications WHERE person_id = ? AND lower(direction) = 'inbound'",
        (person_id,),
    ).fetchone()["c"]
    active_campaigns = db.execute(
        "SELECT COUNT(*) AS c FROM sequence_enrollments WHERE person_id = ? AND status = 'Active'",
        (person_id,),
    ).fetchone()["c"]
    note_count = db.execute(
        "SELECT COUNT(*) AS c FROM person_notes WHERE person_id = ?",
        (person_id,),
    ).fetchone()["c"]
    prop_refs = db.execute(
        "SELECT COUNT(*) AS c FROM properties WHERE owner_person_id = ? OR resident_person_id = ?",
        (person_id, person_id),
    ).fetchone()["c"]
    score = 0
    score += phone_count * 8
    score += email_count * 7
    score += addr_count * 6
    score += social_count * 4
    score += comm_count * 3
    score += inbound_count * 4
    score += active_campaigns * 12
    score += note_count * 2
    score += prop_refs * 8
    if (row["primary_phone"] or "").strip():
        score += 2
    if (row["primary_email"] or "").strip():
        score += 2
    return score


def merge_people_records(db, winner_id, loser_id, reason=""):
    if not winner_id or not loser_id or winner_id == loser_id:
        return winner_id
    winner = db.execute("SELECT * FROM people WHERE id = ?", (winner_id,)).fetchone()
    loser = db.execute("SELECT * FROM people WHERE id = ?", (loser_id,)).fetchone()
    if not winner or not loser:
        return winner_id

    # Keep winner's core identity; only fill blank fields from loser.
    db.execute(
        """
        UPDATE people
        SET primary_phone = COALESCE(NULLIF(primary_phone, ''), NULLIF(?, ''), primary_phone),
            primary_email = COALESCE(NULLIF(primary_email, ''), NULLIF(?, ''), primary_email),
            notes = CASE
                WHEN COALESCE(NULLIF(notes, ''), '') = '' THEN COALESCE(NULLIF(?, ''), notes)
                WHEN COALESCE(NULLIF(?, ''), '') = '' THEN notes
                WHEN instr(notes, ?) > 0 THEN notes
                ELSE notes || '\n\n[Merged from person ' || ? || ']\n' || ?
            END,
            age = COALESCE(age, ?),
            deceased = CASE WHEN deceased = 1 OR ? = 1 THEN 1 ELSE 0 END,
            birth_year = COALESCE(NULLIF(birth_year, ''), NULLIF(?, ''), birth_year),
            deceased_date = COALESCE(NULLIF(deceased_date, ''), NULLIF(?, ''), deceased_date),
            bankruptcy = CASE WHEN bankruptcy = 1 OR ? = 1 THEN 1 ELSE 0 END,
            employer = COALESCE(NULLIF(employer, ''), NULLIF(?, ''), employer)
        WHERE id = ?
        """,
        (
            loser["primary_phone"],
            loser["primary_email"],
            loser["notes"],
            loser["notes"],
            loser["notes"] or "",
            loser_id,
            loser["notes"] or "",
            loser["age"],
            1 if int(loser["deceased"] or 0) == 1 else 0,
            loser["birth_year"],
            loser["deceased_date"],
            1 if int(loser["bankruptcy"] or 0) == 1 else 0,
            loser["employer"],
            winner_id,
        ),
    )

    # Merge touchpoints with dedupe and best-field carry-over.
    winner_tps = db.execute("SELECT * FROM touchpoints WHERE person_id = ?", (winner_id,)).fetchall()
    winner_map = {}
    for tp in winner_tps:
        winner_map[_touchpoint_key(tp["channel_type"], tp["value"])] = tp
    loser_tps = db.execute("SELECT * FROM touchpoints WHERE person_id = ?", (loser_id,)).fetchall()
    for tp in loser_tps:
        key = _touchpoint_key(tp["channel_type"], tp["value"])
        existing = winner_map.get(key)
        if not existing:
            db.execute("UPDATE touchpoints SET person_id = ? WHERE id = ?", (winner_id, tp["id"]))
            continue
        merged_label = (existing["channel_label"] or "").strip() or (tp["channel_label"] or "").strip()
        merged_status = existing["status"] if _status_rank(existing["status"]) >= _status_rank(tp["status"]) else tp["status"]
        existing_note = (existing["note"] or "").strip()
        loser_note = (tp["note"] or "").strip()
        if loser_note and loser_note not in existing_note:
            merged_note = (existing_note + ("\n\n" if existing_note else "") + loser_note).strip()
        else:
            merged_note = existing_note
        last_attempted = (existing["last_attempted"] or "").strip()
        other_attempted = (tp["last_attempted"] or "").strip()
        if other_attempted and (not last_attempted or other_attempted > last_attempted):
            last_attempted = other_attempted
        db.execute(
            """
            UPDATE touchpoints
            SET channel_label = ?, status = ?, note = ?, last_attempted = ?
            WHERE id = ?
            """,
            (merged_label, merged_status, merged_note, last_attempted, existing["id"]),
        )
        db.execute("DELETE FROM touchpoints WHERE id = ?", (tp["id"],))

    # Merge social accounts.
    winner_social = db.execute("SELECT * FROM social_accounts WHERE person_id = ?", (winner_id,)).fetchall()
    social_keys = set()
    for s in winner_social:
        key = (
            (s["platform"] or "").strip().lower(),
            (s["handle"] or "").strip().lower(),
            (s["url"] or "").strip().lower(),
        )
        social_keys.add(key)
    loser_social = db.execute("SELECT * FROM social_accounts WHERE person_id = ?", (loser_id,)).fetchall()
    for s in loser_social:
        key = (
            (s["platform"] or "").strip().lower(),
            (s["handle"] or "").strip().lower(),
            (s["url"] or "").strip().lower(),
        )
        if key in social_keys:
            db.execute("DELETE FROM social_accounts WHERE id = ?", (s["id"],))
            continue
        db.execute("UPDATE social_accounts SET person_id = ? WHERE id = ?", (winner_id, s["id"]))
        social_keys.add(key)

    # Merge addresses.
    winner_addr_ids = {
        r["address_id"]
        for r in db.execute("SELECT address_id FROM person_addresses WHERE person_id = ?", (winner_id,)).fetchall()
    }
    loser_addrs = db.execute("SELECT id, address_id FROM person_addresses WHERE person_id = ?", (loser_id,)).fetchall()
    for a in loser_addrs:
        if a["address_id"] in winner_addr_ids:
            db.execute("DELETE FROM person_addresses WHERE id = ?", (a["id"],))
            continue
        db.execute("UPDATE person_addresses SET person_id = ? WHERE id = ?", (winner_id, a["id"]))
        winner_addr_ids.add(a["address_id"])

    # Merge relationships while preventing duplicates/self-links.
    rels = db.execute(
        """
        SELECT id, subject_person_id, related_person_id, relationship_type, note
        FROM person_relationships
        WHERE subject_person_id = ? OR related_person_id = ?
        """,
        (loser_id, loser_id),
    ).fetchall()
    for r in rels:
        new_subject = winner_id if r["subject_person_id"] == loser_id else r["subject_person_id"]
        new_related = winner_id if r["related_person_id"] == loser_id else r["related_person_id"]
        if new_subject == new_related:
            db.execute("DELETE FROM person_relationships WHERE id = ?", (r["id"],))
            continue
        existing = db.execute(
            """
            SELECT id, note
            FROM person_relationships
            WHERE subject_person_id = ? AND related_person_id = ? AND relationship_type = ?
            LIMIT 1
            """,
            (new_subject, new_related, r["relationship_type"]),
        ).fetchone()
        if existing and existing["id"] != r["id"]:
            old_note = (existing["note"] or "").strip()
            add_note = (r["note"] or "").strip()
            if add_note and add_note not in old_note:
                merged = (old_note + ("\n\n" if old_note else "") + add_note).strip()
                db.execute("UPDATE person_relationships SET note = ? WHERE id = ?", (merged, existing["id"]))
            db.execute("DELETE FROM person_relationships WHERE id = ?", (r["id"],))
        else:
            db.execute(
                "UPDATE person_relationships SET subject_person_id = ?, related_person_id = ? WHERE id = ?",
                (new_subject, new_related, r["id"]),
            )

    # Update all references to winner.
    db.execute("UPDATE communications SET person_id = ? WHERE person_id = ?", (winner_id, loser_id))
    db.execute("UPDATE activity_log SET person_id = ? WHERE person_id = ?", (winner_id, loser_id))
    db.execute("UPDATE mail_orders SET person_id = ? WHERE person_id = ?", (winner_id, loser_id))
    db.execute("UPDATE skiptrace_runs SET person_id = ? WHERE person_id = ?", (winner_id, loser_id))
    db.execute("UPDATE person_notes SET person_id = ? WHERE person_id = ?", (winner_id, loser_id))
    db.execute("UPDATE bulk_sms_job_items SET person_id = ? WHERE person_id = ?", (winner_id, loser_id))
    db.execute("UPDATE obituary_sources SET subject_person_id = ? WHERE subject_person_id = ?", (winner_id, loser_id))
    db.execute("UPDATE obituary_expansion_runs SET root_subject_person_id = ? WHERE root_subject_person_id = ?", (winner_id, loser_id))
    db.execute(
        "UPDATE obituary_expansion_items SET subject_person_id = ? WHERE subject_person_id = ?",
        (winner_id, loser_id),
    )
    db.execute(
        "UPDATE obituary_expansion_items SET related_person_id = ? WHERE related_person_id = ?",
        (winner_id, loser_id),
    )
    db.execute("UPDATE properties SET owner_person_id = ? WHERE owner_person_id = ?", (winner_id, loser_id))
    db.execute("UPDATE properties SET resident_person_id = ? WHERE resident_person_id = ?", (winner_id, loser_id))

    # Merge sequence enrollments safely.
    loser_enrollments = db.execute(
        """
        SELECT * FROM sequence_enrollments
        WHERE person_id = ?
        ORDER BY id
        """,
        (loser_id,),
    ).fetchall()
    for e in loser_enrollments:
        existing = db.execute(
            """
            SELECT *
            FROM sequence_enrollments
            WHERE campaign_id = ? AND property_id = ? AND person_id = ?
            LIMIT 1
            """,
            (e["campaign_id"], e["property_id"], winner_id),
        ).fetchone()
        if not existing:
            db.execute("UPDATE sequence_enrollments SET person_id = ? WHERE id = ?", (winner_id, e["id"]))
            continue
        target_enrollment_id = existing["id"]
        db.execute(
            "UPDATE sequence_events SET enrollment_id = ? WHERE enrollment_id = ?",
            (target_enrollment_id, e["id"]),
        )
        existing_status = (existing["status"] or "").strip().lower()
        e_status = (e["status"] or "").strip().lower()
        merged_status = existing["status"]
        if "active" in (existing_status, e_status):
            merged_status = "Active"
        elif existing_status != "completed" and e_status == "completed":
            merged_status = "Completed"
        next_run = existing["next_run_at"] or e["next_run_at"]
        if existing["next_run_at"] and e["next_run_at"]:
            next_run = min(existing["next_run_at"], e["next_run_at"])
        db.execute(
            """
            UPDATE sequence_enrollments
            SET status = ?,
                next_run_at = ?,
                last_step_order = CASE WHEN COALESCE(last_step_order, 0) >= ? THEN last_step_order ELSE ? END,
                stopped_reason = COALESCE(NULLIF(stopped_reason, ''), NULLIF(?, ''), stopped_reason),
                completed_at = CASE
                    WHEN completed_at IS NULL THEN ?
                    WHEN ? IS NULL THEN completed_at
                    WHEN completed_at <= ? THEN completed_at
                    ELSE ?
                END
            WHERE id = ?
            """,
            (
                merged_status,
                next_run,
                int(e["last_step_order"] or 0),
                int(e["last_step_order"] or 0),
                e["stopped_reason"],
                e["completed_at"],
                e["completed_at"],
                e["completed_at"],
                e["completed_at"],
                target_enrollment_id,
            ),
        )
        db.execute("DELETE FROM sequence_enrollments WHERE id = ?", (e["id"],))

    add_person_note(
        db,
        winner_id,
        "System Merge",
        f"Merged duplicate person record #{loser_id} into #{winner_id}. {reason}".strip(),
    )
    cleanup_person_contact_and_relationship_duplicates(db, winner_id)
    db.execute("DELETE FROM people WHERE id = ?", (loser_id,))
    return winner_id


def merge_duplicate_people_by_name(db, person_id, reason=""):
    row = db.execute("SELECT id, first_name, last_name FROM people WHERE id = ?", (person_id,)).fetchone()
    if not row:
        return person_id
    n_first, n_last = normalize_first_last(row["first_name"], row["last_name"])
    candidates = db.execute(
        "SELECT id, first_name, last_name FROM people WHERE lower(last_name) = lower(?)",
        (n_last,),
    ).fetchall()
    dup_ids = []
    for c in candidates:
        c_first, c_last = normalize_first_last(c["first_name"], c["last_name"])
        if c_first.lower() == n_first.lower() and c_last.lower() == n_last.lower():
            dup_ids.append(c["id"])
    dup_ids = sorted(set(dup_ids))
    if len(dup_ids) <= 1:
        return person_id
    scored = []
    for pid in dup_ids:
        scored.append((person_merge_score(db, pid), -pid, pid))
    scored.sort(reverse=True)
    winner_id = scored[0][2]
    for _score, _neg, pid in scored[1:]:
        winner_id = merge_people_records(
            db,
            winner_id,
            pid,
            reason=reason or f"Name-based dedupe for {n_first} {n_last}",
        )
    return winner_id


def dedupe_people_by_exact_name_birth_year(db, person_ids=None, reason=""):
    if person_ids:
        clean_ids = sorted({int(pid) for pid in person_ids if str(pid).isdigit()})
        if not clean_ids:
            return {"merged_count": 0, "groups_merged": 0}
        placeholders = ",".join(["?"] * len(clean_ids))
        rows = db.execute(
            f"""
            SELECT id, first_name, last_name, COALESCE(NULLIF(trim(birth_year), ''), '') AS birth_year
            FROM people
            WHERE id IN ({placeholders})
            """,
            tuple(clean_ids),
        ).fetchall()
    else:
        rows = db.execute(
            """
            SELECT id, first_name, last_name, COALESCE(NULLIF(trim(birth_year), ''), '') AS birth_year
            FROM people
            """
        ).fetchall()

    groups = {}
    for r in rows:
        norm_first, norm_last = normalize_first_last(r["first_name"], r["last_name"])
        key = (
            (norm_first or "").strip().lower(),
            (norm_last or "").strip().lower(),
            (r["birth_year"] or "").strip(),
        )
        if not key[0] or not key[1] or not key[2]:
            continue
        groups.setdefault(key, []).append(r["id"])

    merged_count = 0
    groups_merged = 0
    for _key, ids in groups.items():
        uniq_ids = sorted(set(ids))
        if len(uniq_ids) <= 1:
            continue
        groups_merged += 1
        scored = sorted(
            ((person_merge_score(db, pid), -pid, pid) for pid in uniq_ids),
            reverse=True,
        )
        winner_id = scored[0][2]
        for _score, _neg_pid, loser_id in scored[1:]:
            winner_id = merge_people_records(
                db,
                winner_id,
                loser_id,
                reason=reason or "Exact name+birth year dedupe",
            )
            merged_count += 1
    return {"merged_count": merged_count, "groups_merged": groups_merged}


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
    touched_person_ids = set()
    now_stamp = datetime.now(EST_TZ).strftime("%Y-%m-%d %I:%M %p ET")
    source_row = db.execute("SELECT id, first_name, last_name, birth_year FROM people WHERE id = ?", (person_id,)).fetchone()
    source_first = (source_row["first_name"] if source_row else "").strip().lower()
    source_last = (source_row["last_name"] if source_row else "").strip().lower()
    source_birth_year = (source_row["birth_year"] if source_row else "") or ""

    def person_signature(first_name, last_name, birth_year=""):
        norm_first, norm_last = normalize_first_last(first_name, last_name)
        return (
            (norm_first or "").strip().lower(),
            (norm_last or "").strip().lower(),
            (birth_year or "").strip(),
        )

    def composed_name(name_obj, fallback_name=""):
        if isinstance(name_obj, dict):
            first = (name_obj.get("first_name") or "").strip()
            middle = (name_obj.get("middle_name") or "").strip()
            last = (name_obj.get("last_name") or "").strip()
            suffix = (name_obj.get("suffix") or "").strip()
            if first or middle or last:
                return first or "Unknown", middle, last or "Unknown", suffix
        first, middle, last, suffix = split_name_first_middle_last((fallback_name or "").strip())
        return first, middle, last, suffix

    def upsert_person_metrics(target_person_id, payload_person):
        age_val = payload_person.get("age")
        deceased_val = bool(payload_person.get("deceased")) if payload_person.get("deceased") is not None else False
        dob_my = (payload_person.get("date_of_birth_month_year") or "").strip()
        birth_year = dob_my.split("-")[-1].strip() if dob_my and "-" in dob_my else ""
        bankruptcy_val = bool(payload_person.get("bankruptcy")) if payload_person.get("bankruptcy") is not None else False
        employers = [e.get("name", "").strip() for e in (payload_person.get("employers") or []) if (e.get("name") or "").strip()]
        employer_text = ", ".join(sorted(set(employers))) if employers else ""
        deceased_date = (
            (payload_person.get("date_of_death") or payload_person.get("deceased_date") or "").strip()
            if isinstance(payload_person, dict)
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
                target_person_id,
            ),
        )

    def import_contact_data(target_person_id, payload_person):
        nonlocal phones_added, emails_added, addresses_added
        for ph in payload_person.get("phone_numbers") or []:
            number = (ph.get("e164_format") or ph.get("local_format") or "").strip()
            if not number or touchpoint_exists(db, target_person_id, "Phone", number):
                continue
            ptype = (ph.get("type") or "").strip().lower()
            label = {"mobile": "Mobile", "landline": "Landline", "voip": "VoIP", "fax": "Fax"}.get(ptype, "Unknown")
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
                (target_person_id, label, number, status, json.dumps(note_meta), ""),
            )
            phones_added += 1

        for em in payload_person.get("emails") or []:
            email = (em.get("email_address") or "").strip()
            if not email or touchpoint_exists(db, target_person_id, "Email", email):
                continue
            db.execute(
                """
                INSERT INTO touchpoints (person_id, channel_type, channel_label, value, status, note, last_attempted)
                VALUES (?, 'Email', 'Email', ?, 'Unknown', ?, ?)
                """,
                (target_person_id, email, "Imported from SkipSherpa person lookup", ""),
            )
            emails_added += 1

        for a in payload_person.get("addresses") or []:
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
                (target_person_id, street, city, state, zipcode),
            ).fetchone()
            if exists:
                continue
            addr_id = create_address(db, street, city, state, zipcode)
            db.execute(
                "INSERT INTO person_addresses (person_id, address_id, label) VALUES (?, ?, ?)",
                (target_person_id, addr_id, "SkipSherpa Address"),
            )
            addresses_added += 1

    def set_person_status(target_person_id, status_value):
        if not target_person_id:
            return
        db.execute("UPDATE people SET outreach_status = ? WHERE id = ?", (status_value, target_person_id))

    def find_or_create_person(first_name, middle_name, last_name, birth_year, create_note):
        norm_first, norm_last = normalize_first_last(first_name, last_name)
        candidates = db.execute(
            """
            SELECT id, first_name, middle_name, last_name, COALESCE(NULLIF(trim(birth_year), ''), '') AS birth_year
            FROM people
            WHERE lower(last_name) = lower(?)
            ORDER BY id DESC
            """,
            (norm_last,),
        ).fetchall()
        best_id = None
        for c in candidates:
            c_first, c_last = normalize_first_last(c["first_name"], c["last_name"])
            if c_first.lower() != norm_first.lower() or c_last.lower() != norm_last.lower():
                continue
            c_birth = (c["birth_year"] or "").strip()
            if birth_year and c_birth and c_birth == birth_year:
                if not (c["middle_name"] or "").strip() and (middle_name or "").strip():
                    db.execute("UPDATE people SET middle_name = ? WHERE id = ?", (middle_name.strip(), c["id"]))
                return c["id"]
            if best_id is None:
                best_id = c["id"]
        if best_id is not None:
            existing_middle = db.execute("SELECT middle_name FROM people WHERE id = ?", (best_id,)).fetchone()
            if existing_middle and not (existing_middle["middle_name"] or "").strip() and (middle_name or "").strip():
                db.execute("UPDATE people SET middle_name = ? WHERE id = ?", (middle_name.strip(), best_id))
            return best_id
        return create_person(
            db,
            first_name,
            last_name,
            middle_name=middle_name,
            notes=create_note,
        )

    def ensure_relationship(subject_id, related_id, relationship_type, note_obj):
        nonlocal relatives_added
        if not subject_id or not related_id or subject_id == related_id:
            return
        wanted_rel = (relationship_type or "Unknown").strip() or "Unknown"
        existing_any = db.execute(
            """
            SELECT id, relationship_type FROM person_relationships
            WHERE subject_person_id = ? AND related_person_id = ?
            ORDER BY id ASC
            """,
            (subject_id, related_id),
        ).fetchall()
        if existing_any:
            keep = sorted(
                existing_any,
                key=lambda r: (-_relationship_rank(r["relationship_type"]), r["id"]),
            )[0]
            if _relationship_rank(wanted_rel) > _relationship_rank(keep["relationship_type"]):
                db.execute(
                    "UPDATE person_relationships SET relationship_type = ? WHERE id = ?",
                    (wanted_rel, keep["id"]),
                )
            for row in existing_any:
                if row["id"] == keep["id"]:
                    continue
                db.execute("DELETE FROM person_relationships WHERE id = ?", (row["id"],))
            return
        db.execute(
            """
            INSERT INTO person_relationships (subject_person_id, related_person_id, relationship_type, note)
            VALUES (?, ?, ?, ?)
            """,
            (
                subject_id,
                related_id,
                wanted_rel,
                f"Imported from SkipSherpa person lookup | payload={json.dumps(note_obj)}",
            ),
        )
        relatives_added += 1

    relative_signatures = set()
    matched_signatures = set()
    for p in person_results:
        p_name_obj = p.get("person_name") if isinstance(p.get("person_name"), dict) else {}
        p_first, _p_middle, p_last, _p_suffix = composed_name(p_name_obj, p.get("name") or "")
        p_dob = (p.get("date_of_birth_month_year") or "").strip()
        p_birth_year = p_dob.split("-")[-1].strip() if p_dob and "-" in p_dob else ""
        matched_signatures.add(person_signature(p_first, p_last, p_birth_year))
        matched_signatures.add(person_signature(p_first, p_last, ""))
        for rel in p.get("relatives") or []:
            rel_name_obj = rel.get("person_name") if isinstance(rel.get("person_name"), dict) else {}
            rel_first, _rel_middle, rel_last, _rel_suffix = composed_name(rel_name_obj, rel.get("name") or "")
            rel_dob = (rel.get("date_of_birth_month_year") or "").strip()
            rel_birth_year = rel_dob.split("-")[-1].strip() if rel_dob and "-" in rel_dob else ""
            relative_signatures.add(person_signature(rel_first, rel_last, rel_birth_year))
            relative_signatures.add(person_signature(rel_first, rel_last, ""))

    for p in person_results:
        p_name = p.get("person_name") if isinstance(p.get("person_name"), dict) else {}
        first_name, middle_name, last_name, _suffix = composed_name(p_name, p.get("name") or "")
        target_birth_year = ""
        dob_my = (p.get("date_of_birth_month_year") or "").strip()
        if dob_my and "-" in dob_my:
            target_birth_year = dob_my.split("-")[-1].strip()

        source_norm_first, source_norm_last = normalize_first_last(source_first, source_last)
        target_norm_first, target_norm_last = normalize_first_last(first_name, last_name)
        is_source_match = (
            target_norm_first == source_norm_first
            and target_norm_last == source_norm_last
            and (not target_birth_year or not source_birth_year or target_birth_year == source_birth_year)
        )
        if is_source_match:
            target_id = person_id
        else:
            target_id = find_or_create_person(
                first_name,
                middle_name,
                last_name,
                target_birth_year,
                f"Imported from SkipSherpa for property {property_id}",
            )

        upsert_person_metrics(target_id, p)
        import_contact_data(target_id, p)
        cleanup_person_contact_and_relationship_duplicates(db, target_id)
        touched_person_ids.add(int(target_id))
        phone_count = len(p.get("phone_numbers") or [])
        email_count = len(p.get("emails") or [])
        address_count = len(p.get("addresses") or [])
        has_info = (phone_count + email_count + address_count) > 0
        relationship_type = "pFamily Member"
        ensure_relationship(person_id, target_id, relationship_type, p)
        if phone_count > 0:
            set_person_status(target_id, "Skip Traced")
        elif has_info:
            set_person_status(target_id, "Skipped")
        else:
            set_person_status(target_id, "Skipped, No Info")
        add_person_note(
            db,
            target_id,
            "SkipSherpa",
            f"Skiptrace run on {now_stamp} for Property {property_id}. This person was returned and listed as connected to the house.",
            {"source_person_id": person_id, "property_id": property_id},
        )

        for rel in p.get("relatives") or []:
            rel_name_obj = rel.get("person_name") if isinstance(rel.get("person_name"), dict) else {}
            rel_first, rel_middle, rel_last, _rel_suffix = composed_name(rel_name_obj, rel.get("name") or "")
            rel_dob = (rel.get("date_of_birth_month_year") or "").strip()
            rel_birth_year = rel_dob.split("-")[-1].strip() if rel_dob and "-" in rel_dob else ""
            rel_id = find_or_create_person(
                rel_first,
                rel_middle,
                rel_last,
                rel_birth_year,
                f"Imported from SkipSherpa relative for property {property_id}",
            )
            upsert_person_metrics(
                rel_id,
                {
                    "age": rel.get("age"),
                    "deceased": rel.get("deceased"),
                    "date_of_birth_month_year": rel.get("date_of_birth_month_year"),
                    "bankruptcy": False,
                    "employers": [],
                },
            )
            cleanup_person_contact_and_relationship_duplicates(db, rel_id)
            touched_person_ids.add(int(rel_id))
            rel_has_info_row = db.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM touchpoints WHERE person_id = ? AND lower(channel_type) = 'phone') AS phone_count,
                    (SELECT COUNT(*) FROM touchpoints WHERE person_id = ? AND lower(channel_type) = 'email') AS email_count,
                    (SELECT COUNT(*) FROM person_addresses WHERE person_id = ?) AS address_count
                """,
                (rel_id, rel_id, rel_id),
            ).fetchone()
            rel_phone_count = int(rel_has_info_row["phone_count"] or 0)
            rel_email_count = int(rel_has_info_row["email_count"] or 0)
            rel_address_count = int(rel_has_info_row["address_count"] or 0)
            rel_has_info = (rel_phone_count + rel_email_count + rel_address_count) > 0
            rel_is_matched = (
                person_signature(rel_first, rel_last, rel_birth_year) in matched_signatures
                or person_signature(rel_first, rel_last, "") in matched_signatures
            )
            rel_relationship_type = "pFamily Member" if rel_is_matched else "Relative"
            ensure_relationship(person_id, rel_id, rel_relationship_type, rel)
            if rel_relationship_type == "Relative":
                if rel_phone_count > 0:
                    set_person_status(rel_id, "Skipped")
                else:
                    set_person_status(rel_id, "Not Skipped")
            else:
                if rel_phone_count > 0:
                    set_person_status(rel_id, "Skip Traced")
                elif rel_has_info:
                    set_person_status(rel_id, "Skipped")
                else:
                    set_person_status(rel_id, "Skipped, No Info")
            source_full_name = " ".join(x for x in [first_name, middle_name, last_name] if x).strip() or f"Person #{target_id}"
            source_link = f"/person/{target_id}?property_id={property_id}"
            add_person_note(
                db,
                rel_id,
                "SkipSherpa",
                f"Skiptrace run on {now_stamp} for Property {property_id}. This person was listed as a relative of {source_full_name} ({source_link}) and connected to the house network.",
                {"source_person_id": target_id, "property_id": property_id},
            )

    dedupe_result = dedupe_people_by_exact_name_birth_year(
        db,
        person_ids=touched_person_ids,
        reason=f"SkipSherpa import dedupe for property {property_id}",
    )
    cleanup_person_contact_and_relationship_duplicates(db, person_id)

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
            f"Phones+{phones_added}, Emails+{emails_added}, Addresses+{addresses_added}, Relatives+{relatives_added}, Deduped+{dedupe_result.get('merged_count', 0)}",
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
                "deduped_people_merged": dedupe_result.get("merged_count", 0),
                "deduped_groups": dedupe_result.get("groups_merged", 0),
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
        "deduped_people_merged": dedupe_result.get("merged_count", 0),
        "deduped_groups": dedupe_result.get("groups_merged", 0),
    }


def fetch_search_results_duckduckgo(query, max_results=8):
    headers = {"User-Agent": "Mozilla/5.0 (compatible; DeepProspectCRM/1.0)"}
    url = "https://duckduckgo.com/html/"
    resp = requests.get(url, params={"q": query}, headers=headers, timeout=15)
    resp.raise_for_status()
    html = resp.text
    blocks = re.findall(r'(?is)<div class="result__body".*?</div>\s*</div>', html)
    out = []
    for b in blocks:
        link_match = re.search(r'(?is)<a[^>]+class="result__a"[^>]+href="([^"]+)"[^>]*>(.*?)</a>', b)
        snip_match = re.search(r'(?is)<a[^>]+class="result__snippet"[^>]*>(.*?)</a>', b)
        if not link_match:
            continue
        link = unescape(re.sub(r"&amp;", "&", link_match.group(1))).strip()
        title = re.sub(r"(?is)<[^>]+>", " ", link_match.group(2))
        snippet = re.sub(r"(?is)<[^>]+>", " ", snip_match.group(1) if snip_match else "")
        title = re.sub(r"\s+", " ", unescape(title)).strip()
        snippet = re.sub(r"\s+", " ", unescape(snippet)).strip()
        out.append({"title": title, "url": link, "snippet": snippet})
        if len(out) >= max_results:
            break
    return out


def summarize_person_web_intel(first_name, last_name, location_city, location_state, results):
    full_name = f"{(first_name or '').strip()} {(last_name or '').strip()}".strip()
    city_state_pattern = re.compile(r"\b([A-Z][A-Za-z\-\s]{1,32}?),\s*([A-Z]{2})\b")
    age_pattern = re.compile(r"\b(?:age|aged)\s*(\d{2,3})\b", re.IGNORECASE)
    social_domains = ["linkedin.com", "facebook.com", "instagram.com", "x.com", "twitter.com"]
    city_state_hits = {}
    age_hits = {}
    social_hits = []
    evidence = []

    for r in results:
        text = f"{r.get('title','')} {r.get('snippet','')}".strip()
        lower_text = text.lower()
        name_match = full_name.lower() in lower_text if full_name else False
        loc_match = False
        if location_city and location_state:
            loc_match = (location_city.lower() in lower_text and location_state.lower() in lower_text)
        for m in city_state_pattern.finditer(text):
            city_raw = m.group(1).strip()
            state_raw = m.group(2).strip()
            city_clean = re.sub(r"[^A-Za-z\-\s]", " ", city_raw)
            city_clean = re.sub(r"\s+", " ", city_clean).strip()
            if len(city_clean.split()) > 4:
                continue
            if full_name and any(tok.lower() in city_clean.lower() for tok in full_name.split() if len(tok) > 2):
                continue
            key = f"{city_clean}, {state_raw}"
            city_state_hits[key] = city_state_hits.get(key, 0) + 1
        for m in age_pattern.finditer(text):
            age = m.group(1)
            age_hits[age] = age_hits.get(age, 0) + 1
        url = (r.get("url") or "").lower()
        for dom in social_domains:
            if dom in url:
                social_hits.append(r.get("url"))
                break
        confidence = 0.35
        if name_match:
            confidence += 0.25
        if loc_match:
            confidence += 0.2
        if any(dom in url for dom in social_domains):
            confidence += 0.1
        evidence.append(
            {
                "title": r.get("title"),
                "url": r.get("url"),
                "snippet": r.get("snippet"),
                "confidence": round(min(0.95, confidence), 2),
            }
        )

    top_locations = sorted(city_state_hits.items(), key=lambda kv: kv[1], reverse=True)[:5]
    top_ages = sorted(age_hits.items(), key=lambda kv: kv[1], reverse=True)[:3]
    dedup_social = []
    seen = set()
    for s in social_hits:
        if s and s not in seen:
            dedup_social.append(s)
            seen.add(s)

    overall = 0.18
    if top_locations:
        overall += 0.24
    if dedup_social:
        overall += 0.18
    if top_ages:
        overall += 0.1
    if results:
        overall += 0.08

    return {
        "query_name": full_name,
        "top_city_state_mentions": [{"location": k, "mentions": v} for k, v in top_locations],
        "approx_age_mentions": [{"age": k, "mentions": v} for k, v in top_ages],
        "social_profiles": dedup_social[:8],
        "evidence": evidence[:10],
        "overall_confidence": round(min(0.88, overall), 2),
    }


def run_person_web_intel_fallback(db, property_id, person_id, first_name, last_name, street, city, state):
    query_variants = []
    full_name = f"{first_name} {last_name}".strip()
    if full_name:
        query_variants.append(f"\"{full_name}\" \"{city}, {state}\"")
        query_variants.append(f"\"{full_name}\" \"{state}\"")
    if street:
        query_variants.append(f"\"{full_name}\" \"{street}\" \"{city}\"")
    query_variants.append(f"\"{last_name}\" \"{city}\" \"{state}\"")

    aggregate = []
    errors = []
    for q in query_variants[:4]:
        try:
            rows = fetch_search_results_duckduckgo(q, max_results=6)
            for r in rows:
                item = dict(r)
                item["query"] = q
                aggregate.append(item)
        except Exception as exc:
            errors.append({"query": q, "error": str(exc)})
    dedup = []
    seen = set()
    for r in aggregate:
        key = (r.get("url") or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        dedup.append(r)

    intel = summarize_person_web_intel(first_name, last_name, city, state, dedup)
    confidence = intel.get("overall_confidence", 0.0)
    level = "High" if confidence >= 0.75 else ("Medium" if confidence >= 0.5 else "Low")
    note_lines = [
        f"Web Intel fallback executed for {full_name}.",
        f"Confidence: {level} ({confidence})",
        f"Top location hints: {', '.join([x['location'] for x in intel.get('top_city_state_mentions', [])]) or 'none'}",
        f"Approx age hints: {', '.join([x['age'] for x in intel.get('approx_age_mentions', [])]) or 'none'}",
        f"Social links found: {len(intel.get('social_profiles', []))}",
    ]
    add_person_note(
        db,
        person_id,
        "Web Intel Fallback",
        "\n".join(note_lines),
        {
            "property_id": property_id,
            "search_queries": query_variants[:4],
            "intel": intel,
            "errors": errors,
        },
    )
    db.execute(
        """
        INSERT INTO activity_log (property_id, person_id, activity_type, outcome, note)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            property_id,
            person_id,
            "Web Intel Fallback",
            f"Confidence {level}",
            json.dumps({"confidence": confidence, "social_count": len(intel.get('social_profiles', [])), "location_hints": intel.get('top_city_state_mentions', [])}),
        ),
    )
    return {"level": level, "confidence": confidence, "intel": intel, "errors": errors}


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


def ensure_default_sequence_campaign(db):
    row = db.execute("SELECT id FROM sequence_campaigns ORDER BY id ASC LIMIT 1").fetchone()
    if row:
        return row["id"]
    cur = db.execute(
        """
        INSERT INTO sequence_campaigns (name, description, status, stop_on_reply, send_window_start, send_window_end, timezone)
        VALUES (?, ?, 'Active', 1, '10:00', '16:30', 'America/New_York')
        """,
        (
            "Default Outreach Sequence",
            "Starter cadence: SMS, Email, SMS. Stops when lead replies.",
        ),
    )
    campaign_id = cur.lastrowid
    db.execute(
        """
        INSERT INTO sequence_steps (campaign_id, step_order, delay_minutes, channel, subject_template, body_template)
        VALUES (?, 1, 0, 'SMS', '', 'Hi {first_name}, quick question re: {property_address}. Are you the right contact?')
        """,
        (campaign_id,),
    )
    db.execute(
        """
        INSERT INTO sequence_steps (campaign_id, step_order, delay_minutes, channel, subject_template, body_template)
        VALUES (?, 2, 2 * 24 * 60, 'EMAIL', 'Question re: {property_address}', 'Hi {first_name}, can you help me with a quick question on {property_address}?')
        """,
        (campaign_id,),
    )
    db.execute(
        """
        INSERT INTO sequence_steps (campaign_id, step_order, delay_minutes, channel, subject_template, body_template)
        VALUES (?, 3, 3 * 24 * 60, 'SMS', '', 'Following up on {property_address}. If this is not a good number, please let me know.')
        """,
        (campaign_id,),
    )
    return campaign_id


def get_sequence_campaigns(db, only_active=False):
    where = "WHERE status = 'Active'" if only_active else ""
    return db.execute(
        f"""
        SELECT *
        FROM sequence_campaigns
        {where}
        ORDER BY created_at DESC, id DESC
        """
    ).fetchall()


def get_sequence_steps(db, campaign_id):
    return db.execute(
        """
        SELECT *
        FROM sequence_steps
        WHERE campaign_id = ? AND COALESCE(is_active, 1) = 1
        ORDER BY step_order ASC, id ASC
        """,
        (campaign_id,),
    ).fetchall()


def parse_hhmm(value, default_hour, default_minute):
    text = (value or "").strip()
    m = re.match(r"^(\d{1,2}):(\d{2})$", text)
    if not m:
        return default_hour, default_minute
    h = max(0, min(23, int(m.group(1))))
    mn = max(0, min(59, int(m.group(2))))
    return h, mn


def get_zoneinfo_safe(name):
    key = (name or "").strip() or "America/New_York"
    try:
        return ZoneInfo(key)
    except Exception:
        return EST_TZ


def sequence_window_adjust(dt_utc, campaign_row):
    tz = get_zoneinfo_safe(campaign_row["timezone"] if campaign_row else "America/New_York")
    local = dt_utc.replace(tzinfo=timezone.utc).astimezone(tz)
    sh, sm = parse_hhmm((campaign_row["send_window_start"] if campaign_row else "") or "10:00", 10, 0)
    eh, em = parse_hhmm((campaign_row["send_window_end"] if campaign_row else "") or "16:30", 16, 30)
    start = local.replace(hour=sh, minute=sm, second=0, microsecond=0)
    end = local.replace(hour=eh, minute=em, second=0, microsecond=0)
    if local < start:
        local = start
    elif local > end:
        local = (local + timedelta(days=1)).replace(hour=sh, minute=sm, second=0, microsecond=0)
    return local.astimezone(timezone.utc).replace(tzinfo=None)


def render_sequence_template(template_text, person_row, property_row, owner_row=None):
    text = template_text or ""
    property_address = ""
    property_city = ""
    property_state = ""
    property_zip = ""
    if property_row:
        property_address = (property_row["street"] or "").strip()
        property_city = (property_row["city"] or "").strip()
        property_state = (property_row["state"] or "").strip()
        property_zip = (property_row["postal_code"] or "").strip()
    owner_first = (owner_row["first_name"] if owner_row else "") or ""
    owner_last = (owner_row["last_name"] if owner_row else "") or ""
    owner_full = f"{owner_first} {owner_last}".strip()
    person_first = (person_row["first_name"] if person_row else "") or ""
    person_last = (person_row["last_name"] if person_row else "") or ""
    person_full = f"{person_first} {person_last}".strip()
    replacements = {
        "{first_name}": person_first,
        "{last_name}": person_last,
        "{full_name}": person_full,
        "{owner_first_name}": owner_first,
        "{owner_last_name}": owner_last,
        "{owner_full_name}": owner_full,
        "{property_address}": property_address,
        "{property_city}": property_city,
        "{property_state}": property_state,
        "{property_zip}": property_zip,
    }
    for k, v in replacements.items():
        text = text.replace(k, v or "")
    return text.strip()


def get_best_phone_for_person(db, person_id):
    rows = db.execute(
        """
        SELECT value, channel_label
        FROM touchpoints
        WHERE person_id = ? AND lower(channel_type) = 'phone' AND trim(COALESCE(value,'')) <> ''
        ORDER BY CASE
            WHEN lower(COALESCE(channel_label,'')) = 'mobile' THEN 0
            WHEN lower(COALESCE(channel_label,'')) = 'unknown' THEN 1
            ELSE 2
        END, id DESC
        LIMIT 1
        """,
        (person_id,),
    ).fetchone()
    if rows:
        return (rows["value"] or "").strip()
    p = db.execute("SELECT primary_phone FROM people WHERE id = ?", (person_id,)).fetchone()
    return (p["primary_phone"] or "").strip() if p else ""


def get_best_email_for_person(db, person_id):
    row = db.execute(
        """
        SELECT value
        FROM touchpoints
        WHERE person_id = ? AND lower(channel_type) = 'email' AND trim(COALESCE(value,'')) <> ''
        ORDER BY id DESC
        LIMIT 1
        """,
        (person_id,),
    ).fetchone()
    if row:
        return (row["value"] or "").strip()
    p = db.execute("SELECT primary_email FROM people WHERE id = ?", (person_id,)).fetchone()
    return (p["primary_email"] or "").strip() if p else ""


BLOCKED_CONTACT_STATUSES = {
    "incorrect",
    "dnc",
    "dnt",
    "dead",
    "undeliverable",
    "not in service",
    "invalid",
    "bounced",
    "opt out",
}


def is_sms_channel_label_allowed(channel_label):
    label = (channel_label or "").strip().lower()
    # Never text known non-SMS destinations.
    if label in {"landline", "fax"}:
        return False
    return label in {"mobile", "unknown", "voip", ""}


def is_sms_touchpoint_allowed(channel_label, status):
    label = (channel_label or "").strip().lower()
    st = (status or "").strip().lower()
    if not is_sms_channel_label_allowed(label):
        return False
    if st in BLOCKED_CONTACT_STATUSES:
        return False
    return True


def validate_sms_recipient_for_person(db, person_id, to_number):
    normalized = normalize_phone(to_number)
    if not normalized:
        return False, "invalid phone format"
    if not person_id:
        return True, ""
    rows = db.execute(
        """
        SELECT id, channel_label, status, value
        FROM touchpoints
        WHERE person_id = ? AND lower(channel_type) = 'phone'
        """,
        (person_id,),
    ).fetchall()
    matches = [r for r in rows if normalize_phone(r["value"]) == normalized]
    if not matches:
        return True, ""
    for r in matches:
        if not is_sms_channel_label_allowed(r["channel_label"]):
            return False, f"phone type '{r['channel_label'] or 'Unknown'}' is not SMS-eligible"
        st = (r["status"] or "").strip().lower()
        if st in BLOCKED_CONTACT_STATUSES:
            return False, f"phone status '{r['status']}' is blocked for SMS"
    return True, ""


def get_sequence_sms_targets(db, person_id):
    rows = db.execute(
        """
        SELECT id, value, channel_label, status
        FROM touchpoints
        WHERE person_id = ? AND lower(channel_type) = 'phone' AND trim(COALESCE(value,'')) <> ''
        ORDER BY id DESC
        """,
        (person_id,),
    ).fetchall()
    keep = []
    skipped = []
    seen = set()
    for r in rows:
        value = (r["value"] or "").strip()
        label = (r["channel_label"] or "").strip().lower()
        status = (r["status"] or "").strip().lower()
        norm = normalize_phone(value)
        if not norm:
            skipped.append({"value": value, "reason": "invalid phone format"})
            continue
        if norm in seen:
            continue
        seen.add(norm)
        if label not in {"mobile", "unknown"}:
            skipped.append({"value": value, "reason": "not mobile/unknown"})
            continue
        if not is_sms_touchpoint_allowed(r["channel_label"], r["status"]):
            skipped.append({"value": value, "reason": f"status={status}"})
            continue
        keep.append(value)
    return keep, skipped


def get_manual_sms_targets_for_person(db, person_id):
    rows = db.execute(
        """
        SELECT value, channel_label, status
        FROM touchpoints
        WHERE person_id = ? AND lower(channel_type) = 'phone' AND trim(COALESCE(value,'')) <> ''
        ORDER BY
            CASE
                WHEN lower(COALESCE(channel_label,'')) = 'mobile' THEN 0
                WHEN lower(COALESCE(channel_label,'')) = 'unknown' THEN 1
                WHEN lower(COALESCE(channel_label,'')) = 'voip' THEN 2
                ELSE 3
            END,
            id DESC
        """,
        (person_id,),
    ).fetchall()
    keep = []
    seen = set()
    for r in rows:
        value = (r["value"] or "").strip()
        norm = normalize_phone(value)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        label = (r["channel_label"] or "").strip().lower()
        if label not in {"mobile", "unknown", "voip"}:
            continue
        if not is_sms_touchpoint_allowed(r["channel_label"], r["status"]):
            continue
        keep.append(value)
    return keep


def get_sequence_email_targets(db, person_id):
    rows = db.execute(
        """
        SELECT id, value, status
        FROM touchpoints
        WHERE person_id = ? AND lower(channel_type) = 'email' AND trim(COALESCE(value,'')) <> ''
        ORDER BY id DESC
        """,
        (person_id,),
    ).fetchall()
    keep = []
    skipped = []
    seen = set()
    for r in rows:
        value = (r["value"] or "").strip()
        status = (r["status"] or "").strip().lower()
        key = value.lower()
        if not value or key in seen:
            continue
        seen.add(key)
        if status in BLOCKED_CONTACT_STATUSES:
            skipped.append({"value": value, "reason": f"status={status}"})
            continue
        if not is_likely_valid_email(value):
            skipped.append({"value": value, "reason": "invalid email"})
            continue
        keep.append(value)
    p = db.execute("SELECT primary_email FROM people WHERE id = ?", (person_id,)).fetchone()
    primary = (p["primary_email"] or "").strip() if p else ""
    pkey = primary.lower()
    if primary and pkey not in seen:
        if is_likely_valid_email(primary):
            keep.append(primary)
        else:
            skipped.append({"value": primary, "reason": "invalid primary email"})
    return keep, skipped


def get_manual_email_targets_for_person(db, person_id):
    rows = db.execute(
        """
        SELECT value, status
        FROM touchpoints
        WHERE person_id = ? AND lower(channel_type) = 'email' AND trim(COALESCE(value,'')) <> ''
        ORDER BY id DESC
        """,
        (person_id,),
    ).fetchall()
    keep = []
    seen = set()
    blocked = set()
    for r in rows:
        value = (r["value"] or "").strip()
        key = value.lower()
        if not value or key in seen:
            continue
        seen.add(key)
        status = (r["status"] or "").strip().lower()
        if status in BLOCKED_CONTACT_STATUSES:
            blocked.add(key)
            continue
        if not is_likely_valid_email(value):
            continue
        keep.append(value)
    p = db.execute("SELECT primary_email FROM people WHERE id = ?", (person_id,)).fetchone()
    primary = (p["primary_email"] or "").strip() if p else ""
    pkey = primary.lower()
    if primary and pkey not in seen and pkey not in blocked and is_likely_valid_email(primary):
        keep.append(primary)
    return keep


def validate_email_recipient_for_person(db, person_id, to_email):
    email = (to_email or "").strip()
    if not is_likely_valid_email(email):
        return False, "invalid email format"
    if not person_id:
        return True, ""
    rows = db.execute(
        """
        SELECT status, value
        FROM touchpoints
        WHERE person_id = ? AND lower(channel_type) = 'email'
        """,
        (person_id,),
    ).fetchall()
    matches = [r for r in rows if ((r["value"] or "").strip().lower() == email.lower())]
    if not matches:
        return True, ""
    for r in matches:
        st = (r["status"] or "").strip().lower()
        if st in BLOCKED_CONTACT_STATUSES:
            return False, f"email status '{r['status']}' is blocked"
    return True, ""


def extract_steps_from_form(form, prefix):
    orders = form.getlist(f"{prefix}order[]")
    delays = form.getlist(f"{prefix}delay[]")
    channels = form.getlist(f"{prefix}channel[]")
    subjects = form.getlist(f"{prefix}subject[]")
    bodies = form.getlist(f"{prefix}body[]")
    steps = []
    max_len = max(len(orders), len(delays), len(channels), len(subjects), len(bodies), 0)
    for i in range(max_len):
        order_val = orders[i] if i < len(orders) else str(i + 1)
        delay_val = delays[i] if i < len(delays) else "0"
        channel_val = channels[i] if i < len(channels) else "SMS"
        subject_val = subjects[i] if i < len(subjects) else ""
        body_val = bodies[i] if i < len(bodies) else ""
        body_text = (body_val or "").strip()
        if not body_text:
            continue
        try:
            order_int = max(1, int(str(order_val).strip() or str(i + 1)))
        except ValueError:
            order_int = i + 1
        try:
            delay_hours = max(0.0, float(str(delay_val).strip() or "0"))
        except ValueError:
            delay_hours = 0.0
        delay_int = int(round(delay_hours * 60))
        channel = (channel_val or "SMS").strip().upper()
        if channel not in {"SMS", "EMAIL"}:
            channel = "SMS"
        steps.append(
            {
                "order": order_int,
                "delay_minutes": delay_int,
                "channel": channel,
                "subject_template": (subject_val or "").strip(),
                "body_template": body_text,
            }
        )
    steps.sort(key=lambda s: (s["order"],))
    normalized = []
    idx = 1
    for s in steps:
        normalized.append(
            {
                "order": idx,
                "delay_minutes": s["delay_minutes"],
                "delay_hours": round((s["delay_minutes"] or 0) / 60.0, 4),
                "channel": s["channel"],
                "subject_template": s["subject_template"],
                "body_template": s["body_template"],
            }
        )
        idx += 1
    return normalized


def enroll_person_in_sequence(db, campaign_id, property_id, person_id):
    if not str(campaign_id).isdigit() or not str(property_id).isdigit() or not str(person_id).isdigit():
        raise ValueError("campaign_id, property_id, and person_id are required")
    campaign_id = int(campaign_id)
    property_id = int(property_id)
    person_id = int(person_id)
    person = db.execute("SELECT id, deceased FROM people WHERE id = ?", (person_id,)).fetchone()
    if not person:
        raise ValueError("Person not found")
    if int(person["deceased"] or 0) == 1:
        raise ValueError("Deceased person cannot be enrolled")
    campaign = db.execute("SELECT * FROM sequence_campaigns WHERE id = ?", (campaign_id,)).fetchone()
    if not campaign:
        raise ValueError("Campaign not found")
    steps = get_sequence_steps(db, campaign_id)
    if not steps:
        raise ValueError("Campaign has no active steps")
    phone_targets, _ = get_sequence_sms_targets(db, person_id)
    email_targets, _ = get_sequence_email_targets(db, person_id)
    has_phone = bool(phone_targets)
    has_email = bool(email_targets)
    if not has_phone and not has_email:
        raise ValueError("No usable contact info for this person")
    needed_channels = {str(s["channel"] or "SMS").upper() for s in steps}
    if "SMS" in needed_channels and not has_phone and "EMAIL" not in needed_channels:
        raise ValueError("Campaign requires SMS but no phone is available")
    if "EMAIL" in needed_channels and not has_email and "SMS" not in needed_channels:
        raise ValueError("Campaign requires Email but no valid email is available")
    exists = db.execute(
        """
        SELECT id
        FROM sequence_enrollments
        WHERE campaign_id = ? AND property_id = ? AND person_id = ? AND status = 'Active'
        LIMIT 1
        """,
        (campaign_id, property_id, person_id),
    ).fetchone()
    if exists:
        return exists["id"], False
    now_utc = datetime.utcnow()
    next_run = sequence_window_adjust(now_utc, campaign)
    cur = db.execute(
        """
        INSERT INTO sequence_enrollments (campaign_id, property_id, person_id, status, started_at, next_run_at, last_step_order)
        VALUES (?, ?, ?, 'Active', ?, ?, 0)
        """,
        (campaign_id, property_id, person_id, format_db_time(now_utc), format_db_time(next_run)),
    )
    return cur.lastrowid, True


def run_sequence_tick():
    ensure_db()
    db = open_sqlite_connection()
    try:
        now = datetime.utcnow()
        due = db.execute(
            """
            SELECT e.*, c.name AS campaign_name, c.stop_on_reply, c.send_window_start, c.send_window_end, c.timezone
            FROM sequence_enrollments e
            JOIN sequence_campaigns c ON c.id = e.campaign_id
            WHERE e.status = 'Active'
              AND c.status = 'Active'
              AND COALESCE(e.next_run_at, '') <= ?
            ORDER BY e.next_run_at ASC, e.id ASC
            LIMIT 25
            """,
            (format_db_time(now),),
        ).fetchall()

        for enr in due:
            campaign = {
                "send_window_start": enr["send_window_start"],
                "send_window_end": enr["send_window_end"],
                "timezone": enr["timezone"],
            }
            current_slot = sequence_window_adjust(now, campaign)
            if current_slot > now + timedelta(seconds=10):
                db.execute(
                    "UPDATE sequence_enrollments SET next_run_at = ? WHERE id = ?",
                    (format_db_time(current_slot), enr["id"]),
                )
                continue

            person = db.execute("SELECT * FROM people WHERE id = ?", (enr["person_id"],)).fetchone()
            prop = db.execute(
                """
                SELECT p.id, p.owner_person_id, a.street, a.city, a.state, a.postal_code
                FROM properties p
                JOIN addresses a ON a.id = p.property_address_id
                WHERE p.id = ?
                """,
                (enr["property_id"],),
            ).fetchone()
            if not person or not prop:
                db.execute(
                    "UPDATE sequence_enrollments SET status = 'Stopped', stopped_reason = ?, completed_at = ? WHERE id = ?",
                    ("Missing person or property context", format_db_time(now), enr["id"]),
                )
                continue

            owner = db.execute("SELECT * FROM people WHERE id = ?", (prop["owner_person_id"],)).fetchone() if prop["owner_person_id"] else None

            if int(enr["stop_on_reply"] or 0) == 1:
                inbound = db.execute(
                    """
                    SELECT id
                    FROM communications
                    WHERE property_id = ? AND person_id = ? AND lower(direction) = 'inbound'
                      AND sent_at >= ?
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (enr["property_id"], enr["person_id"], enr["started_at"]),
                ).fetchone()
                if inbound:
                    db.execute(
                        "UPDATE sequence_enrollments SET status = 'Stopped', stopped_reason = ?, completed_at = ? WHERE id = ?",
                        ("Reply received", format_db_time(now), enr["id"]),
                    )
                    continue

            steps = get_sequence_steps(db, enr["campaign_id"])
            next_step = None
            for st in steps:
                if int(st["step_order"] or 0) > int(enr["last_step_order"] or 0):
                    next_step = st
                    break
            if not next_step:
                db.execute(
                    "UPDATE sequence_enrollments SET status = 'Completed', completed_at = ? WHERE id = ?",
                    (format_db_time(now), enr["id"]),
                )
                continue

            channel = (next_step["channel"] or "SMS").strip().upper()
            status = "Sent"
            comm_id = None
            error_text = ""
            try:
                body = render_sequence_template(next_step["body_template"], person, prop, owner)
                if channel == "EMAIL":
                    email_targets, email_skipped = get_sequence_email_targets(db, enr["person_id"])
                    if not email_targets:
                        raise ValueError("No valid email targets for sequence step")
                    subject = render_sequence_template(next_step["subject_template"] or "", person, prop, owner)
                    sent_count = 0
                    failures = []
                    for to_email in email_targets:
                        try:
                            sent = send_gmail_email(
                                db,
                                to_email=to_email,
                                subject=subject,
                                body=body,
                                property_id=enr["property_id"],
                                person_id=enr["person_id"],
                            )
                            ext_id = sent.get("message_id") or ""
                            cur = db.execute(
                                """
                                INSERT INTO communications (property_id, person_id, channel, direction, from_number, to_number, body, status, is_read, sent_at, external_id)
                                VALUES (?, ?, 'EMAIL', 'Outbound', ?, ?, ?, ?, 1, ?, ?)
                                """,
                                (
                                    enr["property_id"],
                                    enr["person_id"],
                                    "Email sent via Gmail",
                                    to_email,
                                    body,
                                    "Sent",
                                    format_db_time(now),
                                    ext_id,
                                ),
                            )
                            if comm_id is None:
                                comm_id = cur.lastrowid
                            backfill_outbound_gmail_ids(db, cur.lastrowid)
                            sent_count += 1
                        except Exception as send_exc:
                            failures.append(f"{to_email}: {send_exc}")
                    if sent_count == 0:
                        raise ValueError("; ".join(failures) if failures else "No email messages sent")
                    skip_txt = ", ".join([f"{s['value']} ({s['reason']})" for s in email_skipped[:5]])
                    fail_txt = "; ".join(failures[:3])
                    bits = []
                    if email_skipped:
                        bits.append(f"Skipped {len(email_skipped)}")
                    if failures:
                        bits.append(f"Failed {len(failures)}")
                    if bits:
                        error_text = " | ".join(bits)
                        if skip_txt:
                            error_text += f" | {skip_txt}"
                        if fail_txt:
                            error_text += f" | {fail_txt}"
                else:
                    sms_targets, sms_skipped = get_sequence_sms_targets(db, enr["person_id"])
                    if not sms_targets:
                        raise ValueError("No eligible mobile targets for sequence step")
                    sent_count = 0
                    failures = []
                    from_num = get_deep_dive_sms_number(db)
                    for to_number in sms_targets:
                        try:
                            send_res = send_smrtphone_sms(to_number, body, from_number=from_num)
                            sms_status = send_res.get("status") or "Sent"
                            ext_id = send_res.get("sms_id") or ""
                            cur = db.execute(
                                """
                                INSERT INTO communications (property_id, person_id, channel, direction, from_number, to_number, body, status, is_read, sent_at, external_id)
                                VALUES (?, ?, 'SMS', 'Outbound', ?, ?, ?, ?, 1, ?, ?)
                                """,
                                (
                                    enr["property_id"],
                                    enr["person_id"],
                                    from_num,
                                    to_number,
                                    body,
                                    sms_status,
                                    format_db_time(now),
                                    ext_id,
                                ),
                            )
                            if comm_id is None:
                                comm_id = cur.lastrowid
                            update_person_outreach_status_for_sms(db, enr["person_id"], "outbound_success")
                            sent_count += 1
                        except Exception as send_exc:
                            apply_touchpoint_status_inference(db, to_number, str(send_exc))
                            failures.append(f"{to_number}: {send_exc}")
                    if sent_count == 0:
                        raise ValueError("; ".join(failures) if failures else "No SMS messages sent")
                    skip_txt = ", ".join([f"{s['value']} ({s['reason']})" for s in sms_skipped[:5]])
                    fail_txt = "; ".join(failures[:3])
                    bits = []
                    if sms_skipped:
                        bits.append(f"Skipped {len(sms_skipped)}")
                    if failures:
                        bits.append(f"Failed {len(failures)}")
                    if bits:
                        error_text = " | ".join(bits)
                        if skip_txt:
                            error_text += f" | {skip_txt}"
                        if fail_txt:
                            error_text += f" | {fail_txt}"
            except Exception as exc:
                status = "Failed"
                error_text = str(exc)

            db.execute(
                """
                INSERT INTO sequence_events (enrollment_id, step_id, step_order, channel, status, communication_id, error_text)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    enr["id"],
                    next_step["id"] if next_step else None,
                    next_step["step_order"] if next_step else None,
                    channel,
                    status,
                    comm_id,
                    error_text,
                ),
            )

            if status != "Sent":
                retry_at = sequence_window_adjust(now + timedelta(minutes=20), campaign)
                db.execute(
                    "UPDATE sequence_enrollments SET next_run_at = ?, stopped_reason = ? WHERE id = ?",
                    (format_db_time(retry_at), error_text[:250], enr["id"]),
                )
                continue

            next_after = None
            for st in steps:
                if int(st["step_order"] or 0) > int(next_step["step_order"] or 0):
                    next_after = st
                    break
            if next_after:
                delay_minutes = max(0, int(next_after["delay_minutes"] or 0))
                due_utc = sequence_window_adjust(now + timedelta(minutes=delay_minutes), campaign)
                db.execute(
                    "UPDATE sequence_enrollments SET last_step_order = ?, next_run_at = ?, stopped_reason = NULL WHERE id = ?",
                    (next_step["step_order"], format_db_time(due_utc), enr["id"]),
                )
            else:
                db.execute(
                    "UPDATE sequence_enrollments SET last_step_order = ?, status = 'Completed', completed_at = ?, next_run_at = NULL, stopped_reason = NULL WHERE id = ?",
                    (next_step["step_order"], format_db_time(now), enr["id"]),
                )
        db.commit()
    except Exception:
        db.rollback()
        try:
            log_app_error(
                db,
                source="sequence_worker",
                error_message="run_sequence_tick failed",
                details=traceback.format_exc(),
                route="run_sequence_tick",
                status_code=500,
            )
            db.commit()
        except Exception:
            db.rollback()
    finally:
        db.close()


def infer_touchpoint_update_from_status(status_text):
    s = (status_text or "").strip().lower()
    if not s:
        return {}

    update = {}
    if "landline" in s:
        update["channel_label"] = "Landline"
    if "mobile" in s or "wireless" in s or "cell" in s:
        update["channel_label"] = "Mobile"
    if "voip" in s:
        update["channel_label"] = "VoIP"
    if "fax" in s:
        update["channel_label"] = "Fax"
    if any(x in s for x in ["undeliverable", "not deliver", "no route", "failed", "failure"]):
        update["status"] = "Undeliverable"
    if any(x in s for x in ["no longer in service", "not in service", "disconnected", "deactivated"]):
        update["status"] = "Not in service"
    if "invalid" in s:
        update["status"] = "Incorrect"
    if "dnc" in s or "do not call" in s:
        update["status"] = "DNC"
    if "dnt" in s or "do not text" in s:
        update["status"] = "DNT"
    if any(x in s for x in ["delivered", "sent"]):
        # Only elevate to "Correct" when no higher-priority failure signal exists.
        if update.get("status") in (None, "", "Unknown"):
            update["status"] = "Correct"
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


def get_setting(db, key, default=""):
    row = db.execute("SELECT value FROM app_settings WHERE key = ?", ((key or "").strip(),)).fetchone()
    if not row:
        return default
    return row["value"] if row["value"] is not None else default


def set_setting(db, key, value):
    key = (key or "").strip()
    db.execute(
        """
        INSERT INTO app_settings (key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            updated_at = excluded.updated_at
        """,
        (key, (value or "").strip(), datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")),
    )


def _parse_uploaded_json_rows(upload):
    if not upload:
        return []
    raw = upload.read()
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="ignore")
    raw = (raw or "").strip()
    if not raw:
        return []
    parsed = json.loads(raw)
    if isinstance(parsed, dict):
        if isinstance(parsed.get("rows"), list):
            parsed = parsed.get("rows")
        else:
            parsed = [parsed]
    if not isinstance(parsed, list):
        raise ValueError("Uploaded JSON must be an array of row objects.")
    cleaned = []
    for item in parsed:
        if isinstance(item, dict):
            cleaned.append(item)
    return cleaned


def import_referral_bundle_rows(db, realtor_rows, referral_rows, push_rows):
    counts = {
        "referral_realtors": 0,
        "reisift_referrals": 0,
        "referral_push_activity": 0,
    }

    for r in realtor_rows:
        db.execute(
            """
            INSERT INTO referral_realtors (id, first_name, last_name, email, phone, brokerage, target_markets, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                first_name = excluded.first_name,
                last_name = excluded.last_name,
                email = excluded.email,
                phone = excluded.phone,
                brokerage = excluded.brokerage,
                target_markets = excluded.target_markets,
                created_at = excluded.created_at
            """,
            (
                r.get("id"),
                r.get("first_name"),
                r.get("last_name"),
                r.get("email"),
                r.get("phone"),
                r.get("brokerage"),
                r.get("target_markets"),
                r.get("created_at"),
            ),
        )
        counts["referral_realtors"] += 1

    for r in referral_rows:
        db.execute(
            """
            INSERT INTO reisift_referrals (
                id, property_uuid, status, full_address, owner_names, payload_json,
                referral_status, winning_realtor_id, referral_notes, is_active, last_synced_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(property_uuid) DO UPDATE SET
                status = excluded.status,
                full_address = excluded.full_address,
                owner_names = excluded.owner_names,
                payload_json = excluded.payload_json,
                referral_status = excluded.referral_status,
                winning_realtor_id = excluded.winning_realtor_id,
                referral_notes = excluded.referral_notes,
                is_active = excluded.is_active,
                last_synced_at = excluded.last_synced_at
            """,
            (
                r.get("id"),
                r.get("property_uuid"),
                r.get("status"),
                r.get("full_address"),
                r.get("owner_names"),
                r.get("payload_json"),
                r.get("referral_status"),
                r.get("winning_realtor_id"),
                r.get("referral_notes"),
                r.get("is_active"),
                r.get("last_synced_at"),
            ),
        )
        counts["reisift_referrals"] += 1

    for r in push_rows:
        db.execute(
            """
            INSERT INTO referral_push_activity (
                id, property_uuid, realtor_id, to_number, message_body, status, external_id, response_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                property_uuid = excluded.property_uuid,
                realtor_id = excluded.realtor_id,
                to_number = excluded.to_number,
                message_body = excluded.message_body,
                status = excluded.status,
                external_id = excluded.external_id,
                response_json = excluded.response_json,
                created_at = excluded.created_at
            """,
            (
                r.get("id"),
                r.get("property_uuid"),
                r.get("realtor_id"),
                r.get("to_number"),
                r.get("message_body"),
                r.get("status"),
                r.get("external_id"),
                r.get("response_json"),
                r.get("created_at"),
            ),
        )
        counts["referral_push_activity"] += 1
    return counts


def get_direct_mail_settings(db):
    return {
        "sender_first_name": get_setting(db, "dm_sender_first_name", ""),
        "sender_last_name": get_setting(db, "dm_sender_last_name", ""),
        "sender_company_name": get_setting(db, "dm_sender_company_name", ""),
        "sender_address1": get_setting(db, "dm_sender_address1", ""),
        "sender_address2": get_setting(db, "dm_sender_address2", ""),
        "sender_city": get_setting(db, "dm_sender_city", ""),
        "sender_state": get_setting(db, "dm_sender_state", ""),
        "sender_zip": get_setting(db, "dm_sender_zip", ""),
        "sender_phone": get_setting(db, "dm_sender_phone", ""),
        "sender_email": get_setting(db, "dm_sender_email", ""),
        "sender_website": get_setting(db, "dm_sender_website", ""),
        "postage_type": get_setting(db, "dm_postage_type", ""),
        "envelope_type": get_setting(db, "dm_envelope_type", ""),
    }


def get_deep_dive_sms_number(db):
    return (get_setting(db, "deep_dive_smrtphone_from", "") or "").strip() or SMRTPHONE_FROM_NUMBER


def get_referral_sms_number(db):
    return (get_setting(db, "referral_smrtphone_from", "") or "").strip() or SMRTPHONE_FROM_NUMBER


def get_email_settings(db):
    return {
        "from_name": get_setting(db, "email_from_name", ""),
        "from_address": get_setting(db, "email_from_address", ""),
        "provider": (get_setting(db, "email_provider", "gmail_api") or "gmail_api").strip().lower(),
        "resend_api_key": get_setting(db, "email_resend_api_key", ""),
        "resend_base_url": get_setting(db, "email_resend_base_url", "https://api.resend.com"),
        "gmail_api_client_id": get_setting(db, "email_gmail_api_client_id", ""),
        "gmail_api_client_secret": get_setting(db, "email_gmail_api_client_secret", ""),
        "gmail_api_refresh_token": get_setting(db, "email_gmail_api_refresh_token", ""),
        "gmail_webhook_token": get_setting(db, "email_gmail_webhook_token", ""),
    }


def get_slack_settings(db):
    return {
        "webhook_url": get_setting(db, "slack_webhook_url", ""),
        "signing_secret": get_setting(db, "slack_signing_secret", ""),
        "default_channel": get_setting(db, "slack_default_channel", ""),
    }


def get_automation_settings(db):
    clever_enabled_raw = get_setting(db, "automation_clever_leads_enabled", "")
    if not clever_enabled_raw:
        clever_enabled_raw = get_setting(db, "automation_test_enabled", "1")
    reisift_placeholder_raw = get_setting(db, "automation_reisift_placeholder_enabled", "1")
    auto_send_realtors_raw = get_setting(db, "automation_auto_send_realtors_enabled", "1")
    return {
        "clever_leads_enabled": (clever_enabled_raw or "0").strip() in {"1", "true", "TRUE", "yes", "on"},
        "reisift_placeholder_enabled": (reisift_placeholder_raw or "0").strip() in {"1", "true", "TRUE", "yes", "on"},
        "auto_send_realtors_enabled": (auto_send_realtors_raw or "0").strip() in {"1", "true", "TRUE", "yes", "on"},
    }


def get_integration_api_key(db):
    return (get_setting(db, "integration_api_key", "") or os.getenv("INTEGRATION_API_KEY", "")).strip()


def integration_auth_ok(db, req):
    expected = get_integration_api_key(db)
    if not expected:
        return False
    provided = (
        (req.headers.get("X-Integration-Key") or "").strip()
        or (req.headers.get("Authorization") or "").replace("Bearer", "").strip()
    )
    if not provided:
        return False
    return hmac.compare_digest(provided, expected)


def send_slack_notification(db, text, blocks=None, channel=""):
    settings = get_slack_settings(db)
    webhook_url = (settings.get("webhook_url") or "").strip()
    if not webhook_url:
        return {"ok": False, "error": "Slack webhook not configured"}
    payload = {"text": (text or "").strip() or "Notification"}
    target_channel = (channel or settings.get("default_channel") or "").strip()
    if target_channel:
        payload["channel"] = target_channel
    if isinstance(blocks, list) and blocks:
        payload["blocks"] = blocks
    response = requests.post(
        webhook_url,
        headers={"Content-Type": "application/json"},
        json=payload,
        timeout=15,
    )
    if not response.ok:
        raise ValueError(f"Slack webhook failed ({response.status_code}): {response.text}")
    return {"ok": True}


def verify_slack_signature(req, signing_secret):
    secret = (signing_secret or "").strip()
    if not secret:
        return False, "Missing Slack signing secret configuration"
    timestamp = (req.headers.get("X-Slack-Request-Timestamp") or "").strip()
    signature = (req.headers.get("X-Slack-Signature") or "").strip()
    if not timestamp or not signature:
        return False, "Missing Slack signature headers"
    try:
        ts = int(timestamp)
    except ValueError:
        return False, "Invalid Slack timestamp"
    now_ts = int(time.time())
    if abs(now_ts - ts) > 60 * 5:
        return False, "Slack timestamp too old"
    body = req.get_data(as_text=True) or ""
    basestring = f"v0:{timestamp}:{body}"
    digest = hmac.new(secret.encode("utf-8"), basestring.encode("utf-8"), hashlib.sha256).hexdigest()
    expected = f"v0={digest}"
    if not hmac.compare_digest(expected, signature):
        return False, "Slack signature mismatch"
    return True, ""


def upsert_integration_event(db, source, event_key, payload):
    key = (event_key or "").strip()
    if not key:
        db.execute(
            "INSERT INTO integration_events (source, event_key, payload_json) VALUES (?, NULL, ?)",
            ((source or "").strip(), json.dumps(payload or {})),
        )
        return True
    exists = db.execute(
        "SELECT id FROM integration_events WHERE source = ? AND event_key = ?",
        ((source or "").strip(), key),
    ).fetchone()
    if exists:
        return False
    db.execute(
        "INSERT INTO integration_events (source, event_key, payload_json) VALUES (?, ?, ?)",
        ((source or "").strip(), key, json.dumps(payload or {})),
    )
    return True


def _row_value_by_keys(row_data, keys):
    if not isinstance(row_data, dict):
        return ""
    lowered = {str(k).strip().lower(): v for k, v in row_data.items()}
    for key in keys:
        val = lowered.get(str(key).strip().lower())
        if val is not None and str(val).strip():
            return str(val).strip()
    return ""


def _parse_seller_name_for_owner(name):
    raw = (name or "").strip()
    if not raw:
        return {"first_name": "", "last_name": ""}
    if "," in raw:
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        if len(parts) >= 2:
            return {"first_name": parts[1], "last_name": parts[0]}
    bits = [b for b in raw.split() if b.strip()]
    if len(bits) == 1:
        return {"first_name": bits[0], "last_name": ""}
    # Keep first/last deterministic for downstream CRMs that expect strict split fields.
    return {"first_name": bits[0], "last_name": bits[-1]}


def _build_clever_lead_notes(row_data):
    owner_name = _row_value_by_keys(row_data, ["Seller Name"])
    owner_split = _parse_seller_name_for_owner(owner_name)
    fields = [
        ("Create Date", _row_value_by_keys(row_data, ["Create Date"])),
        ("Portal Link (Provide Updates)", _row_value_by_keys(row_data, ["Portal Link (Provide Updates)", "Portal Link"])),
        ("Seller Name", _row_value_by_keys(row_data, ["Seller Name"])),
        ("Owner First Name", owner_split.get("first_name") or ""),
        ("Owner Last Name", owner_split.get("last_name") or ""),
        ("Address", _row_value_by_keys(row_data, ["Address"])),
        ("Phone Number", _row_value_by_keys(row_data, ["Phone Number", "Phone"])),
        ("Email", _row_value_by_keys(row_data, ["Email"])),
        ("Time Frame", _row_value_by_keys(row_data, ["Time Frame"])),
        ("Property Type", _row_value_by_keys(row_data, ["Property Type"])),
        ("Home Condition", _row_value_by_keys(row_data, ["Home Condition"])),
        ("Market Value Estimate", _row_value_by_keys(row_data, ["Market Value Estimate"])),
        ("Asking Price", _row_value_by_keys(row_data, ["Asking Price"])),
        ("Occupancy Type", _row_value_by_keys(row_data, ["Occupancy Type"])),
        ("Listed On Market", _row_value_by_keys(row_data, ["Listed On Market"])),
        ("Investor Internal Notes", _row_value_by_keys(row_data, ["Investor Internal Notes"])),
        ("Clever Notes", _row_value_by_keys(row_data, ["Clever Notes"])),
    ]
    lines = ["Source: Clever Leads (Google Sheets)"]
    for label, value in fields:
        if value:
            lines.append(f"{label}: {value}")
    return "\n".join(lines)


def add_clever_webhook_note(db, event_key, note_body, payload=None):
    db.execute(
        """
        INSERT INTO clever_lead_webhook_notes (event_key, note_body, payload_json)
        VALUES (?, ?, ?)
        """,
        (
            (event_key or "").strip() or None,
            (note_body or "").strip(),
            json.dumps(payload or {}),
        ),
    )


def _get_google_service_account_info():
    raw = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON: {exc}") from exc


def fetch_google_sheet_rows():
    if not GOOGLE_SHEETS_SPREADSHEET_ID:
        raise ValueError("GOOGLE_SHEETS_SPREADSHEET_ID is not configured.")
    if service_account is None or GoogleAuthRequest is None:
        raise ValueError("google-auth dependency is not available.")
    info = _get_google_service_account_info()
    if not info:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON is not configured.")

    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    creds.refresh(GoogleAuthRequest())
    range_name = f"{GOOGLE_SHEETS_WORKSHEET_NAME}!A:ZZ"
    endpoint = (
        f"https://sheets.googleapis.com/v4/spreadsheets/"
        f"{quote(GOOGLE_SHEETS_SPREADSHEET_ID, safe='')}/values/{quote(range_name, safe='!')}"
    )
    response = requests.get(
        endpoint,
        headers={"Authorization": f"Bearer {creds.token}"},
        params={"majorDimension": "ROWS"},
        timeout=30,
    )
    if not response.ok:
        raise ValueError(f"Google Sheets API failed ({response.status_code}): {response.text}")
    payload = response.json() or {}
    rows = payload.get("values") or []
    if not rows:
        return []
    headers = [str(h or "").strip() for h in (rows[0] or [])]
    data_rows = []
    for idx, values in enumerate(rows[1:], start=2):
        row_map = {}
        for col_i, header in enumerate(headers):
            if not header:
                continue
            row_map[header] = str(values[col_i]).strip() if col_i < len(values) else ""
        if any(str(v).strip() for v in row_map.values()):
            data_rows.append({"row_index": idx, "row_data": row_map})
    return data_rows


def process_clever_lead_payload(db, payload, source_label="webhook"):
    spreadsheet_id = (payload.get("spreadsheet_id") or payload.get("spreadsheetId") or "").strip()
    sheet_name = (payload.get("sheet_name") or payload.get("sheetName") or "").strip()
    row_index = payload.get("row_index") if payload.get("row_index") is not None else payload.get("rowIndex")
    row_data = payload.get("row_data") if isinstance(payload.get("row_data"), dict) else (payload.get("rowData") or {})
    event_key = (
        (payload.get("event_key") or "").strip()
        or (payload.get("eventKey") or "").strip()
        or f"{spreadsheet_id}:{sheet_name}:{row_index}"
    )
    address = _row_value_by_keys(row_data, ["Address", "address", "Property Address"])
    seller_name = _row_value_by_keys(row_data, ["Seller Name", "seller_name", "owner", "Owner Name"])
    phone = _row_value_by_keys(row_data, ["Phone Number", "phone", "Phone"])
    email = _row_value_by_keys(row_data, ["Email", "email"])
    portal_link = _row_value_by_keys(row_data, ["Portal Link (Provide Updates)", "Portal Link"])

    is_new = upsert_integration_event(
        db,
        CLEVER_LEADS_EVENT_SOURCE,
        event_key,
        payload,
    )
    if source_label == "webhook" or is_new:
        note_lines = [
            f"Clever Leads payload received ({source_label})",
            f"Event Key: {event_key or '-'}",
            f"Sheet: {sheet_name or '-'}",
            f"Row: {row_index if row_index is not None else '-'}",
            f"Address: {address or '-'}",
            f"Seller: {seller_name or '-'}",
            f"Phone: {phone or '-'}",
            f"Email: {email or '-'}",
        ]
        add_clever_webhook_note(
            db,
            event_key,
            "\n".join(note_lines),
            payload,
        )
    db.commit()
    if not is_new:
        return {"ok": True, "duplicate": True, "event_key": event_key}

    if address:
        try:
            owner_name = _parse_seller_name_for_owner(seller_name)
            owner_payload = {}
            if owner_name.get("first_name") or owner_name.get("last_name"):
                owner_payload["first_name"] = owner_name.get("first_name") or "Unknown"
                owner_payload["last_name"] = owner_name.get("last_name") or "Owner"
            if email:
                owner_payload["emails"] = [email]
            if phone:
                owner_payload["phones"] = [{"number": phone, "type": "UNKNOWN", "status": "UNKNOWN"}]

            clever_notes = _build_clever_lead_notes(row_data)
            create_payload = {
                "search": address,
                "status": "new_lead",
                "lists": "Clever",
                "tags": "clever,google_sheet",
                "notes": clever_notes,
                "owner": owner_payload,
            }
            result = create_reisift_property_from_search(create_payload)
            created = result.get("created") or {}
            created_uuid = (result.get("created_uuid") or created.get("uuid") or created.get("id") or "").strip()
            sift_status = (created.get("status") or result.get("create_payload", {}).get("status") or "new_lead").strip()
            sift_link = f"https://app.reisift.io/records/properties/{created_uuid}/details?page=1" if created_uuid else ""
            msg_lines = [
                "New Clever Lead Received",
                f"SIFT Status: {sift_status}",
                f"Address: {address}",
            ]
            if seller_name:
                msg_lines.append(f"Seller: {seller_name}")
            if portal_link:
                msg_lines.append(f"Portal: {portal_link}")
            if sift_link:
                msg_lines.append(f"SIFT Record: {sift_link}")
            post_status_result = {"ok": False, "skipped": True, "reason": "missing_created_uuid"}
            if created_uuid:
                try:
                    post_status_result = reisift_update_property_status(reisift_get_access_token(), created_uuid, "refer_lead")
                    msg_lines.append("Post-Enrich Status: refer_lead")
                except Exception as status_exc:
                    post_status_result = {"ok": False, "error": str(status_exc)}
                    msg_lines.append(f"Post-Enrich Status Error: {status_exc}")
            referral_refresh_result = None
            refresh_error = None
            # SIFT status updates can take a few seconds to show in search; retry refresh briefly.
            for attempt in range(3):
                try:
                    if attempt > 0:
                        time.sleep(2)
                    referral_refresh_result = refresh_reisift_referrals_cache(db)
                    msg_lines.append("Referral Queue: auto-refreshed")
                    refresh_error = None
                    break
                except Exception as refresh_exc:
                    refresh_error = refresh_exc
                    referral_refresh_result = {"ok": False, "error": str(refresh_exc), "attempt": attempt + 1}
            if refresh_error:
                msg_lines.append(f"Referral Refresh Error: {refresh_error}")
            send_slack_notification(db, "\n".join(msg_lines))
            status_note = (
                "Post-Enrich Status: refer_lead"
                if post_status_result and post_status_result.get("response") is not None
                else f"Post-Enrich Status: failed ({post_status_result.get('error') or post_status_result.get('reason')})"
            )
            add_clever_webhook_note(
                db,
                event_key,
                "\n".join(
                    [
                        "Clever Leads processing result",
                        "Result: success",
                        f"SIFT Status: {sift_status}",
                        status_note,
                        f"SIFT Record: {sift_link or '-'}",
                    ]
                ),
                {
                    "created": created,
                    "created_uuid": created_uuid,
                    "status": sift_status,
                    "post_enrich_status_update": post_status_result,
                    "referral_cache_refresh": referral_refresh_result,
                },
            )
            db.commit()
        except Exception as exc:
            err_text = str(exc)
            try:
                send_slack_notification(
                    db,
                    "\n".join(
                        [
                            "New Clever Lead Received",
                            "SIFT Status: failed",
                            f"Address: {address}",
                            f"Error: {err_text}",
                        ]
                    ),
                )
            except Exception:
                pass
            log_app_error(
                db,
                source="integrations_google_sheets_row_added_api",
                error_message=err_text,
                details=traceback.format_exc(),
                route="/api/integrations/google-sheets/row-added",
                status_code=500,
            )
            add_clever_webhook_note(
                db,
                event_key,
                "\n".join(
                    [
                        "Clever Leads processing result",
                        "Result: failed",
                        f"Error: {err_text}",
                    ]
                ),
                {"error": err_text},
            )
            db.commit()
            return {"ok": False, "error": err_text, "event_key": event_key}
    return {"ok": True, "event_key": event_key}


def email_sender_label(settings):
    provider = ((settings or {}).get("provider") or "gmail_api").strip().lower()
    if provider == "resend":
        return "Email sent via Resend"
    if provider == "gmail_api":
        return "Email sent via Gmail API"
    return "Email sent via Gmail"


def generate_open_tracking_token():
    return secrets.token_urlsafe(24)


def build_open_tracking_url(token):
    if not token:
        return ""
    try:
        return url_for("email_open_tracking_pixel", token=token, _external=True)
    except Exception:
        return ""


OPEN_TRACK_PIXEL_GIF = base64.b64decode("R0lGODlhAQABAIABAP///wAAACwAAAAAAQABAAACAkQBADs=")


def email_thread_token(property_id=None, person_id=None):
    p = int(property_id) if str(property_id or "").isdigit() else 0
    pe = int(person_id) if str(person_id or "").isdigit() else 0
    return f"[DP-P{p}-PE{pe}]"


def extract_message_ids(raw_header):
    if not raw_header:
        return []
    vals = re.findall(r"<[^>]+>", str(raw_header))
    return [v.strip() for v in vals if v.strip()]


def decode_mime_header(value):
    if not value:
        return ""
    try:
        return str(make_header(decode_header(value)))
    except Exception:
        return str(value)


def extract_email_address(raw):
    if not raw:
        return ""
    m = re.search(r"<([^>]+)>", raw)
    if m:
        return m.group(1).strip().lower()
    return str(raw).strip().lower()


def extract_candidate_emails(text):
    if not text:
        return []
    found = re.findall(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", str(text))
    return list(dict.fromkeys([f.strip().lower() for f in found if f and "@" in f]))


def extract_bounce_target_email(body_text, known_emails):
    body = str(body_text or "")
    candidates = []
    for pattern in [
        r"Final-Recipient:\s*rfc822;\s*([^\s;]+@[^\s;]+)",
        r"Original-Recipient:\s*rfc822;\s*([^\s;]+@[^\s;]+)",
        r"Diagnostic-Code:.*?([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})",
    ]:
        m = re.search(pattern, body, flags=re.IGNORECASE | re.DOTALL)
        if m:
            candidates.append(m.group(1).strip().lower())
    candidates.extend(extract_candidate_emails(body))
    for em in candidates:
        if em in known_emails:
            return em
    return candidates[0] if candidates else ""


EMAIL_RE = re.compile(r"^[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}$", re.IGNORECASE)


def is_likely_valid_email(raw):
    email = (raw or "").strip()
    if not email or not EMAIL_RE.match(email):
        return False
    domain = email.split("@", 1)[1].strip().lower()
    if not domain or "." not in domain:
        return False
    try:
        socket.getaddrinfo(domain, 25)
    except Exception:
        return False
    return True


def clean_inbound_email_body(raw_body):
    text = (raw_body or "").replace("\r\n", "\n").replace("\r", "\n")
    if not text.strip():
        return ""

    inline_break_patterns = [
        r"\sOn .+ wrote:\s*.*$",
        r"\nOn .+ wrote:\s*.*$",
        r"\nFrom:\s.+$",
        r"\n---+\s*Original Message\s*---+.*$",
    ]
    for pat in inline_break_patterns:
        m = re.search(pat, text, flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)
        if m:
            text = text[: m.start()]
            break

    if not text.strip():
        return ""

    lines = text.split("\n")
    out = []
    stop_patterns = [
        r"^On .+ wrote:\s*$",
        r"^From:\s.+$",
        r"^Sent:\s.+$",
        r"^To:\s.+$",
        r"^Subject:\s.+$",
        r"^---+\s*Original Message\s*---+$",
    ]

    for line in lines:
        trimmed = line.strip()
        if trimmed.startswith(">"):
            break
        should_stop = any(re.match(p, trimmed, flags=re.IGNORECASE) for p in stop_patterns)
        if should_stop:
            break
        out.append(line.rstrip())

    cleaned = "\n".join(out).strip()
    if not cleaned:
        return ""

    sig_markers = [
        r"\n--\s*$",
        r"\nThanks,\s*$",
        r"\nBest,\s*$",
        r"\nRegards,\s*$",
    ]
    for pat in sig_markers:
        m = re.search(pat, cleaned, flags=re.IGNORECASE | re.MULTILINE)
        if m:
            candidate = cleaned[: m.start()].strip()
            if candidate:
                cleaned = candidate
            break

    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    return cleaned


def _decode_mailbox_name(raw_line):
    if isinstance(raw_line, bytes):
        txt = raw_line.decode(errors="ignore")
    else:
        txt = str(raw_line or "")
    m = re.search(r'"([^"]+)"\s*$', txt)
    return m.group(1) if m else ""


def _find_sent_mailbox(imap_conn):
    try:
        typ, boxes = imap_conn.list()
        if typ != "OK" or not boxes:
            return None
        for b in boxes:
            t = b.decode(errors="ignore") if isinstance(b, bytes) else str(b)
            if "\\Sent" in t:
                name = _decode_mailbox_name(b)
                if name:
                    return name
        for b in boxes:
            name = _decode_mailbox_name(b)
            low = name.lower()
            if "sent" in low:
                return name
    except Exception:
        return None
    return None


def _extract_imap_fetch_meta(meta_chunk):
    text = meta_chunk.decode(errors="ignore") if isinstance(meta_chunk, bytes) else str(meta_chunk or "")
    thr = re.search(r"X-GM-THRID\s+(\d+)", text)
    msg = re.search(r"X-GM-MSGID\s+(\d+)", text)
    return (msg.group(1) if msg else ""), (thr.group(1) if thr else "")


def _is_ip_literal(host):
    return bool(re.match(r"^\d{1,3}(?:\.\d{1,3}){3}$", str(host or "").strip()))


def _resolve_ipv4_addresses(host):
    resolved = []
    seen = set()
    try:
        infos = socket.getaddrinfo(host, None, socket.AF_INET, socket.SOCK_STREAM)
    except Exception:
        return resolved
    for info in infos:
        addr = (info[4] or [""])[0]
        if addr and addr not in seen:
            seen.add(addr)
            resolved.append(addr)
    return resolved


def _smtp_send_with_fallback(smtp_host, smtp_port, from_addr, app_pw, message):
    candidates = []
    seen = set()

    def add(host, port, use_ssl):
        key = (str(host).strip(), int(port), bool(use_ssl))
        if key not in seen:
            seen.add(key)
            candidates.append(key)

    add(smtp_host, smtp_port, False)
    if not (str(smtp_host).strip().lower() == "smtp.gmail.com" and int(smtp_port) == 587):
        add("smtp.gmail.com", 587, False)
    add("smtp.gmail.com", 465, True)

    errors = []
    for host, port, use_ssl in candidates:
        try:
            if use_ssl:
                tls_ctx = ssl.create_default_context()
                with smtplib.SMTP_SSL(host, port, timeout=30, context=tls_ctx) as server:
                    server.login(from_addr, app_pw)
                    server.send_message(message)
                    return
            else:
                tls_ctx = ssl.create_default_context()
                with smtplib.SMTP(host, port, timeout=30) as server:
                    server.ehlo()
                    server.starttls(context=tls_ctx)
                    server.ehlo()
                    server.login(from_addr, app_pw)
                    server.send_message(message)
                    return
        except Exception as exc:
            errors.append(f"{host}:{port} ({'SSL' if use_ssl else 'STARTTLS'}): {exc}")
            for ip in _resolve_ipv4_addresses(host):
                try:
                    tls_ctx_ip = ssl.create_default_context()
                    tls_ctx_ip.check_hostname = False
                    if use_ssl:
                        with smtplib.SMTP_SSL(ip, port, timeout=30, context=tls_ctx_ip) as server:
                            server.login(from_addr, app_pw)
                            server.send_message(message)
                            return
                    else:
                        with smtplib.SMTP(ip, port, timeout=30) as server:
                            server.ehlo()
                            server.starttls(context=tls_ctx_ip)
                            server.ehlo()
                            server.login(from_addr, app_pw)
                            server.send_message(message)
                            return
                except Exception as ip_exc:
                    errors.append(f"{ip}:{port} ({'SSL' if use_ssl else 'STARTTLS'}): {ip_exc}")
    raise ValueError("SMTP delivery failed across fallbacks: " + " | ".join(errors[-6:]))


def _smtp_probe_with_fallback(smtp_host, smtp_port, from_addr, app_pw):
    candidates = []
    seen = set()

    def add(host, port, use_ssl):
        key = (str(host).strip(), int(port), bool(use_ssl))
        if key not in seen:
            seen.add(key)
            candidates.append(key)

    add(smtp_host, smtp_port, False)
    if not (str(smtp_host).strip().lower() == "smtp.gmail.com" and int(smtp_port) == 587):
        add("smtp.gmail.com", 587, False)
    add("smtp.gmail.com", 465, True)

    errors = []
    for host, port, use_ssl in candidates:
        try:
            if use_ssl:
                tls_ctx = ssl.create_default_context()
                with smtplib.SMTP_SSL(host, port, timeout=30, context=tls_ctx) as server:
                    server.login(from_addr, app_pw)
                    return True, ""
            else:
                tls_ctx = ssl.create_default_context()
                with smtplib.SMTP(host, port, timeout=30) as server:
                    server.ehlo()
                    server.starttls(context=tls_ctx)
                    server.ehlo()
                    server.login(from_addr, app_pw)
                    return True, ""
        except Exception as exc:
            errors.append(f"{host}:{port} ({'SSL' if use_ssl else 'STARTTLS'}): {exc}")
            for ip in _resolve_ipv4_addresses(host):
                try:
                    tls_ctx_ip = ssl.create_default_context()
                    tls_ctx_ip.check_hostname = False
                    if use_ssl:
                        with smtplib.SMTP_SSL(ip, port, timeout=30, context=tls_ctx_ip) as server:
                            server.login(from_addr, app_pw)
                            return True, ""
                    else:
                        with smtplib.SMTP(ip, port, timeout=30) as server:
                            server.ehlo()
                            server.starttls(context=tls_ctx_ip)
                            server.ehlo()
                            server.login(from_addr, app_pw)
                            return True, ""
                except Exception as ip_exc:
                    errors.append(f"{ip}:{port} ({'SSL' if use_ssl else 'STARTTLS'}): {ip_exc}")
    return False, "SMTP login failed across fallbacks: " + " | ".join(errors[-6:])


def _open_imap_with_fallback(imap_host, imap_port, from_addr, app_pw):
    hosts = []
    for h in [imap_host, "imap.gmail.com"]:
        h = (h or "").strip()
        if h and h not in hosts:
            hosts.append(h)
    errors = []
    for host in hosts:
        try:
            ctx = ssl.create_default_context()
            conn = imaplib.IMAP4_SSL(host, imap_port, ssl_context=ctx)
            conn.login(from_addr, app_pw)
            return conn
        except Exception as exc:
            errors.append(f"{host}:{imap_port}: {exc}")
            for ip in _resolve_ipv4_addresses(host):
                try:
                    ctx_ip = ssl.create_default_context()
                    ctx_ip.check_hostname = False
                    conn = imaplib.IMAP4_SSL(ip, imap_port, ssl_context=ctx_ip)
                    conn.login(from_addr, app_pw)
                    return conn
                except Exception as ip_exc:
                    errors.append(f"{ip}:{imap_port}: {ip_exc}")
    raise ValueError("IMAP login failed across fallbacks: " + " | ".join(errors[-6:]))


def _send_resend_email(settings, to_email, subject, body, property_id=None, person_id=None, open_tracking_url=""):
    api_key = (settings.get("resend_api_key") or "").strip()
    from_addr = (settings.get("from_address") or "").strip()
    from_name = (settings.get("from_name") or "").strip()
    base_url = (settings.get("resend_base_url") or "https://api.resend.com").strip().rstrip("/")
    if not api_key:
        raise ValueError("Resend API key missing in email settings.")
    if not from_addr:
        raise ValueError("From address is required for Resend.")
    from_field = f"{from_name} <{from_addr}>" if from_name else from_addr
    html_body = f"<div>{(body or '').replace(chr(10), '<br>')}</div>"
    if open_tracking_url:
        html_body += (
            f'<img src="{open_tracking_url}" width="1" height="1" '
            f'style="display:block;border:0;outline:none;text-decoration:none;" alt=""/>'
        )
    payload = {
        "from": from_field,
        "to": [to_email],
        "subject": subject or "",
        "text": body or "",
        "html": html_body,
        "headers": {
            "X-DeepProspect-Property-ID": str(property_id or ""),
            "X-DeepProspect-Person-ID": str(person_id or ""),
            "X-DeepProspect-Thread": email_thread_token(property_id, person_id),
        },
    }
    response = requests.post(
        f"{base_url}/emails",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=30,
    )
    try:
        data = response.json()
    except ValueError:
        data = {"raw_text": response.text}
    if not response.ok:
        raise ValueError(f"Resend send failed ({response.status_code}): {data}")
    message_id = str(data.get("id") or "").strip()
    return {"message_id": message_id, "subject": subject or "", "provider": "resend"}


def _get_gmail_api_access_token(settings):
    client_id = (settings.get("gmail_api_client_id") or "").strip()
    client_secret = (settings.get("gmail_api_client_secret") or "").strip()
    refresh_token = (settings.get("gmail_api_refresh_token") or "").strip()
    if not client_id or not client_secret or not refresh_token:
        raise ValueError("Gmail API OAuth settings incomplete: client id, client secret, and refresh token are required.")
    token_res = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        },
        timeout=20,
    )
    try:
        token_body = token_res.json()
    except ValueError:
        token_body = {"raw_text": token_res.text}
    if not token_res.ok:
        raise ValueError(f"Gmail OAuth token refresh failed ({token_res.status_code}): {token_body}")
    access_token = (token_body.get("access_token") or "").strip()
    if not access_token:
        raise ValueError("Gmail OAuth token refresh did not return an access_token.")
    return access_token


def _send_gmail_api_email(settings, to_email, subject, body, property_id=None, person_id=None, open_tracking_url=""):
    from_addr = (settings.get("from_address") or "").strip()
    from_name = (settings.get("from_name") or "").strip()
    if not from_addr:
        raise ValueError("From address is required for Gmail API.")
    full_subject = (subject or "").strip()
    if not full_subject:
        full_subject = "Question re: Property Address"

    msg = EmailMessage()
    msg["From"] = f"{from_name} <{from_addr}>" if from_name else from_addr
    msg["To"] = to_email
    msg["Subject"] = full_subject
    msg["Message-ID"] = make_msgid(domain=(from_addr.split("@", 1)[1] if "@" in from_addr else None))
    msg["X-DeepProspect-Property-ID"] = str(property_id or "")
    msg["X-DeepProspect-Person-ID"] = str(person_id or "")
    msg["X-DeepProspect-Thread"] = email_thread_token(property_id, person_id)
    msg.set_content(body or "")
    if open_tracking_url:
        html_body = (
            f"<div>{(body or '').replace(chr(10), '<br>')}</div>"
            f'<img src="{open_tracking_url}" width="1" height="1" '
            f'style="display:block;border:0;outline:none;text-decoration:none;" alt=""/>'
        )
        msg.add_alternative(html_body, subtype="html")

    raw_message = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
    access_token = _get_gmail_api_access_token(settings)
    send_res = requests.post(
        "https://gmail.googleapis.com/gmail/v1/users/me/messages/send",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
        json={"raw": raw_message},
        timeout=30,
    )
    try:
        send_body = send_res.json()
    except ValueError:
        send_body = {"raw_text": send_res.text}
    if not send_res.ok:
        raise ValueError(f"Gmail API send failed ({send_res.status_code}): {send_body}")
    message_id = str(send_body.get("id") or msg.get("Message-ID") or "").strip()
    return {"message_id": message_id, "subject": full_subject, "provider": "gmail_api", "response": send_body}


def _gmail_header_value(headers, key):
    key_l = (key or "").strip().lower()
    for h in headers or []:
        name = (h.get("name") or "").strip().lower()
        if name == key_l:
            return h.get("value") or ""
    return ""


def _gmail_extract_plain_text(payload):
    if not isinstance(payload, dict):
        return ""
    mime = (payload.get("mimeType") or "").lower()
    body = payload.get("body") or {}
    data = (body.get("data") or "").strip()
    if mime == "text/plain" and data:
        try:
            return base64.urlsafe_b64decode(data + "===").decode("utf-8", errors="replace")
        except Exception:
            return ""
    for part in payload.get("parts") or []:
        text = _gmail_extract_plain_text(part)
        if text:
            return text
    return ""


def _process_gmail_api_message(db, settings, message_obj):
    payload = message_obj.get("payload") or {}
    headers = payload.get("headers") or []
    from_addr = (settings.get("from_address") or "").strip().lower()
    from_email = extract_email_address(_gmail_header_value(headers, "From"))
    to_email = extract_email_address(_gmail_header_value(headers, "To"))
    if from_email and from_addr and from_email == from_addr:
        return False

    gm_msgid = str(message_obj.get("id") or "").strip()
    gm_thrid = str(message_obj.get("threadId") or "").strip()
    ext_id = (_gmail_header_value(headers, "Message-ID") or "").strip()
    if not ext_id:
        ext_id = f"GMAIL-{gm_msgid}"

    if ext_id:
        exists = db.execute("SELECT id FROM communications WHERE external_id = ?", (ext_id,)).fetchone()
        if exists:
            return False
    if gm_msgid:
        exists2 = db.execute("SELECT id FROM communications WHERE gmail_msgid = ?", (gm_msgid,)).fetchone()
        if exists2:
            return False

    subject = decode_mime_header(_gmail_header_value(headers, "Subject"))
    in_reply_to = (_gmail_header_value(headers, "In-Reply-To") or "").strip()
    refs = (_gmail_header_value(headers, "References") or "").strip()
    raw_body = _gmail_extract_plain_text(payload)
    body = clean_inbound_email_body(raw_body)
    is_bounce = (
        "mailer-daemon" in (from_email or "")
        or "postmaster" in (from_email or "")
        or any(x in (subject or "").lower() for x in ["undeliverable", "delivery status notification", "delivery failure", "returned mail"])
    )

    known_email_rows = db.execute(
        """
        SELECT lower(value) AS email
        FROM touchpoints
        WHERE lower(channel_type) = 'email' AND value IS NOT NULL AND trim(value) <> ''
        UNION
        SELECT lower(primary_email) AS email
        FROM people
        WHERE primary_email IS NOT NULL AND trim(primary_email) <> ''
        """
    ).fetchall()
    known_emails = {str(r["email"]).strip().lower() for r in known_email_rows if (r["email"] or "").strip()}
    bounce_target_email = extract_bounce_target_email(raw_body, known_emails) if is_bounce else ""

    property_id = None
    person_id = None
    token_source = f"{subject or ''} {_gmail_header_value(headers, 'X-DeepProspect-Thread') or ''} {raw_body or ''} {body or ''}"
    token_match = re.search(r"\[DP-P(\d+)-PE(\d+)\]", token_source)
    if token_match:
        property_id = int(token_match.group(1)) if token_match.group(1).isdigit() and token_match.group(1) != "0" else None
        person_id = int(token_match.group(2)) if token_match.group(2).isdigit() and token_match.group(2) != "0" else None
    if person_id is None:
        h_person = (_gmail_header_value(headers, "X-DeepProspect-Person-ID") or "").strip()
        if h_person.isdigit():
            person_id = int(h_person)
    if property_id is None:
        h_prop = (_gmail_header_value(headers, "X-DeepProspect-Property-ID") or "").strip()
        if h_prop.isdigit():
            property_id = int(h_prop)

    if person_id is None or property_id is None:
        ref_ids = []
        ref_ids.extend(extract_message_ids(in_reply_to))
        ref_ids.extend(extract_message_ids(refs))
        for ref in ref_ids:
            prior = db.execute(
                """
                SELECT id, property_id, person_id
                FROM communications
                WHERE upper(channel) = 'EMAIL' AND external_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (ref,),
            ).fetchone()
            if prior:
                if property_id is None:
                    property_id = prior["property_id"]
                if person_id is None:
                    person_id = prior["person_id"]
                break

    if (person_id is None or property_id is None) and gm_thrid:
        prior = db.execute(
            """
            SELECT property_id, person_id
            FROM communications
            WHERE upper(channel) = 'EMAIL' AND gmail_thread_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (gm_thrid,),
        ).fetchone()
        if prior:
            property_id = property_id or prior["property_id"]
            person_id = person_id or prior["person_id"]

    if person_id is None and from_email:
        tp = db.execute(
            """
            SELECT person_id
            FROM touchpoints
            WHERE lower(channel_type) = 'email' AND lower(value) = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (from_email.lower(),),
        ).fetchone()
        if tp:
            person_id = tp["person_id"]

    if person_id is None and bounce_target_email:
        tp_b = db.execute(
            """
            SELECT person_id
            FROM touchpoints
            WHERE lower(channel_type) = 'email' AND lower(value) = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (bounce_target_email.lower(),),
        ).fetchone()
        if tp_b:
            person_id = tp_b["person_id"]

    if property_id is None and person_id is not None:
        pr = db.execute(
            "SELECT id FROM properties WHERE owner_person_id = ? OR resident_person_id = ? ORDER BY created_at DESC LIMIT 1",
            (person_id, person_id),
        ).fetchone()
        if pr:
            property_id = pr["id"]

    if property_id is None:
        return False

    if is_bounce and bounce_target_email:
        db.execute(
            """
            UPDATE touchpoints
            SET status = 'Bounced'
            WHERE lower(channel_type) = 'email' AND lower(value) = lower(?)
            """,
            (bounce_target_email,),
        )

    status_label = "Bounced" if is_bounce else "Received"
    db.execute(
        """
        INSERT INTO communications (property_id, person_id, channel, direction, from_number, to_number, body, status, is_read, sent_at, external_id, gmail_msgid, gmail_thread_id, in_reply_to)
        VALUES (?, ?, 'EMAIL', 'Inbound', ?, ?, ?, ?, 0, ?, ?, ?, ?, ?)
        """,
        (
            property_id,
            person_id,
            from_email,
            to_email or (settings.get("from_address") or ""),
            (body or "").strip(),
            status_label,
            format_db_time(datetime.utcnow()),
            ext_id,
            gm_msgid or None,
            gm_thrid or None,
            in_reply_to or None,
        ),
    )
    return True


def process_gmail_api_inbound_once(max_messages=20):
    ensure_db()
    db = open_sqlite_connection()
    try:
        s = get_email_settings(db)
        if (s.get("provider") or "").strip().lower() != "gmail_api":
            return {"ok": True, "skipped": "provider_not_gmail_api"}
        access_token = _get_gmail_api_access_token(s)
        from_addr = (s.get("from_address") or "").strip()
        if not from_addr:
            return {"ok": False, "error": "From address is missing for Gmail API inbound processing."}
        q = f"to:{from_addr} -from:{from_addr} newer_than:7d"
        list_res = requests.get(
            "https://gmail.googleapis.com/gmail/v1/users/me/messages",
            headers={"Authorization": f"Bearer {access_token}"},
            params={"q": q, "maxResults": max_messages},
            timeout=30,
        )
        list_body = list_res.json() if list_res.content else {}
        if not list_res.ok:
            raise ValueError(f"Gmail API list failed ({list_res.status_code}): {list_body}")
        messages = list_body.get("messages") or []
        inserted = 0
        for m in messages:
            msg_id = (m.get("id") or "").strip()
            if not msg_id:
                continue
            get_res = requests.get(
                f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{msg_id}",
                headers={"Authorization": f"Bearer {access_token}"},
                params={"format": "full"},
                timeout=30,
            )
            get_body = get_res.json() if get_res.content else {}
            if not get_res.ok:
                continue
            if _process_gmail_api_message(db, s, get_body):
                inserted += 1
        db.commit()
        return {"ok": True, "fetched": len(messages), "inserted": inserted}
    except Exception as exc:
        db.rollback()
        log_app_error(
            db,
            source="gmail_api_inbound",
            error_message=str(exc),
            details=traceback.format_exc(),
            route="process_gmail_api_inbound_once",
            status_code=500,
        )
        db.commit()
        return {"ok": False, "error": str(exc)}
    finally:
        db.close()


def send_gmail_email(db, to_email, subject, body, property_id=None, person_id=None, open_tracking_url=""):
    s = get_email_settings(db)
    provider = (s.get("provider") or "gmail_api").strip().lower()
    if provider == "resend":
        return _send_resend_email(
            s,
            to_email,
            subject,
            body,
            property_id=property_id,
            person_id=person_id,
            open_tracking_url=open_tracking_url,
        )
    if provider == "gmail_api":
        return _send_gmail_api_email(
            s,
            to_email,
            subject,
            body,
            property_id=property_id,
            person_id=person_id,
            open_tracking_url=open_tracking_url,
        )
    raise ValueError("Unsupported email provider. Use gmail_api or resend.")


def backfill_outbound_gmail_ids(db, communication_id):
    row = db.execute(
        """
        SELECT id, channel, direction, external_id, gmail_msgid, gmail_thread_id, in_reply_to
        FROM communications
        WHERE id = ?
        """,
        (communication_id,),
    ).fetchone()
    if not row:
        return
    if (row["channel"] or "").strip().upper() != "EMAIL" or (row["direction"] or "").strip().title() != "Outbound":
        return
    settings = get_email_settings(db)
    if (settings.get("provider") or "").strip().lower() != "gmail_api":
        return
    access_token = _get_gmail_api_access_token(settings)

    ext = (row["external_id"] or "").strip()
    gmail_msgid = (row["gmail_msgid"] or "").strip()
    message_obj = None
    message_id = ""

    # Gmail API message IDs are opaque tokens; RFC822 Message-IDs usually contain '@' and angle brackets.
    ext_is_rfc = ("@" in ext and "<" in ext and ">" in ext) or ("@" in ext and ext.startswith("<"))
    candidate_gmail_id = gmail_msgid or ("" if ext_is_rfc else ext)

    if candidate_gmail_id:
        res = requests.get(
            f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{candidate_gmail_id}",
            headers={"Authorization": f"Bearer {access_token}"},
            params={"format": "metadata", "metadataHeaders": ["Message-ID", "In-Reply-To"]},
            timeout=20,
        )
        if res.ok:
            message_obj = res.json() if res.content else {}
            message_id = candidate_gmail_id

    if message_obj is None and ext_is_rfc:
        query = ext.strip("<>")
        list_res = requests.get(
            "https://gmail.googleapis.com/gmail/v1/users/me/messages",
            headers={"Authorization": f"Bearer {access_token}"},
            params={"q": f"rfc822msgid:{query}", "maxResults": 1},
            timeout=20,
        )
        if list_res.ok:
            list_body = list_res.json() if list_res.content else {}
            msgs = list_body.get("messages") or []
            if msgs:
                message_id = (msgs[0].get("id") or "").strip()
                if message_id:
                    get_res = requests.get(
                        f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{message_id}",
                        headers={"Authorization": f"Bearer {access_token}"},
                        params={"format": "metadata", "metadataHeaders": ["Message-ID", "In-Reply-To"]},
                        timeout=20,
                    )
                    if get_res.ok:
                        message_obj = get_res.json() if get_res.content else {}

    if not isinstance(message_obj, dict):
        return
    payload = message_obj.get("payload") if isinstance(message_obj.get("payload"), dict) else {}
    headers = payload.get("headers") if isinstance(payload.get("headers"), list) else []
    rfc_message_id = (_gmail_header_value(headers, "Message-ID") or "").strip()
    thread_id = (message_obj.get("threadId") or "").strip()
    actual_gmail_id = (message_obj.get("id") or message_id or "").strip()
    in_reply_to = (_gmail_header_value(headers, "In-Reply-To") or "").strip()

    db.execute(
        """
        UPDATE communications
        SET external_id = COALESCE(NULLIF(?, ''), external_id),
            gmail_msgid = COALESCE(NULLIF(?, ''), gmail_msgid),
            gmail_thread_id = COALESCE(NULLIF(?, ''), gmail_thread_id),
            in_reply_to = COALESCE(NULLIF(?, ''), in_reply_to)
        WHERE id = ?
        """,
        (
            rfc_message_id or ext,
            actual_gmail_id,
            thread_id,
            in_reply_to or (row["in_reply_to"] or ""),
            communication_id,
        ),
    )


def test_email_login(db):
    s = get_email_settings(db)
    provider = (s.get("provider") or "gmail_api").strip().lower()
    if provider == "gmail_api":
        api_ok = False
        api_error = ""
        try:
            _get_gmail_api_access_token(s)
            api_ok = True
        except Exception as exc:
            api_error = str(exc)
        return {
            "ok": api_ok,
            "gmail_api_ok": api_ok,
            "gmail_api_error": api_error,
            "error": (f"Gmail API: {api_error}" if api_error else ""),
        }
    if provider == "resend":
        api_key = (s.get("resend_api_key") or "").strip()
        from_addr = (s.get("from_address") or "").strip()
        ok = bool(api_key and from_addr)
        err = "" if ok else "Resend requires API key and from address."
        return {"ok": ok, "error": err, "resend_ok": ok}
    return {
        "ok": False,
        "error": "Unsupported email provider. Use gmail_api or resend.",
    }

def split_markets(markets_csv):
    if not markets_csv:
        return []
    parts = [x.strip() for x in str(markets_csv).split(",")]
    return [x for x in parts if x]


def join_markets(markets):
    vals = []
    for m in markets or []:
        s = (m or "").strip()
        if s and s not in vals:
            vals.append(s)
    return ", ".join(vals)


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


def poll_gmail_inbound_once():
    return


def start_email_poll_worker():
    return


def run_referral_on_market_refresh_once():
    ensure_db()
    db = open_sqlite_connection()
    try:
        today_et = datetime.now(EST_TZ).strftime("%Y-%m-%d")
        last_run_date = (get_setting(db, "referral_on_market_last_run_date", "") or "").strip()
        if last_run_date == today_et:
            return {"ok": True, "skipped": "already_ran_today", "date": today_et}
        result = refresh_reisift_referrals_cache(db)
        set_setting(db, "referral_on_market_last_run_date", today_et)
        db.commit()
        try:
            send_slack_notification(
                db,
                "\n".join(
                    [
                        "Nightly Referral Refresh Complete",
                        f"Date (ET): {today_et}",
                        f"Synced: {result.get('synced', 0)}",
                        f"On-Market statuses updated from cached ReiSIFT property payload.",
                    ]
                ),
            )
        except Exception:
            pass
        return {"ok": True, "result": result, "date": today_et}
    except Exception as exc:
        db.rollback()
        log_app_error(
            db,
            source="referral_on_market_worker",
            error_message=str(exc),
            details=traceback.format_exc(),
            route="run_referral_on_market_refresh_once",
            status_code=500,
        )
        db.commit()
        return {"ok": False, "error": str(exc)}
    finally:
        db.close()


def start_referral_on_market_worker():
    global REFERRAL_MARKET_WORKER_STARTED
    if REFERRAL_MARKET_WORKER_STARTED:
        return
    REFERRAL_MARKET_WORKER_STARTED = True

    def worker():
        while True:
            try:
                now_et = datetime.now(EST_TZ)
                if now_et.hour == 0 and now_et.minute < 10:
                    run_referral_on_market_refresh_once()
                    # Prevent duplicate refreshes during the midnight window.
                    time.sleep(600)
                    continue
            except Exception:
                pass
            time.sleep(60)

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()


def run_clever_leads_poll_once():
    ensure_db()
    db = open_sqlite_connection()
    try:
        automation = get_automation_settings(db)
        if not automation.get("clever_leads_enabled"):
            return {"ok": True, "skipped": "disabled"}
        if not GOOGLE_SHEETS_SPREADSHEET_ID or not (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip():
            return {"ok": True, "skipped": "missing_google_sheets_config"}
        if service_account is None or GoogleAuthRequest is None:
            return {"ok": True, "skipped": "google_auth_dependency_missing"}
        rows = fetch_google_sheet_rows()
        if GOOGLE_SHEETS_POLL_MODE == "latest_only" and rows:
            rows = [rows[-1]]
        processed = 0
        duplicates = 0
        failed = 0
        for item in rows:
            row_index = item.get("row_index")
            row_data = item.get("row_data") if isinstance(item.get("row_data"), dict) else {}
            payload = {
                "spreadsheet_id": GOOGLE_SHEETS_SPREADSHEET_ID,
                "sheet_name": GOOGLE_SHEETS_WORKSHEET_NAME,
                "row_index": row_index,
                "event_key": f"{GOOGLE_SHEETS_SPREADSHEET_ID}:{GOOGLE_SHEETS_WORKSHEET_NAME}:{row_index}",
                "row_data": row_data,
            }
            result = process_clever_lead_payload(db, payload, source_label="poller")
            if result.get("duplicate"):
                duplicates += 1
            elif result.get("ok"):
                processed += 1
            else:
                failed += 1
        return {"ok": True, "processed": processed, "duplicates": duplicates, "failed": failed, "rows": len(rows)}
    except Exception as exc:
        db.rollback()
        log_app_error(
            db,
            source="clever_leads_worker",
            error_message=str(exc),
            details=traceback.format_exc(),
            route="run_clever_leads_poll_once",
            status_code=500,
        )
        db.commit()
        return {"ok": False, "error": str(exc)}
    finally:
        db.close()


def start_clever_leads_worker():
    global CLEVER_LEADS_WORKER_STARTED
    if CLEVER_LEADS_WORKER_STARTED:
        return
    CLEVER_LEADS_WORKER_STARTED = True

    def worker():
        while True:
            try:
                run_clever_leads_poll_once()
            except Exception:
                pass
            time.sleep(GOOGLE_SHEETS_POLL_SECONDS)

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()


def _queue_call_analysis_lead_action(db, job_row, analysis):
    property_id = job_row["property_id"]
    person_id = job_row["person_id"]
    if not property_id:
        return
    next_step = (analysis.get("next_best_step") or "Follow Up").strip()
    mapping = {
        "Schedule Appointment": ("ScheduleAppointment", "High"),
        "Follow Up": ("FollowUpTouchDue", "Medium"),
        "Escalate To Relative": ("EscalateToRelativeOutreach", "High"),
        "Deep Dive": ("EscalateToDeepDiveResearch", "High"),
        "Mark Not Interested": ("HumanReviewDisposition", "High"),
        "Mark Opt Out": ("HumanReviewDisposition", "High"),
        "No Action": ("ReviewInboundAndNextBestAction", "Low"),
    }
    action_type, priority = mapping.get(next_step, ("ReviewInboundAndNextBestAction", "Medium"))
    reason = (
        f"Call analysis recommends: {next_step}. "
        f"Intent: {(analysis.get('caller_intent') or 'Unknown')}. "
        f"Sentiment: {(analysis.get('sentiment') or 'Unknown')}."
    )
    _upsert_lead_action(
        db,
        int(property_id),
        person_id,
        action_type,
        priority,
        reason,
        {
            "source": "call_recording_analysis",
            "call_sid": job_row["call_sid"],
            "recording_url": job_row["recording_url"],
            "analysis": analysis,
        },
    )


def _create_agent_diagnosis_review_action(db, property_id, person_id, call_sid, diagnosis, attempted_fix, proposed_resolution, priority="Medium", extra=None):
    if not property_id:
        return False
    reason = (
        f"Agent diagnosis for call {call_sid or '-'}: {diagnosis}. "
        f"Attempted: {attempted_fix}. Proposed: {proposed_resolution}."
    )
    payload = {
        "source": "agent_call_issue_handler",
        "call_sid": call_sid,
        "diagnosis": diagnosis,
        "attempted_fix": attempted_fix,
        "proposed_resolution": proposed_resolution,
    }
    if extra:
        payload["extra"] = extra
    return _upsert_lead_action(
        db,
        int(property_id),
        person_id,
        "AgentDiagnosisReview",
        priority,
        reason,
        payload,
    )


def _recommend_no_answer_next_step(db, property_id, person_id):
    prop = db.execute("SELECT status FROM properties WHERE id = ?", (property_id,)).fetchone()
    status = _normalize_reisift_status((prop["status"] if prop else "") or "")
    if status in {"not interested", "opt-out", "dead lead"}:
        return "No Action", "Lead status is dispositioned; no-answer call should not trigger additional outreach."

    row = db.execute(
        """
        SELECT COUNT(*) AS c
        FROM call_recording_jobs
        WHERE property_id = ?
          AND COALESCE(person_id, 0) = COALESCE(?, 0)
          AND (
                lower(payload_json) LIKE '%\"call_outcome\": \"missed\"%'
             OR lower(payload_json) LIKE '%\"call_outcome\": \"no_answer\"%'
             OR lower(payload_json) LIKE '%\"call_outcome\": \"failed\"%'
             OR lower(payload_json) LIKE '%\"calloutcome\": \"missed\"%'
             OR lower(payload_json) LIKE '%\"calloutcome\": \"no_answer\"%'
             OR lower(payload_json) LIKE '%\"calloutcome\": \"failed\"%'
          )
        """,
        (property_id, person_id),
    ).fetchone()
    no_answer_attempts = int((row["c"] if row else 0) or 0)

    if no_answer_attempts >= 3:
        return "Escalate To Relative", "Three or more no-answer outcomes detected; escalate outreach path."
    if status in {"new lead", "no contact new lead"}:
        return "Follow Up", "New lead with no answer; retry follow-up cadence."
    return "Follow Up", "No-answer outcome; keep lead in follow-up workflow."


def _detect_audio_extension(content_type, fallback_url=""):
    ctype = (content_type or "").split(";")[0].strip().lower()
    guessed_ext = mimetypes.guess_extension(ctype) if ctype else None
    if guessed_ext:
        return guessed_ext
    path = urlsplit(fallback_url or "").path or ""
    if "." in path:
        ext = "." + path.rsplit(".", 1)[-1].lower()
        if len(ext) <= 6:
            return ext
    return ".mp3"


def process_single_call_recording_job(db, job_row):
    job_id = int(job_row["id"])
    call_sid = (job_row["call_sid"] or "").strip()
    recording_url = (job_row["recording_url"] or "").strip()
    from_number = (job_row["from_number"] or "").strip()
    to_number = (job_row["to_number"] or "").strip()
    property_id = job_row["property_id"]
    person_id = job_row["person_id"]

    # Retry recording-url lookup if not present yet.
    lookup_raw = {}
    fetch_status = (job_row["fetch_status"] or "Queued").strip()
    if not recording_url:
        lookup = fetch_smrtphone_recording_url(call_sid)
        lookup_raw = lookup.get("raw") or {}
        recording_url = (lookup.get("recording_url") or "").strip()
        fetch_status = "Fetched" if recording_url else "No Recording URL"
        db.execute(
            """
            UPDATE call_recording_jobs
            SET recording_url = ?, fetch_status = ?, payload_json = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                recording_url,
                fetch_status,
                json.dumps({"recording_lookup": lookup_raw}),
                job_id,
            ),
        )
        if not recording_url:
            payload_obj = parse_json_object(job_row["payload_json"] or "{}")
            webhook = payload_obj.get("webhook") if isinstance(payload_obj.get("webhook"), dict) else {}
            outcome = str(
                webhook.get("callOutcome")
                or webhook.get("call_outcome")
                or payload_obj.get("call_outcome")
                or ""
            ).strip().lower()
            analysis_status = "No Recording"
            if outcome in NO_ANSWER_OUTCOMES:
                if property_id:
                    recommended_step, reason = _recommend_no_answer_next_step(db, int(property_id), person_id)
                    upsert_agent_signal(
                        db,
                        source_type="call",
                        source_key=f"call-no-answer:{call_sid}",
                        property_id=property_id,
                        person_id=person_id,
                        channel="CALL",
                        intent="No Answer",
                        sentiment="Unknown",
                        confidence=0.85,
                        recommended_next_step=recommended_step,
                        summary_text=f"No-answer call outcome. {reason}",
                        is_spam=False,
                        do_not_contact=False,
                        payload={"call_sid": call_sid, "call_outcome": outcome, "reason": reason},
                    )
                    route_new_agent_signals_once(db, limit=25)
            else:
                if property_id:
                    _create_agent_diagnosis_review_action(
                        db,
                        property_id=int(property_id),
                        person_id=person_id,
                        call_sid=call_sid,
                        diagnosis="Recording URL unavailable for completed call",
                        attempted_fix="Attempted smrtPhone recording lookup using call SID",
                        proposed_resolution="Monitor for delayed recording availability or validate provider recording configuration.",
                        priority="Medium",
                        extra={"fetch_status": fetch_status},
                    )
            db.execute(
                "UPDATE call_recording_jobs SET analysis_status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (analysis_status, job_id),
            )
            return {"job_id": job_id, "status": "no_recording_url", "call_outcome": outcome}

    audio_res = requests.get(recording_url, timeout=60)
    audio_res.raise_for_status()
    audio_bytes = audio_res.content
    ext = _detect_audio_extension(audio_res.headers.get("content-type"), recording_url)
    filename = f"{call_sid or f'call_{job_id}'}{ext}"
    mime_type = (audio_res.headers.get("content-type") or "").split(";")[0].strip() or "application/octet-stream"

    transcription = transcribe_recording_with_openai(audio_bytes, filename=filename, mime_type=mime_type)
    transcript = (transcription.get("transcript") or "").strip()
    analysis = analyze_call_transcript_with_openai(transcript, property_id=property_id, person_id=person_id)
    summary_text = (analysis.get("summary") or "").strip()

    db.execute(
        """
        UPDATE call_recording_jobs
        SET fetch_status = 'Fetched',
            analysis_status = 'Completed',
            transcript_text = ?,
            summary_text = ?,
            analysis_json = ?,
            payload_json = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (
            transcript,
            summary_text,
            json.dumps(analysis),
            json.dumps(
                {
                    "recording_url": recording_url,
                    "transcription_raw": transcription.get("raw") or {},
                    "analysis": analysis,
                }
            ),
            job_id,
        ),
    )
    if person_id:
        note_body = (
            f"Call analyzed for SID {call_sid}.\n"
            f"Summary: {summary_text or '-'}\n"
            f"Intent: {analysis.get('caller_intent') or '-'}\n"
            f"Recommended Next Step: {analysis.get('next_best_step') or '-'}\n"
            f"Coaching: {', '.join(analysis.get('caller_improvement') or []) or '-'}"
        )
        add_person_note(db, person_id, "Call Recording Analysis", note_body, analysis)

    upsert_agent_signal(
        db,
        source_type="call",
        source_key=f"call:{call_sid}",
        property_id=property_id,
        person_id=person_id,
        channel="CALL",
        intent=(analysis.get("caller_intent") or "").strip(),
        sentiment=(analysis.get("sentiment") or "Unknown").strip(),
        confidence=float(analysis.get("confidence") or 0.0),
        recommended_next_step=(analysis.get("next_best_step") or "").strip(),
        summary_text=summary_text,
        is_spam=False,
        do_not_contact=(analysis.get("next_best_step") in {"Mark Not Interested", "Mark Opt Out"}),
        payload={"call_sid": call_sid, "analysis": analysis, "recording_url": recording_url},
    )
    route_new_agent_signals_once(db, limit=25)
    try:
        send_slack_notification(
            db,
            "\n".join(
                [
                    "Call Analysis Complete",
                    f"Call SID: {call_sid}",
                    f"Property ID: {property_id or '-'}",
                    f"Person ID: {person_id or '-'}",
                    f"Next Step: {analysis.get('next_best_step') or '-'}",
                ]
            ),
        )
    except Exception:
        pass
    return {"job_id": job_id, "status": "completed", "call_sid": call_sid, "next_step": analysis.get("next_best_step")}


def run_call_recording_analysis_once(limit=2):
    ensure_db()
    db = open_sqlite_connection()
    try:
        rows = db.execute(
            """
            SELECT *
            FROM call_recording_jobs
            WHERE analysis_status IN ('Pending', 'Retry')
            ORDER BY id ASC
            LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()
        processed = 0
        completed = 0
        failed = 0
        details = []
        for row in rows:
            processed += 1
            try:
                out = process_single_call_recording_job(db, row)
                details.append(out)
                if out.get("status") == "completed":
                    completed += 1
            except Exception as exc:
                failed += 1
                payload = parse_json_object(row["payload_json"] or "{}")
                retry_count = int(payload.get("retry_count", 0) or 0) + 1
                payload["retry_count"] = retry_count
                payload["error"] = str(exc)
                db.execute(
                    """
                    UPDATE call_recording_jobs
                    SET analysis_status = ?,
                        fetch_status = CASE WHEN fetch_status = 'Queued' THEN 'Error' ELSE fetch_status END,
                        payload_json = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (
                        "Retry" if retry_count < CALL_ANALYSIS_MAX_RETRIES else "Failed",
                        json.dumps(payload),
                        row["id"],
                    ),
                )
                if row["property_id"]:
                    _create_agent_diagnosis_review_action(
                        db,
                        property_id=int(row["property_id"]),
                        person_id=row["person_id"],
                        call_sid=row["call_sid"],
                        diagnosis="Call analysis failed",
                        attempted_fix=f"Automatic retry {retry_count}/{CALL_ANALYSIS_MAX_RETRIES}",
                        proposed_resolution=(
                            "Retry analysis automatically."
                            if retry_count < CALL_ANALYSIS_MAX_RETRIES
                            else "Retries exhausted; review payload/provider recording quality."
                        ),
                        priority="Medium" if retry_count < CALL_ANALYSIS_MAX_RETRIES else "High",
                        extra={"error": str(exc)},
                    )
                details.append({"job_id": row["id"], "status": "error", "error": str(exc)})
                log_app_error(
                    db,
                    source="call_recording_worker",
                    error_message=str(exc),
                    details=traceback.format_exc(),
                    route="run_call_recording_analysis_once",
                    status_code=500,
                )
        db.commit()
        return {"ok": True, "processed": processed, "completed": completed, "failed": failed, "details": details}
    except Exception as exc:
        db.rollback()
        log_app_error(
            db,
            source="call_recording_worker",
            error_message=str(exc),
            details=traceback.format_exc(),
            route="run_call_recording_analysis_once",
            status_code=500,
        )
        db.commit()
        return {"ok": False, "error": str(exc)}
    finally:
        db.close()


def start_call_recording_worker():
    global CALL_RECORDING_WORKER_STARTED
    if CALL_RECORDING_WORKER_STARTED or not CALL_RECORDING_WORKER_ENABLED:
        return
    CALL_RECORDING_WORKER_STARTED = True

    def worker():
        while True:
            try:
                run_call_recording_analysis_once(limit=2)
            except Exception:
                pass
            time.sleep(CALL_RECORDING_POLL_SECONDS)

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()


def upsert_agent_signal(
    db,
    source_type,
    source_key,
    property_id=None,
    person_id=None,
    channel="",
    intent="",
    sentiment="",
    confidence=0.0,
    recommended_next_step="",
    summary_text="",
    is_spam=False,
    do_not_contact=False,
    payload=None,
):
    row = db.execute("SELECT id FROM agent_signals WHERE source_key = ?", ((source_key or "").strip(),)).fetchone()
    payload_json = json.dumps(payload or {})
    args = (
        (source_type or "").strip() or "unknown",
        (source_key or "").strip(),
        property_id,
        person_id,
        (channel or "").strip(),
        (intent or "").strip(),
        (sentiment or "").strip(),
        float(confidence or 0.0),
        (recommended_next_step or "").strip(),
        (summary_text or "").strip(),
        1 if is_spam else 0,
        1 if do_not_contact else 0,
        payload_json,
    )
    if row:
        db.execute(
            """
            UPDATE agent_signals
            SET source_type = ?, property_id = ?, person_id = ?, channel = ?, intent = ?, sentiment = ?,
                confidence = ?, recommended_next_step = ?, summary_text = ?, is_spam = ?, do_not_contact = ?,
                payload_json = ?, routing_status = 'new', updated_at = CURRENT_TIMESTAMP
            WHERE source_key = ?
            """,
            args + ((source_key or "").strip(),),
        )
        return row["id"]
    cur = db.execute(
        """
        INSERT INTO agent_signals
        (source_type, source_key, property_id, person_id, channel, intent, sentiment, confidence, recommended_next_step, summary_text, is_spam, do_not_contact, payload_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        args,
    )
    return cur.lastrowid


def _normalize_reisift_status(status_value):
    raw = str(status_value or "").strip().lower().replace("_", " ")
    raw = re.sub(r"\s+", " ", raw).strip()
    aliases = {
        "new_lead": "new lead",
        "no contact new lead": "no contact new lead",
        "nurture new lead": "nurture new lead",
        "hot lead": "hot lead",
        "warm lead": "warm lead",
        "cold lead": "cold lead",
        "ghosting lead": "ghosting lead",
        "dead leads": "dead lead",
        "dead lead": "dead lead",
        "not interested": "not interested",
        "not_interested": "not interested",
        "opt out": "opt-out",
        "opt-out": "opt-out",
        "listed": "listed",
        "refer lead": "refer lead",
        "refer_lead": "refer lead",
    }
    return aliases.get(raw, raw)


def _playbook_priority_for_action(property_status, action_type, fallback_priority):
    status_key = _normalize_reisift_status(property_status)
    row = REISIFT_PLAYBOOK_PRIORITY.get(status_key, {})
    return row.get(action_type, fallback_priority)


def _map_signal_to_action(signal_row, property_status=""):
    next_step = (signal_row["recommended_next_step"] or "").strip()
    mapping = {
        "Schedule Appointment": ("ScheduleAppointment", "High"),
        "Follow Up": ("FollowUpTouchDue", "Medium"),
        "Escalate To Relative": ("EscalateToRelativeOutreach", "High"),
        "Deep Dive": ("EscalateToDeepDiveResearch", "High"),
        "Mark Not Interested": ("HumanReviewDisposition", "High"),
        "Mark Opt Out": ("HumanReviewDisposition", "High"),
        "No Action": ("ReviewInboundAndNextBestAction", "Low"),
    }
    action_type, base_priority = mapping.get(next_step, ("ReviewInboundAndNextBestAction", "Medium"))
    priority = _playbook_priority_for_action(property_status, action_type, base_priority)
    return action_type, priority


def route_new_agent_signals_once(db, limit=50):
    rows = db.execute(
        """
        SELECT *
        FROM agent_signals
        WHERE routing_status = 'new'
        ORDER BY id ASC
        LIMIT ?
        """,
        (max(1, int(limit)),),
    ).fetchall()
    routed = 0
    skipped = 0
    for row in rows:
        property_id = row["property_id"]
        person_id = row["person_id"]
        payload = parse_json_object(row["payload_json"] or "{}")
        if not property_id:
            db.execute(
                "UPDATE agent_signals SET routing_status = 'unmapped', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (row["id"],),
            )
            skipped += 1
            continue
        prop = db.execute("SELECT status FROM properties WHERE id = ?", (property_id,)).fetchone()
        prop_status = str((prop["status"] if prop else "") or "").strip().lower()
        is_conflict = bool(row["do_not_contact"]) and prop_status in ACTIVE_LEAD_STATUSES
        if is_conflict:
            _upsert_lead_action(
                db,
                int(property_id),
                person_id,
                "HumanReviewDisposition",
                "High",
                "Conflict detected: do-not-contact signal conflicts with active lead status. Human review required.",
                {"signal_id": row["id"], "property_status": prop_status, "signal": payload},
            )
        elif row["is_spam"]:
            _upsert_lead_action(
                db,
                int(property_id),
                person_id,
                "ReviewInboundAndNextBestAction",
                "Low",
                "Signal classified as spam/solicitation. Verify and dismiss if not business related.",
                {"signal_id": row["id"], "signal": payload},
            )
        else:
            action_type, priority = _map_signal_to_action(row, prop_status)
            _upsert_lead_action(
                db,
                int(property_id),
                person_id,
                action_type,
                priority,
                (
                    f"Agent signal ({row['source_type']}) recommends: {(row['recommended_next_step'] or 'review')}. "
                    f"Priority derived from ReiSift playbook status '{prop_status or '-'}'."
                ),
                {"signal_id": row["id"], "signal": payload},
            )
        db.execute(
            "UPDATE agent_signals SET routing_status = 'routed', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (row["id"],),
        )
        routed += 1
    return {"processed": len(rows), "routed": routed, "skipped": skipped}


def _system_phone_numbers(db):
    vals = set()
    for raw in [SMRTPHONE_FROM_NUMBER, get_setting(db, "deep_dive_smrtphone_from", ""), get_setting(db, "referral_smrtphone_from", "")]:
        n = normalize_phone(raw)
        if n:
            vals.add(n)
    return vals


def _thread_counterpart(system_numbers, direction, from_number, to_number):
    from_norm = normalize_phone(from_number)
    to_norm = normalize_phone(to_number)
    if direction == "outbound":
        return to_norm or from_norm
    if direction == "inbound":
        return from_norm or to_norm
    if from_norm and from_norm not in system_numbers:
        return from_norm
    if to_norm and to_norm not in system_numbers:
        return to_norm
    return from_norm or to_norm


def analyze_sms_thread_with_openai(messages, property_id=None, person_id=None, counterpart=""):
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise ValueError("OPENAI_API_KEY is not set")
    if not messages:
        raise ValueError("messages required")
    model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini").strip() or "gpt-4.1-mini"
    text_lines = []
    for m in messages:
        ts = m.get("sent_at") or m.get("created_at") or ""
        direction = (m.get("direction") or "").strip().title()
        body = (m.get("body") or "").strip()
        if not body:
            continue
        text_lines.append(f"[{ts}] {direction}: {body}")
    transcript = "\n".join(text_lines)[:16000]
    schema = {
        "name": "sms_thread_analysis",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "summary": {"type": "string"},
                "intent": {"type": "string"},
                "sentiment": {"type": "string", "enum": ["Positive", "Neutral", "Negative", "Mixed", "Unknown"]},
                "conversation_type": {"type": "string", "enum": ["Business", "Spam", "Wrong Number", "Unknown"]},
                "recommended_next_step": {
                    "type": "string",
                    "enum": [
                        "Schedule Appointment",
                        "Follow Up",
                        "Escalate To Relative",
                        "Deep Dive",
                        "Mark Not Interested",
                        "Mark Opt Out",
                        "No Action",
                    ],
                },
                "confidence": {"type": "number"},
                "key_facts": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["summary", "intent", "sentiment", "conversation_type", "recommended_next_step", "confidence", "key_facts"],
        },
        "strict": True,
    }
    prompt = (
        "Analyze this SMS thread for lead management.\n"
        f"Property ID: {property_id or '-'} | Person ID: {person_id or '-'} | Counterpart: {counterpart or '-'}\n"
        "Return only valid JSON."
        f"\n\nThread:\n{transcript}"
    )
    response = requests.post(
        "https://api.openai.com/v1/responses",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": model,
            "input": [
                {"role": "system", "content": [{"type": "input_text", "text": "You are an SMS conversation analyst for a real estate CRM."}]},
                {"role": "user", "content": [{"type": "input_text", "text": prompt}]},
            ],
            "text": {"format": {"type": "json_schema", "name": schema["name"], "schema": schema["schema"], "strict": True}},
        },
        timeout=60,
    )
    response.raise_for_status()
    data = response.json()
    output_text = (data.get("output_text") or "").strip()
    if output_text:
        return json.loads(output_text)
    for item in data.get("output", []):
        for part in item.get("content", []):
            if part.get("type") in {"output_text", "text"} and part.get("text"):
                return json.loads(part["text"])
    raise ValueError("No JSON output returned by sms analyzer")


def run_sms_analysis_once(lookback_days=14, limit_threads=30):
    ensure_db()
    db = open_sqlite_connection()
    try:
        cutoff = format_db_time(datetime.utcnow() - timedelta(days=max(1, int(lookback_days))))
        rows = db.execute(
            """
            SELECT id, property_id, person_id, direction, from_number, to_number, body, status, sent_at, created_at
            FROM communications
            WHERE upper(channel) = 'SMS'
              AND COALESCE(sent_at, created_at) >= ?
            ORDER BY COALESCE(sent_at, created_at) ASC, id ASC
            """,
            (cutoff,),
        ).fetchall()
        system_numbers = _system_phone_numbers(db)
        threads = {}
        for r in rows:
            direction = str(r["direction"] or "").strip().lower()
            counterpart = _thread_counterpart(system_numbers, direction, r["from_number"], r["to_number"])
            if not counterpart:
                continue
            key = f"{int(r['property_id'] or 0)}:{int(r['person_id'] or 0)}:{counterpart}"
            t = threads.setdefault(
                key,
                {
                    "thread_key": key,
                    "property_id": r["property_id"],
                    "person_id": r["person_id"],
                    "counterpart": counterpart,
                    "messages": [],
                },
            )
            t["messages"].append(dict(r))
        thread_list = sorted(threads.values(), key=lambda x: (x["messages"][-1].get("sent_at") or x["messages"][-1].get("created_at") or ""), reverse=True)
        processed = 0
        analyzed = 0
        failed = 0
        details = []
        for t in thread_list[: max(1, int(limit_threads))]:
            processed += 1
            msgs = t["messages"]
            hash_input = "\n".join([f"{m.get('direction','')}|{m.get('from_number','')}|{m.get('to_number','')}|{m.get('body','')}" for m in msgs])
            transcript_hash = hashlib.sha1(hash_input.encode("utf-8", errors="ignore")).hexdigest()
            state = db.execute("SELECT transcript_hash FROM sms_thread_state WHERE thread_key = ?", (t["thread_key"],)).fetchone()
            last_at = msgs[-1].get("sent_at") or msgs[-1].get("created_at") or format_db_time(datetime.utcnow())
            if state and (state["transcript_hash"] or "") == transcript_hash:
                db.execute(
                    "UPDATE sms_thread_state SET last_message_at = ?, message_count = ?, updated_at = CURRENT_TIMESTAMP WHERE thread_key = ?",
                    (last_at, len(msgs), t["thread_key"]),
                )
                details.append({"thread_key": t["thread_key"], "status": "unchanged"})
                continue
            try:
                analysis = analyze_sms_thread_with_openai(
                    msgs,
                    property_id=t["property_id"],
                    person_id=t["person_id"],
                    counterpart=t["counterpart"],
                )
                sentiment = (analysis.get("sentiment") or "Unknown").strip()
                intent = (analysis.get("intent") or "").strip()
                recommended = (analysis.get("recommended_next_step") or "").strip()
                conv_type = (analysis.get("conversation_type") or "Unknown").strip()
                is_spam = conv_type.lower() in {"spam", "wrong number"}
                dnc = recommended in {"Mark Opt Out", "Mark Not Interested"}
                upsert_agent_signal(
                    db,
                    source_type="sms",
                    source_key=f"sms_thread:{t['thread_key']}:{transcript_hash}",
                    property_id=t["property_id"],
                    person_id=t["person_id"],
                    channel="SMS",
                    intent=intent,
                    sentiment=sentiment,
                    confidence=float(analysis.get("confidence") or 0.0),
                    recommended_next_step=recommended,
                    summary_text=(analysis.get("summary") or "").strip(),
                    is_spam=is_spam,
                    do_not_contact=dnc,
                    payload={"thread_key": t["thread_key"], "counterpart": t["counterpart"], "analysis": analysis, "messages": msgs[-30:]},
                )
                analyzed += 1
                details.append({"thread_key": t["thread_key"], "status": "analyzed", "recommended_next_step": recommended})
            except Exception as exc:
                failed += 1
                details.append({"thread_key": t["thread_key"], "status": "error", "error": str(exc)})
                log_app_error(
                    db,
                    source="sms_analysis_worker",
                    error_message=str(exc),
                    details=traceback.format_exc(),
                    route="run_sms_analysis_once",
                    status_code=500,
                )
            db.execute(
                """
                INSERT INTO sms_thread_state (thread_key, property_id, person_id, counterpart_number, last_message_at, message_count, transcript_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(thread_key) DO UPDATE SET
                    property_id = excluded.property_id,
                    person_id = excluded.person_id,
                    counterpart_number = excluded.counterpart_number,
                    last_message_at = excluded.last_message_at,
                    message_count = excluded.message_count,
                    transcript_hash = excluded.transcript_hash,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (t["thread_key"], t["property_id"], t["person_id"], t["counterpart"], last_at, len(msgs), transcript_hash),
            )
        route_summary = route_new_agent_signals_once(db, limit=200)
        db.commit()
        return {"ok": True, "processed": processed, "analyzed": analyzed, "failed": failed, "routed": route_summary.get("routed", 0), "details": details[:50]}
    except Exception as exc:
        db.rollback()
        log_app_error(
            db,
            source="sms_analysis_worker",
            error_message=str(exc),
            details=traceback.format_exc(),
            route="run_sms_analysis_once",
            status_code=500,
        )
        db.commit()
        return {"ok": False, "error": str(exc)}
    finally:
        db.close()


def start_sms_analysis_worker():
    global SMS_ANALYSIS_WORKER_STARTED
    if SMS_ANALYSIS_WORKER_STARTED or not SMS_ANALYSIS_WORKER_ENABLED:
        return
    SMS_ANALYSIS_WORKER_STARTED = True

    def worker():
        while True:
            try:
                run_sms_analysis_once(lookback_days=14, limit_threads=20)
            except Exception:
                pass
            time.sleep(SMS_ANALYSIS_POLL_SECONDS)

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()


def build_agent_advisor_snapshot(db, window_hours=72):
    cutoff_dt = datetime.utcnow() - timedelta(hours=max(1, int(window_hours or 72)))
    cutoff = format_db_time(cutoff_dt)
    snapshot = {
        "window_hours": int(window_hours or 72),
        "window_cutoff": cutoff,
        "generated_at": format_db_time(datetime.utcnow()),
    }

    signal_counts = db.execute(
        """
        SELECT
            SUM(CASE WHEN source_type = 'sms' THEN 1 ELSE 0 END) AS sms_total,
            SUM(CASE WHEN source_type = 'call' THEN 1 ELSE 0 END) AS call_total,
            SUM(CASE WHEN routing_status = 'new' THEN 1 ELSE 0 END) AS new_total,
            SUM(CASE WHEN routing_status = 'routed' THEN 1 ELSE 0 END) AS routed_total,
            SUM(CASE WHEN do_not_contact = 1 THEN 1 ELSE 0 END) AS dnc_total,
            SUM(CASE WHEN is_spam = 1 THEN 1 ELSE 0 END) AS spam_total
        FROM agent_signals
        WHERE updated_at >= ?
        """,
        (cutoff,),
    ).fetchone()
    snapshot["signals"] = {
        "sms_total": int((signal_counts["sms_total"] if signal_counts else 0) or 0),
        "call_total": int((signal_counts["call_total"] if signal_counts else 0) or 0),
        "new_total": int((signal_counts["new_total"] if signal_counts else 0) or 0),
        "routed_total": int((signal_counts["routed_total"] if signal_counts else 0) or 0),
        "dnc_total": int((signal_counts["dnc_total"] if signal_counts else 0) or 0),
        "spam_total": int((signal_counts["spam_total"] if signal_counts else 0) or 0),
    }

    action_counts = db.execute(
        """
        SELECT
            SUM(CASE WHEN status = 'Pending' THEN 1 ELSE 0 END) AS pending_total,
            SUM(CASE WHEN status = 'Pending' AND priority = 'High' THEN 1 ELSE 0 END) AS high_pending_total,
            SUM(CASE WHEN status = 'Completed' AND updated_at >= ? THEN 1 ELSE 0 END) AS completed_recent
        FROM lead_management_actions
        """,
        (cutoff,),
    ).fetchone()
    stale_counts = db.execute(
        """
        SELECT
            SUM(CASE WHEN status = 'Pending' AND datetime(updated_at) <= datetime('now', '-2 day') THEN 1 ELSE 0 END) AS stale_pending_2d
        FROM lead_management_actions
        """
    ).fetchone()
    snapshot["actions"] = {
        "pending_total": int((action_counts["pending_total"] if action_counts else 0) or 0),
        "high_pending_total": int((action_counts["high_pending_total"] if action_counts else 0) or 0),
        "completed_recent": int((action_counts["completed_recent"] if action_counts else 0) or 0),
        "stale_pending_2d": int((stale_counts["stale_pending_2d"] if stale_counts else 0) or 0),
    }

    call_counts = db.execute(
        """
        SELECT
            SUM(CASE WHEN analysis_status = 'Completed' AND updated_at >= ? THEN 1 ELSE 0 END) AS completed_recent,
            SUM(CASE WHEN analysis_status IN ('Pending', 'Retry') THEN 1 ELSE 0 END) AS pending_total,
            SUM(CASE WHEN analysis_status = 'Failed' THEN 1 ELSE 0 END) AS failed_total
        FROM call_recording_jobs
        """,
        (cutoff,),
    ).fetchone()
    snapshot["calls"] = {
        "completed_recent": int((call_counts["completed_recent"] if call_counts else 0) or 0),
        "pending_total": int((call_counts["pending_total"] if call_counts else 0) or 0),
        "failed_total": int((call_counts["failed_total"] if call_counts else 0) or 0),
    }

    latest_run = db.execute(
        """
        SELECT id, status, created_at, completed_at, summary_json
        FROM lead_monitor_runs
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    snapshot["latest_run"] = {
        "id": int(latest_run["id"]) if latest_run else 0,
        "status": str(latest_run["status"]) if latest_run else "",
        "created_at": str(latest_run["created_at"]) if latest_run else "",
        "completed_at": str(latest_run["completed_at"]) if latest_run else "",
        "summary": parse_json_object(latest_run["summary_json"]) if latest_run else {},
    }

    pending_actions = db.execute(
        """
        SELECT id, property_id, person_id, action_type, priority, status, reason, updated_at
        FROM lead_management_actions
        WHERE status = 'Pending'
        ORDER BY
            CASE priority WHEN 'High' THEN 0 WHEN 'Medium' THEN 1 ELSE 2 END,
            updated_at ASC,
            id ASC
        LIMIT 8
        """
    ).fetchall()
    snapshot["top_pending_actions"] = [
        {
            "id": int(r["id"]),
            "property_id": int(r["property_id"] or 0),
            "person_id": int(r["person_id"] or 0),
            "action_type": str(r["action_type"] or ""),
            "priority": str(r["priority"] or ""),
            "reason": str(r["reason"] or ""),
            "updated_at": str(r["updated_at"] or ""),
        }
        for r in pending_actions
    ]
    return snapshot


def build_agent_advisor_recommendations(snapshot):
    signals = snapshot.get("signals", {})
    actions = snapshot.get("actions", {})
    calls = snapshot.get("calls", {})
    recs = []
    if int(actions.get("high_pending_total", 0)) > 0:
        recs.append(
            {
                "priority": "High",
                "title": "Clear high-priority lead actions",
                "reason": f"{actions.get('high_pending_total', 0)} high-priority items are pending.",
                "next_step": "Review and approve/dismiss high-priority items first.",
            }
        )
    if int(signals.get("new_total", 0)) > 0:
        recs.append(
            {
                "priority": "High" if int(signals.get("new_total", 0)) >= 5 else "Medium",
                "title": "Route new communication signals",
                "reason": f"{signals.get('new_total', 0)} signals remain unrouted.",
                "next_step": "Run SMS/call analysis routing and confirm resulting actions.",
            }
        )
    if int(calls.get("pending_total", 0)) > 0:
        recs.append(
            {
                "priority": "Medium",
                "title": "Process pending call analysis",
                "reason": f"{calls.get('pending_total', 0)} call recordings are waiting.",
                "next_step": "Run call analysis to keep lead intent current.",
            }
        )
    if int(actions.get("stale_pending_2d", 0)) > 0:
        recs.append(
            {
                "priority": "Medium",
                "title": "Resolve stale pending actions",
                "reason": f"{actions.get('stale_pending_2d', 0)} pending actions are older than 2 days.",
                "next_step": "Escalate stale items to explicit owner decisions.",
            }
        )
    if int(signals.get("dnc_total", 0)) > 0:
        recs.append(
            {
                "priority": "High",
                "title": "Validate do-not-contact constraints",
                "reason": f"{signals.get('dnc_total', 0)} recent signals indicate do-not-contact risk.",
                "next_step": "Confirm no outbound actions are queued for these leads.",
            }
        )
    if not recs:
        recs.append(
            {
                "priority": "Low",
                "title": "Pipeline stable",
                "reason": "No high-risk backlog was detected.",
                "next_step": "Continue normal monitoring cadence.",
            }
        )
    return recs[:6]


def fallback_agent_advisor_summary(snapshot, recommendations):
    signals = snapshot.get("signals", {})
    actions = snapshot.get("actions", {})
    calls = snapshot.get("calls", {})
    lines = [
        f"Window: last {snapshot.get('window_hours', 72)}h",
        (
            f"Signals - SMS: {signals.get('sms_total', 0)}, Calls: {signals.get('call_total', 0)}, "
            f"New: {signals.get('new_total', 0)}, Routed: {signals.get('routed_total', 0)}"
        ),
        (
            f"Actions - Pending: {actions.get('pending_total', 0)}, High Priority: {actions.get('high_pending_total', 0)}, "
            f"Stale >2d: {actions.get('stale_pending_2d', 0)}"
        ),
        (
            f"Call Analysis - Pending: {calls.get('pending_total', 0)}, "
            f"Completed (window): {calls.get('completed_recent', 0)}, Failed: {calls.get('failed_total', 0)}"
        ),
        "Top Recommendations:",
    ]
    for idx, rec in enumerate(recommendations[:4], start=1):
        lines.append(f"{idx}. [{rec['priority']}] {rec['title']} - {rec['next_step']}")
    return "\n".join(lines)


def generate_agent_advisor_summary_with_openai(snapshot, recommendations):
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        return {"mode": "fallback", "summary_text": fallback_agent_advisor_summary(snapshot, recommendations), "json": {}}
    model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini").strip() or "gpt-4.1-mini"
    schema = {
        "name": "agent_advisor_summary",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "executive_summary": {"type": "string"},
                "top_priorities": {"type": "array", "items": {"type": "string"}},
                "risks": {"type": "array", "items": {"type": "string"}},
                "next_12h_plan": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["executive_summary", "top_priorities", "risks", "next_12h_plan"],
        },
    }
    prompt = (
        "You are Agent 3 (Advisor Synthesizer) for a real-estate CRM.\n"
        "Create a concise operator brief from the snapshot and deterministic recommendations.\n"
        "Focus on actionability and risk control.\n\n"
        f"Snapshot JSON:\n{json.dumps(snapshot, ensure_ascii=True)}\n\n"
        f"Recommendations JSON:\n{json.dumps(recommendations, ensure_ascii=True)}"
    )
    response = requests.post(
        "https://api.openai.com/v1/responses",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": model,
            "input": [
                {
                    "role": "system",
                    "content": [{"type": "input_text", "text": "You synthesize CRM agent outputs into prioritized, practical briefings."}],
                },
                {"role": "user", "content": [{"type": "input_text", "text": prompt}]},
            ],
            "text": {"format": {"type": "json_schema", "name": schema["name"], "schema": schema["schema"], "strict": True}},
        },
        timeout=60,
    )
    response.raise_for_status()
    data = response.json()
    output_text = (data.get("output_text") or "").strip()
    payload = json.loads(output_text) if output_text else {}
    if not payload:
        for item in data.get("output", []):
            for part in item.get("content", []):
                if part.get("type") in {"output_text", "text"} and part.get("text"):
                    payload = json.loads(part["text"])
                    break
            if payload:
                break
    if not payload:
        raise ValueError("No JSON output returned by advisor summary")
    summary_text = (
        f"{payload.get('executive_summary', '').strip()}\n\n"
        f"Top priorities:\n- " + "\n- ".join(payload.get("top_priorities", [])[:5]) + "\n\n"
        f"Risks:\n- " + "\n- ".join(payload.get("risks", [])[:5]) + "\n\n"
        f"Next 12h plan:\n- " + "\n- ".join(payload.get("next_12h_plan", [])[:6])
    ).strip()
    return {"mode": "openai", "summary_text": summary_text, "json": payload}


def answer_agent_advisor_question(question, snapshot, recommendations, qa_history):
    q = (question or "").strip()
    if not q:
        raise ValueError("Question is required.")
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        return (
            "OPENAI_API_KEY is not configured, so this is deterministic guidance.\n"
            + fallback_agent_advisor_summary(snapshot, recommendations)
        )
    model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini").strip() or "gpt-4.1-mini"
    compact_history = [{"q": (h.get("question") or "")[:300], "a": (h.get("response_text") or "")[:500]} for h in qa_history[:6]]
    prompt = (
        "You are Agent 3 (Advisor Synthesizer). Answer the operator's question using only provided context.\n"
        "If context is insufficient, state exactly what is missing.\n\n"
        f"Question: {q}\n\n"
        f"Snapshot: {json.dumps(snapshot, ensure_ascii=True)}\n\n"
        f"Deterministic recommendations: {json.dumps(recommendations, ensure_ascii=True)}\n\n"
        f"Recent Q&A: {json.dumps(compact_history, ensure_ascii=True)}"
    )
    response = requests.post(
        "https://api.openai.com/v1/responses",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": model,
            "input": [
                {"role": "system", "content": [{"type": "input_text", "text": "Answer operational CRM questions clearly and directly."}]},
                {"role": "user", "content": [{"type": "input_text", "text": prompt}]},
            ],
        },
        timeout=60,
    )
    response.raise_for_status()
    data = response.json()
    text = (data.get("output_text") or "").strip()
    if text:
        return text
    for item in data.get("output", []):
        for part in item.get("content", []):
            if part.get("type") in {"output_text", "text"} and part.get("text"):
                return str(part["text"]).strip()
    raise ValueError("No output returned by advisor Q&A")


def run_bulk_sms_tick():
    run_sequence_tick()
    ensure_db()
    db = open_sqlite_connection()
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
            from_number = get_deep_dive_sms_number(db)
            try:
                sent = send_smrtphone_sms(to_number, message, from_number=from_number)
                status = sent.get("status") or "Sent"
                cur = db.execute(
                    """
                    INSERT INTO communications (property_id, person_id, channel, direction, from_number, to_number, body, status, is_read, sent_at, external_id)
                    VALUES (?, ?, 'SMS', 'Outbound', ?, ?, ?, ?, 1, ?, ?)
                    """,
                    (
                        item["property_id"],
                        item["person_id"],
                        from_number,
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
                apply_touchpoint_status_inference(db, to_number, str(exc))
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


def normalize_name_token(token):
    t = re.sub(r"[^A-Za-z'\-\.]", "", (token or "").strip())
    return t.strip()


def split_name_first_middle_last(full_name):
    raw = re.sub(r"\s+", " ", (full_name or "").strip())
    if not raw:
        return ("Unknown", "", "Unknown", "")
    tokens = [normalize_name_token(t) for t in raw.split(" ") if normalize_name_token(t)]
    if not tokens:
        return ("Unknown", "", "Unknown", "")

    suffixes = {"jr", "jr.", "sr", "sr.", "ii", "iii", "iv", "v"}
    particles = {"de", "del", "la", "le", "van", "von", "di", "da", "st", "saint"}

    suffix = ""
    while len(tokens) > 1 and tokens[-1].lower() in suffixes:
        suffix = tokens.pop()

    first_name = tokens[0]
    if len(tokens) == 1:
        return (first_name, "", "Unknown", suffix)

    middle_tokens = []
    if len(tokens) >= 3 and tokens[-2].lower() in particles:
        last_name = f"{tokens[-2]} {tokens[-1]}"
        middle_tokens = tokens[1:-2]
    else:
        last_name = tokens[-1]
        middle_tokens = tokens[1:-1]
    middle_name = " ".join([m for m in middle_tokens if m]).strip()
    return (first_name, middle_name, last_name, suffix)


def split_name_first_last(full_name):
    first_name, _middle_name, last_name, suffix = split_name_first_middle_last(full_name)
    return (first_name, last_name, suffix)


def normalize_first_last(first_name, last_name):
    combined = " ".join([str(first_name or "").strip(), str(last_name or "").strip()]).strip()
    first, last, _suffix = split_name_first_last(combined)
    return first, last


def normalize_first_middle_last(first_name, middle_name, last_name):
    combined = " ".join(
        [str(first_name or "").strip(), str(middle_name or "").strip(), str(last_name or "").strip()]
    ).strip()
    first, middle, last, _suffix = split_name_first_middle_last(combined)
    return first, middle, last


def normalize_people_name_data(db):
    rows = db.execute("SELECT id, first_name, middle_name, last_name FROM people").fetchall()
    for r in rows:
        first = (r["first_name"] or "").strip()
        middle = (r["middle_name"] or "").strip()
        last = (r["last_name"] or "").strip()
        n_first, n_middle, n_last = normalize_first_middle_last(first, middle, last)
        if not n_first:
            n_first = first or "Unknown"
        if n_middle is None:
            n_middle = middle or ""
        if not n_last:
            n_last = last or "Unknown"
        if n_first != first or n_middle != middle or n_last != last:
            db.execute(
                "UPDATE people SET first_name = ?, middle_name = ?, last_name = ? WHERE id = ?",
                (n_first, n_middle, n_last, r["id"]),
            )


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
    first_name, last_name, suffix = split_name_first_last(full_name)
    if first_name == "Unknown" and last_name == "Unknown":
        return None

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
            extra = f"{notes} ParsedSuffix:{suffix}".strip() if suffix else notes
            db.execute(
                "UPDATE people SET notes = trim(coalesce(notes,'') || ' ' || ?) WHERE id = ?",
                (extra, row["id"]),
            )
        return row["id"]

    create_notes = f"{notes} ParsedSuffix:{suffix}".strip() if suffix else notes
    return create_person(db, first_name, last_name, notes=create_notes)


def find_or_create_person_by_full_name_smart(db, full_name, notes=""):
    cleaned = clean_name(full_name) or (full_name or "").strip()
    parts = [p for p in cleaned.split() if p]
    if len(parts) < 2:
        return find_or_create_person_by_full_name(db, cleaned, notes=notes)
    first_name, last_name, suffix = split_name_first_last(cleaned)
    exact = db.execute(
        """
        SELECT id
        FROM people
        WHERE lower(first_name) = lower(?) AND lower(last_name) = lower(?)
        LIMIT 1
        """,
        (first_name, last_name),
    ).fetchone()
    if exact:
        if notes:
            extra = f"{notes} ParsedSuffix:{suffix}".strip() if suffix else notes
            db.execute("UPDATE people SET notes = trim(coalesce(notes,'') || ' ' || ?) WHERE id = ?", (extra, exact["id"]))
        return exact["id"]

    last_token = (last_name or "").split(" ")[-1].lower()
    candidates = db.execute(
        """
        SELECT id, first_name, last_name
        FROM people
        WHERE lower(first_name) = lower(?)
        """,
        (first_name,),
    ).fetchall()
    for c in candidates:
        c_last = (c["last_name"] or "").strip().lower()
        c_parts = [x for x in c_last.split() if x]
        if c_last == last_name.lower() or (c_parts and c_parts[-1] == last_token):
            if notes:
                extra = f"{notes} ParsedSuffix:{suffix}".strip() if suffix else notes
                db.execute("UPDATE people SET notes = trim(coalesce(notes,'') || ' ' || ?) WHERE id = ?", (extra, c["id"]))
            return c["id"]

    create_notes = f"{notes} ParsedSuffix:{suffix}".strip() if suffix else notes
    return create_person(db, first_name, last_name, notes=create_notes)


def relationship_exists(db, subject_person_id, related_person_id, relationship_type):
    row = db.execute(
        """
        SELECT id
        FROM person_relationships
        WHERE subject_person_id = ? AND related_person_id = ? AND lower(relationship_type) = lower(?)
        LIMIT 1
        """,
        (subject_person_id, related_person_id, relationship_type),
    ).fetchone()
    return bool(row)


def find_or_create_person_by_name_parts(db, first_name, last_name, notes=""):
    first_name, last_name = normalize_first_last(first_name, last_name)
    first_name = first_name or "Unknown"
    last_name = last_name or "Unknown"
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
        relative_id = find_or_create_person_by_full_name_smart(
            db,
            item["name"],
            notes=f"Imported from obituary: {source_url}",
        )
        count_after = db.execute("SELECT COUNT(*) AS c FROM people").fetchone()["c"]
        if count_after > count_before:
            created_people += 1

        context = extract_obituary_context(raw_text, item['name'])
        note = f"Obituary import ({item['relative_status']}, marital: {item['marital_status']}) | Source: {context}"
        if not relationship_exists(db, subject_person_id, relative_id, item["relationship_type"]):
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
            spouse_id = find_or_create_person_by_full_name_smart(
                db,
                spouse_name,
                notes=f"Imported spouse from obituary: {source_url}",
            )
            if not relationship_exists(db, relative_id, spouse_id, "Spouse"):
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
        relative_id = find_or_create_person_by_full_name_smart(
            db,
            item["name"],
            notes=f"Imported from obituary: {source_url}",
        )
        count_after = db.execute("SELECT COUNT(*) AS c FROM people").fetchone()["c"]
        if count_after > count_before:
            created_people += 1

        context = extract_obituary_context(raw_text, item['name'])
        note = f"Obituary import ({item['relative_status']}, marital: {item['marital_status']}) | {item.get('note', '')} | Source: {context}".strip()
        if not relationship_exists(db, subject_person_id, relative_id, item["relationship_type"]):
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
            spouse_id = find_or_create_person_by_full_name_smart(
                db,
                spouse_name,
                notes=f"Imported spouse from obituary: {source_url}",
            )
            if not relationship_exists(db, relative_id, spouse_id, "Spouse"):
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
        "raw_excerpt": raw_text[:2000],
    }


def import_obituary_ai_into_property(db, property_id, subject_person_id, source_url, subject_name=""):
    raw_text = fetch_obituary_text(source_url)
    ai_result = call_openai_obituary_agent(raw_text, subject_name=subject_name)
    normalized = normalize_ai_obituary_result(ai_result)
    return import_normalized_obituary(db, property_id, subject_person_id, source_url, raw_text, normalized)


def parse_expansion_url_map(raw_text):
    def key_for(name):
        return re.sub(r"[^a-z0-9]+", " ", (name or "").lower()).strip()

    mapping = {}
    for line in (raw_text or "").splitlines():
        item = line.strip()
        if not item:
            continue
        parts = re.split(r"\s*\|\s*|\s*=\s*", item, maxsplit=1)
        if len(parts) != 2:
            continue
        name = clean_name(parts[0]) or parts[0].strip()
        url = parts[1].strip()
        if not name or not url.lower().startswith("http"):
            continue
        mapping[key_for(name)] = url
    return mapping


def location_hint_for_name(raw_text, name):
    if not raw_text or not name:
        return ""
    pattern = re.compile(
        rf"{re.escape(name)}[^.{{}}]{{0,120}}?\b(?:of|from|in)\s+([A-Z][A-Za-z\-\s]+,\s*[A-Z]{{2}})",
        re.IGNORECASE,
    )
    m = pattern.search(raw_text)
    if m:
        return m.group(1).strip()
    return ""


def obituary_item_confidence(item, depth=1):
    base = 0.82 if depth == 1 else 0.74
    status = (item.get("relative_status") or "").strip().lower()
    rel = (item.get("relationship_type") or "").strip().lower()
    note = (item.get("note") or "").strip().lower()
    if status in {"living", "deceased"}:
        base += 0.04
    if rel in {"spouse", "child", "sibling", "parent"}:
        base += 0.03
    if any(t in note for t in ["inferred", "possible", "uncertain", "guess"]):
        base -= 0.18
    return round(max(0.25, min(0.98, base)), 2)


def log_obituary_expansion_item(
    db,
    run_id,
    property_id,
    subject_person_id,
    related_person_id,
    depth,
    person_name,
    relationship_type,
    relative_status,
    confidence,
    processing_status,
    source_url,
    note,
    details=None,
):
    db.execute(
        """
        INSERT INTO obituary_expansion_items
        (run_id, property_id, subject_person_id, related_person_id, depth, person_name, relationship_type, relative_status, confidence, processing_status, source_url, note, details_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            property_id,
            subject_person_id,
            related_person_id,
            depth,
            person_name,
            relationship_type,
            relative_status,
            confidence,
            processing_status,
            source_url,
            note,
            json.dumps(details or {}),
        ),
    )


def import_obituary_ai_with_expansion(
    db,
    property_id,
    subject_person_id,
    source_url,
    subject_name="",
    expand_deceased=False,
    max_depth=1,
    expansion_url_map=None,
):
    max_depth = max(1, min(3, int(max_depth or 1)))
    expansion_url_map = expansion_url_map or {}
    run_cur = db.execute(
        """
        INSERT INTO obituary_expansion_runs (property_id, root_subject_person_id, root_source_url, max_depth, expand_deceased, status)
        VALUES (?, ?, ?, ?, ?, 'Running')
        """,
        (property_id, subject_person_id, source_url, max_depth, 1 if expand_deceased else 0),
    )
    run_id = run_cur.lastrowid
    queue = [(subject_person_id, source_url, 1, subject_name or "")]
    visited = set()
    processed = 0
    queued_secondary = 0
    missing_secondary = 0
    low_confidence = 0
    errors = []

    while queue:
        current_person_id, current_url, depth, current_subject_name = queue.pop(0)
        key = (current_person_id, current_url.strip().lower())
        if key in visited:
            continue
        visited.add(key)
        try:
            summary = import_obituary_ai_into_property(
                db,
                property_id=property_id,
                subject_person_id=current_person_id,
                source_url=current_url,
                subject_name=current_subject_name,
            )
            processed += 1
            raw_text = (summary.get("raw_excerpt") or "").strip()
            for item in (summary.get("surviving_relatives") or []) + (summary.get("preceded_relatives") or []):
                full_name = (item.get("name") or "").strip()
                if not full_name:
                    continue
                related_id = find_or_create_person_by_full_name_smart(db, full_name)
                conf = obituary_item_confidence(item, depth=depth)
                if conf < 0.65:
                    low_confidence += 1
                rel_status = (item.get("relative_status") or "Unknown").strip()
                rel_type = (item.get("relationship_type") or "Relative").strip()
                loc_hint = location_hint_for_name(raw_text, full_name)
                status = "Imported"
                note_bits = []
                if loc_hint:
                    note_bits.append(f"location hint: {loc_hint}")
                if expand_deceased and depth < max_depth and rel_status.lower() == "deceased":
                    lookup_key = re.sub(r"[^a-z0-9]+", " ", full_name.lower()).strip()
                    next_url = expansion_url_map.get(lookup_key, "")
                    if next_url:
                        next_person = db.execute("SELECT first_name, last_name FROM people WHERE id = ?", (related_id,)).fetchone()
                        next_name = f"{next_person['first_name']} {next_person['last_name']}".strip() if next_person else full_name
                        queue.append((related_id, next_url, depth + 1, next_name))
                        status = "Queued Secondary Search"
                        queued_secondary += 1
                    else:
                        status = "Missing Secondary URL"
                        missing_secondary += 1
                log_obituary_expansion_item(
                    db=db,
                    run_id=run_id,
                    property_id=property_id,
                    subject_person_id=current_person_id,
                    related_person_id=related_id,
                    depth=depth,
                    person_name=full_name,
                    relationship_type=rel_type,
                    relative_status=rel_status,
                    confidence=conf,
                    processing_status=status,
                    source_url=current_url,
                    note="; ".join(note_bits),
                    details={"marital_status": item.get("marital_status"), "spouse_name": item.get("spouse_name"), "ai_note": item.get("note")},
                )
        except Exception as exc:
            errors.append(str(exc))
            log_obituary_expansion_item(
                db=db,
                run_id=run_id,
                property_id=property_id,
                subject_person_id=current_person_id,
                related_person_id=None,
                depth=depth,
                person_name=current_subject_name or f"Person {current_person_id}",
                relationship_type="",
                relative_status="",
                confidence=0.25,
                processing_status="Failed",
                source_url=current_url,
                note=str(exc),
                details={"error": str(exc)},
            )

    summary = {
        "processed_obituaries": processed,
        "queued_secondary": queued_secondary,
        "missing_secondary_url": missing_secondary,
        "low_confidence_count": low_confidence,
        "errors": errors,
        "expand_deceased": bool(expand_deceased),
        "max_depth": max_depth,
    }
    db.execute(
        """
        UPDATE obituary_expansion_runs
        SET status = ?, summary_json = ?, completed_at = ?
        WHERE id = ?
        """,
        ("Completed" if not errors else "Completed with Warnings", json.dumps(summary), format_db_time(datetime.utcnow()), run_id),
    )
    return {"run_id": run_id, **summary}


def reisift_get_access_token():
    api_key = os.getenv("REISIFT_API_KEY", "").strip()
    email = os.getenv("REISIFT_EMAIL", "").strip()
    password = os.getenv("REISIFT_PASSWORD", "").strip()
    if api_key:
        try:
            verify = requests.get(
                f"{REISIFT_BASE_URL}/api/internal/user/",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "x-reisift-ui-version": REISIFT_UI_VERSION,
                },
                timeout=15,
            )
            if verify.ok:
                return api_key
        except requests.RequestException:
            pass

    if not email or not password:
        raise ValueError("Missing REISIFT credentials. Set REISIFT_EMAIL and REISIFT_PASSWORD.")

    response = requests.post(
        f"{REISIFT_BASE_URL}/api/token/",
        headers={"Content-Type": "application/json"},
        json={"email": email, "password": password, "remember": True, "agree": True},
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    token = (payload.get("access") or "").strip()
    if not token:
        raise ValueError("REISIFT login did not return an access token.")
    return token


def reisift_auth_headers(token, extra=None):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "x-reisift-ui-version": REISIFT_UI_VERSION,
    }
    if isinstance(extra, dict):
        headers.update(extra)
    return headers


def reisift_task_writes_enabled():
    return bool(REISIFT_TASK_WRITE_ENABLED)


def ensure_reisift_task_writes_enabled():
    if not reisift_task_writes_enabled():
        raise ValueError(
            "ReiSift task writes are disabled (REISIFT_TASK_WRITE_ENABLED=false). "
            "Task actions remain notification-only in this app."
        )


def parse_csv_list(value):
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    raw = str(value or "").strip()
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


def _reisift_find_first_map_id(payload):
    stack = [payload]
    while stack:
        current = stack.pop(0)
        if isinstance(current, dict):
            for key in ("map_id", "id"):
                if key in current:
                    candidate = str(current.get(key) or "").strip()
                    if candidate and candidate.isdigit():
                        return candidate
            for value in current.values():
                if isinstance(value, (dict, list)):
                    stack.append(value)
        elif isinstance(current, list):
            for item in current:
                if isinstance(item, (dict, list)):
                    stack.append(item)
    return ""


def _reisift_find_first_address_dict(payload):
    stack = [payload]
    while stack:
        current = stack.pop(0)
        if isinstance(current, dict):
            if any(k in current for k in ("street", "city", "state", "postal_code", "zip")):
                street = (current.get("street") or current.get("address1") or "").strip()
                city = (current.get("city") or "").strip()
                state = (current.get("state") or "").strip()
                postal_code = (current.get("postal_code") or current.get("zip") or current.get("zipcode") or "").strip()
                if street and city:
                    return {
                        "street": street,
                        "city": city,
                        "state": state,
                        "postal_code": postal_code,
                    }
            for value in current.values():
                if isinstance(value, (dict, list)):
                    stack.append(value)
        elif isinstance(current, list):
            for item in current:
                if isinstance(item, (dict, list)):
                    stack.append(item)
    return {"street": "", "city": "", "state": "", "postal_code": ""}


def _reisift_find_first_owner_dict(payload):
    stack = [payload]
    while stack:
        current = stack.pop(0)
        if isinstance(current, dict):
            owner = current.get("owner")
            if isinstance(owner, dict):
                return owner
            if any(k in current for k in ("first_name", "last_name", "full_name", "phones", "emails")):
                return current
            for value in current.values():
                if isinstance(value, (dict, list)):
                    stack.append(value)
        elif isinstance(current, list):
            for item in current:
                if isinstance(item, (dict, list)):
                    stack.append(item)
    return {}


def _reisift_find_owner_uuid(payload):
    if not isinstance(payload, (dict, list)):
        return ""
    queue = [payload]
    while queue:
        current = queue.pop(0)
        if isinstance(current, dict):
            for key in ("owner_uuid", "owner_id"):
                candidate = str(current.get(key) or "").strip()
                if candidate:
                    return candidate
            owner = current.get("owner")
            if isinstance(owner, dict):
                for key in ("uuid", "id"):
                    candidate = str(owner.get(key) or "").strip()
                    if candidate:
                        return candidate
            owners = current.get("owners")
            if isinstance(owners, list):
                for owner_item in owners:
                    if not isinstance(owner_item, dict):
                        continue
                    for key in ("uuid", "id"):
                        candidate = str(owner_item.get(key) or "").strip()
                        if candidate:
                            return candidate
            for value in current.values():
                if isinstance(value, (dict, list)):
                    queue.append(value)
        elif isinstance(current, list):
            for item in current:
                if isinstance(item, (dict, list)):
                    queue.append(item)
    return ""


def _reisift_parse_owner_name(owner):
    if not isinstance(owner, dict):
        return {"first_name": "", "last_name": ""}
    first = (owner.get("first_name") or "").strip()
    last = (owner.get("last_name") or "").strip()
    if first and last:
        return {"first_name": first, "last_name": last}
    full = (owner.get("full_name") or owner.get("name") or "").strip()
    if full:
        parts = full.split()
        if len(parts) == 1:
            return {"first_name": parts[0], "last_name": ""}
        return {"first_name": parts[0], "last_name": " ".join(parts[1:]).strip()}
    return {"first_name": first, "last_name": last}


def _reisift_parse_owner_emails(owner):
    emails = []
    seen = set()
    if not isinstance(owner, dict):
        return emails
    for item in owner.get("emails") or []:
        value = ""
        if isinstance(item, dict):
            value = (item.get("email") or item.get("value") or "").strip()
        else:
            value = str(item or "").strip()
        key = value.lower()
        if not value or key in seen:
            continue
        seen.add(key)
        emails.append(value)
    return emails


def _reisift_parse_owner_phones(owner):
    phones = []
    seen = set()
    if not isinstance(owner, dict):
        return phones
    for item in owner.get("phones") or []:
        number = ""
        p_type = "UNKNOWN"
        p_status = "UNKNOWN"
        if isinstance(item, dict):
            number = (item.get("number") or item.get("phone") or "").strip()
            p_type = (item.get("type") or "UNKNOWN").strip().upper()
            p_status = (item.get("status") or "UNKNOWN").strip().upper()
        else:
            number = str(item or "").strip()
        norm = normalize_phone(number)
        if not number or not norm or norm in seen:
            continue
        seen.add(norm)
        phones.append({"number": norm, "type": p_type, "tags": [], "status": p_status})
    return phones


def _reisift_build_property_create_payload(address_info_payload, input_payload):
    input_payload = input_payload or {}
    source_address = _reisift_find_first_address_dict(address_info_payload)
    source_owner = _reisift_find_first_owner_dict(address_info_payload)
    source_owner_name = _reisift_parse_owner_name(source_owner)
    source_owner_emails = _reisift_parse_owner_emails(source_owner)
    source_owner_phones = _reisift_parse_owner_phones(source_owner)

    override_owner = input_payload.get("owner") if isinstance(input_payload.get("owner"), dict) else {}
    first_name = (override_owner.get("first_name") or source_owner_name["first_name"] or "").strip() or "Unknown"
    last_name = (override_owner.get("last_name") or source_owner_name["last_name"] or "").strip() or "Owner"
    company = override_owner.get("company")

    override_emails = override_owner.get("emails")
    if isinstance(override_emails, list) and override_emails:
        emails = [str(x).strip() for x in override_emails if str(x).strip()]
    else:
        emails = source_owner_emails

    override_phones = override_owner.get("phones")
    if isinstance(override_phones, list) and override_phones:
        normalized = []
        seen = set()
        for p in override_phones:
            if isinstance(p, dict):
                num = normalize_phone(p.get("number") or p.get("phone") or "")
                p_type = (p.get("type") or "UNKNOWN").strip().upper()
                p_status = (p.get("status") or "UNKNOWN").strip().upper()
            else:
                num = normalize_phone(str(p or ""))
                p_type = "UNKNOWN"
                p_status = "UNKNOWN"
            if not num or num in seen:
                continue
            seen.add(num)
            normalized.append({"number": num, "type": p_type, "tags": [], "status": p_status})
        phones = normalized
    else:
        phones = source_owner_phones

    owner_address = {
        "street": (override_owner.get("address_street") or source_address["street"] or "").strip(),
        "city": (override_owner.get("address_city") or source_address["city"] or "").strip(),
        "state": (override_owner.get("address_state") or source_address["state"] or "").strip(),
        "postal_code": (override_owner.get("address_postal_code") or source_address["postal_code"] or "").strip(),
    }

    property_address = {
        "street": (input_payload.get("street") or source_address["street"] or "").strip(),
        "city": (input_payload.get("city") or source_address["city"] or "").strip(),
        "state": (input_payload.get("state") or source_address["state"] or "").strip(),
        "postal_code": (input_payload.get("postal_code") or source_address["postal_code"] or "").strip(),
    }

    if not property_address["street"] or not property_address["city"]:
        raise ValueError("ReiSift address-info did not return a valid property address.")

    lists = parse_csv_list(input_payload.get("lists"))
    tags = parse_csv_list(input_payload.get("tags"))
    # Per workflow, new inserts should always enter ReiSift as new_lead.
    status = "new_lead"
    notes = (input_payload.get("notes") or "").strip()

    owner_payload = {
        "first_name": first_name,
        "last_name": last_name,
        "company": company if company else None,
        "address": owner_address,
        "emails": emails,
        "emails_info": {e: {"verified": False} for e in emails},
        "primary_email": None,
        "phones": phones,
        "primary_phone": None,
    }
    return {
        "address": property_address,
        "status": status,
        "lists": lists,
        "tags": tags,
        "notes": notes,
        "owner": owner_payload,
    }


def _parse_search_address_simple(search):
    text = (search or "").strip()
    text = re.sub(r"\s*,\s*United States\s*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip()
    street = city = state = postal_code = ""

    m = re.match(r"^(.*?),\s*([^,]+),\s*([A-Za-z]{2}|[A-Za-z ]+)\s+(\d{5}(?:-\d{4})?)\s*$", text)
    if m:
        street, city, state, postal_code = m.group(1).strip(), m.group(2).strip(), m.group(3).strip(), m.group(4).strip()
    else:
        m2 = re.match(r"^(.*?)\s+([^,]+),?\s+([A-Za-z]{2})\s+(\d{5}(?:-\d{4})?)\s*$", text)
        if m2:
            street, city, state, postal_code = m2.group(1).strip(), m2.group(2).strip(), m2.group(3).strip(), m2.group(4).strip()
    if len(state) > 2:
        state_map = {
            "new jersey": "NJ",
            "new york": "NY",
            "california": "CA",
            "connecticut": "CT",
            "pennsylvania": "PA",
            "massachusetts": "MA",
            "illinois": "IL",
            "north carolina": "NC",
            "virginia": "VA",
            "texas": "TX",
            "florida": "FL",
        }
        state = state_map.get(state.strip().lower(), state[:2].upper())
    return {
        "street": street,
        "city": city,
        "state": state.upper(),
        "postal_code": postal_code,
    }


def _reisift_build_property_create_payload_from_input(input_payload):
    input_payload = input_payload or {}
    search = (input_payload.get("search") or input_payload.get("address_search") or "").strip()
    parsed = _parse_search_address_simple(search)
    property_address = {
        "street": (input_payload.get("street") or parsed["street"] or "").strip(),
        "city": (input_payload.get("city") or parsed["city"] or "").strip(),
        "state": (input_payload.get("state") or parsed["state"] or "").strip(),
        "postal_code": (input_payload.get("postal_code") or parsed["postal_code"] or "").strip(),
    }
    if not property_address["street"] or not property_address["city"]:
        raise ValueError("Could not parse property address without map lookup.")

    override_owner = input_payload.get("owner") if isinstance(input_payload.get("owner"), dict) else {}
    owner_name = _reisift_parse_owner_name(override_owner)
    first_name = (override_owner.get("first_name") or owner_name["first_name"] or "").strip() or "Unknown"
    last_name = (override_owner.get("last_name") or owner_name["last_name"] or "").strip() or "Owner"
    emails = _reisift_parse_owner_emails(override_owner)
    phones = _reisift_parse_owner_phones(override_owner)
    owner_address = {
        "street": (override_owner.get("address_street") or property_address["street"]).strip(),
        "city": (override_owner.get("address_city") or property_address["city"]).strip(),
        "state": (override_owner.get("address_state") or property_address["state"]).strip(),
        "postal_code": (override_owner.get("address_postal_code") or property_address["postal_code"]).strip(),
    }
    lists = parse_csv_list(input_payload.get("lists"))
    tags = parse_csv_list(input_payload.get("tags"))
    notes = (input_payload.get("notes") or "").strip()
    return {
        "address": property_address,
        "status": "new_lead",
        "lists": lists,
        "tags": tags,
        "notes": notes,
        "owner": {
            "first_name": first_name,
            "last_name": last_name,
            "company": override_owner.get("company"),
            "address": owner_address,
            "emails": emails,
            "emails_info": {e: {"verified": False} for e in emails},
            "primary_email": None,
            "phones": phones,
            "primary_phone": None,
        },
    }


def reisift_enrich_property_uuid(token, property_uuid):
    property_uuid = str(property_uuid or "").strip()
    if not property_uuid:
        raise ValueError("property_uuid is required for enrich.")
    enrich_payload = {
        "query": {
            "must": {
                "properties": [property_uuid],
            },
        },
        "enrich_property": True,
        "enrich_owner": True,
        "replace_owner": False,
    }
    response = requests.post(
        f"{REISIFT_BASE_URL}/api/internal/property/enrich/",
        headers=reisift_auth_headers(token),
        json=enrich_payload,
        timeout=45,
    )
    try:
        body = response.json()
    except ValueError:
        body = {"raw_text": response.text}
    if not response.ok:
        raise ValueError(f"ReiSift enrich failed ({response.status_code}): {body}")
    return {"request": enrich_payload, "response": body}


def reisift_update_property_status(token, property_uuid, status_slug):
    property_uuid = str(property_uuid or "").strip()
    status_slug = str(status_slug or "").strip()
    if not property_uuid:
        raise ValueError("property_uuid is required for status update.")
    if not status_slug:
        raise ValueError("status_slug is required for status update.")
    response = requests.post(
        f"{REISIFT_BASE_URL}/api/internal/property/{property_uuid}/status/",
        headers=reisift_auth_headers(token),
        json={"status": status_slug},
        timeout=30,
    )
    try:
        body = response.json()
    except ValueError:
        body = {"raw_text": response.text}
    if not response.ok:
        raise ValueError(f"ReiSift status update failed ({response.status_code}): {body}")
    return {"request": {"status": status_slug}, "response": body}


def reisift_upsert_owner_contacts(token, owner_uuid, phones, emails):
    owner_uuid = str(owner_uuid or "").strip()
    if not owner_uuid:
        return {"ok": False, "error": "owner_uuid missing"}
    out = {"ok": True, "owner_uuid": owner_uuid, "phones": None, "emails": None}
    normalized_phones = []
    seen_phones = set()
    for p in phones or []:
        if isinstance(p, dict):
            number = normalize_phone(p.get("number") or p.get("phone") or "")
            p_type = (p.get("type") or "UNKNOWN").strip().upper()
        else:
            number = normalize_phone(str(p or ""))
            p_type = "UNKNOWN"
        if not number or number in seen_phones:
            continue
        seen_phones.add(number)
        normalized_phones.append({"type": p_type or "UNKNOWN", "tags": [], "number": number})

    normalized_emails = []
    seen_emails = set()
    for e in emails or []:
        val = str(e or "").strip().lower()
        if not val or val in seen_emails:
            continue
        seen_emails.add(val)
        normalized_emails.append(val)

    if normalized_phones:
        phone_payload = {"phones": normalized_phones}
        phone_res = requests.post(
            f"{REISIFT_BASE_URL}/api/internal/owner/{owner_uuid}/upsert-phones/",
            headers=reisift_auth_headers(token),
            json=phone_payload,
            timeout=30,
        )
        try:
            phone_body = phone_res.json()
        except ValueError:
            phone_body = {"raw_text": phone_res.text}
        if not phone_res.ok:
            raise ValueError(f"ReiSift owner phone upsert failed ({phone_res.status_code}): {phone_body}")
        out["phones"] = {"request": phone_payload, "response": phone_body}

    if normalized_emails:
        email_payload = {"emails": normalized_emails}
        email_res = requests.post(
            f"{REISIFT_BASE_URL}/api/internal/owner/{owner_uuid}/upsert-emails/",
            headers=reisift_auth_headers(token),
            json=email_payload,
            timeout=30,
        )
        try:
            email_body = email_res.json()
        except ValueError:
            email_body = {"raw_text": email_res.text}
        if not email_res.ok:
            raise ValueError(f"ReiSift owner email upsert failed ({email_res.status_code}): {email_body}")
        out["emails"] = {"request": email_payload, "response": email_body}

    return out


def create_reisift_property_from_search(input_payload):
    search = (input_payload.get("search") or input_payload.get("address_search") or "").strip()
    if not search:
        raise ValueError("search is required.")
    token = reisift_get_access_token()

    use_map_lookup = not REISIFT_DISABLE_MAP_LOOKUP and not bool(input_payload.get("skip_map_lookup"))
    map_id = None
    autocomplete_payload = {}
    address_info_payload = {}
    if use_map_lookup:
        autocomplete_body = {
            "search": search,
            "entity_types": input_payload.get("entity_types")
            or ["state", "county", "municipality", "address", "zip"],
        }
        autocomplete = requests.post(
            f"{REISIFT_MAP_BASE_URL}/properties/search-autocomplete/",
            headers=reisift_auth_headers(token),
            json=autocomplete_body,
            timeout=30,
        )
        autocomplete.raise_for_status()
        autocomplete_payload = autocomplete.json()
        map_id = _reisift_find_first_map_id(autocomplete_payload)
        if not map_id:
            raise ValueError("Could not resolve map_id from ReiSift map autocomplete response.")

        address_info_res = requests.post(
            f"{REISIFT_BASE_URL}/api/internal/property/address-info-from-map-id/",
            headers=reisift_auth_headers(token),
            json={"map_id": map_id},
            timeout=30,
        )
        address_info_res.raise_for_status()
        address_info_payload = address_info_res.json()
        create_payload = _reisift_build_property_create_payload(address_info_payload, input_payload)
    else:
        create_payload = _reisift_build_property_create_payload_from_input(input_payload)
    create_res = requests.post(
        f"{REISIFT_BASE_URL}/api/internal/property/",
        headers=reisift_auth_headers(token),
        json=create_payload,
        timeout=45,
    )
    create_body = {}
    try:
        create_body = create_res.json()
    except ValueError:
        create_body = {"raw_text": create_res.text}
    create_res.raise_for_status()
    created_uuid = str(create_body.get("uuid") or create_body.get("id") or "").strip()
    owner_uuid = _reisift_find_owner_uuid(create_body)
    owner_payload = create_payload.get("owner") if isinstance(create_payload.get("owner"), dict) else {}
    owner_phones = _reisift_parse_owner_phones(owner_payload)
    owner_emails = _reisift_parse_owner_emails(owner_payload)
    owner_contact_sync = None
    if owner_uuid and (owner_phones or owner_emails):
        try:
            owner_contact_sync = reisift_upsert_owner_contacts(token, owner_uuid, owner_phones, owner_emails)
        except Exception as exc:
            owner_contact_sync = {"ok": False, "owner_uuid": owner_uuid, "error": str(exc)}

    # Wait 2-5 seconds before enrich to allow create propagation.
    enrich_result = None
    enrich_wait_seconds = random.randint(2, 5)
    if created_uuid:
        time.sleep(enrich_wait_seconds)
        enrich_result = reisift_enrich_property_uuid(token, created_uuid)

    return {
        "search": search,
        "map_id": map_id,
        "create_payload": create_payload,
        "created": create_body,
        "created_uuid": created_uuid,
        "owner_uuid": owner_uuid,
        "owner_contact_sync": owner_contact_sync,
        "enrich_wait_seconds": enrich_wait_seconds if created_uuid else 0,
        "enrich": enrich_result,
        "autocomplete": autocomplete_payload,
        "address_info": address_info_payload,
    }


def fetch_reisift_referrals():
    status_slug = os.getenv("REISIFT_REFERRAL_STATUS", "refer_lead").strip() or "refer_lead"
    token = reisift_get_access_token()
    rows, total = reisift_search_referral_rows(token, status_slug)

    results = []
    for row in rows:
        address = row.get("address") or {}
        owners = row.get("owners") or []
        owner_names = []
        for owner in owners:
            full = (owner.get("full_name") or "").strip()
            if full:
                owner_names.append(full)
                continue
            first = (owner.get("first_name") or "").strip()
            last = (owner.get("last_name") or "").strip()
            combined = " ".join(x for x in [first, last] if x)
            if combined:
                owner_names.append(combined)
        full_address = (
            (address.get("full_address") or "").strip()
            or ", ".join(
                x
                for x in [
                    address.get("street"),
                    address.get("city"),
                    address.get("state"),
                    address.get("zip"),
                ]
                if x
            )
        )
        results.append(
            {
                "uuid": row.get("uuid"),
                "status": row.get("status"),
                "full_address": full_address,
                "owners": owner_names,
            }
        )
    return {"count": total or len(results), "status_slug": status_slug, "results": results}


def reisift_search_referral_rows(token, status_slug):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "x-reisift-ui-version": REISIFT_UI_VERSION,
        "x-http-method-override": "GET",
    }
    body = {
        "limit": 200,
        "offset": 0,
        "ordering": "-list_count",
        "query": {"must": {"any_property_status": [status_slug]}},
    }
    response = requests.post(
        f"{REISIFT_BASE_URL}/api/internal/property/",
        headers=headers,
        json=body,
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    rows = payload.get("results") or []
    return rows, int(payload.get("count") or len(rows))


def fetch_reisift_property_payload(token, property_uuid):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "x-reisift-ui-version": REISIFT_UI_VERSION,
    }
    response = requests.get(
        f"{REISIFT_BASE_URL}/api/internal/property/{property_uuid}/",
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def fetch_reisift_property_log_rollup(token, property_uuid, max_rows=200):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "x-reisift-ui-version": REISIFT_UI_VERSION,
    }
    offset = 0
    limit = 100
    inspected = 0
    outbound_calls = 0
    inbound_calls = 0
    outbound_sms = 0
    inbound_sms = 0
    outbound_email = 0
    inbound_email = 0
    seen_call_ids = set()
    seen_sms_ids = set()
    seen_email_ids = set()
    events = []
    tasks = []
    seen_task_ids = set()
    property_created_at = None

    while inspected < max_rows:
        response = requests.get(
            f"{REISIFT_BASE_URL}/api/internal/property/{property_uuid}/logs/?offset={offset}&limit={limit}",
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json() if response.headers.get("content-type", "").lower().startswith("application/json") else {}
        results = payload.get("results") if isinstance(payload, dict) else []
        if not isinstance(results, list) or not results:
            break

        for item in results:
            if not isinstance(item, dict):
                continue
            event_type = (item.get("event_type") or "").strip().lower()
            if not event_type:
                continue
            if event_type == "property.created" and not property_created_at:
                property_created_at = item.get("timestamp")
            if len(events) < 50:
                events.append(
                    {
                        "event_type": item.get("event_type"),
                        "timestamp": item.get("timestamp"),
                        "source": item.get("source"),
                        "title": (item.get("payload") or {}).get("task", {}).get("title") if isinstance(item.get("payload"), dict) else "",
                    }
                )
            if event_type.startswith("task."):
                payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
                task = payload.get("task") if isinstance(payload.get("task"), dict) else {}
                task_uuid = (task.get("uuid") or "").strip() or f"evt:{item.get('timestamp')}:{event_type}"
                if task_uuid not in seen_task_ids:
                    seen_task_ids.add(task_uuid)
                    created_by = task.get("created_by") if isinstance(task.get("created_by"), dict) else {}
                    creator_name = " ".join(
                        x
                        for x in [
                            (created_by.get("first_name") or "").strip(),
                            (created_by.get("last_name") or "").strip(),
                        ]
                        if x
                    )
                    if not creator_name:
                        author_extra = item.get("author_extra_info") if isinstance(item.get("author_extra_info"), dict) else {}
                        creator_name = " ".join(
                            x
                            for x in [
                                (author_extra.get("first_name") or "").strip(),
                                (author_extra.get("last_name") or "").strip(),
                            ]
                            if x
                        ) or (item.get("author") or "")
                    tasks.append(
                        {
                            "event_type": "task",
                            "title": (task.get("title") or "").strip() or "-",
                            "due_date": task.get("due_date"),
                            "created_by_name": creator_name or "-",
                        }
                    )
            payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}

            if event_type.startswith("owner.call."):
                call = payload.get("call") if isinstance(payload.get("call"), dict) else {}
                call_id = (call.get("uuid") or "").strip() or f"evt:{item.get('timestamp')}:{event_type}"
                if call_id in seen_call_ids:
                    continue
                direction = (call.get("direction") or "").strip().lower()
                if not direction:
                    if event_type.endswith(".incoming") or event_type.endswith(".received"):
                        direction = "inbound"
                    else:
                        direction = "outbound"
                seen_call_ids.add(call_id)
                if direction == "inbound":
                    inbound_calls += 1
                else:
                    outbound_calls += 1
                continue

            if event_type.startswith("owner.sms."):
                sms = payload.get("sms") if isinstance(payload.get("sms"), dict) else {}
                sms_id = (sms.get("uuid") or "").strip() or f"evt:{item.get('timestamp')}:{event_type}"
                if sms_id in seen_sms_ids:
                    continue
                direction = (sms.get("direction") or "").strip().lower()
                if not direction:
                    if event_type.endswith(".received") or event_type.endswith(".replied"):
                        direction = "inbound"
                    else:
                        direction = "outbound"
                seen_sms_ids.add(sms_id)
                if direction == "inbound":
                    inbound_sms += 1
                else:
                    outbound_sms += 1
                continue

            if event_type.startswith("owner.email."):
                email = payload.get("email") if isinstance(payload.get("email"), dict) else {}
                email_id = (email.get("uuid") or "").strip() or f"evt:{item.get('timestamp')}:{event_type}"
                if email_id in seen_email_ids:
                    continue
                direction = (email.get("direction") or "").strip().lower()
                if not direction:
                    if event_type.endswith(".received") or event_type.endswith(".replied"):
                        direction = "inbound"
                    else:
                        direction = "outbound"
                seen_email_ids.add(email_id)
                if direction == "inbound":
                    inbound_email += 1
                else:
                    outbound_email += 1

        inspected += len(results)
        if len(results) < limit:
            break
        offset += limit

    return {
        "outbound_calls": outbound_calls,
        "outbound_sms": outbound_sms,
        "outbound_email": outbound_email,
        "inbound_calls": inbound_calls,
        "inbound_sms": inbound_sms,
        "inbound_email": inbound_email,
        "inbound_responses": inbound_calls + inbound_sms + inbound_email,
        "property_created_at": property_created_at,
        "events": events,
        "tasks": tasks,
    }


def summarize_reisift_property(payload):
    address = payload.get("address") or {}
    full_address = ", ".join(
        x
        for x in [
            address.get("street"),
            address.get("city"),
            address.get("state"),
            address.get("postal_code") or address.get("zip"),
        ]
        if x
    )
    owner_names = []
    owner = payload.get("owner") or {}
    if owner:
        owner_full = " ".join(
            x for x in [(owner.get("first_name") or "").strip(), (owner.get("last_name") or "").strip()] if x
        )
        if owner_full:
            owner_names.append(owner_full)
    for o in payload.get("owners") or []:
        full = (o.get("full_name") or "").strip()
        if full:
            owner_names.append(full)
            continue
        combined = " ".join(
            x for x in [(o.get("first_name") or "").strip(), (o.get("last_name") or "").strip()] if x
        )
        if combined:
            owner_names.append(combined)
    return {
        "status": payload.get("status"),
        "full_address": full_address,
        "owner_names": ", ".join(dict.fromkeys(owner_names)),
    }


def infer_county_from_reisift_payload(payload):
    if not isinstance(payload, dict):
        return ""
    address = payload.get("address") if isinstance(payload.get("address"), dict) else {}
    county = (address.get("county") or address.get("county_name") or "").strip()
    if county:
        return county
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    county = (metadata.get("county") or metadata.get("county_name") or "").strip()
    if county:
        return county
    return ""


def infer_on_market_status_from_payload(payload):
    if not isinstance(payload, dict):
        return "Unknown"
    for key in ("listed_on_market", "listedOnMarket", "on_market", "onMarket"):
        val = payload.get(key)
        if isinstance(val, bool):
            return "On Market" if val else "Off Market"
        if isinstance(val, str) and val.strip():
            text = val.strip().lower()
            if text in {"yes", "true", "listed", "on market", "active"}:
                return "On Market"
            if text in {"no", "false", "off market", "not listed"}:
                return "Off Market"
    notes = str(payload.get("notes") or "")
    if "Listed On Market: Yes" in notes:
        return "On Market"
    if "Listed On Market: No" in notes:
        return "Off Market"
    return "Unknown"


def upsert_reisift_referral(db, property_uuid, payload, is_active=1):
    summary = summarize_reisift_property(payload)
    county = infer_county_from_reisift_payload(payload)
    on_market_status = infer_on_market_status_from_payload(payload)
    db.execute(
        """
        INSERT INTO reisift_referrals
            (property_uuid, status, full_address, owner_names, county, on_market_status, payload_json, is_active, last_synced_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(property_uuid) DO UPDATE SET
            status = excluded.status,
            full_address = excluded.full_address,
            owner_names = excluded.owner_names,
            county = excluded.county,
            on_market_status = excluded.on_market_status,
            payload_json = excluded.payload_json,
            is_active = excluded.is_active,
            last_synced_at = excluded.last_synced_at
        """,
        (
            property_uuid,
            summary["status"],
            summary["full_address"],
            summary["owner_names"],
            county,
            on_market_status,
            json.dumps(payload),
            int(bool(is_active)),
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        ),
    )


def queue_referral_auto_send_candidates(db):
    watermark_raw = (get_setting(db, "referral_auto_send_last_referral_id", "0") or "0").strip()
    try:
        watermark = int(watermark_raw)
    except (TypeError, ValueError):
        watermark = 0

    # Initialize watermark to current max to avoid backfilling historical referrals.
    if watermark <= 0:
        current_max_row = db.execute("SELECT COALESCE(MAX(id), 0) AS max_id FROM reisift_referrals").fetchone()
        current_max_id = int((current_max_row["max_id"] if current_max_row else 0) or 0)
        set_setting(db, "referral_auto_send_last_referral_id", str(current_max_id))
        return {"queued": 0, "no_realtor": 0, "initialized_watermark": current_max_id}

    rows = db.execute(
        """
        SELECT id, property_uuid, full_address, county, owner_names
        FROM reisift_referrals
        WHERE is_active = 1
          AND id > ?
          AND lower(COALESCE(referral_status, 'Untouched')) = 'untouched'
        ORDER BY id ASC
        """
        ,
        (watermark,),
    ).fetchall()
    queued = 0
    no_realtor = 0
    max_seen_id = watermark
    for row in rows:
        row_id = int((row["id"] or 0))
        if row_id > max_seen_id:
            max_seen_id = row_id
        property_uuid = (row["property_uuid"] or "").strip()
        county = (row["county"] or "").strip()
        if not property_uuid:
            continue
        existing = db.execute(
            "SELECT id FROM referral_auto_send_queue WHERE property_uuid = ? LIMIT 1",
            (property_uuid,),
        ).fetchone()
        if existing:
            continue
        if not county:
            db.execute(
                """
                INSERT INTO referral_auto_send_queue (property_uuid, realtor_id, county, queue_status, note, payload_json)
                VALUES (?, NULL, ?, 'No Realtor', ?, ?)
                """,
                (
                    property_uuid,
                    county,
                    "County missing on referral; waiting for manual routing.",
                    json.dumps({"full_address": row["full_address"], "owner_names": row["owner_names"]}),
                ),
            )
            no_realtor += 1
            continue

        realtors = db.execute(
            """
            SELECT id, first_name, last_name
            FROM referral_realtors
            WHERE lower(target_markets) LIKE ?
            ORDER BY id ASC
            """,
            (f"%{county.lower()}%",),
        ).fetchall()
        if not realtors:
            db.execute(
                """
                INSERT INTO referral_auto_send_queue (property_uuid, realtor_id, county, queue_status, note, payload_json)
                VALUES (?, NULL, ?, 'No Realtor', ?, ?)
                """,
                (
                    property_uuid,
                    county,
                    "No realtor currently tagged for this county.",
                    json.dumps({"full_address": row["full_address"], "owner_names": row["owner_names"]}),
                ),
            )
            no_realtor += 1
            continue
        for realtor in realtors:
            db.execute(
                """
                INSERT INTO referral_auto_send_queue (property_uuid, realtor_id, county, queue_status, note, payload_json)
                VALUES (?, ?, ?, 'Queued', ?, ?)
                """,
                (
                    property_uuid,
                    realtor["id"],
                    county,
                    "Queued for review before auto-send.",
                    json.dumps({"full_address": row["full_address"], "owner_names": row["owner_names"]}),
                ),
            )
            queued += 1
    if max_seen_id > watermark:
        set_setting(db, "referral_auto_send_last_referral_id", str(max_seen_id))
    return {"queued": queued, "no_realtor": no_realtor}


def refresh_reisift_referrals_cache(db):
    status_slug = os.getenv("REISIFT_REFERRAL_STATUS", "refer_lead").strip() or "refer_lead"
    token = reisift_get_access_token()
    rows, total = reisift_search_referral_rows(token, status_slug)

    db.execute("UPDATE reisift_referrals SET is_active = 0")
    synced = 0
    errors = []
    for row in rows:
        property_uuid = (row.get("uuid") or "").strip()
        if not property_uuid:
            continue
        try:
            details = fetch_reisift_property_payload(token, property_uuid)
            upsert_reisift_referral(db, property_uuid, details, is_active=1)
            synced += 1
        except Exception as exc:
            errors.append(f"{property_uuid}: {exc}")
            # Fallback to row payload when detail call fails.
            upsert_reisift_referral(db, property_uuid, row, is_active=1)
            synced += 1

    queue_summary = queue_referral_auto_send_candidates(db)
    db.commit()
    return {
        "status_slug": status_slug,
        "total": total or len(rows),
        "synced": synced,
        "errors": errors,
        "queue_summary": queue_summary,
    }


def get_cached_referrals(db):
    rows = db.execute(
        """
        SELECT r.property_uuid, r.status, r.full_address, r.owner_names, r.payload_json, r.last_synced_at,
               r.county, COALESCE(r.on_market_status, 'Unknown') AS on_market_status,
               COALESCE(r.referral_status, 'Untouched') AS referral_status,
               COALESCE(a.push_count, 0) AS push_count
        FROM reisift_referrals r
        LEFT JOIN (
            SELECT property_uuid, COUNT(*) AS push_count
            FROM referral_push_activity
            GROUP BY property_uuid
        ) a ON a.property_uuid = r.property_uuid
        WHERE r.is_active = 1
        ORDER BY r.last_synced_at DESC, r.id DESC
        """
    ).fetchall()
    return rows


def parse_iso_datetime(value):
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    direct = parse_db_time(text)
    if direct is not None:
        return direct
    try:
        normalized = text.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except ValueError:
        return None


def reisift_added_at_dt(search_row, detail_payload):
    candidates = []
    if isinstance(search_row, dict):
        candidates.extend([search_row.get("created"), search_row.get("created_at")])
    if isinstance(detail_payload, dict):
        candidates.extend([detail_payload.get("created"), detail_payload.get("created_at")])
    for item in candidates:
        parsed = parse_iso_datetime(item)
        if parsed is not None:
            return parsed
    return None


def find_local_property_id_for_reisift_payload(db, payload):
    address = payload.get("address") if isinstance(payload, dict) else {}
    if not isinstance(address, dict):
        return None
    street = (address.get("street") or "").strip().lower()
    city = (address.get("city") or "").strip().lower()
    state = (address.get("state") or "").strip().lower()
    postal = (address.get("postal_code") or address.get("zip") or "").strip().lower()
    if not street or not city:
        return None
    row = db.execute(
        """
        SELECT p.id
        FROM properties p
        JOIN addresses a ON a.id = p.property_address_id
        WHERE lower(a.street) = ?
          AND lower(a.city) = ?
          AND lower(a.state) = ?
          AND lower(a.postal_code) = ?
        ORDER BY p.id DESC
        LIMIT 1
        """,
        (street, city, state, postal),
    ).fetchone()
    return row["id"] if row else None


def get_local_property_activity_rollup(db, property_id):
    if not property_id:
        return {
            "outbound_calls": 0,
            "outbound_sms": 0,
            "outbound_email": 0,
            "inbound_responses": 0,
        }
    outbound_sms = db.execute(
        """
        SELECT COUNT(*) AS c
        FROM communications
        WHERE property_id = ?
          AND upper(channel) = 'SMS'
          AND lower(direction) = 'outbound'
        """,
        (property_id,),
    ).fetchone()["c"]
    outbound_email = db.execute(
        """
        SELECT COUNT(*) AS c
        FROM communications
        WHERE property_id = ?
          AND upper(channel) = 'EMAIL'
          AND lower(direction) = 'outbound'
        """,
        (property_id,),
    ).fetchone()["c"]
    inbound_responses = db.execute(
        """
        SELECT COUNT(*) AS c
        FROM communications
        WHERE property_id = ?
          AND lower(direction) = 'inbound'
          AND upper(channel) IN ('SMS', 'EMAIL')
        """,
        (property_id,),
    ).fetchone()["c"]
    outbound_calls = db.execute(
        """
        SELECT COUNT(*) AS c
        FROM activity_log
        WHERE property_id = ?
          AND lower(activity_type) LIKE 'call%'
        """,
        (property_id,),
    ).fetchone()["c"]
    return {
        "outbound_calls": int(outbound_calls or 0),
        "outbound_sms": int(outbound_sms or 0),
        "outbound_email": int(outbound_email or 0),
        "inbound_responses": int(inbound_responses or 0),
    }


def upsert_reisift_followup(
    db,
    property_uuid,
    payload,
    added_at,
    outbound_calls,
    outbound_sms,
    outbound_email,
    inbound_responses,
    events,
    tasks,
    is_active=1,
):
    summary = summarize_reisift_property(payload)
    execute_with_retry(
        db,
        """
        INSERT INTO reisift_followups
            (property_uuid, status, full_address, owner_names, added_at, outbound_calls, outbound_sms, outbound_email, inbound_responses, events_json, tasks_json, payload_json, is_active, last_synced_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(property_uuid) DO UPDATE SET
            status = excluded.status,
            full_address = excluded.full_address,
            owner_names = excluded.owner_names,
            added_at = excluded.added_at,
            outbound_calls = excluded.outbound_calls,
            outbound_sms = excluded.outbound_sms,
            outbound_email = excluded.outbound_email,
            inbound_responses = excluded.inbound_responses,
            events_json = excluded.events_json,
            tasks_json = excluded.tasks_json,
            payload_json = excluded.payload_json,
            is_active = excluded.is_active,
            last_synced_at = excluded.last_synced_at
        """,
        (
            property_uuid,
            summary["status"],
            summary["full_address"],
            summary["owner_names"],
            format_db_time(added_at) if added_at else None,
            int(outbound_calls or 0),
            int(outbound_sms or 0),
            int(outbound_email or 0),
            int(inbound_responses or 0),
            json.dumps(events or []),
            json.dumps(tasks or []),
            json.dumps(payload),
            int(bool(is_active)),
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        ),
    )


def build_followups_owner_updated_range(lookback_days):
    token = reisift_get_access_token()
    now_est = datetime.now(EST_TZ)
    days = max(1, min(90, int(lookback_days or 7)))
    start_local = (now_est - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = now_est.replace(hour=23, minute=59, second=59, microsecond=999000)
    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)
    owner_updated_range = [
        start_utc.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        end_utc.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
    ]
    return token, days, owner_updated_range


def refresh_reisift_followups_search_cache(db, lookback_days=7):
    token, days, owner_updated_range = build_followups_owner_updated_range(lookback_days)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "x-reisift-ui-version": REISIFT_UI_VERSION,
        "x-http-method-override": "GET",
    }
    offset = 0
    page_size = 200
    synced = 0
    scanned = 0
    errors = []
    # Hard reset cache first so only the current retrieve remains in the table.
    execute_with_retry(db, "DELETE FROM reisift_followups")
    commit_with_retry(db)
    while offset < 1000:
        body = {
            "limit": page_size,
            "offset": offset,
            "ordering": "-created",
            "query": {
                "must": {
                    "property_type": "clean",
                    "owner_updated": owner_updated_range,
                    "must_not": {
                        "all_tags": [REISIFT_FOLLOWUPS_EXCLUDE_TAG],
                    },
                },
            },
        }
        response = requests.post(
            f"{REISIFT_BASE_URL}/api/internal/property/",
            headers=headers,
            json=body,
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        rows = payload.get("results") or []
        if not rows:
            break
        for row in rows:
            scanned += 1
            property_uuid = (row.get("uuid") or "").strip()
            if not property_uuid:
                continue
            added_dt = reisift_added_at_dt(row, row)
            upsert_reisift_followup(
                db=db,
                property_uuid=property_uuid,
                payload=row,
                added_at=added_dt,
                outbound_calls=0,
                outbound_sms=0,
                outbound_email=0,
                inbound_responses=0,
                events=[],
                tasks=[],
                is_active=1,
            )
            synced += 1
        if len(rows) < page_size:
            break
        offset += page_size
    commit_with_retry(db)
    return {
        "synced": synced,
        "scanned": scanned,
        "errors": errors,
        "lookback_days": days,
        "owner_updated_range": owner_updated_range,
    }


def enrich_reisift_followups_cache(db):
    token = reisift_get_access_token()
    rows = db.execute(
        "SELECT property_uuid, added_at, payload_json FROM reisift_followups WHERE is_active = 1 ORDER BY id DESC"
    ).fetchall()
    updated = 0
    errors = []
    for row in rows:
        property_uuid = (row["property_uuid"] or "").strip()
        if not property_uuid:
            continue
        details = {}
        try:
            details = fetch_reisift_property_payload(token, property_uuid)
        except Exception as exc:
            errors.append(f"{property_uuid}: {exc}")
            try:
                details = json.loads(row["payload_json"] or "{}")
            except Exception:
                details = {}
        sift_rollup = {
            "outbound_calls": 0,
            "outbound_sms": 0,
            "outbound_email": 0,
            "inbound_responses": 0,
            "events": [],
            "tasks": [],
            "property_created_at": None,
        }
        try:
            sift_rollup = fetch_reisift_property_log_rollup(token, property_uuid)
        except Exception as exc:
            errors.append(f"{property_uuid} logs: {exc}")

        added_dt = parse_flexible_datetime(sift_rollup.get("property_created_at"))
        if added_dt is None:
            added_dt = parse_flexible_datetime(row["added_at"])
        local_property_id = find_local_property_id_for_reisift_payload(db, details)
        local_rollup = get_local_property_activity_rollup(db, local_property_id)
        outbound_calls = max(local_rollup["outbound_calls"], sift_rollup["outbound_calls"])
        outbound_sms = max(local_rollup["outbound_sms"], sift_rollup["outbound_sms"])
        outbound_email = max(local_rollup["outbound_email"], sift_rollup["outbound_email"])
        inbound_responses = max(local_rollup["inbound_responses"], sift_rollup["inbound_responses"])
        upsert_reisift_followup(
            db=db,
            property_uuid=property_uuid,
            payload=details,
            added_at=added_dt,
            outbound_calls=outbound_calls,
            outbound_sms=outbound_sms,
            outbound_email=outbound_email,
            inbound_responses=inbound_responses,
            events=sift_rollup.get("events") or [],
            tasks=sift_rollup.get("tasks") or [],
            is_active=1,
        )
        updated += 1
    commit_with_retry(db)
    return {"updated": updated, "errors": errors}


def get_cached_followups(db, sort_dir="desc"):
    sort_sql = "DESC" if str(sort_dir).lower() != "asc" else "ASC"
    return db.execute(
        f"""
        SELECT f.*,
               CASE WHEN r.property_uuid IS NULL THEN 0 ELSE 1 END AS has_referral_cache
        FROM reisift_followups f
        LEFT JOIN reisift_referrals r ON r.property_uuid = f.property_uuid
        WHERE f.is_active = 1
        ORDER BY
            CASE WHEN f.added_at IS NULL OR trim(f.added_at) = '' THEN 1 ELSE 0 END {sort_sql},
            f.added_at {sort_sql},
            f.last_synced_at {sort_sql},
            f.id {sort_sql}
        """
    ).fetchall()


def parse_followup_json_list(raw_value):
    if not raw_value:
        return []
    try:
        data = json.loads(raw_value)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def extract_owner_contacts_from_payload(payload):
    owner = payload.get("owner") if isinstance(payload, dict) else {}
    if not isinstance(owner, dict):
        return [], []

    phones = []
    seen_phones = set()
    for p in owner.get("phones") or []:
        value = ""
        p_type = "Unknown"
        p_status = "Unknown"
        if isinstance(p, dict):
            value = (p.get("number") or p.get("phone") or "").strip()
            p_type = (p.get("type") or "Unknown").strip() or "Unknown"
            p_status = (p.get("status") or "Unknown").strip() or "Unknown"
        else:
            value = str(p or "").strip()
        key = normalize_phone(value) or value
        if not value or key in seen_phones:
            continue
        seen_phones.add(key)
        phones.append({"value": value, "type": p_type, "status": p_status})

    emails = []
    seen_emails = set()
    for e in owner.get("emails") or []:
        value = ""
        status = "Unknown"
        if isinstance(e, dict):
            value = (e.get("email") or "").strip()
            status = (e.get("status") or "Unknown").strip() or "Unknown"
        else:
            value = str(e or "").strip()
        key = value.lower()
        if not value or key in seen_emails:
            continue
        seen_emails.add(key)
        emails.append({"value": value, "status": status})
    return phones, emails


def extract_property_lists(payload):
    lists = payload.get("lists") if isinstance(payload, dict) else []
    if not isinstance(lists, list):
        return ""
    names = []
    for item in lists:
        if isinstance(item, dict):
            val = (item.get("title") or item.get("name") or "").strip()
        else:
            val = str(item or "").strip()
        if val:
            names.append(val)
    dedup = list(dict.fromkeys(names))
    return ", ".join(dedup)


def extract_payload_address(payload):
    address = payload.get("address") if isinstance(payload, dict) else {}
    if not isinstance(address, dict):
        return {"street": "", "city": "", "state": "", "postal_code": ""}
    return {
        "street": (address.get("street") or "").strip(),
        "city": (address.get("city") or "").strip(),
        "state": (address.get("state") or "").strip(),
        "postal_code": (address.get("postal_code") or address.get("zip") or "").strip(),
    }


def find_local_property_by_address(db, street, city, state, postal_code):
    if not street or not city:
        return None
    row = db.execute(
        """
        SELECT p.id
        FROM properties p
        JOIN addresses a ON a.id = p.property_address_id
        WHERE lower(a.street) = lower(?)
          AND lower(a.city) = lower(?)
          AND lower(a.state) = lower(?)
          AND lower(a.postal_code) = lower(?)
        ORDER BY p.id DESC
        LIMIT 1
        """,
        (street, city, state, postal_code),
    ).fetchone()
    return row["id"] if row else None


def build_market_status_helper(address_text):
    address = " ".join((address_text or "").strip().split())
    if not address:
        return {"query": "", "links": []}
    encoded = quote_plus(address)
    return {
        "query": address,
        "links": [
            {
                "label": "Zillow Search",
                "url": f"https://www.google.com/search?q=site%3Azillow.com+{encoded}",
            },
            {
                "label": "Realtor.com Search",
                "url": f"https://www.google.com/search?q=site%3Arealtor.com+{encoded}",
            },
            {
                "label": "Redfin Search",
                "url": f"https://www.google.com/search?q=site%3Aredfin.com+{encoded}",
            },
            {
                "label": "General Search",
                "url": f"https://www.google.com/search?q={encoded}",
            },
        ],
    }


@app.route("/healthz")
def healthz():
    return jsonify({"ok": True}), 200


@app.route("/login", methods=["GET", "POST"])
def login():
    if not APP_AUTH_ENABLED:
        return redirect(url_for("dashboard"))
    next_target = (request.args.get("next") or request.form.get("next") or "").strip()
    error = ""
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        if verify_app_login(username, password):
            session["auth_ok"] = True
            session["auth_user"] = username
            if is_safe_next_path(next_target):
                return redirect(next_target)
            return redirect(url_for("dashboard"))
        error = "Invalid username or password."
    config_warning = ""
    if not app_auth_is_configured():
        config_warning = (
            "APP_AUTH is enabled, but credentials are not configured. "
            "Set APP_AUTH_USERNAME and APP_AUTH_PASSWORD or APP_AUTH_PASSWORD_HASH."
        )
    return render_template("login.html", error=error, next_target=next_target, config_warning=config_warning)


@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    if APP_AUTH_ENABLED:
        return redirect(url_for("login"))
    return redirect(url_for("dashboard"))


def communication_counts_for_people(db, person_ids, property_id=None):
    clean_ids = []
    seen = set()
    for pid in person_ids or []:
        try:
            val = int(pid)
        except Exception:
            continue
        if val <= 0 or val in seen:
            continue
        seen.add(val)
        clean_ids.append(val)
    if not clean_ids:
        return {}
    placeholders = ",".join(["?"] * len(clean_ids))
    where = [f"person_id IN ({placeholders})"]
    params = list(clean_ids)
    if property_id is not None:
        where.append("property_id = ?")
        params.append(int(property_id))
    rows = db.execute(
        f"""
        SELECT person_id,
               SUM(CASE WHEN upper(channel)='SMS' AND lower(direction)='outbound' THEN 1 ELSE 0 END) AS sms_outbound,
               SUM(CASE WHEN upper(channel)='SMS' AND lower(direction)='inbound' THEN 1 ELSE 0 END) AS sms_inbound,
               SUM(CASE WHEN upper(channel)='EMAIL' AND lower(direction)='outbound' THEN 1 ELSE 0 END) AS email_outbound,
               SUM(CASE WHEN upper(channel)='EMAIL' AND lower(direction)='inbound' THEN 1 ELSE 0 END) AS email_inbound
        FROM communications
        WHERE {' AND '.join(where)}
        GROUP BY person_id
        """,
        tuple(params),
    ).fetchall()
    out = {}
    for r in rows:
        out[int(r["person_id"])] = {
            "sms_outbound": int(r["sms_outbound"] or 0),
            "sms_inbound": int(r["sms_inbound"] or 0),
            "email_outbound": int(r["email_outbound"] or 0),
            "email_inbound": int(r["email_inbound"] or 0),
        }
    return out


def _upsert_lead_action(db, property_id, person_id, action_type, priority, reason, payload):
    dedupe_key = f"{int(property_id)}:{int(person_id or 0)}:{action_type}"
    existing = db.execute(
        "SELECT id, status FROM lead_management_actions WHERE dedupe_key = ?",
        (dedupe_key,),
    ).fetchone()
    payload_json = json.dumps(payload or {})
    if existing:
        status = (existing["status"] or "Pending").strip()
        if status in {"Completed", "Dismissed"}:
            return False
        db.execute(
            """
            UPDATE lead_management_actions
            SET priority = ?, reason = ?, payload_json = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (priority, reason, payload_json, existing["id"]),
        )
        return False
    db.execute(
        """
        INSERT INTO lead_management_actions
        (dedupe_key, property_id, person_id, action_type, priority, status, reason, payload_json)
        VALUES (?, ?, ?, ?, ?, 'Pending', ?, ?)
        """,
        (dedupe_key, property_id, person_id, action_type, priority, reason, payload_json),
    )
    return True


def _property_comm_snapshot(db, property_id):
    row = db.execute(
        """
        SELECT
            SUM(CASE WHEN lower(direction) = 'outbound' THEN 1 ELSE 0 END) AS outbound_total,
            SUM(CASE WHEN lower(direction) = 'inbound' THEN 1 ELSE 0 END) AS inbound_total,
            SUM(CASE WHEN upper(channel) = 'SMS' AND lower(direction) = 'outbound' THEN 1 ELSE 0 END) AS sms_outbound,
            SUM(CASE WHEN upper(channel) = 'SMS' AND lower(direction) = 'inbound' THEN 1 ELSE 0 END) AS sms_inbound,
            SUM(CASE WHEN upper(channel) = 'EMAIL' AND lower(direction) = 'outbound' THEN 1 ELSE 0 END) AS email_outbound,
            SUM(CASE WHEN upper(channel) = 'EMAIL' AND lower(direction) = 'inbound' THEN 1 ELSE 0 END) AS email_inbound,
            MAX(CASE WHEN lower(direction) = 'outbound' THEN COALESCE(sent_at, created_at) END) AS last_outbound_at,
            MAX(CASE WHEN lower(direction) = 'inbound' THEN COALESCE(sent_at, created_at) END) AS last_inbound_at
        FROM communications
        WHERE property_id = ?
        """,
        (property_id,),
    ).fetchone()
    latest_inbound = db.execute(
        """
        SELECT lower(COALESCE(body, '')) AS body
        FROM communications
        WHERE property_id = ? AND lower(direction) = 'inbound'
        ORDER BY COALESCE(sent_at, created_at) DESC, id DESC
        LIMIT 1
        """,
        (property_id,),
    ).fetchone()
    return {
        "outbound_total": int((row["outbound_total"] if row else 0) or 0),
        "inbound_total": int((row["inbound_total"] if row else 0) or 0),
        "sms_outbound": int((row["sms_outbound"] if row else 0) or 0),
        "sms_inbound": int((row["sms_inbound"] if row else 0) or 0),
        "email_outbound": int((row["email_outbound"] if row else 0) or 0),
        "email_inbound": int((row["email_inbound"] if row else 0) or 0),
        "last_outbound_at": (row["last_outbound_at"] if row else "") or "",
        "last_inbound_at": (row["last_inbound_at"] if row else "") or "",
        "latest_inbound_body": (latest_inbound["body"] if latest_inbound else "") or "",
    }


def _owner_relative_contact_count(db, owner_person_id):
    if not owner_person_id:
        return 0
    row = db.execute(
        """
        SELECT COUNT(*) AS c
        FROM person_relationships pr
        JOIN touchpoints t ON t.person_id = pr.related_person_id
        WHERE pr.subject_person_id = ?
          AND pr.related_person_id != pr.subject_person_id
          AND lower(COALESCE(t.channel_type, '')) IN ('phone', 'email')
          AND trim(COALESCE(t.value, '')) != ''
        """,
        (owner_person_id,),
    ).fetchone()
    return int((row["c"] if row else 0) or 0)


def evaluate_property_lead_management(db, property_row):
    property_id = int(property_row["id"])
    owner_person_id = property_row["owner_person_id"]
    prop_status = str(property_row["status"] or "").strip().lower()
    snapshot = _property_comm_snapshot(db, property_id)
    relative_contact_count = _owner_relative_contact_count(db, owner_person_id)
    created_actions = 0
    skipped = False

    if prop_status in LEAD_STOP_STATUSES:
        return {"property_id": property_id, "skipped": True, "created_actions": 0, "snapshot": snapshot}

    latest_inbound = snapshot["latest_inbound_body"]
    has_negative_intent = any(token in latest_inbound for token in LEAD_NEGATIVE_INTENT_TOKENS)
    if has_negative_intent:
        created_actions += int(
            _upsert_lead_action(
                db,
                property_id,
                owner_person_id,
                "HumanReviewDisposition",
                "High",
                "Inbound response appears negative and requires a human decision before additional outreach.",
                {"snapshot": snapshot, "reason": "negative_intent_detected"},
            )
        )
        return {"property_id": property_id, "skipped": skipped, "created_actions": created_actions, "snapshot": snapshot}

    if snapshot["outbound_total"] >= 3 and snapshot["inbound_total"] == 0:
        if relative_contact_count > 0:
            created_actions += int(
                _upsert_lead_action(
                    db,
                    property_id,
                    owner_person_id,
                    "EscalateToRelativeOutreach",
                    "High",
                    "No inbound responses after 3+ touches. Escalate outreach to known relatives/associates.",
                    {"snapshot": snapshot, "relative_contact_count": relative_contact_count},
                )
            )
        else:
            created_actions += int(
                _upsert_lead_action(
                    db,
                    property_id,
                    owner_person_id,
                    "EscalateToDeepDiveResearch",
                    "High",
                    "No inbound responses after 3+ touches and no relative contact channels on file.",
                    {"snapshot": snapshot, "relative_contact_count": relative_contact_count},
                )
            )
    elif snapshot["outbound_total"] >= 1 and snapshot["inbound_total"] == 0:
        created_actions += int(
            _upsert_lead_action(
                db,
                property_id,
                owner_person_id,
                "FollowUpTouchDue",
                "Medium",
                "Lead has outreach activity but no inbound response yet. Schedule next compliant touchpoint.",
                {"snapshot": snapshot},
            )
        )

    if snapshot["inbound_total"] > 0 and snapshot["outbound_total"] > 0:
        created_actions += int(
            _upsert_lead_action(
                db,
                property_id,
                owner_person_id,
                "ReviewInboundAndNextBestAction",
                "Medium",
                "Inbound activity exists. Confirm sentiment and set next best action manually.",
                {"snapshot": snapshot},
            )
        )

    return {"property_id": property_id, "skipped": skipped, "created_actions": created_actions, "snapshot": snapshot}


def run_lead_monitor_once(db, source="manual", property_id=None):
    run_cur = db.execute(
        """
        INSERT INTO lead_monitor_runs (source, property_id, status, summary_json)
        VALUES (?, ?, 'Running', ?)
        """,
        ((source or "manual")[:40], property_id, "{}"),
    )
    run_id = run_cur.lastrowid
    where = []
    params = []
    if property_id:
        where.append("p.id = ?")
        params.append(int(property_id))
    rows = db.execute(
        f"""
        SELECT p.id, p.owner_person_id, p.status
        FROM properties p
        {'WHERE ' + ' AND '.join(where) if where else ''}
        ORDER BY p.id DESC
        """,
        tuple(params),
    ).fetchall()
    inspected = 0
    skipped = 0
    created_actions = 0
    details = []
    for row in rows:
        inspected += 1
        result = evaluate_property_lead_management(db, row)
        created_actions += int(result.get("created_actions") or 0)
        if result.get("skipped"):
            skipped += 1
        if len(details) < 25:
            details.append(
                {
                    "property_id": result["property_id"],
                    "created_actions": result["created_actions"],
                    "skipped": bool(result.get("skipped")),
                    "outbound_total": (result.get("snapshot") or {}).get("outbound_total", 0),
                    "inbound_total": (result.get("snapshot") or {}).get("inbound_total", 0),
                }
            )
    summary = {
        "inspected": inspected,
        "skipped": skipped,
        "created_actions": created_actions,
        "details": details,
    }
    db.execute(
        """
        UPDATE lead_monitor_runs
        SET status = 'Completed',
            summary_json = ?,
            completed_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (json.dumps(summary), run_id),
    )
    return {"run_id": run_id, **summary}


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
               op.middle_name AS owner_middle,
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


@app.route("/coming-soon")
def coming_soon():
    return render_template("coming_soon.html")


@app.route("/agents")
def agents_page():
    ensure_db()
    db = get_db()
    notice = (request.args.get("notice") or "").strip()
    queue_counts = db.execute(
        """
        SELECT
            SUM(CASE WHEN queue_status = 'Queued' THEN 1 ELSE 0 END) AS queued_count,
            SUM(CASE WHEN queue_status = 'No Realtor' THEN 1 ELSE 0 END) AS no_realtor_count
        FROM referral_auto_send_queue
        """
    ).fetchone()
    summary = {
        "queued_count": int((queue_counts["queued_count"] or 0) if queue_counts else 0),
        "no_realtor_count": int((queue_counts["no_realtor_count"] or 0) if queue_counts else 0),
    }
    lead_counts = db.execute(
        """
        SELECT
            SUM(CASE WHEN status = 'Pending' THEN 1 ELSE 0 END) AS pending_count,
            SUM(CASE WHEN status = 'Pending' AND priority = 'High' THEN 1 ELSE 0 END) AS high_count,
            SUM(CASE WHEN status = 'Pending' AND action_type IN ('EscalateToRelativeOutreach', 'EscalateToDeepDiveResearch') THEN 1 ELSE 0 END) AS escalation_count
        FROM lead_management_actions
        """
    ).fetchone()
    lead_summary = {
        "pending_count": int((lead_counts["pending_count"] if lead_counts else 0) or 0),
        "high_count": int((lead_counts["high_count"] if lead_counts else 0) or 0),
        "escalation_count": int((lead_counts["escalation_count"] if lead_counts else 0) or 0),
    }
    lead_actions = db.execute(
        """
        SELECT a.id, a.property_id, a.person_id, a.action_type, a.priority, a.status, a.reason, a.created_at, a.updated_at,
               adr.street, adr.city, adr.state, adr.postal_code,
               pe.first_name, pe.middle_name, pe.last_name
        FROM lead_management_actions a
        LEFT JOIN properties p ON p.id = a.property_id
        LEFT JOIN addresses adr ON adr.id = p.property_address_id
        LEFT JOIN people pe ON pe.id = a.person_id
        ORDER BY
            CASE a.status WHEN 'Pending' THEN 0 WHEN 'Approved' THEN 1 WHEN 'Completed' THEN 2 ELSE 3 END,
            CASE a.priority WHEN 'High' THEN 0 WHEN 'Medium' THEN 1 ELSE 2 END,
            a.updated_at DESC,
            a.id DESC
        LIMIT 200
        """
    ).fetchall()
    latest_run = db.execute(
        """
        SELECT id, source, property_id, status, summary_json, created_at, completed_at
        FROM lead_monitor_runs
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    latest_run_summary = {}
    if latest_run and latest_run["summary_json"]:
        latest_run_summary = parse_json_object(latest_run["summary_json"])
    call_job_counts = db.execute(
        """
        SELECT
            SUM(CASE WHEN analysis_status IN ('Pending', 'Retry') THEN 1 ELSE 0 END) AS pending_count,
            SUM(CASE WHEN analysis_status = 'Completed' THEN 1 ELSE 0 END) AS completed_count
        FROM call_recording_jobs
        """
    ).fetchone()
    call_job_summary = {
        "pending_count": int((call_job_counts["pending_count"] if call_job_counts else 0) or 0),
        "completed_count": int((call_job_counts["completed_count"] if call_job_counts else 0) or 0),
    }
    call_jobs = db.execute(
        """
        SELECT id, call_sid, property_id, person_id, from_number, to_number, fetch_status, analysis_status, summary_text, created_at, updated_at
        FROM call_recording_jobs
        ORDER BY id DESC
        LIMIT 100
        """
    ).fetchall()
    signal_counts = db.execute(
        """
        SELECT
            SUM(CASE WHEN routing_status = 'new' THEN 1 ELSE 0 END) AS new_count,
            SUM(CASE WHEN routing_status = 'routed' THEN 1 ELSE 0 END) AS routed_count,
            SUM(CASE WHEN source_type = 'sms' THEN 1 ELSE 0 END) AS sms_count,
            SUM(CASE WHEN source_type = 'call' THEN 1 ELSE 0 END) AS call_count
        FROM agent_signals
        """
    ).fetchone()
    signal_summary = {
        "new_count": int((signal_counts["new_count"] if signal_counts else 0) or 0),
        "routed_count": int((signal_counts["routed_count"] if signal_counts else 0) or 0),
        "sms_count": int((signal_counts["sms_count"] if signal_counts else 0) or 0),
        "call_count": int((signal_counts["call_count"] if signal_counts else 0) or 0),
    }
    signals = db.execute(
        """
        SELECT id, source_type, source_key, property_id, person_id, intent, sentiment, confidence, recommended_next_step, routing_status, summary_text, updated_at
        FROM agent_signals
        ORDER BY id DESC
        LIMIT 100
        """
    ).fetchall()
    advisor_snapshot = build_agent_advisor_snapshot(db, window_hours=72)
    advisor_recommendations = build_agent_advisor_recommendations(advisor_snapshot)
    advisor_summary_row = db.execute(
        """
        SELECT id, response_text, response_json, snapshot_json, created_at
        FROM agent_advisor_logs
        WHERE log_type = 'summary'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    advisor_qa_rows = db.execute(
        """
        SELECT id, question, response_text, created_at
        FROM agent_advisor_logs
        WHERE log_type = 'qa'
        ORDER BY id DESC
        LIMIT 12
        """
    ).fetchall()
    return render_template(
        "agents.html",
        agents=AGENT_DEFINITIONS,
        summary=summary,
        lead_summary=lead_summary,
        lead_actions=lead_actions,
        lead_action_statuses=LEAD_ACTION_STATUSES,
        latest_run=latest_run,
        latest_run_summary=latest_run_summary,
        call_job_summary=call_job_summary,
        call_jobs=call_jobs,
        signal_summary=signal_summary,
        signals=signals,
        advisor_snapshot=advisor_snapshot,
        advisor_recommendations=advisor_recommendations,
        advisor_summary_row=advisor_summary_row,
        advisor_qa_rows=advisor_qa_rows,
        notice=notice,
    )


@app.route("/agents/lead-monitor/run", methods=["POST"])
def run_lead_monitor_route():
    ensure_db()
    db = get_db()
    property_raw = (request.form.get("property_id") or "").strip()
    property_id = int(property_raw) if property_raw.isdigit() else None
    try:
        data = run_lead_monitor_once(db, source="manual", property_id=property_id)
        db.commit()
        notice = (
            f"Lead monitor run #{data['run_id']} completed. "
            f"Inspected {data['inspected']} properties, created {data['created_actions']} action(s), skipped {data['skipped']}."
        )
        return redirect(url_for("agents_page", notice=notice))
    except Exception as exc:
        db.rollback()
        return redirect(url_for("agents_page", notice=f"Lead monitor failed: {exc}"))


@app.route("/agents/lead-actions/<int:action_id>/status", methods=["POST"])
def update_lead_action_status_route(action_id):
    ensure_db()
    db = get_db()
    status = (request.form.get("status") or "").strip()
    if status not in LEAD_ACTION_STATUSES:
        return redirect(url_for("agents_page", notice="Invalid lead action status."))
    row = db.execute("SELECT id FROM lead_management_actions WHERE id = ?", (action_id,)).fetchone()
    if not row:
        return redirect(url_for("agents_page", notice="Lead action not found."))
    db.execute(
        "UPDATE lead_management_actions SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (status, action_id),
    )
    db.commit()
    return redirect(url_for("agents_page", notice=f"Lead action #{action_id} updated to {status}."))


@app.route("/agents/call-recordings/run", methods=["POST"])
def run_call_recordings_route():
    limit_raw = (request.form.get("limit") or "").strip()
    limit = int(limit_raw) if limit_raw.isdigit() else 2
    result = run_call_recording_analysis_once(limit=max(1, min(10, limit)))
    if result.get("ok"):
        return redirect(
            url_for(
                "agents_page",
                notice=(
                    f"Call analysis run complete. Processed {result.get('processed', 0)}, "
                    f"completed {result.get('completed', 0)}, failed {result.get('failed', 0)}."
                ),
            )
        )
    return redirect(url_for("agents_page", notice=f"Call analysis run failed: {result.get('error', 'Unknown error')}"))


@app.route("/agents/sms-analysis/run", methods=["POST"])
def run_sms_analysis_route():
    lookback_raw = (request.form.get("lookback_days") or "").strip()
    limit_raw = (request.form.get("limit_threads") or "").strip()
    lookback = int(lookback_raw) if lookback_raw.isdigit() else 14
    limit = int(limit_raw) if limit_raw.isdigit() else 30
    result = run_sms_analysis_once(lookback_days=max(1, min(90, lookback)), limit_threads=max(1, min(100, limit)))
    if result.get("ok"):
        return redirect(
            url_for(
                "agents_page",
                notice=(
                    f"SMS analysis complete. Processed {result.get('processed', 0)}, "
                    f"analyzed {result.get('analyzed', 0)}, failed {result.get('failed', 0)}, "
                    f"routed {result.get('routed', 0)}."
                ),
            )
        )
    return redirect(url_for("agents_page", notice=f"SMS analysis failed: {result.get('error', 'Unknown error')}"))


@app.route("/agents/advisor/refresh", methods=["POST"])
def run_agent_advisor_refresh_route():
    ensure_db()
    db = get_db()
    try:
        snapshot = build_agent_advisor_snapshot(db, window_hours=72)
        recommendations = build_agent_advisor_recommendations(snapshot)
        summary_result = generate_agent_advisor_summary_with_openai(snapshot, recommendations)
        db.execute(
            """
            INSERT INTO agent_advisor_logs (log_type, response_text, response_json, snapshot_json)
            VALUES (?, ?, ?, ?)
            """,
            (
                "summary",
                summary_result.get("summary_text", ""),
                json.dumps(summary_result.get("json", {})),
                json.dumps({"snapshot": snapshot, "recommendations": recommendations}),
            ),
        )
        db.commit()
        mode = summary_result.get("mode", "fallback")
        return redirect(url_for("agents_page", notice=f"Agent 3 summary refreshed ({mode})."))
    except Exception as exc:
        db.rollback()
        return redirect(url_for("agents_page", notice=f"Agent 3 summary failed: {exc}"))


@app.route("/agents/advisor/ask", methods=["POST"])
def run_agent_advisor_question_route():
    ensure_db()
    db = get_db()
    question = (request.form.get("question") or "").strip()
    if not question:
        return redirect(url_for("agents_page", notice="Agent 3 question is required."))
    try:
        snapshot = build_agent_advisor_snapshot(db, window_hours=72)
        recommendations = build_agent_advisor_recommendations(snapshot)
        history_rows = db.execute(
            """
            SELECT question, response_text, created_at
            FROM agent_advisor_logs
            WHERE log_type = 'qa'
            ORDER BY id DESC
            LIMIT 8
            """
        ).fetchall()
        history = [dict(row) for row in history_rows]
        answer = answer_agent_advisor_question(question, snapshot, recommendations, history)
        db.execute(
            """
            INSERT INTO agent_advisor_logs (log_type, question, response_text, snapshot_json)
            VALUES (?, ?, ?, ?)
            """,
            (
                "qa",
                question,
                answer,
                json.dumps({"snapshot": snapshot, "recommendations": recommendations}),
            ),
        )
        db.commit()
        return redirect(url_for("agents_page", notice="Agent 3 answered your question."))
    except Exception as exc:
        db.rollback()
        return redirect(url_for("agents_page", notice=f"Agent 3 Q&A failed: {exc}"))


@app.route("/referral")
def referral_dashboard():
    ensure_db()
    db = get_db()
    referrals = get_cached_referrals(db)
    total_count = len(referrals)
    error = ""
    notice = request.args.get("notice", "").strip()
    status_slug = os.getenv("REISIFT_REFERRAL_STATUS", "refer_lead").strip() or "refer_lead"
    if not referrals:
        try:
            data = refresh_reisift_referrals_cache(db)
            status_slug = data["status_slug"]
            referrals = get_cached_referrals(db)
            total_count = len(referrals)
            notice = f"Referral cache initialized ({data['synced']} synced)."
        except Exception as exc:
            error = str(exc)
    queue_filter = (request.args.get("queue_filter") or "all").strip().lower()
    valid_queue_filters = {"all", "queued", "no_realtor"}
    if queue_filter not in valid_queue_filters:
        queue_filter = "all"
    queue_sql = """
        SELECT q.*, r.first_name, r.last_name, rr.full_address
        FROM referral_auto_send_queue q
        LEFT JOIN referral_realtors r ON r.id = q.realtor_id
        LEFT JOIN reisift_referrals rr ON rr.property_uuid = q.property_uuid
    """
    if queue_filter == "queued":
        queue_sql += " WHERE q.queue_status = 'Queued'"
    elif queue_filter == "no_realtor":
        queue_sql += " WHERE q.queue_status = 'No Realtor'"
    queue_sql += " ORDER BY q.id DESC LIMIT 200"
    queue_rows = db.execute(
        queue_sql,
    ).fetchall()
    return render_template(
        "referral.html",
        referrals=referrals,
        queue_rows=queue_rows,
        queue_filter=queue_filter,
        total_count=total_count,
        status_slug=status_slug,
        error=error,
        notice=notice,
        referral_statuses=REFERRAL_STATUSES,
    )


@app.route("/referral/refresh", methods=["POST"])
def referral_refresh():
    ensure_db()
    db = get_db()
    notice = ""
    try:
        data = refresh_reisift_referrals_cache(db)
        notice = f"Referral cache refreshed. {data['synced']} rows synced."
        q = data.get("queue_summary") or {}
        notice += f" Queue: {q.get('queued', 0)} queued, {q.get('no_realtor', 0)} with no realtor."
        if data["errors"]:
            notice += f" {len(data['errors'])} rows used fallback payload."
    except Exception as exc:
        return redirect(url_for("referral_dashboard", notice=f"Refresh failed: {exc}"))
    return redirect(url_for("referral_dashboard", notice=notice))


@app.route("/referral/queue/clear", methods=["POST"])
def referral_clear_queue():
    ensure_db()
    db = get_db()
    try:
        cur = db.execute("DELETE FROM referral_auto_send_queue")
        deleted = int(cur.rowcount or 0)
        db.commit()
        return redirect(url_for("referral_dashboard", notice=f"Referral queue cleared. {deleted} row(s) removed."))
    except Exception as exc:
        db.rollback()
        return redirect(url_for("referral_dashboard", notice=f"Clear queue failed: {exc}"))


@app.route("/follow-ups")
def follow_ups_page():
    ensure_db()
    db = get_db()
    lookback_days_raw = (request.args.get("days") or "7").strip()
    sort_order = (request.args.get("sort") or "desc").strip().lower()
    if sort_order not in {"asc", "desc"}:
        sort_order = "desc"
    try:
        lookback_days = max(1, min(90, int(lookback_days_raw)))
    except ValueError:
        lookback_days = 7

    raw_rows = get_cached_followups(db, sort_dir=sort_order)
    rows = []
    for row in raw_rows:
        payload = {}
        if row["payload_json"]:
            try:
                payload = json.loads(row["payload_json"])
            except Exception:
                payload = {}
        phones, emails = extract_owner_contacts_from_payload(payload)
        property_lists = extract_property_lists(payload)
        addr = extract_payload_address(payload)
        deep_dive_property_id = find_local_property_by_address(
            db, addr["street"], addr["city"], addr["state"], addr["postal_code"]
        )
        rows.append(
            {
                **dict(row),
                "phones": phones,
                "emails": emails,
                "property_lists": property_lists,
                "deep_dive_property_id": deep_dive_property_id,
                "events": parse_followup_json_list(row["events_json"]),
                "tasks": parse_followup_json_list(row["tasks_json"]),
            }
        )
    notice = (request.args.get("notice") or "").strip()
    error = (request.args.get("error") or "").strip()
    return render_template(
        "follow_ups.html",
        rows=rows,
        notice=notice,
        error=error,
        lookback_days=lookback_days,
        sort_order=sort_order,
        auto_enrich=(request.args.get("enrich") or "0") == "1",
    )


@app.route("/follow-ups/refresh", methods=["POST"])
def follow_ups_refresh():
    ensure_db()
    db = get_db()
    lookback_days_raw = (request.form.get("days") or request.args.get("days") or "7").strip()
    sort_order = (request.form.get("sort") or request.args.get("sort") or "desc").strip().lower()
    if sort_order not in {"asc", "desc"}:
        sort_order = "desc"
    try:
        lookback_days = max(1, min(90, int(lookback_days_raw)))
    except ValueError:
        lookback_days = 7
    try:
        data = refresh_reisift_followups_search_cache(db, lookback_days=lookback_days)
        notice = f"Follow Ups refreshed. {data['synced']} rows cached from the last {lookback_days} days."
        if data["errors"]:
            notice += f" {len(data['errors'])} detail calls used fallback payload."
        return redirect(url_for("follow_ups_page", notice=notice, days=lookback_days, sort=sort_order, enrich=1))
    except Exception as exc:
        return redirect(url_for("follow_ups_page", error=f"Refresh failed: {exc}", days=lookback_days, sort=sort_order))


@app.route("/api/follow-ups/enrich", methods=["POST"])
def follow_ups_enrich_api():
    ensure_db()
    db = get_db()
    try:
        result = enrich_reisift_followups_cache(db)
        return jsonify({"ok": True, "updated": result["updated"], "errors": result["errors"]})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/follow-ups/<string:property_uuid>/add-deep-dive", methods=["POST"])
def follow_up_add_to_deep_dive(property_uuid):
    ensure_db()
    db = get_db()
    lookback_days_raw = (request.form.get("days") or "7").strip()
    sort_order = (request.form.get("sort") or "desc").strip().lower()
    if sort_order not in {"asc", "desc"}:
        sort_order = "desc"
    try:
        lookback_days = max(1, min(90, int(lookback_days_raw)))
    except ValueError:
        lookback_days = 7

    row = db.execute(
        "SELECT property_uuid, payload_json, last_synced_at FROM reisift_followups WHERE property_uuid = ? LIMIT 1",
        (property_uuid,),
    ).fetchone()
    if not row:
        return redirect(url_for("follow_ups_page", error="Lead not found in New Leads cache.", days=lookback_days, sort=sort_order))

    try:
        payload = json.loads(row["payload_json"] or "{}")
    except Exception:
        payload = {}

    addr = extract_payload_address(payload)
    if not addr["street"] or not addr["city"]:
        return redirect(url_for("follow_ups_page", error="Could not add lead: missing address in payload.", days=lookback_days, sort=sort_order))

    existing_property_id = find_local_property_by_address(db, addr["street"], addr["city"], addr["state"], addr["postal_code"])
    if existing_property_id:
        return redirect(
            url_for(
                "follow_ups_page",
                notice=f"Already in Deep Dive as property #{existing_property_id}.",
                days=lookback_days,
                sort=sort_order,
            )
        )

    owner = payload.get("owner") if isinstance(payload, dict) else {}
    if not isinstance(owner, dict):
        owner = {}
    owner_first = (owner.get("first_name") or "").strip() or "Unknown"
    owner_last = (owner.get("last_name") or "").strip() or "Owner"
    owner_notes = f"Imported from New Leads (ReiSIFT UUID {property_uuid}). Last synced: {row['last_synced_at'] or '-'}."

    address_id = create_address(db, addr["street"], addr["city"], addr["state"], addr["postal_code"])
    owner_id = find_or_create_person_by_name_parts(db, owner_first, owner_last, notes=owner_notes)

    prop_cur = db.execute(
        """
        INSERT INTO properties (property_address_id, owner_person_id, resident_person_id, status, notes)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            address_id,
            owner_id,
            None,
            "Untouched",
            f"Imported from New Leads (ReiSIFT UUID {property_uuid}).",
        ),
    )
    property_id = prop_cur.lastrowid

    for p in (owner.get("phones") or []):
        if isinstance(p, dict):
            number = (p.get("number") or p.get("phone") or "").strip()
            label = (p.get("type") or "Unknown").strip().title()
            status = (p.get("status") or "Unknown").strip().title()
        else:
            number = str(p or "").strip()
            label = "Unknown"
            status = "Unknown"
        if not number or touchpoint_exists(db, owner_id, "Phone", number):
            continue
        db.execute(
            """
            INSERT INTO touchpoints (person_id, channel_type, channel_label, value, status, note, last_attempted)
            VALUES (?, 'Phone', ?, ?, ?, ?, ?)
            """,
            (
                owner_id,
                label,
                number,
                status,
                f"Imported from New Leads (ReiSIFT UUID {property_uuid}).",
                "",
            ),
        )

    for e in (owner.get("emails") or []):
        if isinstance(e, dict):
            email_value = (e.get("email") or "").strip()
            status = (e.get("status") or "Unknown").strip().title()
        else:
            email_value = str(e or "").strip()
            status = "Unknown"
        if not email_value or touchpoint_exists(db, owner_id, "Email", email_value):
            continue
        db.execute(
            """
            INSERT INTO touchpoints (person_id, channel_type, channel_label, value, status, note, last_attempted)
            VALUES (?, 'Email', 'Email', ?, ?, ?, ?)
            """,
            (
                owner_id,
                email_value,
                status,
                f"Imported from New Leads (ReiSIFT UUID {property_uuid}).",
                "",
            ),
        )

    db.commit()
    return redirect(
        url_for(
            "follow_ups_page",
            notice=f"Lead added to Deep Dive as property #{property_id}.",
            days=lookback_days,
            sort=sort_order,
        )
    )


@app.route("/referral/<string:property_uuid>")
def referral_property_detail(property_uuid):
    ensure_db()
    db = get_db()
    notice = (request.args.get("notice") or "").strip()
    row = db.execute(
        """
        SELECT property_uuid, status, full_address, owner_names, payload_json, last_synced_at,
               COALESCE(referral_status, 'Untouched') AS referral_status,
               winning_realtor_id,
               referral_notes
        FROM reisift_referrals
        WHERE property_uuid = ?
        """,
        (property_uuid,),
    ).fetchone()
    if row is None:
        return redirect(url_for("referral_dashboard", notice=f"Referral UUID not found locally: {property_uuid}"))

    payload = {}
    payload_json_pretty = "{}"
    if row["payload_json"]:
        try:
            payload = json.loads(row["payload_json"])
            payload_json_pretty = json.dumps(payload, indent=2)
        except json.JSONDecodeError:
            payload_json_pretty = row["payload_json"]

    phones = []
    emails = []

    owner = payload.get("owner") if isinstance(payload, dict) else {}
    if isinstance(owner, dict):
        for p in owner.get("phones") or []:
            number = (p.get("number") or "").strip()
            if not number:
                continue
            phones.append(
                {
                    "number": number,
                    "type": (p.get("type") or "UNKNOWN").strip(),
                    "status": (p.get("status") or "UNKNOWN").strip(),
                }
            )
        for e in owner.get("emails") or []:
            if isinstance(e, str):
                value = e.strip()
            else:
                value = (e.get("email") or "").strip()
            if value:
                emails.append(value)

    seen_phone = set()
    dedup_phones = []
    for p in phones:
        key = p["number"]
        if key in seen_phone:
            continue
        seen_phone.add(key)
        dedup_phones.append(p)
    phones = dedup_phones

    seen_email = set()
    dedup_emails = []
    for e in emails:
        key = e.lower()
        if key in seen_email:
            continue
        seen_email.add(key)
        dedup_emails.append(e)
    emails = dedup_emails

    market_filter = (request.args.get("market") or "").strip()
    realtor_where = ["1=1"]
    realtor_params = []
    if market_filter:
        realtor_where.append("lower(target_markets) LIKE ?")
        realtor_params.append(f"%{market_filter.lower()}%")
    realtors = db.execute(
        f"""
        SELECT *
        FROM referral_realtors
        WHERE {' AND '.join(realtor_where)}
        ORDER BY lower(last_name), lower(first_name), id DESC
        """,
        tuple(realtor_params),
    ).fetchall()

    push_activity = db.execute(
        """
        SELECT a.*, r.first_name, r.last_name, r.brokerage
        FROM referral_push_activity a
        JOIN referral_realtors r ON r.id = a.realtor_id
        WHERE a.property_uuid = ?
        ORDER BY a.created_at DESC, a.id DESC
        """,
        (property_uuid,),
    ).fetchall()

    winning_realtor = None
    if row["winning_realtor_id"]:
        winning_realtor = db.execute(
            "SELECT id, first_name, last_name, brokerage FROM referral_realtors WHERE id = ?",
            (row["winning_realtor_id"],),
        ).fetchone()

    return render_template(
        "referral_property.html",
        referral=row,
        payload=payload,
        payload_json_pretty=payload_json_pretty,
        phones=phones,
        emails=emails,
        realtors=realtors,
        push_activity=push_activity,
        market_filter=market_filter,
        notice=notice,
        nj_counties=NJ_COUNTIES,
        referral_statuses=REFERRAL_STATUSES,
        winning_realtor=winning_realtor,
    )


@app.route("/referral/<string:property_uuid>/status", methods=["POST"])
def referral_update_status(property_uuid):
    ensure_db()
    db = get_db()
    status = (request.form.get("referral_status") or "").strip()
    if status not in REFERRAL_STATUSES:
        status = "Untouched"
    db.execute(
        "UPDATE reisift_referrals SET referral_status = ?, last_synced_at = last_synced_at WHERE property_uuid = ?",
        (status, property_uuid),
    )
    db.commit()
    return redirect(url_for("referral_dashboard", notice=f"Referral status updated to {status}."))


@app.route("/referral/<string:property_uuid>/manage", methods=["POST"])
def referral_manage_detail(property_uuid):
    ensure_db()
    db = get_db()
    status = (request.form.get("referral_status") or "").strip()
    if status not in REFERRAL_STATUSES:
        status = "Untouched"
    winner_raw = (request.form.get("winning_realtor_id") or "").strip()
    winner_id = int(winner_raw) if winner_raw.isdigit() else None
    notes = (request.form.get("referral_notes") or "").strip()
    db.execute(
        """
        UPDATE reisift_referrals
        SET referral_status = ?, winning_realtor_id = ?, referral_notes = ?
        WHERE property_uuid = ?
        """,
        (status, winner_id, notes, property_uuid),
    )
    db.commit()
    return redirect(url_for("referral_property_detail", property_uuid=property_uuid, notice="Referral management updated."))


def get_referral_property_summary(payload):
    full_address = ""
    contact_phone = ""
    owner_name = ""
    contact_email = ""
    if not isinstance(payload, dict):
        return {"full_address": "", "contact_phone": "", "owner_name": "", "contact_email": ""}
    full_address = (payload.get("full_address") or "").strip()
    if not full_address:
        a = payload.get("address") if isinstance(payload.get("address"), dict) else {}
        full_address = ", ".join(
            x for x in [a.get("street"), a.get("city"), a.get("state"), a.get("zip")] if x
        ).strip(", ")
    owner = payload.get("owner") if isinstance(payload.get("owner"), dict) else {}
    owner_name = (owner.get("name") or owner.get("full_name") or "").strip()
    if not owner_name:
        first = (owner.get("first_name") or "").strip()
        last = (owner.get("last_name") or "").strip()
        owner_name = " ".join(x for x in [first, last] if x).strip()
    phones = owner.get("phones") if isinstance(owner.get("phones"), list) else []
    for p in phones:
        if isinstance(p, dict):
            val = (p.get("number") or "").strip()
        else:
            val = str(p or "").strip()
        if val:
            contact_phone = val
            break
    emails = owner.get("emails") if isinstance(owner.get("emails"), list) else []
    for e in emails:
        if isinstance(e, dict):
            val = (e.get("email") or "").strip()
        else:
            val = str(e or "").strip()
        if val:
            contact_email = val
            break
    return {
        "full_address": full_address,
        "contact_phone": contact_phone,
        "owner_name": owner_name,
        "contact_email": contact_email,
    }


@app.route("/referral/realtors", methods=["GET", "POST"])
def referral_realtors_page():
    ensure_db()
    db = get_db()
    notice = (request.args.get("notice") or "").strip()
    if request.method == "POST":
        first_name = (request.form.get("first_name") or "").strip()
        last_name = (request.form.get("last_name") or "").strip()
        if not first_name or not last_name:
            notice = "First and last name are required."
        else:
            markets = join_markets(request.form.getlist("target_markets"))
            db.execute(
                """
                INSERT INTO referral_realtors (first_name, last_name, email, phone, brokerage, target_markets)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    first_name,
                    last_name,
                    (request.form.get("email") or "").strip(),
                    (request.form.get("phone") or "").strip(),
                    (request.form.get("brokerage") or "").strip(),
                    markets,
                ),
            )
            db.commit()
            notice = "Realtor added."

    q = (request.args.get("q") or "").strip()
    market = (request.args.get("market") or "").strip()
    params = []
    where = ["1=1"]
    if q:
        where.append("(lower(first_name || ' ' || last_name) LIKE ? OR lower(brokerage) LIKE ?)")
        params.extend([f"%{q.lower()}%", f"%{q.lower()}%"])
    if market:
        where.append("lower(target_markets) LIKE ?")
        params.append(f"%{market.lower()}%")
    rows = db.execute(
        f"""
        SELECT *
        FROM referral_realtors
        WHERE {' AND '.join(where)}
        ORDER BY lower(last_name), lower(first_name), id DESC
        """,
        tuple(params),
    ).fetchall()
    return render_template(
        "referral_realtors.html",
        realtors=rows,
        q=q,
        market=market,
        notice=notice,
        nj_counties=NJ_COUNTIES,
    )


@app.route("/referral/realtors/upload", methods=["POST"])
def referral_realtors_upload():
    ensure_db()
    db = get_db()
    file = request.files.get("realtors_file")
    default_markets = request.form.getlist("target_markets")
    if not file or not file.filename:
        return redirect(url_for("referral_realtors_page", q="", market="", notice="No file selected."))
    try:
        text = file.read().decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(text))
    except Exception:
        return redirect(url_for("referral_realtors_page", notice="Could not read CSV file."))

    added = 0
    for row in reader:
        first = (row.get("Realtor First") or row.get("First Name") or row.get("first_name") or "").strip()
        last = (row.get("Last Name") or row.get("Realtor Last") or row.get("last_name") or "").strip()
        if not first or not last:
            continue
        email = (row.get("Email") or row.get("email") or "").strip()
        phone = (row.get("Phone") or row.get("phone") or "").strip()
        brokerage = (row.get("Brokerage") or row.get("brokerage") or "").strip()
        row_market = (row.get("Target Market") or row.get("target_market") or row.get("Markets") or "").strip()
        markets = split_markets(row_market) if row_market else default_markets
        db.execute(
            """
            INSERT INTO referral_realtors (first_name, last_name, email, phone, brokerage, target_markets)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (first, last, email, phone, brokerage, join_markets(markets)),
        )
        added += 1
    db.commit()
    return redirect(url_for("referral_realtors_page", notice=f"Upload complete. {added} realtors added."))


@app.route("/referral/realtors/<int:realtor_id>/update", methods=["POST"])
def referral_realtor_update(realtor_id):
    ensure_db()
    db = get_db()
    row = db.execute("SELECT id FROM referral_realtors WHERE id = ?", (realtor_id,)).fetchone()
    q = (request.form.get("_q") or "").strip()
    market = (request.form.get("_market") or "").strip()
    if not row:
        return redirect(url_for("referral_realtors_page", q=q, market=market, notice="Realtor not found."))

    first_name = (request.form.get("first_name") or "").strip()
    last_name = (request.form.get("last_name") or "").strip()
    if not first_name or not last_name:
        return redirect(url_for("referral_realtors_page", q=q, market=market, notice="First and last name are required."))

    markets_raw = request.form.get("target_markets", "")
    markets = join_markets(split_markets(markets_raw))
    db.execute(
        """
        UPDATE referral_realtors
        SET first_name = ?, last_name = ?, email = ?, phone = ?, brokerage = ?, target_markets = ?
        WHERE id = ?
        """,
        (
            first_name,
            last_name,
            (request.form.get("email") or "").strip(),
            (request.form.get("phone") or "").strip(),
            (request.form.get("brokerage") or "").strip(),
            markets,
            realtor_id,
        ),
    )
    db.commit()
    return redirect(url_for("referral_realtors_page", q=q, market=market, notice="Realtor updated."))


@app.route("/referral/realtors/<int:realtor_id>/delete", methods=["POST"])
def referral_realtor_delete(realtor_id):
    ensure_db()
    db = get_db()
    q = (request.form.get("_q") or "").strip()
    market = (request.form.get("_market") or "").strip()
    row = db.execute("SELECT id FROM referral_realtors WHERE id = ?", (realtor_id,)).fetchone()
    if not row:
        return redirect(url_for("referral_realtors_page", q=q, market=market, notice="Realtor not found."))

    db.execute("DELETE FROM referral_realtors WHERE id = ?", (realtor_id,))
    db.commit()
    return redirect(url_for("referral_realtors_page", q=q, market=market, notice="Realtor deleted."))


@app.route("/referral/<string:property_uuid>/push-sms", methods=["POST"])
def referral_push_sms(property_uuid):
    ensure_db()
    db = get_db()
    row = db.execute(
        "SELECT property_uuid, full_address, payload_json FROM reisift_referrals WHERE property_uuid = ?",
        (property_uuid,),
    ).fetchone()
    if not row:
        return redirect(url_for("referral_dashboard", notice="Referral property not found."))

    payload = {}
    if row["payload_json"]:
        try:
            payload = json.loads(row["payload_json"])
        except Exception:
            payload = {}
    summary = get_referral_property_summary(payload)
    full_address = summary["full_address"] or (row["full_address"] or "").strip()
    contact_phone = summary["contact_phone"]
    owner_name = summary["owner_name"] or ""
    contact_email = summary.get("contact_email") or ""
    send_mode = (request.form.get("send_mode") or "selected").strip().lower()
    market = (request.form.get("market") or "").strip()
    realtors = []
    if send_mode == "filtered":
        where = ["1=1"]
        params = []
        if market:
            where.append("lower(target_markets) LIKE ?")
            params.append(f"%{market.lower()}%")
        realtors = db.execute(
            f"""
            SELECT *
            FROM referral_realtors
            WHERE {' AND '.join(where)}
            ORDER BY lower(last_name), lower(first_name), id DESC
            """,
            tuple(params),
        ).fetchall()
    else:
        realtor_ids = [x for x in request.form.getlist("realtor_ids") if str(x).isdigit()]
        if not realtor_ids:
            return redirect(
                url_for(
                    "referral_property_detail",
                    property_uuid=property_uuid,
                    notice="Select at least one realtor.",
                    market=market,
                )
            )
        realtors = db.execute(
            f"SELECT * FROM referral_realtors WHERE id IN ({','.join(['?'] * len(realtor_ids))})",
            tuple(int(x) for x in realtor_ids),
        ).fetchall()
    from_number = get_referral_sms_number(db)
    sent_count = 0
    for realtor in realtors:
        to_number = (realtor["phone"] or "").strip()
        if not to_number:
            continue
        lines = [
            "New Listing Lead",
            f"Name: {owner_name or '-'}",
            f"Address: {full_address or '-'}",
            f"Phone: {contact_phone or '-'}",
            f"Email: {contact_email or '-'}",
        ]
        message = "\n".join(lines)
        status = "Failed"
        sms_id = ""
        raw = {}
        try:
            send_res = send_smrtphone_sms(to_number, message, from_number=from_number)
            status = send_res.get("status") or "Sent"
            sms_id = send_res.get("sms_id") or ""
            raw = send_res.get("raw") or {}
            sent_count += 1
        except Exception as exc:
            raw = {"error": str(exc)}
            status = "Failed"
        db.execute(
            """
            INSERT INTO referral_push_activity (property_uuid, realtor_id, to_number, message_body, status, external_id, response_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (property_uuid, realtor["id"], to_number, message, status, sms_id, json.dumps(raw)),
        )
    if sent_count > 0:
        current_row = db.execute(
            "SELECT COALESCE(referral_status, 'Untouched') AS referral_status FROM reisift_referrals WHERE property_uuid = ?",
            (property_uuid,),
        ).fetchone()
        current_status = (current_row["referral_status"] if current_row else "Untouched") or "Untouched"
        if current_status == "Untouched":
            db.execute(
                "UPDATE reisift_referrals SET referral_status = 'Referred' WHERE property_uuid = ?",
                (property_uuid,),
            )
    db.commit()
    return redirect(
        url_for(
            "referral_property_detail",
            property_uuid=property_uuid,
            notice=f"Referral SMS queued/sent to {sent_count} realtor(s).",
            market=market,
        )
    )


@app.route("/api/reisift/properties/create", methods=["POST"])
def reisift_create_property_api():
    ensure_db()
    db = get_db()
    try:
        payload = request.get_json(force=True) or {}
        result = create_reisift_property_from_search(payload)
        created = result.get("created") or {}
        return jsonify(
            {
                "ok": True,
                "map_id": result.get("map_id"),
                "created_uuid": result.get("created_uuid") or created.get("uuid") or created.get("id"),
                "created": created,
                "create_payload": result.get("create_payload"),
                "owner_uuid": result.get("owner_uuid"),
                "owner_contact_sync": result.get("owner_contact_sync"),
                "enrich_wait_seconds": result.get("enrich_wait_seconds", 0),
                "enrich": result.get("enrich"),
            }
        )
    except Exception as exc:
        log_app_error(
            db,
            source="reisift_add_property_api",
            error_message=str(exc),
            details=traceback.format_exc(),
            route="/api/reisift/properties/create",
            status_code=500,
        )
        db.commit()
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/integrations/reisift/create-property", methods=["POST"])
def integrations_reisift_create_property_api():
    ensure_db()
    db = get_db()
    if not integration_auth_ok(db, request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    payload = request.get_json(force=True) or {}
    try:
        result = create_reisift_property_from_search(payload)
        created = result.get("created") or {}
        return jsonify(
            {
                "ok": True,
                "map_id": result.get("map_id"),
                "created_uuid": result.get("created_uuid") or created.get("uuid") or created.get("id"),
                "created": created,
                "owner_uuid": result.get("owner_uuid"),
                "owner_contact_sync": result.get("owner_contact_sync"),
                "enrich": result.get("enrich"),
            }
        )
    except Exception as exc:
        err_text = str(exc)
        search_value = (payload.get("search") or payload.get("address_search") or "").strip()
        if ("map_id" in err_text.lower()) or ("autocomplete" in err_text.lower()):
            try:
                send_slack_notification(
                    db,
                    f"ReiSift intake failed: address not found in map autocomplete. Search='{search_value or '-'}'. Error='{err_text}'",
                )
            except Exception:
                pass
        log_app_error(
            db,
            source="integrations_reisift_create_property_api",
            error_message=err_text,
            details=traceback.format_exc(),
            route="/api/integrations/reisift/create-property",
            status_code=500,
        )
        db.commit()
        return jsonify({"ok": False, "error": err_text}), 500


@app.route("/api/integrations/google-sheets/row-added", methods=["POST"])
def integrations_google_sheets_row_added_api():
    ensure_db()
    db = get_db()
    if not integration_auth_ok(db, request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    payload = request.get_json(force=True) or {}
    result = process_clever_lead_payload(db, payload, source_label="webhook")
    status_code = 500 if not result.get("ok") else 200
    return jsonify(result), status_code


@app.route("/api/admin/import-referral-bundle", methods=["POST"])
def import_referral_bundle_api():
    ensure_db()
    db = get_db()
    try:
        payload = request.get_json(silent=True) or {}
        realtor_rows = payload.get("referral_realtors")
        referral_rows = payload.get("reisift_referrals")
        push_rows = payload.get("referral_push_activity")
        realtor_rows = realtor_rows if isinstance(realtor_rows, list) else []
        referral_rows = referral_rows if isinstance(referral_rows, list) else []
        push_rows = push_rows if isinstance(push_rows, list) else []
        if not (realtor_rows or referral_rows or push_rows):
            return jsonify({"ok": False, "error": "No import rows provided."}), 400
        counts = import_referral_bundle_rows(db, realtor_rows, referral_rows, push_rows)
        db.commit()
        return jsonify({"ok": True, "imported": counts})
    except Exception as exc:
        db.rollback()
        log_app_error(
            db,
            source="import_referral_bundle_api",
            error_message=str(exc),
            details=traceback.format_exc(),
            route="/api/admin/import-referral-bundle",
            status_code=500,
        )
        db.commit()
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/settings", methods=["GET", "POST"])
def settings_page():
    ensure_db()
    db = get_db()
    notice = ""
    error_notice = ""
    active_tab = request.args.get("tab", "").strip() or "direct_mail"
    market_helper_address = (request.args.get("market_helper_address") or "").strip()
    market_helper_result = build_market_status_helper(market_helper_address) if market_helper_address else None
    reisift_add_result = None
    reisift_add_request = {
        "search": (request.args.get("reisift_search") or "").strip(),
        "status": (request.args.get("reisift_status") or "new_lead").strip(),
        "lists": (request.args.get("reisift_lists") or "").strip(),
        "tags": (request.args.get("reisift_tags") or "").strip(),
        "notes": (request.args.get("reisift_notes") or "").strip(),
    }
    if request.method == "POST":
        active_tab = (request.form.get("settings_tab") or active_tab or "direct_mail").strip()
        if active_tab == "direct_mail":
            postage_selected = request.form.get("dm_postage_type", "")
            envelope_selected = request.form.get("dm_envelope_type", "")
            postage_custom = request.form.get("dm_postage_type_custom", "")
            envelope_custom = request.form.get("dm_envelope_type_custom", "")
            postage_value = (postage_custom or postage_selected or "").strip()
            envelope_value = (envelope_custom or envelope_selected or "").strip()
            fields = {
                "dm_sender_first_name": request.form.get("dm_sender_first_name", ""),
                "dm_sender_last_name": request.form.get("dm_sender_last_name", ""),
                "dm_sender_company_name": request.form.get("dm_sender_company_name", ""),
                "dm_sender_address1": request.form.get("dm_sender_address1", ""),
                "dm_sender_city": request.form.get("dm_sender_city", ""),
                "dm_sender_state": request.form.get("dm_sender_state", ""),
                "dm_sender_zip": request.form.get("dm_sender_zip", ""),
                "dm_sender_phone": request.form.get("dm_sender_phone", ""),
                "dm_sender_email": request.form.get("dm_sender_email", ""),
                "dm_sender_website": request.form.get("dm_sender_website", ""),
                "dm_postage_type": postage_value,
                "dm_envelope_type": envelope_value,
            }
            for key, value in fields.items():
                set_setting(db, key, value)
            notice = "Direct mail settings saved."
        elif active_tab == "deep_dive":
            set_setting(db, "deep_dive_smrtphone_from", request.form.get("deep_dive_smrtphone_from", ""))
            notice = "Deep Dive settings saved."
        elif active_tab == "referral":
            set_setting(db, "referral_smrtphone_from", request.form.get("referral_smrtphone_from", ""))
            notice = "Referral settings saved."
        elif active_tab == "email":
            fields = {
                "email_from_name": request.form.get("email_from_name", ""),
                "email_from_address": request.form.get("email_from_address", ""),
                "email_provider": request.form.get("email_provider", "gmail_api"),
                "email_resend_api_key": request.form.get("email_resend_api_key", ""),
                "email_resend_base_url": request.form.get("email_resend_base_url", "https://api.resend.com"),
                "email_gmail_api_client_id": request.form.get("email_gmail_api_client_id", ""),
                "email_gmail_api_client_secret": request.form.get("email_gmail_api_client_secret", ""),
                "email_gmail_api_refresh_token": request.form.get("email_gmail_api_refresh_token", ""),
                "email_gmail_webhook_token": request.form.get("email_gmail_webhook_token", ""),
            }
            for key, value in fields.items():
                set_setting(db, key, value)
            email_action = (request.form.get("email_action") or "").strip().lower()
            if email_action == "test_login":
                check = test_email_login(db)
                if check.get("ok"):
                    notice = "Email settings saved. API login test passed."
                else:
                    error_notice = f"Email settings saved, but login test failed: {check.get('error') or 'Unknown error'}"
                    log_app_error(
                        db,
                        source="email_login_test",
                        error_message=check.get("error") or "Email login test failed",
                        details=json.dumps(check),
                        route="/settings",
                        status_code=400,
                    )
            else:
                notice = "Email settings saved."
        elif active_tab == "integrations":
            fields = {
                "integration_api_key": request.form.get("integration_api_key", ""),
                "slack_webhook_url": request.form.get("slack_webhook_url", ""),
                "slack_signing_secret": request.form.get("slack_signing_secret", ""),
                "slack_default_channel": request.form.get("slack_default_channel", ""),
            }
            for key, value in fields.items():
                set_setting(db, key, value)
            test_action = (request.form.get("integration_action") or "").strip().lower()
            if test_action == "test_slack":
                try:
                    send_slack_notification(db, "DeepSift test notification: Slack integration is configured.")
                    notice = "Integrations saved. Slack test sent."
                except Exception as exc:
                    error_notice = f"Integrations saved, but Slack test failed: {exc}"
                    log_app_error(
                        db,
                        source="slack_test",
                        error_message=str(exc),
                        details=traceback.format_exc(),
                        route="/settings",
                        status_code=400,
                    )
            else:
                notice = "Integrations settings saved."
        elif active_tab == "automations":
            automation_key = (request.form.get("automation_key") or "").strip().lower()
            if automation_key == "clever_leads":
                clever_enabled = (request.form.get("automation_clever_leads_enabled") or "").strip().lower() in {"1", "true", "on", "yes"}
                set_setting(db, "automation_clever_leads_enabled", "1" if clever_enabled else "0")
            elif automation_key == "reisift_placeholder":
                reisift_enabled = (request.form.get("automation_reisift_placeholder_enabled") or "").strip().lower() in {"1", "true", "on", "yes"}
                set_setting(db, "automation_reisift_placeholder_enabled", "1" if reisift_enabled else "0")
            elif automation_key == "auto_send_realtors":
                auto_send_enabled = (request.form.get("automation_auto_send_realtors_enabled") or "").strip().lower() in {"1", "true", "on", "yes"}
                set_setting(db, "automation_auto_send_realtors_enabled", "1" if auto_send_enabled else "0")
            notice = "Automation settings saved."
        elif active_tab == "helpers":
            market_helper_address = (request.form.get("market_helper_address") or "").strip()
            reisift_add_request = {
                "search": (request.form.get("reisift_search") or "").strip(),
                "status": (request.form.get("reisift_status") or "new_lead").strip(),
                "lists": (request.form.get("reisift_lists") or "").strip(),
                "tags": (request.form.get("reisift_tags") or "").strip(),
                "notes": (request.form.get("reisift_notes") or "").strip(),
            }
            helper_action = (request.form.get("helpers_action") or "").strip().lower()
            if market_helper_address:
                market_helper_result = build_market_status_helper(market_helper_address)
                notice = "On-market helper links generated."
            elif helper_action != "reisift_add_property":
                notice = "Enter a property address to generate helper links."
            if helper_action == "reisift_add_property":
                if not reisift_add_request["search"]:
                    error_notice = "ReiSift add-property requires a property search value."
                else:
                    try:
                        reisift_payload = {
                            "search": reisift_add_request["search"],
                            "status": "new_lead",
                            "lists": reisift_add_request["lists"],
                            "tags": reisift_add_request["tags"],
                            "notes": reisift_add_request["notes"],
                        }
                        reisift_add_result = create_reisift_property_from_search(reisift_payload)
                        created_row = reisift_add_result.get("created") or {}
                        created_uuid = (created_row.get("uuid") or created_row.get("id") or "").strip()
                        notice = f"Property pushed to ReiSift successfully. map_id={reisift_add_result.get('map_id') or '-'}"
                        if created_uuid:
                            notice += f", uuid={created_uuid}"
                    except Exception as exc:
                        error_notice = f"ReiSift add-property failed: {exc}"
                        log_app_error(
                            db,
                            source="reisift_add_property",
                            error_message=str(exc),
                            details=traceback.format_exc(),
                            route="/settings",
                            status_code=500,
                        )
            elif helper_action == "import_referral_bundle":
                try:
                    realtor_rows = _parse_uploaded_json_rows(request.files.get("referral_realtors_file"))
                    referral_rows = _parse_uploaded_json_rows(request.files.get("reisift_referrals_file"))
                    push_rows = _parse_uploaded_json_rows(request.files.get("referral_push_activity_file"))
                    if not (realtor_rows or referral_rows or push_rows):
                        raise ValueError("Upload at least one JSON file to import.")
                    counts = import_referral_bundle_rows(db, realtor_rows, referral_rows, push_rows)
                    notice = (
                        "Referral bundle imported. "
                        f"Realtors: {counts['referral_realtors']}, "
                        f"Referrals: {counts['reisift_referrals']}, "
                        f"Push Activity: {counts['referral_push_activity']}."
                    )
                except Exception as exc:
                    db.rollback()
                    error_notice = f"Referral bundle import failed: {exc}"
                    log_app_error(
                        db,
                        source="import_referral_bundle",
                        error_message=str(exc),
                        details=traceback.format_exc(),
                        route="/settings",
                        status_code=500,
                    )
        db.commit()

    settings = get_direct_mail_settings(db)
    email_settings = get_email_settings(db)
    slack_settings = get_slack_settings(db)
    automation_settings = get_automation_settings(db)
    integration_api_key = get_integration_api_key(db)
    deep_dive_smrtphone_from = get_setting(db, "deep_dive_smrtphone_from", SMRTPHONE_FROM_NUMBER)
    referral_smrtphone_from = get_setting(db, "referral_smrtphone_from", SMRTPHONE_FROM_NUMBER)
    postage_options = []
    envelope_options = []
    options_error = ""
    if active_tab == "direct_mail":
        try:
            options = get_template_product_options(OPENLETTERCONNECT_TEMPLATE_ID)
            postage_options = options.get("postage_options") or []
            envelope_options = options.get("envelope_options") or []
        except Exception as exc:
            options_error = str(exc)
            log_app_error(
                db,
                source="direct_mail_options",
                error_message=str(exc),
                details=traceback.format_exc(),
                route="/settings",
                status_code=500,
            )
            db.commit()
    recent_errors = db.execute(
        """
        SELECT id, source, route, status_code, error_message, created_at
        FROM app_errors
        ORDER BY id DESC
        LIMIT 15
        """
    ).fetchall()
    clever_webhook_notes = db.execute(
        """
        SELECT id, event_key, note_body, created_at
        FROM clever_lead_webhook_notes
        ORDER BY id DESC
        LIMIT 50
        """
    ).fetchall()
    return render_template(
        "settings.html",
        dm=settings,
        email_settings=email_settings,
        slack_settings=slack_settings,
        automation_settings=automation_settings,
        integration_api_key=integration_api_key,
        active_tab=active_tab,
        deep_dive_smrtphone_from=deep_dive_smrtphone_from,
        referral_smrtphone_from=referral_smrtphone_from,
        notice=notice,
        postage_options=postage_options,
        envelope_options=envelope_options,
        options_error=options_error,
        error_notice=error_notice,
        recent_errors=recent_errors,
        clever_webhook_notes=clever_webhook_notes,
        clever_poll_minutes=max(int(GOOGLE_SHEETS_POLL_SECONDS / 60), 1),
        template_id=OPENLETTERCONNECT_TEMPLATE_ID,
        market_helper_address=market_helper_address,
        market_helper_result=market_helper_result,
        reisift_add_result=reisift_add_result,
        reisift_add_request=reisift_add_request,
    )


@app.route("/sequences", methods=["GET", "POST"])
def sequences_page():
    ensure_db()
    db = get_db()
    notice = ""
    error_notice = (request.args.get("error") or "").strip()
    if request.method == "POST":
        try:
            name = (request.form.get("name") or "").strip()
            if not name:
                raise ValueError("Campaign name is required")
            description = (request.form.get("description") or "").strip()
            stop_on_reply = 1 if (request.form.get("stop_on_reply") or "1") in {"1", "true", "on", "yes"} else 0
            send_window_start = (request.form.get("send_window_start") or "10:00").strip()
            send_window_end = (request.form.get("send_window_end") or "16:30").strip()
            tz = (request.form.get("timezone") or "America/New_York").strip()
            parsed = extract_steps_from_form(request.form, "new_step_")
            if not parsed:
                raise ValueError("At least one sequence step is required")

            cur = db.execute(
                """
                INSERT INTO sequence_campaigns (name, description, status, stop_on_reply, send_window_start, send_window_end, timezone)
                VALUES (?, ?, 'Active', ?, ?, ?, ?)
                """,
                (name, description, stop_on_reply, send_window_start, send_window_end, tz),
            )
            campaign_id = cur.lastrowid
            for step in parsed:
                db.execute(
                    """
                    INSERT INTO sequence_steps (campaign_id, step_order, delay_minutes, channel, subject_template, body_template, is_active)
                    VALUES (?, ?, ?, ?, ?, ?, 1)
                    """,
                    (
                        campaign_id,
                        step["order"],
                        step["delay_minutes"],
                        step["channel"],
                        step["subject_template"],
                        step["body_template"],
                    ),
                )
            db.commit()
            notice = "Sequence campaign created."
        except Exception as exc:
            db.rollback()
            error_notice = str(exc)

    campaigns = get_sequence_campaigns(db, only_active=False)
    campaign_rows = []
    for c in campaigns:
        steps = get_sequence_steps(db, c["id"])
        steps_view = []
        for s in steps:
            step_dict = dict(s)
            step_dict["delay_hours"] = round((int(s["delay_minutes"] or 0)) / 60.0, 4)
            steps_view.append(step_dict)
        active_count = db.execute(
            "SELECT COUNT(*) AS c FROM sequence_enrollments WHERE campaign_id = ? AND status = 'Active'",
            (c["id"],),
        ).fetchone()["c"]
        campaign_rows.append({"campaign": c, "steps": steps_view, "active_count": active_count})
    property_options = db.execute(
        """
        SELECT p.id, a.street, a.city, a.state, a.postal_code
        FROM properties p
        JOIN addresses a ON a.id = p.property_address_id
        ORDER BY p.created_at DESC, p.id DESC
        LIMIT 500
        """
    ).fetchall()
    person_options = db.execute(
        "SELECT id, first_name, last_name FROM people ORDER BY last_name, first_name LIMIT 1500"
    ).fetchall()
    return render_template(
        "sequences.html",
        campaign_rows=campaign_rows,
        property_options=property_options,
        person_options=person_options,
        notice=notice,
        error_notice=error_notice,
    )


@app.route("/sequences/<int:campaign_id>/update", methods=["POST"])
def update_sequence_campaign(campaign_id):
    ensure_db()
    db = get_db()
    try:
        row = db.execute("SELECT id FROM sequence_campaigns WHERE id = ?", (campaign_id,)).fetchone()
        if not row:
            raise ValueError("Campaign not found")
        name = (request.form.get("name") or "").strip()
        if not name:
            raise ValueError("Campaign name is required")
        description = (request.form.get("description") or "").strip()
        stop_on_reply = 1 if (request.form.get("stop_on_reply") or "1") in {"1", "true", "on", "yes"} else 0
        send_window_start = (request.form.get("send_window_start") or "10:00").strip()
        send_window_end = (request.form.get("send_window_end") or "16:30").strip()
        tz = (request.form.get("timezone") or "America/New_York").strip()
        parsed = extract_steps_from_form(request.form, "edit_step_")
        if not parsed:
            raise ValueError("At least one sequence step is required")
        db.execute(
            """
            UPDATE sequence_campaigns
            SET name = ?, description = ?, stop_on_reply = ?, send_window_start = ?, send_window_end = ?, timezone = ?
            WHERE id = ?
            """,
            (name, description, stop_on_reply, send_window_start, send_window_end, tz, campaign_id),
        )
        db.execute("DELETE FROM sequence_steps WHERE campaign_id = ?", (campaign_id,))
        for step in parsed:
            db.execute(
                """
                INSERT INTO sequence_steps (campaign_id, step_order, delay_minutes, channel, subject_template, body_template, is_active)
                VALUES (?, ?, ?, ?, ?, ?, 1)
                """,
                (
                    campaign_id,
                    step["order"],
                    step["delay_minutes"],
                    step["channel"],
                    step["subject_template"],
                    step["body_template"],
                ),
            )
        db.commit()
        return redirect(url_for("sequences_page"))
    except Exception as exc:
        db.rollback()
        return redirect(url_for("sequences_page", error=str(exc)))


@app.route("/sequences/<int:campaign_id>/toggle", methods=["POST"])
def toggle_sequence_campaign(campaign_id):
    ensure_db()
    db = get_db()
    row = db.execute("SELECT status FROM sequence_campaigns WHERE id = ?", (campaign_id,)).fetchone()
    if row:
        next_status = "Paused" if (row["status"] or "Active") == "Active" else "Active"
        db.execute("UPDATE sequence_campaigns SET status = ? WHERE id = ?", (next_status, campaign_id))
        db.commit()
    return redirect(url_for("sequences_page"))


@app.route("/api/sequences/<int:campaign_id>/preview", methods=["POST"])
def preview_sequence_campaign(campaign_id):
    ensure_db()
    db = get_db()
    payload = request.get_json(force=True)
    property_id = payload.get("property_id")
    person_id = payload.get("person_id")
    if not str(property_id).isdigit() or not str(person_id).isdigit():
        return jsonify({"error": "property_id and person_id are required"}), 400
    property_id = int(property_id)
    person_id = int(person_id)
    campaign = db.execute("SELECT * FROM sequence_campaigns WHERE id = ?", (campaign_id,)).fetchone()
    if not campaign:
        return jsonify({"error": "Campaign not found"}), 404
    person = db.execute("SELECT * FROM people WHERE id = ?", (person_id,)).fetchone()
    prop = db.execute(
        """
        SELECT p.id, p.owner_person_id, a.street, a.city, a.state, a.postal_code
        FROM properties p
        JOIN addresses a ON a.id = p.property_address_id
        WHERE p.id = ?
        """,
        (property_id,),
    ).fetchone()
    if not person or not prop:
        return jsonify({"error": "Person or property not found"}), 404
    owner = db.execute("SELECT * FROM people WHERE id = ?", (prop["owner_person_id"],)).fetchone() if prop["owner_person_id"] else None
    steps = get_sequence_steps(db, campaign_id)
    sms_targets, sms_skipped = get_sequence_sms_targets(db, person_id)
    email_targets, email_skipped = get_sequence_email_targets(db, person_id)
    rendered = []
    for s in steps:
        rendered.append(
            {
                "step_order": s["step_order"],
                "delay_hours": round((int(s["delay_minutes"] or 0)) / 60.0, 4),
                "channel": (s["channel"] or "").upper(),
                "subject": render_sequence_template(s["subject_template"] or "", person, prop, owner),
                "body": render_sequence_template(s["body_template"] or "", person, prop, owner),
            }
        )
    return jsonify(
        {
            "campaign": {"id": campaign["id"], "name": campaign["name"]},
            "person": {"id": person["id"], "name": f"{person['first_name']} {person['last_name']}".strip()},
            "property": {
                "id": prop["id"],
                "address": f"{prop['street']}, {prop['city']}, {prop['state']} {prop['postal_code']}".strip(),
            },
            "targets": {
                "sms_mobile": sms_targets,
                "sms_skipped": sms_skipped,
                "email": email_targets,
                "email_skipped": email_skipped,
            },
            "steps": rendered,
        }
    )


def _property_sequence_targets(db, property_id):
    prop = db.execute("SELECT owner_person_id FROM properties WHERE id = ?", (property_id,)).fetchone()
    if not prop:
        return []
    ids = []
    if prop["owner_person_id"]:
        ids.append(int(prop["owner_person_id"]))
        rels = db.execute(
            "SELECT related_person_id FROM person_relationships WHERE subject_person_id = ? ORDER BY id DESC",
            (prop["owner_person_id"],),
        ).fetchall()
        for r in rels:
            pid = int(r["related_person_id"])
            if pid not in ids:
                ids.append(pid)
    return ids


@app.route("/property/<int:property_id>/sequences/enroll-network", methods=["POST"])
def enroll_property_network_sequence(property_id):
    ensure_db()
    db = get_db()
    campaign_id = (request.form.get("campaign_id") or "").strip()
    if not campaign_id.isdigit():
        return redirect(url_for("property_detail", property_id=property_id))
    for attempt in range(3):
        try:
            created = 0
            skipped = 0
            targets = _property_sequence_targets(db, property_id)
            for pid in targets:
                try:
                    _, is_new = enroll_person_in_sequence(db, int(campaign_id), property_id, pid)
                    if is_new:
                        created += 1
                except ValueError:
                    skipped += 1
            db.commit()
            return redirect(url_for("property_detail", property_id=property_id, seq_notice=f"Sequence enrolled for {created} contact(s). Skipped: {skipped}."))
        except sqlite3.OperationalError as exc:
            db.rollback()
            if "locked" in str(exc).lower() and attempt < 2:
                time.sleep(0.35 * (attempt + 1))
                continue
            raise


@app.route("/property/<int:property_id>/sequences/enroll-person", methods=["POST"])
def enroll_property_person_sequence(property_id):
    ensure_db()
    db = get_db()
    campaign_id = (request.form.get("campaign_id") or "").strip()
    person_id = (request.form.get("person_id") or "").strip()
    if campaign_id.isdigit() and person_id.isdigit():
        for attempt in range(3):
            try:
                enroll_person_in_sequence(db, int(campaign_id), property_id, int(person_id))
                db.commit()
                return redirect(url_for("property_detail", property_id=property_id, seq_notice="Sequence enrollment updated."))
            except ValueError as exc:
                db.rollback()
                return redirect(url_for("property_detail", property_id=property_id, seq_notice=str(exc)))
            except sqlite3.OperationalError as exc:
                db.rollback()
                if "locked" in str(exc).lower() and attempt < 2:
                    time.sleep(0.35 * (attempt + 1))
                    continue
                raise
    return redirect(url_for("property_detail", property_id=property_id))


@app.route("/person/<int:person_id>/sequences/enroll", methods=["POST"])
def enroll_person_sequence_route(person_id):
    ensure_db()
    db = get_db()
    campaign_id = (request.form.get("campaign_id") or "").strip()
    property_id = (request.form.get("property_id") or "").strip()
    if campaign_id.isdigit() and property_id.isdigit():
        for attempt in range(3):
            try:
                enroll_person_in_sequence(db, int(campaign_id), int(property_id), person_id)
                db.commit()
                return redirect(url_for("person_detail", person_id=person_id, property_id=property_id, seq_notice="Sequence enrollment updated."))
            except ValueError as exc:
                db.rollback()
                return redirect(url_for("person_detail", person_id=person_id, property_id=property_id, seq_notice=str(exc)))
            except sqlite3.OperationalError as exc:
                db.rollback()
                if "locked" in str(exc).lower() and attempt < 2:
                    time.sleep(0.35 * (attempt + 1))
                    continue
                raise
    return redirect(url_for("person_detail", person_id=person_id, property_id=property_id))


@app.route("/sequences/enrollments/<int:enrollment_id>/stop", methods=["POST"])
def stop_sequence_enrollment(enrollment_id):
    ensure_db()
    db = get_db()
    redirect_to = (request.form.get("redirect_to") or "").strip()
    db.execute(
        "UPDATE sequence_enrollments SET status = 'Stopped', completed_at = ?, stopped_reason = ? WHERE id = ?",
        (format_db_time(datetime.utcnow()), "Manually stopped", enrollment_id),
    )
    db.commit()
    if redirect_to == "person":
        pid = (request.form.get("person_id") or "").strip()
        prop = (request.form.get("property_id") or "").strip()
        return redirect(url_for("person_detail", person_id=pid, property_id=prop, seq_notice="Sequence enrollment stopped."))
    prop = (request.form.get("property_id") or "").strip()
    if str(prop).isdigit():
        return redirect(url_for("property_detail", property_id=prop, seq_notice="Sequence enrollment stopped."))
    return redirect(url_for("sequences_page"))


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
        middle_name=request.form.get("owner_middle_name", ""),
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


@app.route("/templates/referral-realtors-template.csv")
def referral_realtors_template():
    template = (
        "Realtor First,Last Name,Email,Phone,Brokerage,Target Market\n"
        "Jane,Doe,jane@broker.com,(973)555-1212,ABC Realty,\"Essex, Union\"\n"
        "John,Smith,john@homes.com,(201)555-3434,North Homes,Bergen\n"
    )
    return app.response_class(
        template,
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=referral-realtors-template.csv"},
    )


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
    owner_email_options = []

    if prop["owner_person_id"]:
        owner = db.execute("SELECT * FROM people WHERE id = ?", (prop["owner_person_id"],)).fetchone()
        owner_touchpoints = db.execute(
            """
            SELECT * FROM touchpoints
            WHERE person_id = ? AND lower(channel_type) IN ('phone', 'email')
            ORDER BY
                CASE
                    WHEN lower(channel_type) = 'phone' THEN 0
                    ELSE 1
                END,
                CASE
                    WHEN lower(channel_type) = 'phone' AND lower(channel_label) = 'mobile' THEN 0
                    WHEN lower(channel_type) = 'phone' AND lower(channel_label) = 'landline' THEN 1
                    WHEN lower(channel_type) = 'phone' AND lower(channel_label) = 'voip' THEN 2
                    WHEN lower(channel_type) = 'phone' AND lower(channel_label) = 'fax' THEN 3
                    ELSE 4
                END,
                created_at DESC,
                id DESC
            """,
            (prop["owner_person_id"],),
        ).fetchall()
        owner_relationships = db.execute(
            """
            SELECT r.id AS relationship_id, r.relationship_type, r.note, r.relationship_order,
                   p.id AS person_id, p.first_name, p.middle_name, p.last_name, p.outreach_status, p.deceased,
                   CASE WHEN EXISTS (SELECT 1 FROM skiptrace_runs sr WHERE sr.person_id = p.id) THEN 1 ELSE 0 END AS skip_traced
            FROM person_relationships r
            JOIN people p ON p.id = r.related_person_id
            WHERE r.subject_person_id = ?
            ORDER BY
                CASE WHEN COALESCE(p.deceased, 0) = 1 THEN 1 ELSE 0 END,
                CASE WHEN COALESCE(r.relationship_order, 0) <= 0 THEN 1 ELSE 0 END,
                r.relationship_order ASC,
                p.last_name ASC,
                p.first_name ASC,
                r.id ASC
            """,
            (prop["owner_person_id"],),
        ).fetchall()
        owner_phone_options = get_manual_sms_targets_for_person(db, prop["owner_person_id"])
        owner_email_options = get_manual_email_targets_for_person(db, prop["owner_person_id"])
        if owner_phone_options:
            owner_primary_phone = owner_phone_options[0]

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
    property_comm_summary_rows = []
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
    summary_people = []
    if owner:
        summary_people.append(
            {
                "person_id": int(owner["id"]),
                "full_name": person_name(owner),
                "relationship_type": "Owner",
            }
        )
    for rel in owner_relationships:
        summary_people.append(
            {
                "person_id": int(rel["person_id"]),
                "full_name": person_name(rel),
                "relationship_type": (rel["relationship_type"] or "Unknown"),
            }
        )
    dedup_summary_people = []
    seen_summary_ids = set()
    for row in summary_people:
        pid = int(row["person_id"])
        if pid in seen_summary_ids:
            continue
        seen_summary_ids.add(pid)
        dedup_summary_people.append(row)
    counts_map = communication_counts_for_people(db, [r["person_id"] for r in dedup_summary_people], property_id=property_id)
    for row in dedup_summary_people:
        c = counts_map.get(int(row["person_id"]), {})
        property_comm_summary_rows.append(
            {
                **row,
                "sms_outbound": int(c.get("sms_outbound", 0)),
                "sms_inbound": int(c.get("sms_inbound", 0)),
                "email_outbound": int(c.get("email_outbound", 0)),
                "email_inbound": int(c.get("email_inbound", 0)),
            }
        )
    sequence_campaigns = get_sequence_campaigns(db, only_active=True)
    target_ids = _property_sequence_targets(db, property_id)
    sequence_targets = []
    if target_ids:
        placeholders = ",".join(["?"] * len(target_ids))
        sequence_targets = db.execute(
            f"SELECT id, first_name, last_name FROM people WHERE id IN ({placeholders}) ORDER BY last_name, first_name",
            tuple(target_ids),
        ).fetchall()
    sequence_enrollments = db.execute(
        """
        SELECT e.*, c.name AS campaign_name, p.first_name, p.last_name
        FROM sequence_enrollments e
        JOIN sequence_campaigns c ON c.id = e.campaign_id
        JOIN people p ON p.id = e.person_id
        WHERE e.property_id = ?
        ORDER BY e.created_at DESC, e.id DESC
        LIMIT 100
        """,
        (property_id,),
    ).fetchall()

    return render_template(
        "property_detail.html",
        prop=prop,
        owner=owner,
        owner_touchpoints=owner_touchpoints,
        owner_primary_phone=owner_primary_phone,
        owner_phone_options=owner_phone_options,
        owner_email_options=owner_email_options,
        owner_relationships=owner_relationships,
        all_people=all_people,
        related_people_options=related_people_options,
        sms_rollup=sms_rollup,
        property_comm_summary_rows=property_comm_summary_rows,
        verified_contacts=verified_contacts,
        touchpoints=touchpoints,
        socials=socials,
        activities=activities,
        obituary_imports=obituary_imports,
        communications=communications,
        smrt_from_number=get_deep_dive_sms_number(db),
        social_url=normalize_social_profile_url,
        person_name=person_name,
        sequence_campaigns=sequence_campaigns,
        sequence_targets=sequence_targets,
        sequence_enrollments=sequence_enrollments,
        seq_notice=(request.args.get("seq_notice") or "").strip(),
    )

@app.route("/people", methods=["POST"])
def create_person_route():
    ensure_db()
    db = get_db()
    person_id = create_person(
        db,
        request.form.get("first_name", "Unknown"),
        request.form.get("last_name", "Person"),
        middle_name=request.form.get("middle_name", ""),
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
        """
        SELECT *
        FROM touchpoints
        WHERE person_id = ?
        ORDER BY
            CASE
                WHEN lower(channel_type) = 'phone' THEN 0
                ELSE 1
            END,
            CASE
                WHEN lower(channel_type) = 'phone' AND lower(channel_label) = 'mobile' THEN 0
                WHEN lower(channel_type) = 'phone' AND lower(channel_label) = 'landline' THEN 1
                WHEN lower(channel_type) = 'phone' AND lower(channel_label) = 'voip' THEN 2
                WHEN lower(channel_type) = 'phone' AND lower(channel_label) = 'fax' THEN 3
                ELSE 4
            END,
            created_at DESC,
            id DESC
        """,
        (person_id,),
    ).fetchall()
    socials = db.execute(
        "SELECT * FROM social_accounts WHERE person_id = ? ORDER BY created_at DESC", (person_id,)
    ).fetchall()
    person_primary_phone = ""
    person_phone_options = []
    person_email_options = []
    person_phone_options = get_manual_sms_targets_for_person(db, person_id)
    person_email_options = get_manual_email_targets_for_person(db, person_id)
    if person_phone_options:
        person_primary_phone = person_phone_options[0]

    relationship_rows = db.execute(
        """
        SELECT r.id AS relationship_id, r.relationship_type, r.note, r.relationship_order,
               p.id AS person_id, p.first_name, p.middle_name, p.last_name, p.outreach_status, p.deceased,
               CASE WHEN EXISTS (SELECT 1 FROM skiptrace_runs sr WHERE sr.person_id = p.id) THEN 1 ELSE 0 END AS skip_traced
        FROM person_relationships r
        JOIN people p ON p.id = r.related_person_id
        WHERE r.subject_person_id = ?
        ORDER BY
            CASE WHEN COALESCE(p.deceased, 0) = 1 THEN 1 ELSE 0 END,
            CASE WHEN COALESCE(r.relationship_order, 0) <= 0 THEN 1 ELSE 0 END,
            r.relationship_order ASC,
            p.last_name ASC,
            p.first_name ASC,
            r.id ASC
        """,
        (person_id,),
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
                   op.first_name AS owner_first, op.middle_name AS owner_middle, op.last_name AS owner_last
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
    default_property_context = property_context
    if default_property_id is None:
        direct_prop = db.execute(
            "SELECT id FROM properties WHERE owner_person_id = ? OR resident_person_id = ? ORDER BY created_at DESC LIMIT 1",
            (person_id, person_id),
        ).fetchone()
        if direct_prop:
            default_property_id = direct_prop["id"]
            default_property_context = db.execute(
                """
                SELECT p.id, a.street, a.city, a.state, a.postal_code,
                       op.first_name AS owner_first, op.middle_name AS owner_middle, op.last_name AS owner_last
                FROM properties p
                JOIN addresses a ON a.id = p.property_address_id
                LEFT JOIN people op ON op.id = p.owner_person_id
                WHERE p.id = ?
                """,
                (default_property_id,),
            ).fetchone()

    context_relationship = None
    if default_property_context and default_property_context["id"]:
        owner_row = db.execute("SELECT owner_person_id FROM properties WHERE id = ?", (default_property_context["id"],)).fetchone()
        owner_pid = owner_row["owner_person_id"] if owner_row else None
        if owner_pid and int(owner_pid) != int(person_id):
            context_relationship = db.execute(
                """
                SELECT id, relationship_type
                FROM person_relationships
                WHERE subject_person_id = ? AND related_person_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (owner_pid, person_id),
            ).fetchone()

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
    sequence_campaigns = get_sequence_campaigns(db, only_active=True)
    person_enrollments = db.execute(
        """
        SELECT e.*, c.name AS campaign_name, a.street, a.city, a.state, a.postal_code
        FROM sequence_enrollments e
        JOIN sequence_campaigns c ON c.id = e.campaign_id
        JOIN properties pr ON pr.id = e.property_id
        JOIN addresses a ON a.id = pr.property_address_id
        WHERE e.person_id = ?
        ORDER BY e.created_at DESC, e.id DESC
        LIMIT 100
        """,
        (person_id,),
    ).fetchall()
    skiptrace_origin = db.execute(
        """
        SELECT r.subject_person_id,
               sp.first_name AS source_first_name,
               sp.middle_name AS source_middle_name,
               sp.last_name AS source_last_name
        FROM person_relationships r
        JOIN people sp ON sp.id = r.subject_person_id
        WHERE r.related_person_id = ?
          AND r.note LIKE '%source:skiptrace-person-import%'
        ORDER BY r.id DESC
        LIMIT 1
        """,
        (person_id,),
    ).fetchone()

    dedup_person_summary = [
        {
            "person_id": int(person["id"]),
            "full_name": person_name(person),
            "relationship_type": "Self",
        }
    ]
    person_counts_map = communication_counts_for_people(
        db,
        [r["person_id"] for r in dedup_person_summary],
        property_id=(default_property_id if default_property_id else None),
    )
    person_comm_summary_rows = []
    for row in dedup_person_summary:
        c = person_counts_map.get(int(row["person_id"]), {})
        person_comm_summary_rows.append(
            {
                **row,
                "sms_outbound": int(c.get("sms_outbound", 0)),
                "sms_inbound": int(c.get("sms_inbound", 0)),
                "email_outbound": int(c.get("email_outbound", 0)),
                "email_inbound": int(c.get("email_inbound", 0)),
            }
        )

    return render_template(
        "person_detail.html",
        person=person,
        touchpoints=touchpoints,
        person_primary_phone=person_primary_phone,
        person_phone_options=person_phone_options,
        person_email_options=person_email_options,
        socials=socials,
        relationship_rows=relationship_rows,
        addresses=addresses,
        person_notes=person_notes,
        communications=communications,
        property_context=property_context,
        default_property_context=default_property_context,
        default_property_id=default_property_id,
        context_relationship=context_relationship,
        skiptrace_origin=skiptrace_origin,
        smrt_from_number=get_deep_dive_sms_number(db),
        social_url=normalize_social_profile_url,
        sequence_campaigns=sequence_campaigns,
        person_enrollments=person_enrollments,
        person_comm_summary_rows=person_comm_summary_rows,
        seq_notice=(request.args.get("seq_notice") or "").strip(),
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


def normalize_relationship_order(db, subject_person_id):
    rows = db.execute(
        """
        SELECT id
        FROM person_relationships
        WHERE subject_person_id = ?
        ORDER BY
            CASE WHEN COALESCE(relationship_order, 0) <= 0 THEN 1 ELSE 0 END,
            relationship_order ASC,
            id ASC
        """,
        (subject_person_id,),
    ).fetchall()
    order_val = 10
    for r in rows:
        db.execute(
            "UPDATE person_relationships SET relationship_order = ? WHERE id = ?",
            (order_val, r["id"]),
        )
        order_val += 10


@app.route("/api/relationships/<int:relationship_id>", methods=["PATCH"])
def update_relationship(relationship_id):
    ensure_db()
    db = get_db()
    payload = request.get_json(force=True)
    relationship_type = (payload.get("relationship_type") or "").strip()
    if not relationship_type:
        return jsonify({"error": "relationship_type is required"}), 400
    rel = db.execute("SELECT id FROM person_relationships WHERE id = ?", (relationship_id,)).fetchone()
    if not rel:
        return jsonify({"error": "relationship not found"}), 404
    db.execute(
        "UPDATE person_relationships SET relationship_type = ? WHERE id = ?",
        (relationship_type, relationship_id),
    )
    db.commit()
    return jsonify({"ok": True})


@app.route("/api/properties/<int:property_id>/relationships/<int:relationship_id>/move", methods=["PATCH"])
def move_relationship(property_id, relationship_id):
    ensure_db()
    db = get_db()
    payload = request.get_json(force=True)
    direction = (payload.get("direction") or "").strip().lower()
    if direction not in {"up", "down"}:
        return jsonify({"error": "direction must be 'up' or 'down'"}), 400
    prop = db.execute("SELECT owner_person_id FROM properties WHERE id = ?", (property_id,)).fetchone()
    if not prop or not prop["owner_person_id"]:
        return jsonify({"error": "property owner not found"}), 404
    subject_person_id = int(prop["owner_person_id"])
    rel = db.execute(
        "SELECT id FROM person_relationships WHERE id = ? AND subject_person_id = ?",
        (relationship_id, subject_person_id),
    ).fetchone()
    if not rel:
        return jsonify({"error": "relationship not found"}), 404

    normalize_relationship_order(db, subject_person_id)
    rows = db.execute(
        """
        SELECT id, relationship_order
        FROM person_relationships
        WHERE subject_person_id = ?
        ORDER BY relationship_order ASC, id ASC
        """,
        (subject_person_id,),
    ).fetchall()
    ids = [r["id"] for r in rows]
    idx = ids.index(relationship_id)
    swap_idx = idx - 1 if direction == "up" else idx + 1
    if swap_idx < 0 or swap_idx >= len(rows):
        return jsonify({"ok": True, "moved": False})
    current = rows[idx]
    target = rows[swap_idx]
    db.execute(
        "UPDATE person_relationships SET relationship_order = ? WHERE id = ?",
        (target["relationship_order"], current["id"]),
    )
    db.execute(
        "UPDATE person_relationships SET relationship_order = ? WHERE id = ?",
        (current["relationship_order"], target["id"]),
    )
    db.commit()
    return jsonify({"ok": True, "moved": True})


@app.route("/api/relationships/<int:relationship_id>", methods=["DELETE"])
def delete_relationship(relationship_id):
    ensure_db()
    db = get_db()
    rel = db.execute("SELECT id FROM person_relationships WHERE id = ?", (relationship_id,)).fetchone()
    if not rel:
        return jsonify({"error": "relationship not found"}), 404
    db.execute("DELETE FROM person_relationships WHERE id = ?", (relationship_id,))
    db.commit()
    return jsonify({"ok": True})


@app.route("/api/people/<int:person_id>/name", methods=["PATCH"])
def update_person_name(person_id):
    ensure_db()
    db = get_db()
    payload = request.get_json(force=True)
    first_name = (payload.get("first_name") or "").strip()
    middle_name = (payload.get("middle_name") or "").strip()
    last_name = (payload.get("last_name") or "").strip()
    if not first_name or not last_name:
        return jsonify({"error": "first_name and last_name are required"}), 400
    first_name, middle_name, last_name = normalize_first_middle_last(first_name, middle_name, last_name)

    db.execute(
        "UPDATE people SET first_name = ?, middle_name = ?, last_name = ? WHERE id = ?",
        (first_name, middle_name, last_name, person_id),
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
    from_number = (payload.get("from_number") or get_deep_dive_sms_number(db)).strip()
    external_id = (payload.get("external_id") or "").strip()
    subject = (payload.get("subject") or "").strip()
    open_tracking_token = ""
    open_tracking_url = ""
    if channel == "EMAIL":
        from_number = "Email sent via Gmail"

    if channel == "SMS" and direction == "Outbound":
        if not to_number:
            return jsonify({"error": "to_number is required for SMS"}), 400
        allowed, reason = validate_sms_recipient_for_person(db, person_id, to_number)
        if not allowed:
            return jsonify({"error": f"SMS blocked: {reason}"}), 400
        try:
            send_result = send_smrtphone_sms(to_number, body, from_number=from_number)
            if send_result.get("status"):
                status = send_result["status"]
            external_id = send_result.get("sms_id") or external_id
            update_person_outreach_status_for_sms(db, person_id, "outbound_success")
        except Exception as exc:
            apply_touchpoint_status_inference(db, to_number, str(exc))
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
    elif channel == "EMAIL" and direction == "Outbound":
        ok_email, email_reason = validate_email_recipient_for_person(db, person_id, to_number)
        if not ok_email:
            return jsonify({"error": f"Email blocked: {email_reason}"}), 400
        try:
            open_tracking_token = generate_open_tracking_token()
            open_tracking_url = build_open_tracking_url(open_tracking_token)
            sent = send_gmail_email(
                db,
                to_email=to_number,
                subject=subject,
                body=body,
                property_id=property_id,
                person_id=person_id,
                open_tracking_url=open_tracking_url,
            )
            external_id = sent.get("message_id") or external_id
            status = "Sent"
            from_number = "Email sent via Gmail"
        except Exception as exc:
            status = "Failed"
            cur = db.execute(
                """
                INSERT INTO communications (property_id, person_id, channel, direction, from_number, to_number, body, status, is_read, sent_at, external_id, open_tracking_token)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    open_tracking_token,
                ),
            )
            log_app_error(
                db,
                source="email_send_property",
                error_message=str(exc),
                details=traceback.format_exc(),
                route=request.path,
                status_code=502,
            )
            db.commit()
            row = db.execute("SELECT * FROM communications WHERE id = ?", (cur.lastrowid,)).fetchone()
            return jsonify({"error": str(exc), "communication": dict(row)}), 502

    cur = db.execute(
        """
        INSERT INTO communications (property_id, person_id, channel, direction, from_number, to_number, body, status, is_read, sent_at, external_id, open_tracking_token)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            open_tracking_token,
        ),
    )
    if channel == "EMAIL" and direction == "Outbound":
        try:
            backfill_outbound_gmail_ids(db, cur.lastrowid)
        except Exception as exc:
            log_app_error(
                db,
                source="email_backfill_property",
                error_message=str(exc),
                details=traceback.format_exc(),
                route=request.path,
                status_code=500,
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


@app.route("/api/people/<int:person_id>/communication-targets", methods=["GET"])
def get_person_communication_targets(person_id):
    ensure_db()
    db = get_db()
    person = db.execute("SELECT id FROM people WHERE id = ?", (person_id,)).fetchone()
    if not person:
        return jsonify({"error": "Person not found"}), 404
    sms_targets = get_manual_sms_targets_for_person(db, person_id)
    email_targets = get_manual_email_targets_for_person(db, person_id)
    return jsonify({"sms": sms_targets, "email": email_targets})


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
    from_number = (payload.get("from_number") or get_deep_dive_sms_number(db)).strip()
    external_id = (payload.get("external_id") or "").strip()
    subject = (payload.get("subject") or "").strip()
    open_tracking_token = ""
    open_tracking_url = ""
    if channel == "EMAIL":
        from_number = "Email sent via Gmail"

    if channel == "SMS" and direction == "Outbound":
        if not to_number:
            return jsonify({"error": "to_number is required for SMS"}), 400
        allowed, reason = validate_sms_recipient_for_person(db, person_id, to_number)
        if not allowed:
            return jsonify({"error": f"SMS blocked: {reason}"}), 400
        try:
            send_result = send_smrtphone_sms(to_number, body, from_number=from_number)
            if send_result.get("status"):
                status = send_result["status"]
            external_id = send_result.get("sms_id") or external_id
            update_person_outreach_status_for_sms(db, person_id, "outbound_success")
        except Exception as exc:
            apply_touchpoint_status_inference(db, to_number, str(exc))
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
    elif channel == "EMAIL" and direction == "Outbound":
        ok_email, email_reason = validate_email_recipient_for_person(db, person_id, to_number)
        if not ok_email:
            return jsonify({"error": f"Email blocked: {email_reason}"}), 400
        try:
            open_tracking_token = generate_open_tracking_token()
            open_tracking_url = build_open_tracking_url(open_tracking_token)
            sent = send_gmail_email(
                db,
                to_email=to_number,
                subject=subject,
                body=body,
                property_id=property_id,
                person_id=person_id,
                open_tracking_url=open_tracking_url,
            )
            external_id = sent.get("message_id") or external_id
            status = "Sent"
            from_number = "Email sent via Gmail"
        except ValueError as exc:
            status = "Failed"
            cur = db.execute(
                """
                INSERT INTO communications (property_id, person_id, channel, direction, from_number, to_number, body, status, is_read, sent_at, external_id, open_tracking_token)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    open_tracking_token,
                ),
            )
            log_app_error(
                db,
                source="email_send_person",
                error_message=str(exc),
                details=traceback.format_exc(),
                route=request.path,
                status_code=502,
            )
            db.commit()
            row = db.execute("SELECT * FROM communications WHERE id = ?", (cur.lastrowid,)).fetchone()
            return jsonify({"error": str(exc), "communication": dict(row)}), 502

    cur = db.execute(
        """
        INSERT INTO communications (property_id, person_id, channel, direction, from_number, to_number, body, status, is_read, sent_at, external_id, open_tracking_token)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            open_tracking_token,
        ),
    )
    if channel == "EMAIL" and direction == "Outbound":
        try:
            backfill_outbound_gmail_ids(db, cur.lastrowid)
        except Exception as exc:
            log_app_error(
                db,
                source="email_backfill_person",
                error_message=str(exc),
                details=traceback.format_exc(),
                route=request.path,
                status_code=500,
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
        SELECT id, person_id, value, channel_label, status
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
        if not is_sms_touchpoint_allowed(r["channel_label"], r["status"]):
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
    if int(person["deceased"] or 0) == 1:
        return jsonify({"error": "Cannot skip trace a deceased person."}), 400

    payload = request.get_json(silent=True) or {}
    first_name = (payload.get("first_name") or person["first_name"] or "").strip()
    middle_name = (payload.get("middle_name") or person["middle_name"] or "").strip()
    last_name = (payload.get("last_name") or person["last_name"] or "").strip()
    first_name, middle_name, last_name = normalize_first_middle_last(first_name, middle_name, last_name)
    if person and (
        first_name != (person["first_name"] or "").strip()
        or middle_name != (person["middle_name"] or "").strip()
        or last_name != (person["last_name"] or "").strip()
    ):
        db.execute(
            "UPDATE people SET first_name = ?, middle_name = ?, last_name = ? WHERE id = ?",
            (first_name, middle_name, last_name, person_id),
        )
    if not first_name or not last_name:
        return jsonify({"error": "first_name and last_name are required"}), 400

    try:
        lookup_pkg = call_skipsherpa_person_lookup(
            first_name=first_name,
            middle_name=middle_name,
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


@app.route("/api/properties/<int:property_id>/skip-trace-property", methods=["POST"])
def skip_trace_property_route(property_id):
    ensure_db()
    db = get_db()
    prop = db.execute(
        """
        SELECT p.id, p.owner_person_id, a.street, a.city, a.state, a.postal_code
        FROM properties p
        JOIN addresses a ON a.id = p.property_address_id
        WHERE p.id = ?
        """,
        (property_id,),
    ).fetchone()
    if not prop:
        return jsonify({"error": "property not found"}), 404

    try:
        lookup_pkg = call_skipsherpa_property_lookup(
            street=prop["street"],
            city=prop["city"],
            state=prop["state"],
            zipcode=prop["postal_code"],
        )
        summary = import_skipsherpa_property_result(db, property_id, lookup_pkg)
        db.commit()
        return jsonify({"ok": True, "summary": summary, "raw": lookup_pkg.get("response")}), 200
    except Exception as exc:
        db.rollback()
        return jsonify({"error": str(exc)}), 400


@app.route("/api/properties/<int:property_id>/mail/relative/<int:person_id>", methods=["POST"])
def mail_single_relative_route(property_id, person_id):
    ensure_db()
    db = get_db()
    prop = db.execute(
        """
        SELECT p.id, p.owner_person_id, a.street, a.city, a.state, a.postal_code
        FROM properties p
        JOIN addresses a ON a.id = p.property_address_id
        WHERE p.id = ?
        """,
        (property_id,),
    ).fetchone()
    if not prop:
        return jsonify({"error": "property not found"}), 404

    target_person = db.execute("SELECT deceased FROM people WHERE id = ?", (person_id,)).fetchone()
    if not target_person:
        return jsonify({"error": "person not found"}), 404
    if int(target_person["deceased"] or 0) == 1:
        return jsonify({"error": "Mailer is not available for deceased individuals"}), 400

    contact = _format_person_contact_for_mail(db, prop, person_id)
    if not contact:
        return jsonify({"error": "No mailing address available for this person"}), 400

    try:
        result = place_openletterconnect_order(db, [contact], prop, mode="mail-individual-relative")
        res = result["response"] if isinstance(result, dict) else {}
        external_order_id = str(res.get("id") or res.get("orderId") or res.get("order_id") or "")
        status = str(res.get("status") or "")
        cost = res.get("totalCost") or res.get("cost")
        db.execute(
            """
            INSERT INTO mail_orders (property_id, person_id, mode, template_id, external_order_id, status, cost, recipient_count, request_json, response_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                property_id,
                person_id,
                "individual",
                OPENLETTERCONNECT_TEMPLATE_ID,
                external_order_id,
                status,
                cost,
                1,
                json.dumps(result.get("request")),
                json.dumps(res),
            ),
        )
        add_person_note(
            db,
            person_id,
            "OpenLetterConnect",
            f"Mailer order submitted (template {OPENLETTERCONNECT_TEMPLATE_ID}) for property {property_id}.",
            res,
        )
        db.execute(
            """
            INSERT INTO activity_log (property_id, person_id, activity_type, outcome, note)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                property_id,
                person_id,
                "Direct Mail",
                f"Submitted individual mailer order {external_order_id or '(no id)'}",
                json.dumps(res),
            ),
        )
        db.commit()
        return jsonify({"ok": True, "order": res}), 200
    except Exception as exc:
        db.rollback()
        return jsonify({"error": str(exc)}), 400


@app.route("/api/properties/<int:property_id>/mail/preview/relative/<int:person_id>", methods=["GET"])
def mail_preview_single_relative_route(property_id, person_id):
    ensure_db()
    db = get_db()
    prop = db.execute(
        """
        SELECT p.id, p.owner_person_id, a.street, a.city, a.state, a.postal_code
        FROM properties p
        JOIN addresses a ON a.id = p.property_address_id
        WHERE p.id = ?
        """,
        (property_id,),
    ).fetchone()
    if not prop:
        return jsonify({"error": "property not found"}), 404

    person = db.execute("SELECT deceased FROM people WHERE id = ?", (person_id,)).fetchone()
    if not person:
        return jsonify({"error": "person not found"}), 404
    if int(person["deceased"] or 0) == 1:
        return jsonify({"error": "Mailer is not available for deceased individuals"}), 400

    contact = _format_person_contact_for_mail(db, prop, person_id)
    if not contact:
        return jsonify({"error": "No mailing address available for this person"}), 400

    built = build_openletterconnect_order_payload(
        db,
        [contact],
        prop,
        template_id=OPENLETTERCONNECT_TEMPLATE_ID,
        mode="mail-individual-relative",
    )
    template_data = built.get("template") or {}
    include_proofs = request.args.get("include_proofs", "0").strip().lower() in ("1", "true", "yes")
    proofs = view_openletterconnect_proofs(built["payload"]) if include_proofs else []
    return jsonify(
        {
            "ok": True,
            "mode": "individual",
            "recipient_count": 1,
            "payload": built["payload"],
            "proofs": proofs,
            "template_meta": {
                "id": template_data.get("id"),
                "title": template_data.get("title"),
                "thumbnailUrl": template_data.get("thumbnailUrl"),
                "backThumbnailUrl": template_data.get("backThumbnailUrl"),
                "fields": template_data.get("fields") or [],
            },
        }
    )


@app.route("/api/properties/<int:property_id>/mail/relatives", methods=["POST"])
def mail_all_relatives_route(property_id):
    ensure_db()
    db = get_db()
    prop = db.execute(
        """
        SELECT p.id, p.owner_person_id, a.street, a.city, a.state, a.postal_code
        FROM properties p
        JOIN addresses a ON a.id = p.property_address_id
        WHERE p.id = ?
        """,
        (property_id,),
    ).fetchone()
    if not prop or not prop["owner_person_id"]:
        return jsonify({"error": "property owner not found"}), 404

    rel_rows = db.execute(
        """
        SELECT DISTINCT p.id AS person_id
        FROM person_relationships r
        JOIN people p ON p.id = r.related_person_id
        WHERE r.subject_person_id = ? AND COALESCE(p.deceased, 0) = 0
        """,
        (prop["owner_person_id"],),
    ).fetchall()

    person_ids = []
    if prop["owner_person_id"]:
        owner_row = db.execute(
            "SELECT id FROM people WHERE id = ? AND COALESCE(deceased, 0) = 0",
            (prop["owner_person_id"],),
        ).fetchone()
        if owner_row:
            person_ids.append(owner_row["id"])
    person_ids.extend(r["person_id"] for r in rel_rows)

    contacts = []
    seen = set()
    for person_id in person_ids:
        contact = _format_person_contact_for_mail(db, prop, person_id)
        if not contact:
            continue
        key = (
            (contact.get("firstName") or "").strip().lower(),
            (contact.get("lastName") or "").strip().lower(),
            (contact.get("address1") or "").strip().lower(),
            (contact.get("city") or "").strip().lower(),
            (contact.get("state") or "").strip().lower(),
            (contact.get("zip") or "").strip(),
        )
        if key in seen:
            continue
        seen.add(key)
        contacts.append(contact)

    if not contacts:
        return jsonify({"error": "No mailable relatives with addresses found"}), 400

    try:
        result = place_openletterconnect_order(db, contacts, prop, mode="mail-all-relatives")
        res = result["response"] if isinstance(result, dict) else {}
        external_order_id = str(res.get("id") or res.get("orderId") or res.get("order_id") or "")
        status = str(res.get("status") or "")
        cost = res.get("totalCost") or res.get("cost")
        db.execute(
            """
            INSERT INTO mail_orders (property_id, person_id, mode, template_id, external_order_id, status, cost, recipient_count, request_json, response_json)
            VALUES (?, NULL, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                property_id,
                "all_relatives",
                OPENLETTERCONNECT_TEMPLATE_ID,
                external_order_id,
                status,
                cost,
                len(contacts),
                json.dumps(result.get("request")),
                json.dumps(res),
            ),
        )
        db.execute(
            """
            INSERT INTO activity_log (property_id, person_id, activity_type, outcome, note)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                property_id,
                prop["owner_person_id"],
                "Direct Mail",
                f"Submitted all-relatives mailer order {external_order_id or '(no id)'}",
                json.dumps(res),
            ),
        )
        db.commit()
        return jsonify({"ok": True, "recipient_count": len(contacts), "order": res}), 200
    except Exception as exc:
        db.rollback()
        return jsonify({"error": str(exc)}), 400


@app.route("/api/properties/<int:property_id>/mail/preview/relatives", methods=["GET"])
def mail_preview_all_relatives_route(property_id):
    ensure_db()
    db = get_db()
    prop = db.execute(
        """
        SELECT p.id, p.owner_person_id, a.street, a.city, a.state, a.postal_code
        FROM properties p
        JOIN addresses a ON a.id = p.property_address_id
        WHERE p.id = ?
        """,
        (property_id,),
    ).fetchone()
    if not prop or not prop["owner_person_id"]:
        return jsonify({"error": "property owner not found"}), 404

    rel_rows = db.execute(
        """
        SELECT DISTINCT p.id AS person_id
        FROM person_relationships r
        JOIN people p ON p.id = r.related_person_id
        WHERE r.subject_person_id = ? AND COALESCE(p.deceased, 0) = 0
        """,
        (prop["owner_person_id"],),
    ).fetchall()

    person_ids = []
    owner_row = db.execute(
        "SELECT id FROM people WHERE id = ? AND COALESCE(deceased, 0) = 0",
        (prop["owner_person_id"],),
    ).fetchone()
    if owner_row:
        person_ids.append(owner_row["id"])
    person_ids.extend(r["person_id"] for r in rel_rows)

    contacts = []
    seen = set()
    for person_id in person_ids:
        contact = _format_person_contact_for_mail(db, prop, person_id)
        if not contact:
            continue
        key = (
            (contact.get("firstName") or "").strip().lower(),
            (contact.get("lastName") or "").strip().lower(),
            (contact.get("address1") or "").strip().lower(),
            (contact.get("city") or "").strip().lower(),
            (contact.get("state") or "").strip().lower(),
            (contact.get("zip") or "").strip(),
        )
        if key in seen:
            continue
        seen.add(key)
        contacts.append(contact)

    if not contacts:
        return jsonify({"error": "No mailable individuals with addresses found"}), 400

    built = build_openletterconnect_order_payload(
        db,
        contacts,
        prop,
        template_id=OPENLETTERCONNECT_TEMPLATE_ID,
        mode="mail-all-relatives",
    )
    template_data = built.get("template") or {}
    include_proofs = request.args.get("include_proofs", "0").strip().lower() in ("1", "true", "yes")
    proofs = view_openletterconnect_proofs(built["payload"]) if include_proofs else []
    return jsonify(
        {
            "ok": True,
            "mode": "all",
            "recipient_count": len(contacts),
            "payload": built["payload"],
            "proofs": proofs,
            "template_meta": {
                "id": template_data.get("id"),
                "title": template_data.get("title"),
                "thumbnailUrl": template_data.get("thumbnailUrl"),
                "backThumbnailUrl": template_data.get("backThumbnailUrl"),
                "fields": template_data.get("fields") or [],
            },
        }
    )


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


@app.route("/webhooks/slack/command", methods=["POST"])
def slack_command_webhook():
    ensure_db()
    db = get_db()
    s = get_slack_settings(db)
    secret = (s.get("signing_secret") or "").strip()
    if secret:
        ok, reason = verify_slack_signature(request, secret)
        if not ok:
            return jsonify({"ok": False, "error": reason}), 401
    text = (request.form.get("text") or "").strip()
    cmd = (request.form.get("command") or "").strip()
    user_name = (request.form.get("user_name") or "").strip()
    low = text.lower()
    if low in {"help", "?"}:
        return jsonify(
            {
                "response_type": "ephemeral",
                "text": "Commands: `status` (counts), `health` (app health), `help`.",
            }
        )
    if low in {"status", "counts"}:
        counts = {
            "properties": db.execute("SELECT COUNT(*) AS c FROM properties").fetchone()["c"],
            "people": db.execute("SELECT COUNT(*) AS c FROM people").fetchone()["c"],
            "referrals": db.execute("SELECT COUNT(*) AS c FROM reisift_referrals").fetchone()["c"],
            "realtors": db.execute("SELECT COUNT(*) AS c FROM referral_realtors").fetchone()["c"],
        }
        msg = (
            f"DeepSift status for @{user_name or 'user'}: "
            f"properties={counts['properties']}, people={counts['people']}, "
            f"referrals={counts['referrals']}, realtors={counts['realtors']}"
        )
        return jsonify({"response_type": "ephemeral", "text": msg})
    if low in {"health", "ping"}:
        return jsonify({"response_type": "ephemeral", "text": "DeepSift is online."})
    return jsonify({"response_type": "ephemeral", "text": f"Unknown command `{text or cmd}`. Try `help`."})


@app.route("/webhooks/slack/events", methods=["POST"])
def slack_events_webhook():
    ensure_db()
    db = get_db()
    s = get_slack_settings(db)
    secret = (s.get("signing_secret") or "").strip()
    if secret:
        ok, reason = verify_slack_signature(request, secret)
        if not ok:
            return jsonify({"ok": False, "error": reason}), 401
    payload = request.get_json(force=True, silent=True) or {}
    if payload.get("type") == "url_verification":
        return jsonify({"challenge": payload.get("challenge", "")})
    event = payload.get("event") if isinstance(payload.get("event"), dict) else {}
    event_id = (payload.get("event_id") or "").strip() or f"slack_event_{int(time.time())}"
    is_new = upsert_integration_event(db, "slack_event", event_id, payload)
    db.commit()
    if not is_new:
        return jsonify({"ok": True, "duplicate": True})
    try:
        if event.get("type") == "app_mention":
            text = (event.get("text") or "").strip()
            send_slack_notification(db, f"Slack mention received: {text}")
    except Exception:
        pass
    return jsonify({"ok": True})


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
    event_name = str(payload.get("event") or payload.get("eventType") or "").strip().lower()

    if "incoming" in event_name:
        direction = "Inbound"
    elif "outgoing" in event_name:
        direction = "Outbound"
    else:
        # When event is missing, infer from known SMS flow defaults.
        direction = "Inbound"
    event_type = "outbound" if direction == "Outbound" else "inbound"

    if not message:
        log_smrtphone_webhook_event(
            db,
            event_type,
            payload,
            processing_status="ignored",
            sms_id=sms_id,
            from_number=from_number,
            to_number=to_number,
            error_text="message/body/text is required",
        )
        db.commit()
        return jsonify({"ok": True, "ignored": True, "reason": "message/body/text is required"}), 200

    counterparty_number = from_number if direction == "Inbound" else to_number
    person_id = find_person_id_by_phone(db, counterparty_number)
    property_id = None
    if str(payload.get("property_id", "")).isdigit():
        property_id = int(payload["property_id"])
    elif person_id:
        property_id = find_property_id_for_person(db, person_id)
    if not property_id:
        recent_link = db.execute(
            """
            SELECT property_id, person_id
            FROM communications
            WHERE upper(channel) = 'SMS'
              AND (
                    replace(replace(replace(replace(replace(COALESCE(from_number,''),'+',''),'(',''),')',''),'-',''),' ','') LIKE ?
                 OR replace(replace(replace(replace(replace(COALESCE(to_number,''),'+',''),'(',''),')',''),'-',''),' ','') LIKE ?
              )
            ORDER BY sent_at DESC, id DESC
            LIMIT 1
            """,
            (f"%{normalize_phone(from_number)}%", f"%{normalize_phone(to_number)}%"),
        ).fetchone()
        if recent_link:
            property_id = recent_link["property_id"]
            if not person_id:
                person_id = recent_link["person_id"]
    if not property_id:
        log_smrtphone_webhook_event(
            db,
            event_type,
            payload,
            processing_status="ignored",
            sms_id=sms_id,
            from_number=from_number,
            to_number=to_number,
            error_text=f"Unable to resolve property_id for {direction.lower()} message (event={event_name or 'unknown'})",
        )
        db.commit()
        return jsonify({"ok": True, "ignored": True, "reason": "unresolved property"}), 200
    if not person_id:
        person_id = find_person_id_by_recent_outbound_to_number(db, property_id, counterparty_number)

    existing = None
    if sms_id:
        existing = db.execute("SELECT id FROM communications WHERE external_id = ?", (sms_id,)).fetchone()
    if existing:
        if direction == "Inbound":
            update_person_outreach_status_for_sms(db, person_id, "inbound_received")
        else:
            update_person_outreach_status_for_sms(db, person_id, "outbound_success")
        log_smrtphone_webhook_event(
            db,
            event_type,
            payload,
            processing_status="deduped",
            sms_id=sms_id,
            from_number=from_number,
            to_number=to_number,
            communication_id=existing["id"],
        )
        db.commit()
        return jsonify({"ok": True, "deduped": True, "communication_id": existing["id"]})

    # Handle provider behavior where one callback arrives without smsId and a second with smsId,
    # and dedupe provider outbound callbacks against locally-sent outbound rows.
    if direction == "Inbound":
        recent = find_recent_inbound_match(
            db, property_id, person_id, from_number, to_number, message, seconds=150
        )
    else:
        recent = db.execute(
            """
            SELECT id, external_id
            FROM communications
            WHERE property_id = ?
              AND upper(channel) = 'SMS'
              AND lower(direction) = 'outbound'
              AND COALESCE(to_number, '') = COALESCE(?, '')
              AND COALESCE(body, '') = COALESCE(?, '')
              AND sent_at >= ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (property_id, to_number, message, format_db_time(datetime.utcnow() - timedelta(minutes=30))),
        ).fetchone()
    if recent:
        if direction == "Inbound":
            update_person_outreach_status_for_sms(db, person_id, "inbound_received")
        else:
            update_person_outreach_status_for_sms(db, person_id, "outbound_success")
        if sms_id and not (recent["external_id"] or "").strip():
            db.execute(
                "UPDATE communications SET external_id = ?, status = ? WHERE id = ?",
                (sms_id, status, recent["id"]),
            )
        log_smrtphone_webhook_event(
            db,
            event_type,
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
        VALUES (?, ?, 'SMS', ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            property_id,
            person_id,
            direction,
            from_number,
            to_number,
            message,
            status,
            0 if direction == "Inbound" else 1,
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            sms_id,
        ),
    )
    if direction == "Inbound":
        update_person_outreach_status_for_sms(db, person_id, "inbound_received")
    else:
        update_person_outreach_status_for_sms(db, person_id, "outbound_success")
    log_smrtphone_webhook_event(
        db,
        event_type,
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
    status_detail_text = " ".join(
        [
            str(payload.get("status") or ""),
            str(payload.get("status_text") or ""),
            str(payload.get("statusMessage") or ""),
            str(payload.get("status_message") or ""),
            str(payload.get("message") or ""),
            str(payload.get("error") or ""),
            str(payload.get("error_message") or ""),
            str(payload.get("reason") or ""),
            str(payload.get("description") or ""),
        ]
    ).strip()

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
            apply_touchpoint_status_inference(db, to_number, status_detail_text or status)
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
            apply_touchpoint_status_inference(db, to_number, status_detail_text or status)
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
        apply_touchpoint_status_inference(db, to_number, status_detail_text or status)
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


@app.route("/webhooks/smrtphone/call-completed", methods=["POST"])
def smrtphone_call_completed_webhook():
    ensure_db()
    db = get_db()
    payload = request.get_json(silent=True) or request.form.to_dict() or {}
    call_sid = extract_first_string_by_keys(
        payload,
        ["call_sid", "callSid", "CallSid", "callsid", "sid", "call_id", "callId", "callID", "id"],
    )
    from_number = extract_first_string_by_keys(payload, ["from", "from_number", "fromNumber", "caller", "source", "ani"])
    to_number = extract_first_string_by_keys(payload, ["to", "to_number", "toNumber", "callee", "destination", "dnis"])
    call_outcome = extract_first_string_by_keys(payload, ["callOutcome", "call_outcome", "outcome", "status"])
    direct_recording_url = extract_first_string_by_keys(payload, ["recordingUrl", "recording_url", "recording", "url"])
    normalized_outcome = (call_outcome or "").strip().lower()

    def _resolve_property_person():
        _person_id = find_person_id_by_phone(db, from_number) or find_person_id_by_phone(db, to_number)
        _property_id = None
        if str(payload.get("property_id") or "").isdigit():
            _property_id = int(payload.get("property_id"))
        elif _person_id:
            _property_id = find_property_id_for_person(db, _person_id)
        if not _property_id:
            outbound = db.execute(
                """
                SELECT property_id, person_id
                FROM communications
                WHERE upper(channel) = 'SMS'
                  AND lower(direction) = 'outbound'
                  AND (
                        replace(replace(replace(replace(replace(COALESCE(to_number,''),'+',''),'(',''),')',''),'-',''),' ','') LIKE ?
                     OR replace(replace(replace(replace(replace(COALESCE(from_number,''),'+',''),'(',''),')',''),'-',''),' ','') LIKE ?
                  )
                ORDER BY sent_at DESC, id DESC
                LIMIT 1
                """,
                (f"%{normalize_phone(from_number)}%", f"%{normalize_phone(to_number)}%"),
            ).fetchone()
            if outbound:
                _property_id = outbound["property_id"]
                if not _person_id:
                    _person_id = outbound["person_id"]
        return _property_id, _person_id

    if not call_sid:
        property_id, person_id = _resolve_property_person()
        if property_id:
            _create_agent_diagnosis_review_action(
                db,
                property_id=int(property_id),
                person_id=person_id,
                call_sid="(missing)",
                diagnosis="Call webhook missing call SID",
                attempted_fix="Tried resolving context by from/to numbers and recent outbound thread",
                proposed_resolution="Validate provider webhook payload includes callId/call_sid and resend failed event if possible.",
                priority="High",
                extra={"payload": payload},
            )
        log_smrtphone_webhook_event(
            db,
            "call_completed",
            payload,
            processing_status="error",
            from_number=from_number,
            to_number=to_number,
            error_text="call_sid is required",
        )
        db.commit()
        return jsonify({"ok": True, "ignored": True, "reason": "missing call_sid"}), 200

    existing = db.execute("SELECT id FROM call_recording_jobs WHERE call_sid = ?", (call_sid,)).fetchone()
    if existing:
        log_smrtphone_webhook_event(
            db,
            "call_completed",
            payload,
            processing_status="deduped",
            from_number=from_number,
            to_number=to_number,
            error_text="duplicate call_sid",
        )
        db.commit()
        return jsonify({"ok": True, "deduped": True, "job_id": existing["id"]}), 200

    property_id, person_id = _resolve_property_person()

    recording_url = (direct_recording_url or "").strip()
    fetch_status = "Queued"
    fetch_raw = {}
    if recording_url:
        fetch_status = "Fetched"
        fetch_raw = {"source": "webhook_payload"}
    elif (call_outcome or "").strip().lower() in {"missed", "no_answer", "failed"}:
        fetch_status = "No Recording URL"
        fetch_raw = {"source": "webhook_payload", "reason": f"call_outcome={call_outcome}"}
    else:
        try:
            lookup = fetch_smrtphone_recording_url(call_sid)
            recording_url = (lookup.get("recording_url") or "").strip()
            fetch_raw = lookup.get("raw") or {}
            fetch_status = "Fetched" if recording_url else "No Recording URL"
        except Exception as exc:
            fetch_status = "Error"
            fetch_raw = {"error": str(exc)}

    cur = db.execute(
        """
        INSERT INTO call_recording_jobs
        (call_sid, from_number, to_number, property_id, person_id, recording_url, fetch_status, analysis_status, payload_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'Pending', ?)
        """,
        (
            call_sid,
            from_number,
            to_number,
            property_id,
            person_id,
            recording_url,
            fetch_status,
            json.dumps({"webhook": payload, "recording_lookup": fetch_raw, "call_outcome": call_outcome}),
        ),
    )

    if property_id and normalized_outcome in NO_ANSWER_OUTCOMES:
        recommended_step, reason = _recommend_no_answer_next_step(db, int(property_id), person_id)
        upsert_agent_signal(
            db,
            source_type="call",
            source_key=f"call-no-answer:{call_sid}",
            property_id=property_id,
            person_id=person_id,
            channel="CALL",
            intent="No Answer",
            sentiment="Unknown",
            confidence=0.85,
            recommended_next_step=recommended_step,
            summary_text=f"No-answer call outcome. {reason}",
            is_spam=False,
            do_not_contact=False,
            payload={"call_sid": call_sid, "call_outcome": normalized_outcome, "reason": reason},
        )
        route_new_agent_signals_once(db, limit=25)

    log_smrtphone_webhook_event(
        db,
        "call_completed",
        payload,
        processing_status="stored",
        from_number=from_number,
        to_number=to_number,
    )
    db.commit()
    return jsonify({"ok": True, "job_id": cur.lastrowid, "call_sid": call_sid, "recording_url": recording_url, "fetch_status": fetch_status})


@app.route("/api/notifications/sms-unread", methods=["GET"])
def unread_sms_notifications():
    return unread_notifications()


@app.route("/api/notifications/unread", methods=["GET"])
def unread_notifications():
    ensure_db()
    db = get_db()
    rows = db.execute(
        """
        SELECT id, property_id, person_id, channel, from_number, to_number, status, body, sent_at
        FROM communications
        WHERE lower(direction) = 'inbound' AND COALESCE(is_read, 1) = 0
        ORDER BY sent_at DESC, id DESC
        LIMIT 50
        """
    ).fetchall()
    items = []
    for r in rows:
        item = dict(r)
        if str(item.get("channel", "")).upper() == "SMS":
            inbound_from = (item.get("from_number") or "").strip()
            norm = normalize_phone(inbound_from)
            linked_person_id = item.get("person_id")
            linked_property_id = item.get("property_id")
            if norm:
                outbound = db.execute(
                    """
                    SELECT property_id, person_id
                    FROM communications
                    WHERE upper(channel) = 'SMS'
                      AND lower(direction) = 'outbound'
                    ORDER BY
                      CASE
                        WHEN replace(replace(replace(replace(replace(COALESCE(to_number,''),'+',''),'(',''),')',''),'-',''),' ','') LIKE ? THEN 0
                        ELSE 1
                      END,
                      sent_at DESC,
                      id DESC
                    LIMIT 1
                    """,
                    (f"%{norm}%",),
                ).fetchone()
                if outbound:
                    linked_property_id = outbound["property_id"] or linked_property_id
                    linked_person_id = outbound["person_id"] or linked_person_id
                if not linked_person_id:
                    linked_person_id = find_person_id_by_phone(db, inbound_from)
            item["link_person_id"] = linked_person_id
            item["link_property_id"] = linked_property_id
            if linked_person_id:
                p = db.execute(
                    "SELECT first_name, last_name FROM people WHERE id = ?",
                    (linked_person_id,),
                ).fetchone()
                if p:
                    item["link_person_name"] = f"{p['first_name']} {p['last_name']}".strip()
        items.append(item)
    return jsonify({"count": len(items), "items": items})


def _mail_order_person_ids(order_row):
    ids = set()
    if order_row and order_row["person_id"]:
        ids.add(int(order_row["person_id"]))
    try:
        req = json.loads(order_row["request_json"] or "{}") if order_row else {}
    except Exception:
        req = {}
    contacts = req.get("contacts") if isinstance(req, dict) else []
    if isinstance(contacts, list):
        for c in contacts:
            if not isinstance(c, dict):
                continue
            meta = c.get("meta_data") if isinstance(c.get("meta_data"), dict) else {}
            pid = meta.get("person_id")
            if str(pid).isdigit():
                ids.add(int(pid))
    return sorted(ids)


@app.route("/webhooks/openletterconnect/order-status", methods=["POST"])
def openletterconnect_order_status_webhook():
    ensure_db()
    db = get_db()
    payload = request.get_json(silent=True) or request.form.to_dict() or {}
    external_order_id = str(
        payload.get("id")
        or payload.get("orderId")
        or payload.get("order_id")
        or payload.get("external_order_id")
        or ""
    ).strip()
    status = str(
        payload.get("status")
        or payload.get("orderStatus")
        or payload.get("order_status")
        or payload.get("state")
        or ""
    ).strip()
    if not external_order_id:
        return jsonify({"ok": True, "ignored": True, "reason": "missing order id"}), 200
    rows = db.execute(
        """
        SELECT *
        FROM mail_orders
        WHERE external_order_id = ?
        ORDER BY id DESC
        """,
        (external_order_id,),
    ).fetchall()
    if not rows:
        return jsonify({"ok": True, "ignored": True, "reason": "unknown order id"}), 200

    now_txt = format_db_time(datetime.utcnow())
    note_status = status or "Update received"
    for row in rows:
        db.execute(
            "UPDATE mail_orders SET status = COALESCE(NULLIF(?, ''), status), response_json = ? WHERE id = ?",
            (status, json.dumps(payload), row["id"]),
        )
        person_ids = _mail_order_person_ids(row)
        for pid in person_ids:
            add_person_note(
                db,
                pid,
                "OpenLetterConnect Webhook",
                f"Direct mail order {external_order_id} status: {note_status} ({now_txt}).",
                payload,
            )
        db.execute(
            """
            INSERT INTO activity_log (property_id, person_id, activity_type, outcome, note)
            VALUES (?, ?, 'Direct Mail Webhook', ?, ?)
            """,
            (
                row["property_id"],
                (person_ids[0] if person_ids else row["person_id"]),
                note_status or "Status Update",
                json.dumps(payload),
            ),
        )
    db.commit()
    return jsonify({"ok": True, "updated_orders": len(rows)}), 200


@app.route("/api/notifications/sms-unread/<int:communication_id>/read", methods=["POST"])
def mark_sms_notification_read(communication_id):
    return mark_notification_read(communication_id)


@app.route("/api/notifications/unread/<int:communication_id>/read", methods=["POST"])
def mark_notification_read(communication_id):
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


@app.route("/api/call-recording-jobs", methods=["GET"])
def call_recording_jobs_api():
    ensure_db()
    db = get_db()
    limit_raw = (request.args.get("limit") or "100").strip()
    try:
        limit = max(1, min(500, int(limit_raw)))
    except ValueError:
        limit = 100
    rows = db.execute(
        """
        SELECT id, call_sid, from_number, to_number, property_id, person_id, recording_url,
               fetch_status, analysis_status, summary_text, created_at, updated_at
        FROM call_recording_jobs
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/webhooks/gmail/pubsub", methods=["POST"])
def gmail_pubsub_webhook():
    ensure_db()
    db = get_db()
    try:
        s = get_email_settings(db)
        expected = (s.get("gmail_webhook_token") or "").strip()
        provided = (request.args.get("token") or request.headers.get("X-Gmail-Webhook-Token") or "").strip()
        integration_key = (
            (request.args.get("integration_key") or "").strip()
            or (request.headers.get("X-Integration-Key") or "").strip()
            or (request.headers.get("Authorization") or "").replace("Bearer", "").strip()
        )
        expected_integration_key = get_integration_api_key(db)
        integration_ok = bool(expected_integration_key and integration_key and hmac.compare_digest(integration_key, expected_integration_key))
        token_ok = bool(expected and provided and hmac.compare_digest(provided, expected))
        if expected:
            if not (token_ok or integration_ok):
                return jsonify({"ok": False, "error": "Unauthorized"}), 401
        else:
            if not integration_ok:
                return jsonify({"ok": False, "error": "Unauthorized"}), 401
        payload = request.get_json(silent=True) or {}
        message = payload.get("message") if isinstance(payload.get("message"), dict) else {}
        event_key = (message.get("messageId") or "").strip() or datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
        upsert_integration_event(db, "gmail_pubsub_webhook", event_key, payload)
        # Commit webhook ingest before inbound processing opens its own DB connection.
        # This avoids SQLite write-lock contention between concurrent connections.
        db.commit()
        result = process_gmail_api_inbound_once(max_messages=8)
        return jsonify({"ok": True, "event_key": event_key, "result": result}), 200
    except Exception as exc:
        log_app_error(
            db,
            source="gmail_pubsub_webhook",
            error_message=str(exc),
            details=traceback.format_exc(),
            route=request.path,
            status_code=500,
        )
        db.commit()
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/webhooks/email/open/<token>.gif", methods=["GET"])
def email_open_tracking_pixel(token):
    ensure_db()
    db = get_db()
    clean_token = (token or "").strip()
    if clean_token:
        now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        db.execute(
            """
            UPDATE communications
            SET open_count = COALESCE(open_count, 0) + 1,
                opened_at = COALESCE(opened_at, ?),
                status = CASE
                    WHEN upper(channel) = 'EMAIL' AND lower(direction) = 'outbound' AND (
                        COALESCE(status, '') = '' OR lower(status) IN ('sent', 'queued', 'delivered')
                    )
                    THEN 'Opened'
                    ELSE status
                END
            WHERE open_tracking_token = ?
              AND upper(channel) = 'EMAIL'
              AND lower(direction) = 'outbound'
            """,
            (now_str, clean_token),
        )
        db.commit()
    return app.response_class(
        OPEN_TRACK_PIXEL_GIF,
        mimetype="image/gif",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.route("/webhooks/reisift/automation", methods=["POST"])
def reisift_automation_placeholder_webhook():
    ensure_db()
    db = get_db()
    payload = request.get_json(silent=True) or {}
    event_key = (
        (payload.get("event_key") or "").strip()
        or (payload.get("eventKey") or "").strip()
        or (payload.get("id") or "").strip()
        or datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
    )
    upsert_integration_event(db, "reisift_automation_placeholder", event_key, payload)
    db.commit()
    return jsonify({"ok": True, "placeholder": True, "event_key": event_key}), 200


@app.route("/api/email/poll-now", methods=["POST"])
def email_poll_now():
    try:
        ensure_db()
        db = get_db()
        provider = (get_email_settings(db).get("provider") or "gmail_api").strip().lower()
        if provider == "gmail_api":
            result = process_gmail_api_inbound_once(max_messages=12)
            return jsonify({"ok": bool(result.get("ok")), "result": result}), (200 if result.get("ok") else 500)
        return jsonify({"ok": False, "error": "API-only mode enabled. Select gmail_api or resend."}), 400
    except Exception as exc:
        ensure_db()
        db = get_db()
        log_app_error(
            db,
            source="email_poll_now",
            error_message=str(exc),
            details=traceback.format_exc(),
            route=request.path,
            status_code=500,
        )
        db.commit()
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/email/test-login", methods=["POST"])
def email_test_login():
    ensure_db()
    db = get_db()
    result = test_email_login(db)
    if not result.get("ok"):
        log_app_error(
            db,
            source="email_login_test",
            error_message=result.get("error") or "Email login test failed",
            details=json.dumps(result),
            route=request.path,
            status_code=400,
        )
        db.commit()
        return jsonify(result), 400
    return jsonify(result), 200


@app.route("/api/integrations/call-recordings/run-once", methods=["POST"])
def integrations_call_recordings_run_once_api():
    ensure_db()
    db = get_db()
    token = request.headers.get("X-Integration-Key", "").strip()
    expected = get_integration_api_key(db)
    if not expected:
        return jsonify({"ok": False, "error": "Integration API key is not configured"}), 503
    if not token or token != expected:
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    payload = request.get_json(silent=True) or request.form.to_dict() or {}
    limit_raw = str(payload.get("limit") or request.args.get("limit") or "2").strip()
    try:
        limit = max(1, min(10, int(limit_raw)))
    except ValueError:
        limit = 2
    result = run_call_recording_analysis_once(limit=limit)
    status = 200 if result.get("ok") else 500
    return jsonify(result), status


@app.route("/api/integrations/sms-analysis/run-once", methods=["POST"])
def integrations_sms_analysis_run_once_api():
    ensure_db()
    db = get_db()
    token = request.headers.get("X-Integration-Key", "").strip()
    expected = get_integration_api_key(db)
    if not expected:
        return jsonify({"ok": False, "error": "Integration API key is not configured"}), 503
    if not token or token != expected:
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    payload = request.get_json(silent=True) or request.form.to_dict() or {}
    lookback_raw = str(payload.get("lookback_days") or request.args.get("lookback_days") or "14").strip()
    limit_raw = str(payload.get("limit_threads") or request.args.get("limit_threads") or "30").strip()
    try:
        lookback = max(1, min(90, int(lookback_raw)))
    except ValueError:
        lookback = 14
    try:
        limit = max(1, min(100, int(limit_raw)))
    except ValueError:
        limit = 30
    result = run_sms_analysis_once(lookback_days=lookback, limit_threads=limit)
    status = 200 if result.get("ok") else 500
    return jsonify(result), status


@app.route("/api/app-errors", methods=["GET"])
def api_app_errors():
    ensure_db()
    db = get_db()
    limit_raw = (request.args.get("limit") or "50").strip()
    try:
        limit = max(1, min(200, int(limit_raw)))
    except ValueError:
        limit = 50
    rows = db.execute(
        """
        SELECT id, source, route, status_code, error_message, details, created_at
        FROM app_errors
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.errorhandler(Exception)
def handle_unhandled_exception(exc):
    if isinstance(exc, HTTPException):
        return exc
    ensure_db()
    db = get_db()
    status_code = 500
    route = request.path if request else ""
    log_app_error(
        db,
        source="unhandled_exception",
        error_message=str(exc),
        details=traceback.format_exc(),
        route=route,
        status_code=status_code,
    )
    db.commit()
    if request.path.startswith("/api/"):
        payload = {"error": "Internal server error"}
        if env_flag("EXPOSE_API_ERRORS", False):
            payload["detail"] = str(exc)
        return jsonify(payload), status_code
    return render_template("error.html", message=str(exc)), status_code


if __name__ == "__main__":
    debug_mode = env_flag("FLASK_DEBUG", False)
    host = os.getenv("FLASK_RUN_HOST", "0.0.0.0").strip() or "0.0.0.0"
    try:
        port = int((os.getenv("PORT") or os.getenv("FLASK_RUN_PORT") or "5000").strip())
    except ValueError:
        port = 5000
    ensure_db()
    if (not debug_mode) or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        start_bulk_sms_worker()
        start_email_poll_worker()
        start_clever_leads_worker()
        start_referral_on_market_worker()
        start_call_recording_worker()
        start_sms_analysis_worker()
    app.run(host=host, port=port, debug=debug_mode)


















