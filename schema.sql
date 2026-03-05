PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS people (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    primary_phone TEXT,
    primary_email TEXT,
    age INTEGER,
    deceased INTEGER NOT NULL DEFAULT 0,
    birth_year TEXT,
    deceased_date TEXT,
    bankruptcy INTEGER NOT NULL DEFAULT 0,
    employer TEXT,
    outreach_status TEXT NOT NULL DEFAULT 'No Contact',
    notes TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS addresses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    street TEXT NOT NULL,
    city TEXT NOT NULL,
    state TEXT NOT NULL,
    postal_code TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS person_addresses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id INTEGER NOT NULL,
    address_id INTEGER NOT NULL,
    label TEXT NOT NULL DEFAULT 'Related Address',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(person_id) REFERENCES people(id),
    FOREIGN KEY(address_id) REFERENCES addresses(id)
);

CREATE TABLE IF NOT EXISTS properties (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_address_id INTEGER NOT NULL,
    owner_person_id INTEGER,
    resident_person_id INTEGER,
    status TEXT NOT NULL DEFAULT 'Untouched',
    notes TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(property_address_id) REFERENCES addresses(id),
    FOREIGN KEY(owner_person_id) REFERENCES people(id),
    FOREIGN KEY(resident_person_id) REFERENCES people(id)
);

CREATE TABLE IF NOT EXISTS touchpoints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id INTEGER NOT NULL,
    channel_type TEXT NOT NULL,
    channel_label TEXT,
    value TEXT,
    status TEXT NOT NULL DEFAULT 'Pending',
    note TEXT,
    last_attempted TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(person_id) REFERENCES people(id)
);

CREATE TABLE IF NOT EXISTS social_accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id INTEGER NOT NULL,
    platform TEXT NOT NULL,
    handle TEXT,
    url TEXT,
    status TEXT NOT NULL DEFAULT 'Unverified',
    note TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(person_id) REFERENCES people(id)
);

CREATE TABLE IF NOT EXISTS person_relationships (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    subject_person_id INTEGER NOT NULL,
    related_person_id INTEGER NOT NULL,
    relationship_type TEXT NOT NULL,
    note TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(subject_person_id) REFERENCES people(id),
    FOREIGN KEY(related_person_id) REFERENCES people(id)
);

CREATE TABLE IF NOT EXISTS activity_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_id INTEGER NOT NULL,
    person_id INTEGER,
    activity_type TEXT NOT NULL,
    outcome TEXT,
    note TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(property_id) REFERENCES properties(id),
    FOREIGN KEY(person_id) REFERENCES people(id)
);

CREATE TABLE IF NOT EXISTS communications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_id INTEGER NOT NULL,
    person_id INTEGER,
    channel TEXT NOT NULL DEFAULT 'SMS',
    direction TEXT NOT NULL DEFAULT 'Outbound',
    from_number TEXT,
    to_number TEXT,
    body TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'Queued',
    is_read INTEGER NOT NULL DEFAULT 1,
    sent_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    external_id TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(property_id) REFERENCES properties(id),
    FOREIGN KEY(person_id) REFERENCES people(id)
);

CREATE TABLE IF NOT EXISTS obituary_sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_id INTEGER NOT NULL,
    subject_person_id INTEGER NOT NULL,
    source_url TEXT NOT NULL,
    raw_text TEXT,
    extracted_json TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(property_id) REFERENCES properties(id),
    FOREIGN KEY(subject_person_id) REFERENCES people(id)
);

CREATE TABLE IF NOT EXISTS bulk_sms_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_id INTEGER NOT NULL,
    message TEXT NOT NULL,
    min_interval_minutes INTEGER NOT NULL DEFAULT 2,
    max_interval_minutes INTEGER NOT NULL DEFAULT 5,
    status TEXT NOT NULL DEFAULT 'Active',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TEXT,
    FOREIGN KEY(property_id) REFERENCES properties(id)
);

CREATE TABLE IF NOT EXISTS bulk_sms_job_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,
    property_id INTEGER NOT NULL,
    person_id INTEGER,
    to_number TEXT NOT NULL,
    channel_label TEXT,
    status TEXT NOT NULL DEFAULT 'Pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT,
    sent_comm_id INTEGER,
    external_id TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(job_id) REFERENCES bulk_sms_jobs(id),
    FOREIGN KEY(property_id) REFERENCES properties(id),
    FOREIGN KEY(person_id) REFERENCES people(id),
    FOREIGN KEY(sent_comm_id) REFERENCES communications(id)
);

CREATE TABLE IF NOT EXISTS smrtphone_webhook_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    processing_status TEXT NOT NULL DEFAULT 'received',
    sms_id TEXT,
    from_number TEXT,
    to_number TEXT,
    communication_id INTEGER,
    error_text TEXT,
    payload_json TEXT,
    received_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS skiptrace_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider TEXT NOT NULL DEFAULT 'SkipSherpa',
    property_id INTEGER NOT NULL,
    person_id INTEGER NOT NULL,
    request_json TEXT,
    response_json TEXT,
    summary_json TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(property_id) REFERENCES properties(id),
    FOREIGN KEY(person_id) REFERENCES people(id)
);

CREATE TABLE IF NOT EXISTS person_notes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id INTEGER NOT NULL,
    source TEXT NOT NULL,
    note_body TEXT NOT NULL,
    payload_json TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(person_id) REFERENCES people(id)
);

