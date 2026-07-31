PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS people (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    first_name TEXT NOT NULL,
    middle_name TEXT,
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
    is_verified_deliverable INTEGER,
    is_vacant INTEGER,
    attom_last_sold_date TEXT,
    attom_last_sold_price REAL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS person_addresses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id INTEGER NOT NULL,
    address_id INTEGER NOT NULL,
    label TEXT NOT NULL DEFAULT 'Related Address',
    is_default_mailing INTEGER NOT NULL DEFAULT 0,
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
    is_d4d INTEGER NOT NULL DEFAULT 0,
    d4d_added_at TEXT,
    d4d_added_by TEXT,
    d4d_source TEXT,
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

CREATE TABLE IF NOT EXISTS propertyleads_lead_submissions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_key TEXT NOT NULL UNIQUE,
    lead_id TEXT,
    reisift_property_uuid TEXT,
    reisift_owner_uuid TEXT,
    latest_address TEXT,
    latest_phone TEXT,
    latest_email TEXT,
    latest_name TEXT,
    latest_stage TEXT,
    county TEXT,
    lead_cost TEXT,
    source_label TEXT,
    latest_payload_json TEXT,
    processing_result_json TEXT,
    reisift_status TEXT NOT NULL DEFAULT 'new lead',
    status TEXT NOT NULL DEFAULT 'captured',
    local_property_id INTEGER,
    first_received_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_received_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at TEXT
);

CREATE TABLE IF NOT EXISTS manual_lead_submissions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_key TEXT NOT NULL UNIQUE,
    reisift_property_uuid TEXT NOT NULL,
    reisift_owner_uuid TEXT,
    latest_address TEXT NOT NULL,
    latest_phone TEXT,
    latest_email TEXT,
    latest_name TEXT,
    latest_stage TEXT,
    entry_notes TEXT,
    latest_payload_json TEXT,
    processing_result_json TEXT,
    reisift_status TEXT NOT NULL DEFAULT 'new lead',
    status TEXT NOT NULL DEFAULT 'manual_added',
    local_property_id INTEGER,
    activity_since_at TEXT,
    first_received_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_received_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at TEXT
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

CREATE TABLE IF NOT EXISTS reisift_referrals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_uuid TEXT NOT NULL UNIQUE,
    status TEXT,
    full_address TEXT,
    owner_names TEXT,
    payload_json TEXT,
    referral_status TEXT NOT NULL DEFAULT 'Untouched',
    winning_realtor_id INTEGER,
    referral_notes TEXT,
    county TEXT,
    on_market_status TEXT NOT NULL DEFAULT 'Unknown',
    source_override_json TEXT,
    is_active INTEGER NOT NULL DEFAULT 1,
    last_synced_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS mail_orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_id INTEGER NOT NULL,
    person_id INTEGER,
    mode TEXT NOT NULL,
    template_id INTEGER NOT NULL,
    external_order_id TEXT,
    external_order_item_id TEXT,
    status TEXT,
    cost REAL,
    recipient_count INTEGER NOT NULL DEFAULT 0,
    request_json TEXT,
    response_json TEXT,
    status_updated_at TEXT,
    last_event_type TEXT,
    mailed_at TEXT,
    in_transit_at TEXT,
    delivered_at TEXT,
    bad_address_at TEXT,
    qr_scanned_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(property_id) REFERENCES properties(id),
    FOREIGN KEY(person_id) REFERENCES people(id)
);

CREATE TABLE IF NOT EXISTS openletterconnect_webhook_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_key TEXT,
    event_type TEXT,
    external_order_id TEXT,
    external_order_item_id TEXT,
    verification_status TEXT NOT NULL DEFAULT 'unverified',
    processing_status TEXT NOT NULL DEFAULT 'received',
    error_text TEXT,
    payload_json TEXT,
    headers_json TEXT,
    received_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS app_settings (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS app_errors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    route TEXT,
    status_code INTEGER,
    error_message TEXT NOT NULL,
    details TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    dismissed_at TEXT
);

CREATE TABLE IF NOT EXISTS slack_comp_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    status TEXT NOT NULL DEFAULT 'Queued',
    property_id INTEGER,
    address TEXT,
    raw_text TEXT,
    slack_channel_id TEXT,
    slack_user_id TEXT,
    slack_user_name TEXT,
    response_url TEXT,
    summary_text TEXT,
    analysis_json TEXT,
    report_path TEXT,
    slack_file_id TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TEXT,
    completed_at TEXT,
    FOREIGN KEY(property_id) REFERENCES properties(id)
);

CREATE INDEX IF NOT EXISTS idx_slack_comp_requests_status ON slack_comp_requests(status, id);

CREATE TABLE IF NOT EXISTS referral_realtors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT,
    phone TEXT,
    brokerage TEXT,
    target_markets TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS buyers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    business_name TEXT,
    email TEXT,
    phone TEXT,
    notes TEXT,
    target_counties TEXT,
    buyer_categories TEXT,
    property_types TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS referral_push_activity (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_uuid TEXT NOT NULL,
    realtor_id INTEGER NOT NULL,
    to_number TEXT,
    message_body TEXT NOT NULL,
    status TEXT,
    external_id TEXT,
    response_json TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(realtor_id) REFERENCES referral_realtors(id)
);

CREATE TABLE IF NOT EXISTS sequence_campaigns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    description TEXT,
    status TEXT NOT NULL DEFAULT 'Active',
    stop_on_reply INTEGER NOT NULL DEFAULT 1,
    send_window_start TEXT NOT NULL DEFAULT '10:00',
    send_window_end TEXT NOT NULL DEFAULT '16:30',
    timezone TEXT NOT NULL DEFAULT 'America/New_York',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sequence_steps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_id INTEGER NOT NULL,
    step_order INTEGER NOT NULL,
    delay_minutes INTEGER NOT NULL DEFAULT 0,
    channel TEXT NOT NULL DEFAULT 'SMS',
    subject_template TEXT,
    mail_template_id INTEGER,
    body_template TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(campaign_id) REFERENCES sequence_campaigns(id)
);

CREATE TABLE IF NOT EXISTS sequence_enrollments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_id INTEGER NOT NULL,
    property_id INTEGER NOT NULL,
    person_id INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'Active',
    started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    next_run_at TEXT,
    last_step_order INTEGER NOT NULL DEFAULT 0,
    completed_at TEXT,
    stopped_reason TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(campaign_id) REFERENCES sequence_campaigns(id),
    FOREIGN KEY(property_id) REFERENCES properties(id),
    FOREIGN KEY(person_id) REFERENCES people(id)
);

CREATE TABLE IF NOT EXISTS sequence_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    enrollment_id INTEGER NOT NULL,
    step_id INTEGER,
    step_order INTEGER,
    channel TEXT,
    status TEXT NOT NULL DEFAULT 'Queued',
    communication_id INTEGER,
    error_text TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(enrollment_id) REFERENCES sequence_enrollments(id),
    FOREIGN KEY(step_id) REFERENCES sequence_steps(id),
    FOREIGN KEY(communication_id) REFERENCES communications(id)
);

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
    completed_at TEXT,
    FOREIGN KEY(property_id) REFERENCES properties(id),
    FOREIGN KEY(root_subject_person_id) REFERENCES people(id)
);

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
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(run_id) REFERENCES obituary_expansion_runs(id),
    FOREIGN KEY(property_id) REFERENCES properties(id),
    FOREIGN KEY(subject_person_id) REFERENCES people(id),
    FOREIGN KEY(related_person_id) REFERENCES people(id)
);

CREATE TABLE IF NOT EXISTS reisift_followups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_uuid TEXT NOT NULL UNIQUE,
    status TEXT,
    full_address TEXT,
    owner_names TEXT,
    added_at TEXT,
    outbound_calls INTEGER NOT NULL DEFAULT 0,
    outbound_sms INTEGER NOT NULL DEFAULT 0,
    outbound_email INTEGER NOT NULL DEFAULT 0,
    inbound_responses INTEGER NOT NULL DEFAULT 0,
    events_json TEXT,
    tasks_json TEXT,
    payload_json TEXT,
    is_active INTEGER NOT NULL DEFAULT 1,
    last_synced_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS reisift_new_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_uuid TEXT NOT NULL UNIQUE,
    status TEXT,
    full_address TEXT,
    owner_names TEXT,
    county TEXT,
    added_at TEXT,
    reisift_updated_at TEXT,
    local_property_id INTEGER,
    local_status_before TEXT,
    local_status_after TEXT,
    outbound_calls INTEGER NOT NULL DEFAULT 0,
    inbound_calls INTEGER NOT NULL DEFAULT 0,
    outbound_sms INTEGER NOT NULL DEFAULT 0,
    inbound_sms INTEGER NOT NULL DEFAULT 0,
    outbound_email INTEGER NOT NULL DEFAULT 0,
    inbound_email INTEGER NOT NULL DEFAULT 0,
    inbound_responses INTEGER NOT NULL DEFAULT 0,
    events_json TEXT,
    tasks_json TEXT,
    payload_json TEXT,
    is_active INTEGER NOT NULL DEFAULT 1,
    last_synced_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(local_property_id) REFERENCES properties(id)
);

CREATE INDEX IF NOT EXISTS idx_reisift_new_records_active_county ON reisift_new_records(is_active, county, added_at);
CREATE INDEX IF NOT EXISTS idx_reisift_new_records_local_property ON reisift_new_records(local_property_id);

CREATE TABLE IF NOT EXISTS property_source_info (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_id INTEGER NOT NULL UNIQUE,
    property_uuid TEXT,
    source_info_bucket TEXT,
    sheriff_sale_date TEXT,
    source_info_raw TEXT,
    source_info_json TEXT,
    last_checked_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(property_id) REFERENCES properties(id)
);

CREATE INDEX IF NOT EXISTS idx_property_source_info_uuid ON property_source_info(property_uuid);

CREATE TABLE IF NOT EXISTS sms_automation_routing_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_key TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    is_active INTEGER NOT NULL DEFAULT 1,
    list_contains TEXT,
    bucket TEXT,
    contact_role TEXT,
    sequence_name TEXT,
    template_body TEXT NOT NULL,
    fallback_template_body TEXT,
    review_flags_json TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sms_automation_routing_rules_active ON sms_automation_routing_rules(is_active, priority);

CREATE TABLE IF NOT EXISTS sms_automation_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_key TEXT NOT NULL UNIQUE,
    property_id INTEGER NOT NULL,
    person_id INTEGER,
    touchpoint_id INTEGER,
    phone_number TEXT NOT NULL,
    from_number TEXT,
    contact_role TEXT,
    bucket TEXT,
    rule_key TEXT,
    sequence_name TEXT,
    step_order INTEGER NOT NULL DEFAULT 1,
    message_body TEXT NOT NULL,
    rendered_variables_json TEXT,
    source_info_json TEXT,
    status TEXT NOT NULL DEFAULT 'Draft',
    suppression_reason TEXT,
    scheduled_for TEXT,
    approved_at TEXT,
    sent_at TEXT,
    external_id TEXT,
    communication_id INTEGER,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(property_id) REFERENCES properties(id),
    FOREIGN KEY(person_id) REFERENCES people(id),
    FOREIGN KEY(touchpoint_id) REFERENCES touchpoints(id),
    FOREIGN KEY(communication_id) REFERENCES communications(id)
);

CREATE INDEX IF NOT EXISTS idx_sms_automation_queue_status ON sms_automation_queue(status, scheduled_for);
CREATE INDEX IF NOT EXISTS idx_sms_automation_queue_property ON sms_automation_queue(property_id);
