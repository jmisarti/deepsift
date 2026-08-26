# DeepSift PostgreSQL Migration

## Safety Contract

- Production remains on SQLite until a separate PostgreSQL copy passes schema, row-count, fingerprint, worker, webhook, and UI validation.
- The migration tool refuses to write to PostgreSQL's `public` schema. Its default is `deepsift_shadow`.
- The target connection is read from `POSTGRES_MIGRATION_URL`; the application does not use that variable.
- A live SQLite database is copied through SQLite's online backup API before PostgreSQL loading begins.
- Existing PostgreSQL shadow tables cause an abort unless `--reset-schema` is explicitly supplied.
- `--reset-schema` is limited to clearly named DeepSift shadow, migration, or staging schemas.

## Current Railway Layout

- `deepsift`: live Flask application and `/data/crm.db` SQLite source.
- `deepsift-comp-worker`: existing comp worker.
- `Postgres`: isolated migration target. `deepsift` receives its URL only as `POSTGRES_MIGRATION_URL`; the live app does not read that variable.

## Verified Foundation (2026-08-26)

- Railway PostgreSQL 18.6 is online and isolated from production traffic.
- The immutable `deepsift_shadow` copy contains all 81 application tables, 120 translated secondary indexes, and 65 validated foreign keys.
- All 81 shadow tables match the SQLite snapshot by row count and two order-independent SHA-256 aggregates.
- A separate disposable `deepsift_stage` copy completed in 69.429 seconds from a 432,721,920-byte online SQLite snapshot.
- DeepSift's database adapter passed parameter, date, case-insensitive match, aggregation, generated-ID, rollback, and pooling checks.
- Rollback-only application writes passed for contact-context and EmailOctopus queue upserts, including recovery and logging from an intentionally aborted PostgreSQL transaction.
- DeepSift's complete `ensure_db` startup routine passed against `deepsift_stage` and remained idempotent.
- The primary New Records, Deep Prospecting, SMS Queue, SMS Schedule, Email Clicks, Customer Lookup, Dashboard, Agents, Buyers, and Settings pages all rendered successfully against PostgreSQL.
- The same primary pages passed against a disposable online copy of the production SQLite database, confirming the default backend remains backward-compatible.
- A pooled concurrency test completed 48 overlapping read/write-rollback tasks through 8 connections in 2.055 seconds. All generated IDs were unique and no smoke rows persisted.
- Production remains on SQLite. Neither `CRM_DB_BACKEND` nor `DATABASE_URL` has been switched on the live service.

## Commands

Inspect a SQLite database without contacting PostgreSQL:

```powershell
python -m scripts.postgres_migration --source C:\path\to\crm.db --plan
```

Copy into a fresh shadow schema:

```powershell
$env:POSTGRES_MIGRATION_URL = "<isolated PostgreSQL URL>"
python -m scripts.postgres_migration --source C:\path\to\crm.db --report postgres-migration-report.json
```

Revalidate an existing copy without writing data:

```powershell
python -m scripts.postgres_migration --source C:\path\to\crm.db --validate-only --report postgres-validation-report.json
```

The final report contains no credentials. It records the SQLite schema hash, object counts, PostgreSQL version, and a count plus two order-independent SHA-256 aggregates for every table.

Run the rollback-only adapter and concurrency gates:

```powershell
python -m scripts.postgres_adapter_smoke --schema deepsift_stage
python -m scripts.postgres_concurrency_smoke --schema deepsift_stage --tasks 48 --workers 8
```

Run DeepSift's startup and page-render gate from a staging code bundle:

```powershell
python -m scripts.postgres_app_smoke `
  --code-dir C:\path\to\staging-code `
  --schema deepsift_stage `
  --run-startup `
  --render-operational-pages
```

Application PostgreSQL mode requires `CRM_DB_BACKEND=postgres`, `CRM_POSTGRES_URL`, and `CRM_POSTGRES_SCHEMA`. Pooling is enabled by default and can be tuned with `CRM_POSTGRES_POOL_MIN_SIZE`, `CRM_POSTGRES_POOL_MAX_SIZE`, and `CRM_POSTGRES_POOL_TIMEOUT_SECONDS`.

For the final write freeze, set both `RUN_BACKGROUND_WORKERS=false` and `CRM_WRITE_PAUSED=true`. The application continues to answer `/healthz`, while all other requests receive `503 Service Unavailable` with a `Retry-After` header. Clear `CRM_WRITE_PAUSED` only after the PostgreSQL deployment and post-cutover checks pass.

## Staged Rollout

1. Build and unit-test the shadow copy tooling.
2. Copy a production SQLite snapshot into `deepsift_shadow` and require all 81 table fingerprints to match.
3. Add a dual-dialect application adapter behind an explicit feature flag.
4. Run a separate staging application and workers against the validated PostgreSQL schema. The startup and read/UI portion is complete; provider-facing workers remain gated.
5. Replay representative webhook, SMS, email, refresh, lookup, and admin write workflows in staging with test credentials or outbound providers disabled.
6. Take a final SQLite snapshot, perform a short write freeze, copy the delta/final dataset, and validate again.
7. Point one production release at PostgreSQL with the SQLite snapshot retained for rollback.
8. Monitor database errors, webhook acknowledgements, queue depth, worker leases, and record counts before declaring cutover complete.

No production cutover should occur merely because the data copy succeeds. Application SQL compatibility and concurrent-worker behavior are separate gates.

## Cutover And Backout

The final cutover must use a fresh online SQLite snapshot followed by a short write freeze, final copy, and complete fingerprint validation. Only then should the service receive `CRM_DB_BACKEND=postgres`, `CRM_POSTGRES_URL=${{Postgres.DATABASE_URL}}`, and a production schema name. Keep the final SQLite snapshot and volume unchanged during the observation window; rollback is the removal of the PostgreSQL backend variables followed by a health-checked redeploy against that retained SQLite database.
