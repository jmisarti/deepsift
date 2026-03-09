# Referral/Realtor Migration Log

Generated: 2026-03-08 21:00:48
Source DB: C:\Users\john\OneDrive\OpenAI Codex\crm.db

- referral_realtors: 8 row(s)
- reisift_referrals: 8 row(s)
- referral_push_activity: 9 row(s)

## Included Files
- referral_realtors.json
- reisift_referrals.json
- referral_push_activity.json
- migrate_referral_realtors.sql

## Import Notes
- Run SQL script against the target crm.db.
- Script uses UPSERT logic to avoid duplicate-key failures.
- Scope is referral and realtor data only.
