# Deep Prospect CRM

Property-centric CRM for deep homeowner prospecting, relationship mapping, SMS/email workflows, and ReiSift integrations.

## Stack
- Python 3.13
- Flask
- SQLite (local default) or persistent volume path via `CRM_DB_PATH`

## Local Run
1. Create venv (optional):
   - `python -m venv .venv`
   - `.\.venv\Scripts\Activate.ps1`
2. Install deps:
   - `python -m pip install -r requirements.txt`
3. Copy env template:
   - `Copy-Item .env.example .env`
4. Set needed keys in `.env` (OpenAI, SmrtPhone, SkipSherpa, ReiSift, etc.)
5. Start:
   - `python app.py`
6. Open:
   - `http://127.0.0.1:5000`

## Security Hardening Added
- No hardcoded API key defaults in code.
- Secret key from env (`FLASK_SECRET_KEY` / `SECRET_KEY`).
- Secure session cookie options (`HTTPOnly`, `SameSite=Lax`, optional `Secure`).
- Optional app login gate for all UI/API routes:
  - `APP_AUTH_ENABLED=1`
  - `APP_AUTH_USERNAME=...`
  - `APP_AUTH_PASSWORD=...` (or `APP_AUTH_PASSWORD_HASH`)

Generate a password hash (optional):
```powershell
@'
from werkzeug.security import generate_password_hash
print(generate_password_hash("YourStrongPasswordHere"))
'@ | python -
```

## Railway Deploy (Recommended)
This repo includes:
- `Procfile`
- `railway.json` (healthcheck: `/healthz`)
- Gunicorn in `requirements.txt`

### Deploy Steps
1. Push repo to GitHub.
2. In Railway: New Project -> Deploy from GitHub repo.
3. Add a persistent volume (for SQLite) and set:
   - `CRM_DB_PATH=/data/crm.db`
4. Set Railway Variables from `.env.example`.
5. Important production vars:
   - `FLASK_DEBUG=0`
   - `SESSION_COOKIE_SECURE=1`
   - `APP_AUTH_ENABLED=1`
   - `APP_AUTH_USERNAME=...`
   - `APP_AUTH_PASSWORD` or `APP_AUTH_PASSWORD_HASH`
6. Point webhook providers to your Railway public URL endpoints.

### Secrets Handling
- Keep secrets only in Railway Variables (never commit to git).
- `.env` is gitignored locally.
- Rotate any previously exposed keys before production cutover.

## Notes
- If your app must run 24/7 and receive webhooks reliably, do not depend on local machine + tunnel.
- Selenium/web scraping workers should run as separate workers and push results into this app via API.

## Slack Comp Reports
- Point a Slack slash command at `POST /webhooks/slack/command`.
- Use `comp <address>` or `comp property #128` from the existing Agent Ops command, or map a dedicated `/comp` command to the same endpoint.
- Railway only queues the request and uploads the finished XLSX back to Slack.
- Codex claims queued jobs through `POST /api/slack-comp-requests/claim`, runs the uploaded `real-estate-comping` skill, then completes the job through `POST /api/slack-comp-requests/<id>/complete`.
- XLSX upload requires `SLACK_BOT_TOKEN` or the Integrations tab `Slack Bot Token` setting with Slack file upload permissions.
- This flow is intentionally not RentCast and not an app-side recreation of the comping skill.

