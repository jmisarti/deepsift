# Deep Prospect CRM (MVP)

Property-centric CRM for deep homeowner prospecting.

## What it does
- Tracks properties with owner and resident links.
- Stores contact touchpoints (phone/email/etc) with status and notes.
- Stores social accounts with status and notes.
- Maps relationship graph (relative, associate, neighbor) as references.
- Keeps property activity log.
- Exposes JSON API for light integrations.
- Includes lightweight public web lookup endpoint for prospecting context.
- Imports obituary-based relatives/spouse signals into a property record.
- Adds AI obituary extraction API using OpenAI with strict JSON schema.

## Stack
- Python 3.13
- Flask
- SQLite (local file: `crm.db`)

## Run locally
1. Create virtual environment (optional):
   - `python -m venv .venv`
   - `.\.venv\Scripts\Activate.ps1`
2. Install dependencies:
   - `python -m pip install -r requirements.txt`
3. Set AI env vars (for AI obituary import):
   - `$env:OPENAI_API_KEY="<your_key>"`
   - Optional: `$env:OPENAI_MODEL="gpt-4.1-mini"`
4. Start app:
   - `python app.py`
5. Open:
   - `http://127.0.0.1:5000`

On first run, the app creates schema and seed data using the Jill Smith example.

## Core routes
- UI
  - `/` dashboard
  - `/property/<id>` property workspace
  - `/person/<id>` person profile
  - `POST /property/<id>/obituary-import`
  - `POST /property/<id>/obituary-import-ai`
- API
  - `GET/POST /api/properties`
  - `GET /api/properties/<id>`
  - `POST /api/people/<id>/touchpoints`
  - `GET /api/people/<id>/network`
  - `GET /api/web-search?q=<query>`
  - `POST /api/obituary/extract` with `{ "url": "..." }`
  - `POST /api/obituary/ai-extract` with `{ "url": "...", "subject_name": "John Smith" }`
  - `POST /api/properties/<id>/import-obituary` with `{ "url": "...", "subject_person_id": 1 }`
  - `POST /api/properties/<id>/import-obituary-ai` with `{ "url": "...", "subject_person_id": 1 }`
  - `GET /api/properties/<id>/prospecting-snapshot`

## Notes
- AI obituary extraction requires `OPENAI_API_KEY`.
- Obituary extraction should be reviewed by an operator before outbound actions.
- Web lookup uses DuckDuckGo instant-answer endpoint and returns snippet-level context only.
- This is an MVP foundation designed to expand into stronger identity resolution and data-provider integrations.
