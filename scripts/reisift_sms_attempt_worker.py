"""Dedicated, low-volume ReiSIFT SMS attempt counter worker.

This process owns the external counter reconciliation so the web process can
continue accepting webhooks and sending SMS even when ReiSIFT is slow.
"""

import os
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Importing app must not start its normal in-process worker collection.
os.environ.setdefault("RUN_BACKGROUND_WORKERS", "false")

import app  # noqa: E402


def main():
    app.ensure_db()
    while True:
        result = app.run_reisift_sms_attempt_sync_once()
        if not result.get("ok"):
            print(f"SMS attempt sync error: {result.get('error', 'unknown error')}", flush=True)
        time.sleep(app.SMS_ATTEMPT_SYNC_POLL_SECONDS)


if __name__ == "__main__":
    main()
