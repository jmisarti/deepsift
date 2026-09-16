"""Dedicated, low-volume ReiSIFT SMS attempt counter worker.

This process owns the external counter reconciliation so the web process can
continue accepting webhooks and sending SMS even when ReiSIFT is slow.
"""

import os
import sys
import time
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Importing app must not start its normal in-process worker collection.
os.environ.setdefault("RUN_BACKGROUND_WORKERS", "false")

import app  # noqa: E402


WORKER_READY = False


def start_health_server():
    port_text = os.getenv("PORT", "").strip()
    if not port_text:
        return
    try:
        port = int(port_text)
    except ValueError:
        return

    class HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/healthz":
                self.send_response(404)
                self.end_headers()
                return
            self.send_response(200 if WORKER_READY else 503)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"ok":true}' if WORKER_READY else b'{"ok":false}')

        def log_message(self, _format, *_args):
            return

    server = ThreadingHTTPServer(("0.0.0.0", port), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()


def main():
    global WORKER_READY
    start_health_server()
    app.ensure_db()
    WORKER_READY = True
    while True:
        result = app.run_reisift_sms_attempt_sync_once()
        if not result.get("ok"):
            print(f"SMS attempt sync error: {result.get('error', 'unknown error')}", flush=True)
        time.sleep(app.SMS_ATTEMPT_SYNC_POLL_SECONDS)


if __name__ == "__main__":
    main()
