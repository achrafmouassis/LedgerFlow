"""Private Docker-network gateway; no Docker socket or arbitrary command input."""
import hmac
import json
import os
import subprocess
import sys
import threading
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from ledgerflow_batch.config import DatabaseSettings, DateRange
from .recovery import ingestion_ready, recover_runs

RUN_LOCK = threading.Lock()


def parse_period(body):
    if set(body) != {"start", "end"}:
        raise ValueError("Supply start and end dates only")
    return DateRange(date.fromisoformat(body["start"]), date.fromisoformat(body["end"]))


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass  # Never log headers, credentials or request bodies.

    def respond(self, status, value):
        encoded = json.dumps(value).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        try:
            self.wfile.write(encoded)
        except (BrokenPipeError, ConnectionResetError):
            pass  # Client disconnected; the batch already completed or failed.

    def do_GET(self):
        self.respond(200 if self.path == "/health" else 404, {"service": "batch-worker"})

    def do_POST(self):
        expected = "Bearer " + os.environ["BATCH_WORKER_TOKEN"]
        if not hmac.compare_digest(self.headers.get("Authorization", ""), expected):
            self.respond(401, {"error": "Unauthorized"})
            return
        if self.path != "/run":
            self.respond(404, {"error": "NotFound"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 1024:
                raise ValueError("Invalid body length")
            period = parse_period(json.loads(self.rfile.read(length)))
        except (ValueError, TypeError, KeyError):
            self.respond(400, {"error": "InvalidDateRange"})
            return
        if not RUN_LOCK.acquire(blocking=False):
            self.respond(409, {"error": "BatchAlreadyRunning"})
            return
        try:
            with DatabaseSettings.from_env().connect() as connection:
                recover_runs(connection)
                if not ingestion_ready(connection, period):
                    self.respond(409, {"error": "IngestionNotComplete"})
                    return
            process = subprocess.run(
                [sys.executable, "-m", "ledgerflow_batch", "--from", str(period.start), "--to", str(period.end)],
                capture_output=True, text=True, timeout=3600, check=False,
            )
            # Spark output may include infrastructure details: only return the CLI JSON.
            result = json.loads(process.stdout.strip().splitlines()[-1]) if process.returncode == 0 else {"error": "BatchFailed"}
            self.respond(200 if process.returncode == 0 else 500, result)
        except subprocess.TimeoutExpired:
            self.respond(504, {"error": "BatchTimeout"})
        except Exception as error:
            self.respond(500, {"error": type(error).__name__})
        finally:
            RUN_LOCK.release()


def serve():
    if len(os.environ.get("BATCH_WORKER_TOKEN", "")) < 24:
        raise ValueError("BATCH_WORKER_TOKEN must contain at least 24 characters")
    ThreadingHTTPServer(("0.0.0.0", 8090), Handler).serve_forever()
