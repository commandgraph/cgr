#!/usr/bin/env python3
import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


class DemoHandler(BaseHTTPRequestHandler):
    data_dir: Path

    def _send_json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args) -> None:
        return

    def do_GET(self) -> None:
        if self.path == "/health":
            self._send_json(200, {"status": "ok", "service": "demo-api"})
            return
        if self.path == "/events":
            events_path = self.data_dir / "events.log"
            records_path = self.data_dir / "records.jsonl"
            events = events_path.read_text().splitlines() if events_path.exists() else []
            record_count = len(records_path.read_text().splitlines()) if records_path.exists() else 0
            self._send_json(200, {"events": events, "records": record_count})
            return
        self._send_json(404, {"error": "not found"})

    def do_POST(self) -> None:
        if self.path != "/records":
            self._send_json(404, {"error": "not found"})
            return

        auth = self.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            self._send_json(401, {"error": "missing bearer token"})
            return

        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length).decode("utf-8") if length else "{}"
        payload = json.loads(raw or "{}")

        records_path = self.data_dir / "records.jsonl"
        events_path = self.data_dir / "events.log"
        existing = records_path.read_text().splitlines() if records_path.exists() else []
        record_id = len(existing) + 1

        stored = {"id": record_id, "payload": payload}
        with records_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(stored) + "\n")
        with events_path.open("a", encoding="utf-8") as fh:
            fh.write(f"created:{record_id}:{payload.get('name', 'unknown')}\n")

        self._send_json(201, {"id": record_id, "status": "created", "name": payload.get("name", "")})


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=18080)
    ap.add_argument("--data-dir", required=True)
    args = ap.parse_args()

    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    DemoHandler.data_dir = data_dir

    server = ThreadingHTTPServer((args.host, args.port), DemoHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
