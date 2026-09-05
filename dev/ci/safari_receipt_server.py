#!/usr/bin/env python3
"""Serve a built diagnostic page and retain bounded metadata-only page trace."""
from __future__ import annotations
import argparse
import json
import datetime as dt
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

class Handler(SimpleHTTPRequestHandler):
    root: Path
    trace: Path
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(self.root), **kwargs)
    def do_POST(self):
        if urlparse(self.path).path != "/__safari_trace":
            self.send_error(404)
            return
        try:
            size = int(self.headers.get("Content-Length", "0"))
            if not 0 <= size <= 16_384:
                raise ValueError("invalid content length")
            payload = json.loads(self.rfile.read(size))
            if not isinstance(payload, dict):
                raise ValueError("expected object")
            events = payload.get("events", [])
            if not isinstance(events, list) or len(events) > 1:
                raise ValueError("expected one event")
            # Deliberately retain only the approved metadata schema.
            approved = ({
                "receivedAt": dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds"),
                **{key: event[key] for key in ("origin", "role", "pageRun", "phase", "elapsedMs", "workerElapsedMs", "direction", "messageType", "frameCount", "frameBytes", "errorKind", "operation", "operationId", "tickPhase", "outcome") if key in event},
            } for event in events if isinstance(event, dict))
            with self.trace.open("a") as file:
                for event in approved:
                    file.write(json.dumps(event, sort_keys=True) + "\n")
        except (ValueError, json.JSONDecodeError):
            self.send_error(400)
            return
        self.send_response(204)
        self.end_headers()

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", required=True)
    parser.add_argument("--trace", required=True)
    parser.add_argument("--port", type=int, default=4173)
    args = parser.parse_args()
    Handler.root = Path(args.directory).resolve()
    Handler.trace = Path(args.trace).resolve()
    Handler.trace.parent.mkdir(parents=True, exist_ok=True)
    ThreadingHTTPServer(("127.0.0.1", args.port), Handler).serve_forever()
if __name__ == "__main__": main()
