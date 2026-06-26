#!/usr/bin/env python3
"""Run one scenario benchmark and enrich each JSON line with run metadata."""

from __future__ import annotations

import json
import os
import platform
import socket
import subprocess
import sys
from datetime import datetime, timezone


def cmd_output(args: list[str]) -> str:
    return subprocess.check_output(args, text=True).strip()


def code_dirty() -> bool:
    status = cmd_output(["git", "status", "--short", "--untracked-files=no"])
    for line in status.splitlines():
        parts = line.split(maxsplit=1)
        path = parts[1] if len(parts) > 1 else ""
        if path and not path.startswith("benchmarks/results/"):
            return True
    return False


def main() -> int:
    metadata = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "git_sha": cmd_output(["git", "rev-parse", "HEAD"]),
        "git_dirty": code_dirty(),
        "host": socket.gethostname(),
        "platform": platform.platform(),
        "rustc": cmd_output(["rustc", "--version"]),
        "run_id": os.environ.get("GROOVE_BENCH_RUN_ID", ""),
    }
    for name in sorted(os.environ):
        if name.startswith("GROOVE_"):
            metadata[name.lower()] = os.environ[name]

    proc = subprocess.Popen(
        ["cargo", "bench", "-p", "groove", "--bench", "scenario", "--quiet"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=sys.stderr,
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        stripped = line.strip()
        if not stripped:
            continue
        try:
            payload = json.loads(stripped)
        except json.JSONDecodeError:
            print(line, end="")
            continue
        payload = {**metadata, **payload}
        print(json.dumps(payload, sort_keys=True), flush=True)
    return proc.wait()


if __name__ == "__main__":
    raise SystemExit(main())
