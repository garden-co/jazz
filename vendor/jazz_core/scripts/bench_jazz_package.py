#!/usr/bin/env python3
"""Capture jazz package benches as retained-results JSONL."""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import socket
import subprocess
import sys
from datetime import datetime, timezone
from typing import Any


SUPPORTED_BENCHES = ("sync", "cold_subscription")
BENCH_LINE = re.compile(
    r"^depth (?P<depth>\d+), ahead (?P<ahead>\d+): "
    r"global_current_rows_update = (?P<global>[^,]+), "
    r"local_current_rows_update = (?P<local>.+)$"
)
DURATION = re.compile(r"^(?P<value>\d+(?:\.\d+)?)(?P<unit>ns|\u00b5s|us|ms|s)$")


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


def metadata(bench: str) -> dict[str, Any]:
    data: dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "git_sha": cmd_output(["git", "rev-parse", "HEAD"]),
        "git_dirty": code_dirty(),
        "host": socket.gethostname(),
        "platform": platform.platform(),
        "rustc": cmd_output(["rustc", "--version"]),
        "package": "jazz",
        "bench": bench,
        "run_id": os.environ.get("JAZZ_BENCH_RUN_ID", ""),
    }
    knobs = {
        name: os.environ[name]
        for name in sorted(os.environ)
        if name.startswith("JAZZ_") or name.startswith("GROOVE_")
    }
    if knobs:
        data["knobs"] = knobs
    return data


def cargo_command(bench: str) -> list[str]:
    return ["cargo", "bench", "-p", "jazz", "--bench", bench, "--quiet"]


def duration_us(value: str) -> float:
    match = DURATION.match(value.strip())
    if match is None:
        raise ValueError(f"unsupported duration: {value}")
    number = float(match.group("value"))
    unit = match.group("unit")
    if unit == "ns":
        return number / 1000.0
    if unit in {"\u00b5s", "us"}:
        return number
    if unit == "ms":
        return number * 1000.0
    if unit == "s":
        return number * 1_000_000.0
    raise ValueError(f"unsupported duration unit: {unit}")


def parse_cold_subscription(line: str) -> dict[str, Any] | None:
    match = BENCH_LINE.match(line)
    if match is None:
        return None
    return {
        "scenario": "cold_subscription",
        "depth": int(match.group("depth")),
        "ahead": int(match.group("ahead")),
        "global_current_rows_update_us": duration_us(match.group("global")),
        "local_current_rows_update_us": duration_us(match.group("local")),
    }


def emit_payload(bench: str, base: dict[str, Any], line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return True
    payload: dict[str, Any] | None = None
    try:
        parsed = json.loads(stripped)
        if isinstance(parsed, dict):
            payload = parsed
    except json.JSONDecodeError:
        if bench == "cold_subscription":
            payload = parse_cold_subscription(stripped)
    if payload is None:
        print(line, end="", file=sys.stderr)
        return False
    print(json.dumps({**base, **payload}, sort_keys=True), flush=True)
    return True


def run_bench(bench: str) -> int:
    base = metadata(bench)
    proc = subprocess.Popen(
        cargo_command(bench),
        text=True,
        stdout=subprocess.PIPE,
        stderr=sys.stderr,
    )
    assert proc.stdout is not None
    parse_ok = True
    for line in proc.stdout:
        parse_ok = emit_payload(bench, base, line) and parse_ok
    code = proc.wait()
    if code != 0:
        return code
    return 0 if parse_ok else 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run selected jazz benches and emit metadata-enriched JSONL."
    )
    parser.add_argument(
        "benches",
        nargs="*",
        choices=SUPPORTED_BENCHES,
        default=list(SUPPORTED_BENCHES),
        help="bench names to capture; defaults to sync and cold_subscription",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="list supported benches without running cargo",
    )
    parser.add_argument(
        "--print-command",
        action="store_true",
        help="print cargo commands without running them",
    )
    args = parser.parse_args()

    if args.list:
        print("\n".join(SUPPORTED_BENCHES))
        return 0

    if args.print_command:
        for bench in args.benches:
            print(" ".join(cargo_command(bench)))
        return 0

    status = 0
    for bench in args.benches:
        status = run_bench(bench) or status
    return status


if __name__ == "__main__":
    raise SystemExit(main())
