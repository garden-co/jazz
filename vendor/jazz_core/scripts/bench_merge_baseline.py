#!/usr/bin/env python3
"""Merge fresh engine benchmark rows with frozen comparison baseline rows."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


def key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("scenario"),
        row.get("engine"),
        row.get("subscriptions"),
        row.get("groove_acl_series", ""),
        row.get("groove_bench_repetition", ""),
    )


def load(path: str) -> list[dict[str, Any]]:
    rows = []
    for line in Path(path).read_text().splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def main() -> int:
    if len(sys.argv) != 3:
        print(
            "usage: bench_merge_baseline.py FROZEN_BASELINE.jsonl FRESH_RUN.jsonl",
            file=sys.stderr,
        )
        return 2

    fresh_rows = load(sys.argv[2])
    fresh_keys = {key(row) for row in fresh_rows}
    for row in load(sys.argv[1]):
        if key(row) not in fresh_keys:
            print(json.dumps(row, sort_keys=True))
    for row in fresh_rows:
        print(json.dumps(row, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
