#!/usr/bin/env python3
"""Compare two retained benchmark JSONL files."""

from __future__ import annotations

import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any


COUNTERS = [
    "notifications",
    "notification_records",
    "engine_records_processed",
    "graph_nodes",
    "arrangements",
    "arrangement_rows",
    "arrangement_bytes",
    "logical_nodes_requested",
    "deduped_graph_nodes",
    "result_cache_rows",
]

TIMINGS = ["commit_us", "storage_us", "tick_us"]


def load(path: str) -> dict[tuple[Any, ...], list[dict[str, Any]]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for line in Path(path).read_text().splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        key = (
            row.get("scenario"),
            row.get("engine"),
            row.get("subscriptions"),
            row.get("groove_acl_series", ""),
        )
        grouped[key].append(row)
    return grouped


def median(rows: list[dict[str, Any]], path: str) -> float:
    value: Any
    parts = path.split(".")
    values = []
    for row in rows:
        value = row
        for part in parts:
            value = value[part]
        values.append(float(value))
    return statistics.median(values)


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: bench_compare.py OLD.jsonl NEW.jsonl", file=sys.stderr)
        return 2
    old = load(sys.argv[1])
    new = load(sys.argv[2])
    keys = sorted(set(old) | set(new))
    failed = False
    for key in keys:
        print(f"\n{key}")
        if key not in old or key not in new:
            print("  only present in one file")
            failed = True
            continue
        for counter in COUNTERS:
            old_value = median(old[key], counter)
            new_value = median(new[key], counter)
            marker = "==" if old_value == new_value else "!="
            if old_value != new_value:
                failed = True
            print(f"  {counter}: {old_value:g} {marker} {new_value:g}")
        for timing in TIMINGS:
            old_value = median(old[key], f"{timing}.p50")
            new_value = median(new[key], f"{timing}.p50")
            ratio = new_value / old_value if old_value else float("inf")
            print(f"  {timing}.p50: {old_value:g} -> {new_value:g} ({ratio:.2f}x)")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
