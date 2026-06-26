#!/usr/bin/env python3
"""Print a compact table from benchmark JSONL."""

from __future__ import annotations

import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any


def grouped_rows(path: str) -> dict[tuple[Any, ...], list[dict[str, Any]]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for line in Path(path).read_text().splitlines():
        if line.strip():
            row = json.loads(line)
            groups[
                (
                    row.get("scenario"),
                    row.get("engine"),
                    row.get("subscriptions"),
                    row.get("groove_acl_series", ""),
                )
            ].append(row)
    return groups


def med(rows: list[dict[str, Any]], key: str) -> float:
    value: Any
    values = []
    for row in rows:
        value = row
        for part in key.split("."):
            value = value[part]
        values.append(float(value))
    return statistics.median(values)


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: bench_summary.py RESULTS.jsonl", file=sys.stderr)
        return 2
    print(
        "scenario\tengine\tseries\tsubs\tcommit_p50_us\tcommit_p95_us\t"
        "tick_p50_us\tstorage_p50_us\tnotifications\trows\tgraph_nodes\t"
        "arrangements\tarrangement_rows\tdedupe\tcache_rows"
    )
    for key, rows in sorted(grouped_rows(sys.argv[1]).items()):
        scenario, engine, subscriptions, series = key
        sample = rows[0]
        print(
            "\t".join(
                [
                    str(scenario),
                    str(engine),
                    str(series),
                    str(subscriptions),
                    f"{med(rows, 'commit_us.p50'):.0f}",
                    f"{med(rows, 'commit_us.p95'):.0f}",
                    f"{med(rows, 'tick_us.p50'):.0f}",
                    f"{med(rows, 'storage_us.p50'):.0f}",
                    str(sample["notifications"]),
                    str(sample["notification_records"]),
                    str(sample["graph_nodes"]),
                    str(sample["arrangements"]),
                    str(sample["arrangement_rows"]),
                    f"{sample['dedupe_ratio']:.3f}",
                    str(sample["result_cache_rows"]),
                ]
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
