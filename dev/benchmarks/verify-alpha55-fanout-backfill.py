#!/usr/bin/env python3
"""Fail closed unless the checkout retains every released engine crate byte."""
import datetime
import json
from pathlib import Path
import subprocess


def git(*args):
    return subprocess.check_output(['git', *args], text=True).strip()


metadata = json.loads(Path('dev/benchmarks/alpha55-fanout-backfill.json').read_text())
# The checkout's shallow history need not include the tag: literal tree hashes
# pin every engine crate directly to the reviewed release v2.0.0-alpha.55.
expected = {
    "crates/benchmark-guard": "5fe54fd7e1130757acf7ec736f4f374573d9e41b",
    "crates/groove": "32b9fed681c1116e455f278271953222137d75bb",
    "crates/idb-tree": "9dbe795d4e968243416f00a9d59b571d1e1e9dca",
    "crates/invariant-registry.jsonl": "d446980d2b6bd43376cf5e98e801f3f2267a1e11",
    "crates/jazz": "81ae4e44a00d6ae3817e8bdfad0dfade48a9ddb0",
    "crates/jazz-cli": "4c07a72a4d6c9e5dff97335ff087496bec7344dc",
    "crates/jazz-compression": "66df3d0fb8dbf1b7a07aab2399a46a52c75ba51b",
    "crates/jazz-napi": "61c5ba7277e65b9cdd89a893f76db16871f173c9",
    "crates/jazz-native-relay": "1c87c1fb1c7fa9f9e1cab36f43e45a6c58d5dc93",
    "crates/jazz-native-transport": "5c87f435c9fc59f357b58b5b406e883b6166390e",
    "crates/jazz-otel": "0155968297524ede36e3026c2d9708bb9d7fa27d",
    "crates/jazz-rn": "f4c637b14bd25f1cd54079cfb87fab0f38476ebf",
    "crates/jazz-server": "58afdb13187ccf3a7c994ab159c636a8f6e8602b",
    "crates/jazz-sim": "22b862ea4f6aba18a67b1b75dbbd80f32fb95aa6",
    "crates/jazz-storage-rocksdb": "5bf8ea213104ef151517232fdd0ea6d5367c3006",
    "crates/jazz-storage-sqlite": "2ecd4a09f7a86356109a6a16ade8c78d562e0f91",
    "crates/jazz-testkit": "1ecb585f4c7fd2919f2f3fb15837a561a909f2c4",
    "crates/jazz-wasm": "3300e199111ef99b198bc77e7c9c4bb9c62065ca",
    "crates/package.json": "804d592fc17a8984036c3bbfa117ba2aa2597e25",
    "crates/wasm-tracing": "4dc064f62c484ed26a61b54973cbf18d341ffedc"
}
for path, tree in expected.items():
    if git('rev-parse', f'HEAD:{path}') != tree:
        raise SystemExit(f'released engine differs: {path}')
if git('status', '--porcelain', '--untracked-files=no'):
    raise SystemExit('checkout has tracked modifications')
metadata['harnessSha'] = git('rev-parse', 'HEAD')
metadata['measuredAt'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
metadata['measuredAtSource'] = 'workflow pre-measurement wall clock; CodSpeed run timestamp is authoritative'
metadata['engineTrees'] = expected
Path('target').mkdir(exist_ok=True)
Path('target/alpha55-fanout-backfill-provenance.json').write_text(json.dumps(metadata, indent=2) + '\n')
print(json.dumps(metadata, indent=2))
