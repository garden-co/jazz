#!/usr/bin/env python3
"""Fail closed unless the checkout retains every released engine crate byte."""
import datetime
import json
from pathlib import Path
import subprocess


def git(*args):
    return subprocess.check_output(['git', *args], text=True).strip()


metadata = json.loads(Path('dev/benchmarks/alpha56-fanout-backfill.json').read_text())
# The checkout's shallow history need not include the tag: literal tree hashes
# pin every engine crate directly to the reviewed release v2.0.0-alpha.56.
expected = {
    "crates/benchmark-guard": "5fe54fd7e1130757acf7ec736f4f374573d9e41b",
    "crates/groove": "066b6d1c8e1d89ceea0d3ab32c487a1c32a386d5",
    "crates/idb-tree": "9dbe795d4e968243416f00a9d59b571d1e1e9dca",
    "crates/invariant-registry.jsonl": "37843c65e11cae38ceb7bc47ec5945449e4089ee",
    "crates/jazz": "d3af510741e8f069cf0f2f88ef78990959591867",
    "crates/jazz-cli": "d9a97bf94853abae6ce283f3b9cb5839213e0a2e",
    "crates/jazz-compression": "4f37107aa494891bd18e27c6a5d33a64af18e421",
    "crates/jazz-napi": "0048d488359df41088292f16e7076df68e3280b9",
    "crates/jazz-native-relay": "665fa07f7f63833e51224256134435b28435a28f",
    "crates/jazz-native-transport": "aeec0f59df106c1e869a26f81c0a2acbe00aad28",
    "crates/jazz-otel": "c6daeb856276ea9f8fca11c095c9054ef19f1f52",
    "crates/jazz-rn": "53d1105ef8659b288a625c352b1f133adb293f0a",
    "crates/jazz-server": "82c5604e93cb01b3beb604358b5e179a3c6736db",
    "crates/jazz-sim": "2ec3f65997f08453ad8ac3210c9d74cf865b18da",
    "crates/jazz-storage-rocksdb": "5bf8ea213104ef151517232fdd0ea6d5367c3006",
    "crates/jazz-storage-sqlite": "2ecd4a09f7a86356109a6a16ade8c78d562e0f91",
    "crates/jazz-testkit": "ada7376dba4a9692ef310ee1708974b123c2d46b",
    "crates/jazz-wasm": "4e11a8de2379c7e5bd85f476294d7738c5126c23",
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
Path('target/alpha56-fanout-backfill-provenance.json').write_text(json.dumps(metadata, indent=2) + '\n')
print(json.dumps(metadata, indent=2))
