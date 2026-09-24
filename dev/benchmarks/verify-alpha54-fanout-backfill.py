#!/usr/bin/env python3
"""Fail closed unless the checkout retains every released engine crate byte."""
import datetime
import json
from pathlib import Path
import subprocess


def git(*args):
    return subprocess.check_output(['git', *args], text=True).strip()


metadata = json.loads(Path('dev/benchmarks/alpha54-fanout-backfill.json').read_text())
# The checkout's shallow history need not include the tag: literal tree hashes
# pin every engine crate directly to the reviewed release v2.0.0-alpha.54.
expected = {
    "crates/benchmark-guard": "f76bb8f12b99e4c15a02d6594713d73e0b91addb",
    "crates/groove": "6b5303b300f71c4be092577fab3726bca4115ba8",
    "crates/idb-tree": "37e229133a638dae1517ea90d2e79480b419ca85",
    "crates/invariant-registry.jsonl": "09cb3de6001b1c97c01f7d70d3e75cb26a22fa13",
    "crates/jazz": "233c73cd76e6fd4df199f20c26a31ecff709ff84",
    "crates/jazz-cli": "e1fcc81ddc54a13404320c39bc2356bad609b398",
    "crates/jazz-compression": "af72e45e5daaedd004ecccd14bb0a59364522f27",
    "crates/jazz-napi": "aeaf4f3dc82eb7ba21a3fb0a5b3abc8bb111799e",
    "crates/jazz-native-relay": "8a07b6643512d126eb9d0b6b1d109e5bf81a69e5",
    "crates/jazz-native-transport": "3c8873dc3d95f179d4dea5ca3aa9aa807eda4152",
    "crates/jazz-otel": "631c69b6a6a4e23339d1b5d40df74e0e3aa5ceca",
    "crates/jazz-rn": "34de142325643a494d2fdab800c950fd6d244bfb",
    "crates/jazz-server": "19fa5a5a2d00d144d30bfdfcfc6e061ec7669756",
    "crates/jazz-sim": "7ae81129e98913bc5d41138f309673175fdd8166",
    "crates/jazz-storage-rocksdb": "58731ad6dff71d0020f69f2c336f0e0b3ad77ef0",
    "crates/jazz-storage-sqlite": "1493f1a960632af15eb3ef79fb105bb421b06ba9",
    "crates/jazz-testkit": "2beb43f6f9db2da40d940def47e700b211d1c40e",
    "crates/jazz-wasm": "2a70791e5f5ee5ef46f4c575a37976c428fcad3e",
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
Path('target/alpha54-fanout-backfill-provenance.json').write_text(json.dumps(metadata, indent=2) + '\n')
print(json.dumps(metadata, indent=2))
