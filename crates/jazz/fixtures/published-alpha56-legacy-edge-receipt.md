# Published alpha.56 legacy Edge receipt

`published-alpha56-legacy-edge-receipt-rocksdb.tar.gz.base64` is the RocksDB
root of a persistent native client, written entirely by npm's distributed
`jazz-tools`, `jazz-napi` and `@garden-co/jazz-napi-linux-x64-gnu`
`2.0.0-alpha.56` packages. No workspace Jazz code is built or imported.
`published-alpha56-legacy-edge-receipt.json` records the tarball URLs,
checksums and integrity values, the Linux native binary manifest (source head
`f704e2fad3e88401522ad02a8e331ceff1714eb4`, distinct from the release tag
commit `8af685e184c8691a244da1171a51c745a7208e3b`), the archive checksum, the
producer receipt, and the exact key and value bytes of both stored
`jazz_transactions` records.

It pins the byte state that alpha.56-and-earlier clients hold after a retired
Edge server acknowledged a write that Core never saw:

- `edgeAccepted`: fate tag 1 (Accepted), durability tag 2 (the retired Edge
  tier), `global_time` null. Current code decodes this as Pending/Local and
  replays it to Core through the author-scoped resend scan (#3265).
- `coreConfirmed`: a control write from the same client that reached Core:
  fate tag 1, durability tag 3 (Global), `global_time` present. It must keep
  its Accepted/Global outcome and is never replayed.

The test `published_alpha56_legacy_edge_receipt_reopens_as_pending_local_and_is_resent`
checks both records byte-for-byte in the physical store before current code
opens it and again afterwards (reopen only reads them), then asserts the
decoded fate, durability, tx id, author, row and resend-scan membership.

There is no published fixture for a tag-2 record that already carries a
global time. Alpha.56 stores a global time only together with Global
durability (`ingest_known_transaction` asserts this), so the producer has no
public path to that byte state. The codec rule alone keeps such a record
Accepted.

## Explicit reproduction

This is a maintainer action. CI never runs the producer or downloads
replacement bytes. From the repository root, fetch the pinned packages into an
isolated, ignored prefix. Verify the tarballs and the installed lockfile
integrity against the provenance JSON before running anything:

```sh
mkdir -p target/published-alpha56 && cd target/published-alpha56
npm pack jazz-tools@2.0.0-alpha.56 jazz-napi@2.0.0-alpha.56 --json
sha256sum jazz-tools-2.0.0-alpha.56.tgz jazz-napi-2.0.0-alpha.56.tgz
npm install --ignore-scripts --no-audit --no-fund --prefix . \
  jazz-tools@2.0.0-alpha.56 jazz-napi@2.0.0-alpha.56 undici@8.11.0
cd ../..
node dev/fixtures/produce-alpha56-legacy-edge-receipt.mjs target/published-alpha56 target/published-alpha56/candidate-N
```

The producer starts a Core `JazzServer`, then an Edge `JazzServer` whose
`upstreamUrl` points at a local TCP proxy in front of Core. A persistent
`NativeRuntimeAdapter` client with a fixed local-first identity connects to
the edge. Its first write waits for Global, which proves the chain works. For
the second write, the proxy stops forwarding edge-to-Core bytes at the moment
the client sees the edge acknowledgement. Alpha.56 edges admit a write only
after several permission-scope round trips to Core, and they forward the
admitted commit tens of milliseconds after acknowledging it. So Core never
sees that commit, and the producer checks that no Global acknowledgement
arrives. It refuses an existing output directory.

Rename `candidate-N/rocksdb-alpha56-client` to `rocksdb-epoch-1` and archive
it under that relative name with Python 3 `tarfile.open(..., 'w:gz')`. Then
base64-encode the gzip bytes, wrapped at 76 columns. Transaction times, the
local-first token, RocksDB files and archive metadata are not deterministic,
so a regenerated candidate is new evidence that needs review. It is never a
checksum update to this fixture. The checked-in archive SHA-256 is
`784f1ad4473464784f83bf169be6c13a60b84b408e77728ff93a2dc8630248a7`.
