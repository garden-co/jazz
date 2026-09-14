# Published alpha.54 storage receipt

This fixture was produced by npm's distributed `jazz-tools` and `jazz-napi`
`2.0.0-alpha.54` packages, without rebuilding either package or importing
workspace Jazz code. The production NAPI persistent API uses RocksDB; it does
not expose SQLite. The existing source-produced SQLite corpus remains separate.
This small release receipt pins two historical text versions and proves that
current code reads both, appends a new row, and reopens all three versions.
It supplements the richer source-produced catalogue/branch/large-value corpus.

`published-alpha54-provenance.json` records tarball URLs and SHA-256 checksums,
the Linux native binary manifest, target, and fixture archive checksum. The
published native manifest declares source head
`71676612e331d7bc0f554a8e0fcba244d9b786ec` and a nonempty `dirtyDiff` hash.
The release tag resolves to `7cb8a9b4e088c5810540f221c0d3434069566e6b`.
Those are distinct provenance facts: the tag must not be substituted for the
binary's source head. The manifest pins Cargo.lock and package inputs; the
source Cargo.toml files declare Jazz and Groove `0.1.0`; the npm artifacts
do not publish separate Jazz/Groove crate version attestations.

The archive is immutable, append-only evidence for epoch 1 / alpha.54. CI only
extracts a verified copy into a temporary directory; it never regenerates this
baseline. New release receipts receive new filenames. Pre-settlement alpha
stores remain unsupported. This receipt changes no storage or wire format.

## Explicit reproduction

Run from the repository root, selecting fresh output names. Extract the three
npm tarballs into `target/published-alpha54/{jazz-tools,jazz-napi,jazz-wasm}`;
each contains a `package` directory. Verify each downloaded tarball against
the provenance SHA-256 before executing it. Install the published tools'
ordinary dependencies in this isolated ignored directory, without build scripts:

```sh
mkdir -p target/published-alpha54
npm pack jazz-tools@2.0.0-alpha.54 jazz-napi@2.0.0-alpha.54 jazz-wasm@2.0.0-alpha.54 --pack-destination target/published-alpha54 --json
npm install --ignore-scripts --no-audit --no-fund --omit=optional --prefix target/published-alpha54 @noble/hashes@2.0.1 @scure/bip39@2.0.1 jose@6.2.1 @opentelemetry/api@1.9.0 pluralize-esm@9.0.5
node dev/fixtures/produce-alpha54-native.mjs target/published-alpha54/jazz-tools/package target/published-alpha54/jazz-napi/package target/published-alpha54/candidate-2
node dev/fixtures/produce-alpha54-native.mjs target/published-alpha54/jazz-tools/package target/published-alpha54/jazz-napi/package target/published-alpha54/candidate-2 --reopen
```

The separate Node processes are intentional: alpha.54 can retain its RocksDB
lock after `db.close()` while JavaScript write handles remain alive. Process
exit releases it. The producer refuses an existing output directory. Archive
`candidate-2/rocksdb-epoch-1` under that relative name with Python 3
`tarfile.open(..., 'w:gz')`, then base64-encode the gzip bytes. Backend physical
files, random catalogue identities, and archive metadata are not deterministic;
a regenerated candidate is new evidence requiring review, never a checksum
update to this original fixture. The checked-in original archive SHA-256 is
`10d139b12fd21530fd553ee148e975d4e2f55d11b43bf3bd90d00179f5703575`.

The producer's `local-current-row.base64` is a binding-level semantic receipt,
not a canonical whole-store logical snapshot. Browser release-produced snapshot
coverage and a complete release logical inventory remain issue #2309 work;
this native fixture alone does not close that issue.
