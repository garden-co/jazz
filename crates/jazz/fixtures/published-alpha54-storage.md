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
not a canonical whole-store logical snapshot. The companion browser receipt is described below. A complete release logical
inventory remains issue #2309 work; these fixtures do not yet close that issue.

## Published browser fixture

`packages/jazz-tools/fixtures/published-alpha54-browser-jazz-corpus.json` is a
raw physical snapshot from real Chromium 145.0.7632.6 IndexedDB, produced through
the distributed alpha.54 `createAccountManager`, `createDb`, SharedWorker, and
WASM. It preserves every entry of `pages`, `metadata`, and `storage-manifest`,
including the actual account owner, replica identity, and foreground lease
state. Arrays encode ArrayBuffer bytes for JSON transport; the browser corpus
installer restores those structured-clone types. This is not fake IndexedDB or
a source-rebuilt WASM receipt. The distributed WASM package omits its source manifest, so provenance preserves
the manifest from preview workflow `34428566341`, artifact `10133780366`.
Publish workflow `34434970354` explicitly selected that preview. The preview
WASM and npm WASM compare byte-for-byte equal; both SHA-256 values are
`a161d093cd2a1b1c65749997c2d5ce94fd10c813a1be5ab22982d3ff035248fa`.
The preserved WASM manifest independently records the same source head and
dirty-diff digest as NAPI, along with wasm-opt 117 and release profile.

The public producer creates an authentic local-first account under the synthetic
registry scope `http://127.0.0.1:1`, writes and updates one note, shuts down, and
reopens it before exporting. There is no registry server or upstream repair.
Current browser code installs the checksum-pinned snapshot, reads its original
current row, appends another note, and reopens both rows in the canonical
browser storage compatibility suite.

After extracting and checking the published packages as above, install the full
published tools dependencies in the isolated ignored directory, then run:

```sh
npm install --ignore-scripts --no-audit --no-fund --omit=optional --prefix target/published-alpha54 jazz-tools@2.0.0-alpha.54 jazz-wasm@2.0.0-alpha.54
node dev/fixtures/produce-alpha54-browser.mjs target/published-alpha54/jazz-tools/package target/published-alpha54/jazz-wasm/package target/published-alpha54/browser-final
```

The runner uses workspace esbuild and Playwright only as build/browser tooling;
all Jazz application, worker, and WASM code comes from the published packages.
Its output directory is create-new. `browser-corpus.json` is the immutable
candidate; `browser-receipt.json` records browser version and public result.
Normal CI never runs this producer or downloads replacement baseline bytes.

These release fixtures supplement rather than replace the richer original
source corpora. A complete cross-backend logical inventory remains issue #2309 work; preserving those gaps
avoids representing the released text-history examples as full corpus parity.
