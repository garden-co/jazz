# Compact browser release profile

This builds on [shared engine instantiation](SHARED_ENGINE.md). The canonical
producer now uses fat LTO, one codegen unit and Rust `opt-level=s` for browser
release builds, with Jazz and Groove kept at level 3. The native release,
browser dev/profiling settings, default features, and wasm-opt `-O -g` remain
unchanged. Function names are retained for profiling.

## Size receipt

Exact WASM bytes; gzip level 9 and Brotli quality 11:

| Build                                   |        Raw |      gzip |    Brotli |
| --------------------------------------- | ---------: | --------: | --------: |
| Main `7a113ff8b6`                       | 32,968,262 | 9,450,614 | 5,522,937 |
| Shared engine, existing release profile | 27,470,536 | 8,657,377 | 5,398,696 |
| Shared engine, compact release profile  | 17,489,408 | 6,143,105 | 4,168,614 |

The profile alone saves **36.3% raw / 29.0% gzip / 22.8% Brotli** versus its
shared-engine parent. Combined with that parent, it saves **47.0% / 35.0% /
24.5%** versus main: **1.89× / 1.54× / 1.32× smaller**, respectively. This
does not meet the 3× target in [#3536](https://github.com/garden-co/jazz/issues/3536).
The JS glue stays at about 89 KB raw / 13.5 KB gzip; these figures are not
measurements of a complete application's JS bundle.

The final artifact was produced and sealed by `node dev/artifacts/build.mjs
wasm release`, including source fingerprint and output-hash verification.
Toolchain: Rust 1.93.1, wasm-pack 0.14.0, wasm-bindgen 0.2.117, wasm-opt 117,
macOS arm64. Cargo argument forwarding also exists in CI's wasm-pack 0.13.1;
this change does not depend on the newer custom-profile option. A cold build
or final LTO link may take longer; the cached production rebuild took 82 seconds.

## Runtime tradeoff

Fresh Chromium 145.0.7632.6 processes, same public API fixture and exact source
for both arms. Each process runs memory, then IndexedDB, with persisted data
checked after close/reopen. No compiler/compression jobs ran during timings.
The host was an ordinary desktop with other applications running. These local
samples are not a statistical guarantee or a CI-equivalent gate.

Medians in milliseconds; main versus the combined candidate:

| Phase                         | Main, 2,000 rows | Candidate, 2,000 rows | Main, 10,000 rows | Candidate, 10,000 rows |
| ----------------------------- | ---------------: | --------------------: | ----------------: | ---------------------: |
| Compile module                |            20.45 |                 11.65 |             19.85 |                  13.85 |
| Memory open                   |            55.15 |                 46.85 |             54.15 |                  47.00 |
| Memory insert transaction     |           191.75 |                190.90 |          1,108.95 |               1,122.55 |
| Memory first full read        |           105.30 |                 98.95 |            305.95 |                 304.45 |
| Memory first filtered page    |            20.00 |                 20.25 |             53.00 |                  58.80 |
| Memory ten further full reads |           466.40 |                465.50 |          2,331.80 |               2,359.60 |
| IndexedDB insert transaction  |         2,835.55 |              2,857.70 |         16,870.05 |              16,846.15 |
| IndexedDB first full read     |            47.85 |                 44.55 |            237.65 |                 240.60 |
| IndexedDB first filtered page |            11.75 |                 12.65 |             49.10 |                  52.50 |
| IndexedDB ten full reads      |           439.55 |                452.05 |          2,199.15 |               2,322.50 |
| IndexedDB read after reopen   |            41.70 |                 41.40 |            211.60 |                 211.95 |

There are four samples per arm at 2,000 rows and two per arm at 10,000 rows.
Warm IndexedDB full reads cost about 3–6% in these samples; the 10,000-row
memory page costs about 11% (5.8 ms). Opening and small first reads improve.
This is a size/runtime tradeoff, not a general throughput optimization.
History, IVM, deletes, permissions, sync, persistence, and compression support
are not removed. Broader query and browser coverage remains tracked in #3536.

All application values, identities, filtered membership, and persistence
checks pass. The public generated TypeScript API is identical; low-level
WASM declarations only reorder exports and change generated closure hashes.
Receipts retain every sample and exact artifact hashes in
[receipts/release-profile.json](receipts/release-profile.json).

## Build and failure behavior

`pnpm --filter jazz-wasm build` selects this release profile. The producer
still builds into a fresh staged package and publishes only a complete,
verified generation. For example, a successful build yields matching WASM,
glue and manifest; a failed compiler/optimizer run leaves the previous package
in place. Fast correctness artifacts and profiling builds follow their existing
profiles. Package overrides are forwarded after wasm-pack's output arguments;
native producers receive neither those overrides nor the browser environment.
The producer script is already an input to artifact fingerprinting.

The existing browser large-value test previously treated `reader.read()`'s
`{ value, done }` envelope as the delta. The same failure was reproduced on
unmodified main (six tests passed, one failed). With explicit user approval,
its helper now checks `done === false` and unwraps `value`; its data assertions
are unchanged.

## Reproduce

Preserve each canonical package in a distinct ignored directory, then follow
[README.md](README.md). Use `--rows 2000` and `--rows 10000`, and alternate
both directions. Run compression separately from browser timing. The rejected
smaller/slower profiles are in [the experiment ledger](../rejected-experiments.md).

Tooling friction: a pinned browser/driver pair and the standalone WASM receipt
avoid full JS workspace rebuilds while checking compiler tradeoffs.
