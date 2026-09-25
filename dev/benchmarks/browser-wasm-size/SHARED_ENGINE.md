# Share the browser engine across storage adapters

Measured against main `7a113ff8b673c702ceb75dead96bd7a985965c81` on macOS
arm64, Rust 1.93.1, wasm-pack 0.14.0, wasm-bindgen 0.2.117 and wasm-opt 117.
Both arms use the canonical release producer with the existing optimization
settings and default features. Function names remain available to profilers.
CI currently uses wasm-pack 0.13.1; the local receipt is not a CI result.

## Why ship this

The binding instantiated `Db`, `NodeState`, writes and peer connections for
both `MemoryStorage` and `BrowserStorage`. Their storage was already erased
inside the database. Erasing the adapter at the WASM entrypoint gives these
paths one engine instantiation. It removes 1,813 compiled function bodies.

| WASM bytes        |       Main | Shared engine | Reduction |
| ----------------- | ---------: | ------------: | --------: |
| Raw               | 32,968,262 |    27,470,536 |     16.7% |
| gzip level 9      |  9,450,614 |     8,657,377 |      8.4% |
| Brotli quality 11 |  5,522,937 |     5,398,696 |      2.2% |

Duplicate code compresses well: the raw reduction is substantially larger than
the transfer reduction. These numbers describe the WASM asset, excluding JS glue.

## Browser receipt

Chromium 145.0.7632.6, 2,000 rows, fresh process per sample. The public builders
and actual generated bindings are exercised with memory and IndexedDB storage.
All row values, identities, page membership, and IndexedDB close/reopen results
match. The 112 WASM exports and both generated declaration files are identical.

Medians in milliseconds from two samples per arm:

| Phase                            |     Main | Shared engine |
| -------------------------------- | -------: | ------------: |
| Compile module                   |    23.70 |         17.65 |
| Memory first full read           |   109.45 |        106.95 |
| Memory ten subsequent full reads |   462.55 |        464.10 |
| IndexedDB insert transaction     | 2,835.75 |      2,836.15 |
| IndexedDB ten full reads         |   434.25 |        436.45 |
| IndexedDB read after reopen      |    41.45 |         40.90 |

This bounded local receipt supports a code-size win with similar runtime in
this fixture. It does not establish performance across every query shape.
The repeated-read phase includes independent value checks. Raw samples, hashes
and producer metadata are in [receipts/shared-engine.json](receipts/shared-engine.json).
See [README.md](README.md) for reproduction commands.

## Behavior and boundaries

Memory and browser enum variants remain separate because their scheduling
differs. For example, a memory write still drives queued work immediately;
a browser write still yields through its browser scheduler. Opening IndexedDB
still passes the same owner and scope configuration. Reopening delegates through
the erased storage adapter to the same backend. Closing and failure handling
retain their existing paths. History, deletes, IVM, authorization, persistence,
compression negotiation, and the public API remain supported.

The change introduces one additional storage wrapper at construction; the
adapter forwards the existing storage operations. It does not change the core
database API or serialized storage/wire formats.

Tooling friction: a maintained browser size/runtime receipt avoids rebuilding
the entire JS workspace for each compiler experiment.
