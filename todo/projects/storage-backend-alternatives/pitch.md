# Storage Backend Alternatives

Jazz currently uses Fjall as the sole persistent native storage engine — for cloud servers, Node.js clients, CLI tools, and (planned) React Native. Fjall is a Rust-native LSM-tree KV store maintained by one person. We forked it to add Android support. This is a fragile foundation: a single maintainer, limited production track record, and we're already carrying patches. The server and mobile contexts have fundamentally different constraints (throughput vs. battery/size), so the "one engine everywhere" premise doesn't hold. We should pick the best option for each platform rather than compromising on both.

## Appetite

Small batch (1 week). The Storage trait is already a clean abstraction boundary — the work is wiring new implementations + running conformance tests, not architectural surgery. No migration tooling — we're pre-launch, so clean-slate deploys are fine.

## Solution

Replace Fjall with battle-tested engines optimized for each deployment context:

**Server: RocksDB**

RocksDB is the industry standard for high-throughput embedded KV stores. It has a massive production footprint (Meta, CockroachDB, TiKV), excellent LSM tuning options, and is actively maintained by a large team.

Implementation approach:

- New `RocksDBStorage` struct implementing the `Storage` trait
- Reuse `storage_core.rs` callback-based logic (same pattern as FjallStorage)
- Map Jazz's single keyspace + prefix-scan pattern to RocksDB column families or prefix iterators
- Use RocksDB transactions for write atomicity (maps to Fjall's `write_tx()` / `commit()`)
- Tune compaction and block cache for server workloads (64MB+ cache, level compaction)

Key mapping stays the same — the composite key scheme (`obj:`, `idx:`, `ack:`, `catman:`) works directly as RocksDB keys with prefix iteration.

**Mobile (React Native): redb or heed (LMDB)**

Two candidates to evaluate — one pure-Rust, one battle-tested C engine with Rust bindings:

_redb_ — Pure Rust, copy-on-write B-tree, ACID, single-file, stable 1.0+ file format. No C dependencies — simplest possible cross-compilation story. Fuzz-tested extensively. No at-scale mobile production usage yet, but `native_db` (a higher-level layer on redb) explicitly targets and tests iOS/Android.

_heed_ — Rust wrapper around LMDB, maintained by Meilisearch. LMDB is proven on mobile at scale (powered Realm for years, used by Firefox/Gecko). Zero-copy reads, ~32KB footprint, crash-safe. The C dependency adds minor build complexity but LMDB is tiny and compiles cleanly for ARM. Caveat: fixed map size must be configured upfront, and iOS memory pressure + mmap interactions need testing.

The mobile scope wires up both candidates against the `Storage` trait and picks one based on conformance + benchmarks. Implementation approach (same for both):

- New `XStorage` struct implementing the `Storage` trait
- Same `storage_core.rs` reuse pattern
- Validate the build toolchain for iOS (xcframework) and Android (JNI/NDK)
- Benchmark on real devices: cold start time, memory footprint, battery impact under sync load
- This replaces the planned Fjall-on-RN investigation (`specs/todo/b_launch/react_native_storage_investigation.md`)

**Browser: unchanged.** OpfsBTreeStorage stays — it's purpose-built for the OPFS/SyncAccessHandle constraint and has no Fjall dependency.

**No migration tooling.** We're pre-launch — no existing production data to migrate. Fjall stays in the codebase behind a feature flag as fallback, but new deployments use the new backends directly.

### Fat Marker Sketch

```
Before:
  Server ─── FjallStorage ──┐
  Node.js ── FjallStorage ──┤── Storage trait ── ObjectManager / QueryManager / SyncManager
  CLI ────── FjallStorage ──┤
  RN ─────── FjallStorage? ─┘  (unproven)
  Browser ── OpfsBTreeStorage

After:
  Server ─── RocksDBStorage ────┐
  Node.js ── RocksDBStorage ────┤── Storage trait ── ObjectManager / QueryManager / SyncManager
  CLI ────── RocksDBStorage ────┤
  RN ─────── redb or heed ──────┘
  Browser ── OpfsBTreeStorage
```

## Rabbit Holes

- **RocksDB build complexity.** RocksDB has a heavy C++ build chain — linking it into the existing Cargo workspace, cross-compiling for CI targets (linux-musl, macOS arm64), and keeping build times tolerable are all non-trivial. Need to evaluate `rust-rocksdb` crate maturity and whether we need to pin a specific RocksDB version.
- **redb mobile pioneering.** redb is solid on desktop but no one has run it at scale on mobile. `native_db` tests on iOS/Android but adoption is small. We'd be early adopters.
- **heed/LMDB map size.** LMDB requires a fixed maximum database size at open time. Too small = out-of-space errors; too large on 32-bit = address space issues. Need a sensible default + growth strategy for mobile.
- **heed/LMDB mmap on iOS.** iOS aggressively reclaims memory-mapped pages under pressure. Need to test that LMDB doesn't degrade or crash under low-memory conditions on real devices.
- **Key encoding compatibility.** The current key scheme uses string-encoded composite keys. RocksDB's byte-ordered comparator matches this naturally, but redb's and LMDB's ordering needs verification — especially for index range scans with encoded values.
- **Conformance gaps.** The Storage trait has subtle contracts (flush semantics, close/reopen, index ordering) that may not surface until the conformance suite runs. Budget time for debugging edge cases.

## No-gos

- **No custom storage engine.** We are not building our own B-tree or LSM from scratch.
- **No async Storage trait.** The synchronous contract is load-bearing for the query engine — this project doesn't change the trait shape.
- **No browser changes.** OpfsBTreeStorage is out of scope.
- **No wire protocol changes.** Storage is local-only; sync protocol is unaffected.
- **No Fjall removal.** Fjall stays behind a feature flag as fallback.
- **No migration tooling.** Pre-launch, clean-slate deploys only.

## Testing Strategy

Integration-first, through the `Storage` trait interface:

- **Shared conformance suite.** A single test module parameterized over `Box<dyn Storage>` — every implementation (RocksDB, redb, heed, Fjall, Memory) runs the same tests. Covers: object CRUD, branch operations, commit append/delete, index insert/remove/lookup/range, catalogue manifest ops, flush/close/reopen persistence, ack tier storage.
- **Realistic fixtures.** Multi-user scenarios (alice, bob) with branching, merging, and index-heavy query patterns — not synthetic key/value ping-pong.
- **Platform smoke tests.** Mobile candidates (redb, heed): verify compile and run for iOS/Android targets, compare cold start and memory footprint. RocksDB: verify CI cross-compilation works.
