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

**Mobile (React Native): SQLite**

SQLite is the obvious choice for mobile storage. It ships with iOS and Android — zero additional binary size, zero integration risk. Apple and Google tune it for their platforms' flash, memory, and power characteristics. It has 25 years of production history on every device imaginable.

We evaluated and rejected two alternatives:

_heed (LMDB)_ — Rejected due to fundamental iOS incompatibility. LMDB memory-maps its entire database at open time. iOS has no swap and aggressively kills processes that hold large mmap regions under memory pressure (jetsam). Realm, which uses the same mmap + CoW B-tree architecture, has a long history of production crashes on iOS (`realm-swift#3226`, `#5778`, `#6063`, `#8523`, `#8746` — all `mmap() failed: Cannot allocate memory`). Apple confirmed this is "expected behaviour." LMDB also requires a fixed map size at open time — too small causes `MDB_MAP_FULL` errors, too large exhausts iOS's artificially limited virtual address space (~7 GB without a special entitlement). These are architectural constraints with no clean workaround.

_redb_ — Rejected despite strong raw performance (4-7x faster reads, 7.7x faster individual durable writes vs SQLite in redb's own benchmarks). The advantages don't outweigh the practical costs for mobile: 3x larger file sizes (copy-on-write amplification), slow startup on large databases (known issue — checksum verification + allocator reconstruction), no mobile platform integration (must be bundled), and no production track record on iOS/Android. SQLite is already on the device, already tuned by the OS vendor, and already handles flash storage quirks. redb's write performance edge matters less on mobile where sync is intermittent and battery life trumps throughput.

Implementation approach:

- New `SqliteStorage` struct implementing the `Storage` trait
- Same `storage_core.rs` reuse pattern
- Map composite key scheme to a single `kv(key TEXT PRIMARY KEY, value BLOB)` table with prefix range queries via `WHERE key >= ? AND key < ?`
- WAL mode + `PRAGMA synchronous = NORMAL` for mobile-appropriate durability/performance balance
- Validate the build toolchain for iOS (xcframework) and Android (JNI/NDK)
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
  RN ─────── SqliteStorage ─────┘
  Browser ── OpfsBTreeStorage
```

## Rabbit Holes

- **RocksDB build complexity.** RocksDB has a heavy C++ build chain — linking it into the existing Cargo workspace, cross-compiling for CI targets (linux-musl, macOS arm64), and keeping build times tolerable are all non-trivial. Need to evaluate `rust-rocksdb` crate maturity and whether we need to pin a specific RocksDB version.
- **SQLite KV performance.** SQLite is not designed as a KV store. Prefix range scans via `WHERE key >= ? AND key < ?` should be efficient on an indexed primary key, but need to verify this holds under Jazz's access patterns (frequent small writes, prefix iteration). WAL mode is important for concurrent read/write performance.
- **Key encoding compatibility.** The current key scheme uses string-encoded composite keys. RocksDB's byte-ordered comparator matches this naturally. SQLite's `TEXT` collation uses lexicographic ordering which should match, but needs verification for index range scans with encoded values.
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

- **Shared conformance suite.** A single test module parameterized over `Box<dyn Storage>` — every implementation (RocksDB, SQLite, Fjall, Memory) runs the same tests. Covers: object CRUD, branch operations, commit append/delete, index insert/remove/lookup/range, catalogue manifest ops, flush/close/reopen persistence, ack tier storage.
- **Realistic fixtures.** Multi-user scenarios (alice, bob) with branching, merging, and index-heavy query patterns — not synthetic key/value ping-pong.
- **Platform smoke tests.** SQLite: verify compile and run for iOS/Android targets, compare cold start and memory footprint. RocksDB: verify CI cross-compilation works.
