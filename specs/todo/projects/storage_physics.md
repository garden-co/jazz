# Storage physics

Make high write volume "OK, actually": attack the three stacked write
amplifications — logical (history-keeping + arrangement maintenance), physical
(LSM compaction), and structural (CF layout) — by class, not globally.

## When

Benchmarking & optimization mode: after Plan 5 (capability growth) and with/
after Plan 6 (large values — the value-separation item overlaps). Items 0
(CF-per-class) and 1 (per-class policies) landed 2026-07-03 with receipts.

**Format-freeze policy (decided with Anselm 2026-07-03):** the freeze is NOT
coupled to Plan 7. It is its own late milestone, gated on all three of:
(a) the whole marketed alpha surface is served, (b) perf is proven at the
use-case-like benchmarks, and (c) a deliberate final cleanliness/elegance/
future-proofness pass over the storage and wire formats has happened.
Surprises are expected while chasing (a) and (b); format-breaking changes
remain sanctioned (pre-release regenerate-only, fail-loud on old layouts)
until the freeze milestone is declared. Wire-visible reservations (e.g. the
columnar ViewUpdate variant, jazz ch. 8) are decided as they arise, not
under freeze pressure.

## Items (ordered)

0. **CF-per-physical-class restructure.** Today: ~6 CFs per app table
   (history/register × current/global/ahead) — each CF is a full LSM, so many
   small tables ⇒ memtable floors, WAL-segment pinning by trickle-write CFs
   (⇒ WAL bloat or tiny-SST churn via max_total_wal_size force-flushes),
   fragmented compaction, and add-a-table requiring the drop-and-reopen path.
   Target: ~8–10 class CFs (history, register, global-current, ahead-current,
   changes, arrangements/indexes, content, meta) with table id as key prefix
   (+ prefix extractors for blooms). New table = new prefix, no new CF, no
   reopen; whole-table drop = DeleteRange. Residual cost, stated: L0 mixes hot
   and cold tables (deeper levels re-separate by key range).
1. **Per-class compaction/compression policies** (unlocked by 0):
   append-forever classes (history, register, changes) get FIFO/tiered
   compaction (~1–2× amp instead of leveled 10–30×), heavy zstd, big blocks,
   no blooms; overwrite-hot classes (currents, arrangements) stay leveled +
   blooms + lz4; **transient class (ahead-current, staging) leaves durable
   storage entirely** — memtable-only/WAL-off or in-memory behind the overlay;
   class-rebuildable from history + fates on crash (fail-loud rebuild).
2. **Blind writes via merge operators.** Current-row settlement's arg_max
   winner rule IS a merge operator: make settlement and implicit-1 arrangement
   maintenance logically append-only; compaction becomes the consolidator
   (the LSM's background machinery doing IVM work instead of fighting it).
   Removes ~3 point reads per settled row from the hottest write path.
3. **Value separation** for big payload bytes: history record bodies and
   content extents via RocksDB BlobDB (or Plan-6-owned extent store) —
   compaction shuffles pointers, not payloads. Coordinate with Plan 6.
4. **Per-class write-amp budget as a measured gate.** Bench counters already
   emit write bytes per commit per destination; define budgets per class and
   gate them in the C-appendix discipline (INV-PERF style). "Lots of writes is
   OK" becomes a defended number.
5. **Later direction (recorded, not scheduled): history-as-log.** History is
   semantically a log; a segment store + sparse boundary-arrangement index
   behind the OrderedKvStorage seam replaces the KV representation if budgets
   demand it. The seam keeps the engine question permanently open; RocksDB
   remains the default for overwrite classes.

## Receipts / gates

Medium-profile before/after per item; write bytes per class per commit
(existing counters); S5/S6 storage-ratio gates vs zstd anchors; cold-start
(restart hydration) timings — item 0+1 should also shrink these via fewer,
better-packed files. Config-sensitivity note from 2026-07-02: no safe global
RocksDB knob moved S4 (write-buffer 128MiB neutral; disable-auto-compactions
within noise) — the wins here are structural, which is why this plan exists.

## Dependencies / coordination

- Plan 4 arrangement model (the arrangements class; possibly co-schedule item 0).
- Plan 6 large values (item 3 ownership).
- Canonical topology: this plan is RocksDB-tier (edge/core); OPFS client
  storage has its own physics and is out of scope here.
- Format-breaking: pre-release regenerate-only, fail-loud on old layout (the
  denorm recovery precedent); must land before Plan 7 version tags.
