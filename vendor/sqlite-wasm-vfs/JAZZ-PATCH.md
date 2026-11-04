# IndexedDB commit failure reporting

This is sqlite-wasm-vfs 0.2.0 from crates.io, with the original MIT licence.
Registry archive SHA-256: `7f7a5c9ac229421d577bb5a9bb59048838509958b218dd4e0b3c1214a87c361e`.
`src/sahpool.rs` and `src/lib.rs` are unchanged from that release.

The local patch adds `RelaxedIdbUtil::flush_db(filename)`, a FIFO barrier that
reports earlier persistence errors for that file. The first failed sync is
retained even when the original commit had no waiter. Further syncs and cached
preloads of that file fail until a successful explicit deletion resets it.
Failures for one file do not prevent unrelated files from flushing. Deletion
failures remain latched; successful explicit clear/delete operations reset the
corresponding errors.

The original implementation consumed dirty blocks before awaiting IndexedDB,
then discarded errors for background commits. Waiting for a later dummy-file
deletion could therefore acknowledge data that was never persisted. Retrying
that barrier cannot reconstruct the lost dirty-block set. This patch makes
failure sticky instead of attempting an unsafe automatic retry.

The queue operation and error map are process-local. The IndexedDB database,
object store, keys and SQLite file bytes are unchanged; there is no storage or
wire encoding change and no migration. OPFS code is unchanged. A fresh worker
loads the last persisted state; this patch cannot recover writes previously
acknowledged by older builds after an IndexedDB failure.

Regression coverage is in
`packages/jazz-tools/src/runtime/wasm-sqlite-durability.test.ts`, through the real
WASM API and an IndexedDB implementation with injected request/commit faults.
It covers Local refusal, repeated barriers, unchanged previously persisted blocks,
unrelated files, failed close, cached reopen refusal, failed deletion and explicit
successful reset.

The WASM host also holds each durable worker's downstream frame batch behind
this per-file barrier before exposing it to a tab. This includes both `Stored`
and `Fate` acknowledgements. A failed barrier reports a terminal peer error;
the follower rejects its Local waits without manufacturing an authority
rejection. Upstream traffic stays independent so it can continue supplying
chunks to a pending core read. Wire bytes and message order are unchanged.

The downstream carrier checks `is_flushed` before creating an asynchronous
barrier. A clean file with no queued or in-flight VFS operations publishes
immediately. A pending operation conservatively requires the FIFO barrier;
sticky errors are checked even on the clean path. This avoids one queue turn
per read-only batch without classifying sync message tags as writes. It makes
no end-to-end throughput claim.
