# Deep History Implementation Decision Log

Timebox start: Wed May 27 22:52:41 PDT 2026
Timebox target end: Thu May 28 04:52:41 PDT 2026

## Wed May 27 22:53:30 PDT 2026

Decision: implement the first sealed accepted-history block format as an lz4-compressed compact `Bundle` payload, rather than immediately building a bespoke binary columnar codec.

Why: the compact bundle format already dictionary-encodes repeated ids and preserves exact logical records. Storing that as a block lets us prove the storage movement, indexing, export, and read semantics first. Once those are stable, the block payload can become more columnar without changing the table/index boundary.

Scope impact: the first vertical slice should add `history_blocks`, compact old accepted open rows into compressed block payloads, and teach exports/historical paths to decode those blocks. Rejected blocks and a more specialized per-column codec can follow once the accepted path is real.

## Wed May 27 22:58:24 PDT 2026

Decision: keep no-block export ordering exactly on the old code path. Only sort/dedupe merged history and tx records when a table actually has sealed blocks.

Why: two existing policy/lens tests caught that even harmless-looking tx/history reordering can change validation outcomes. The block path needs deterministic merge order, but the ordinary path should preserve current behavior until compaction is involved.

Scope impact: sealed-block export has an explicit merge/sort step; ordinary export remains structurally unchanged.
