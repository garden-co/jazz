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

## Wed May 27 22:59:25 PDT 2026

Decision: compact transaction rows opportunistically, not absolutely. A sealed tx row can be deleted from `jazz_tx` only if no current row, open history row, rejection, awaiting-dependency state, or implicit-read successor still needs its physical `tx_num`.

Why: `tx_num` is still the open-store relational key for current projections and implicit previous-local-epoch reads. Deleting every tx in a sealed block would require a larger rewrite of those paths. Opportunistic deletion gives immediate metadata savings while preserving the current operational model.

Scope impact: the block payload is the authoritative historical source for deleted tx metadata. Open tx rows remain where they are still operationally referenced.
