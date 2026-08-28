# Native Jazz epoch-1 corpus producer (WIP #2307)

The executable producer is
`node::tests::harness::settlement_baseline_native_jazz_corpus_reopens_and_accepts_mixed_writes`.
It deliberately uses fixed node, row, branch, transaction-time, text, and
byte-scalar inputs, then opens the resulting Jazz root through both production
native adapters under the closed `epoch_1_storage_codec_profile`.

Its settlement-baseline logical-pack SHA-256 is:

```text
fd05bb7c4d1fe89a5eb35cb7031e7af18f150eb6377166e858e135eaee35cbc7
```

The digest is over sorted system and physical application store names and, for
each store, the exact primary-key/value byte pairs in scan order. The physical
application closure includes each permanent table identity's history,
register, global-current, ahead-current, and rejected-version stores.
Direct-record stores use their typed semantic key/value fixture because Groove
intentionally does not expose their raw adapter keys through the public
direct-store facade. The test proves
both adapters produce the same pack, rejects an incomplete stored-codec
profile before touching it, reopens without application writes, writes a new
current-format row, and reopens a third time while preserving the older
history.

A separate planted sensitivity receipt repeats the deterministic producer while
changing only `todos.title`, then while changing only `notes.body`; each change
must alter the digest. This prevents a metadata-only exporter from silently
passing while excluding application rows.

The currently populated historical families are catalogue genesis, durable
node/schema identities, branch-keyed immutable versions/current projections,
transaction/fate/global-change/merge-head records, and a byte scalar. The
registry additionally names catalogue pointers, deletion history, known state,
settled result members, and program facts so an intentionally narrow producer
cannot quietly become the final corpus.

This is an executable producer slice, **not yet the final committed physical
fixture**. The remaining #2307 promotion work is to archive SQLite/RocksDB
settlement-baseline stores with producer revision/checksums, inspect SQLite
read-only and RocksDB physical files, and extend the scenario to populate
provenance, result/program-fact, a true large-value chunk tree, and the typed
catalogue lineage envelopes from #2306. Pre-settlement alpha stores remain
intentionally unsupported.
