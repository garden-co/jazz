# Jazz storage epoch 1 codec-profile receipt

This fixture freezes the closed codec inventory supplied whenever Jazz opens a
durable ordered-KV root. Groove receives these IDs as opaque manifest metadata;
the identifiers below are the complete Jazz-owned byte families reachable from
that root at the epoch-one settlement baseline.

- epoch: `1`
- adapter sample: `memory`, format version `1`
- codec registry, in canonical order:
  `groove.large-value.v1`, `groove.ordered-chunk-storage.v1`,
  `groove.ordered-kv.v1`, `jazz.branch-key.v1`,
  `jazz.catalogue.activation.v1`, `jazz.catalogue.bootstrap-ready.v1`,
  `jazz.catalogue.lens.v1`, `jazz.catalogue.lineage.v1`, `jazz.catalogue.physical-mapping.v1`,
  `jazz.catalogue.schema.v1`, `jazz.catalogue.write-pointer.v1`,
  `jazz.subscription-program-fact-key.v1`
- adapter parameter: `key-order=unsigned-lexicographic`
- SHA-256 of the committed canonical `JSM1` bytes:
  `a3e89ed15b6b2b243fb15c3eef650d843398cf081ecf3be73f650e741349fe96`
- receipt: `storage_codec_profile::tests::epoch_one_jazz_profile_has_a_pinned_manifest_receipt`

An omitted, added, duplicate, or substituted ID fails profile admission before
the adapter decodes or mutates ordinary data. Any incompatible inventory change
requires a new storage epoch, migration decision, and updated fixture; this is
not a per-adapter `Bytes` compatibility exception.

The browser IndexedDB adapter additionally stores `storage-manifest`/
`replica-node-v1`: one random exact 16-byte `NodeUuid` for that physical
replica. It is created atomically with a fresh browser epoch manifest and
validated before Jazz opens, but is deliberately outside this shared codec
profile and its `JSM1` checksum: it identifies a physical transaction issuer,
whereas this fixture identifies a common decode contract. The browser physical
receipt proves same-replica reopen stability and distinct values for independent
stores with the same logical name.

The pre-freeze #2578 cleanup removes the two dormant result codec IDs. The
required-family count changes from 14 to 12; source payload codecs remain
unchanged. Old roots advertising those retired families fail real current
manifest admission. No compatibility profile is selected to bypass this check.
