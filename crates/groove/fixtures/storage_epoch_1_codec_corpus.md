# Storage Epoch 1 codec corpus

This is the maintained, backend-neutral compatibility receipt for the first
settled durable format.

- epoch: `1`
- settlement baseline: `8b946278e` (the current settlement baseline; alpha
  stores before it are unsupported)
- codec registry, in canonical order: `groove.large-value.v1`,
  `groove.ordered-chunk-storage.v1`, `groove.ordered-kv.v1`
- semantic sample: adapter `memory`, format version `1`, the registry above,
  and parameter `key-order=unsigned-lexicographic`
- SHA-256 of the committed byte sample:
  `3e1bcd1af5b49b5ba8257cd312e9be0d65adaa214e5df9e53a538246c75eae3f`
- committed bytes: asserted verbatim by
  `storage::manifest::tests::epoch_1_codec_corpus_round_trips_committed_bytes_exactly`

The tests also reject malformed magic, trailing bytes, a noncanonical codec
list, an unknown epoch, and omitted, extra, or substituted codec registry IDs
before a caller can construct, encode, decode, or admit an epoch-1 root.
It deliberately checks `decode(committed).encode() == committed`; a successful
semantic decode alone is insufficient evidence for a durable format.

It runs in the canonical Rust workspace partition. For focused local work use:
`cargo test -p groove storage::manifest::tests::epoch_1_codec_corpus_`.

Physical RocksDB, SQLite, and IndexedDB files are backend implementation
formats, not this corpus and not file-level interchange. The generic Groove
fixture above contains the mandatory Groove base only. Jazz's composed profile and
exact `JSM1` receipt live in
`jazz/fixtures/storage_epoch_1_jazz_codec_profile.md`; RocksDB, SQLite, and
IndexedDB receive that profile from their Jazz opener and validate it before
ordinary data is admitted. The committed epoch-1 RocksDB and SQLite archives
are historical negative evidence: final V1 admission rejects them. Current
positive native physical fixtures are documented in
`jazz/fixtures/current-native-jazz-corpus-v1.md`. IndexedDB has an
epoch-manifest browser receipt; its full historical-store fixture remains part
of the broader corpus work tracked by #2160.

## Backend-neutral ordered-KV pack

`epoch-1-ordered-kv.pack` is the authoritative logical snapshot used by
physical adapter fixtures. Its exact UTF-8 SHA-256 is
`5892ba4cb484da21f28316b90c260c6e07656ba7cfcc21e4c96944fc52baa2e7`.
Each non-header line is an ordered `column-family<TAB>key-hex<TAB>value-hex`
entry. The ordering is part of the fixture: read-only historical inspection
uses this order, while final V1 adapters reject the retired physical archives
before current writes. Current mixed-write/reopen evidence comes from the
separately produced current corpus. It is deliberately a logical pack rather
than a database-file interchange format; each adapter owns its own physical
fixture and validates its own manifest before admitting current data.
