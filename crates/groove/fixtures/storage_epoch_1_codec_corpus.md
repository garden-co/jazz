# Storage Epoch 1 codec corpus

This is the maintained, backend-neutral compatibility receipt for the first
settled durable format.

- epoch: `1`
- settlement baseline: `8b946278e` (the current settlement baseline; alpha
  stores before it are unsupported)
- codec registry, in canonical order: `groove.ordered-kv.v1`
- semantic sample: adapter `memory`, format version `1`, the registry above,
  and parameter `key-order=unsigned-lexicographic`
- SHA-256 of the committed byte sample:
  `24e06b8313bb1d0ea42d1d7db627e0e1e0fcafccc25bfc6df782d332bc6a7870`
- committed bytes: asserted verbatim by
  `storage::manifest::tests::epoch_1_codec_corpus_round_trips_committed_bytes_exactly`

The test also rejects malformed magic, trailing bytes, a noncanonical
unsorted codec list, and an unknown epoch before a caller can admit a root.
It deliberately checks `decode(committed).encode() == committed`; a successful
semantic decode alone is insufficient evidence for a durable format.

It runs in the canonical Rust workspace partition. For focused local work use:
`cargo test -p groove storage::manifest::tests::epoch_1_codec_corpus_`.

Physical RocksDB, SQLite, and IndexedDB files are backend implementation
formats, not this corpus and not file-level interchange. Historical-store
fixture capture remains follow-up work; the shared manifest corpus covers the
portable epoch boundary today.
