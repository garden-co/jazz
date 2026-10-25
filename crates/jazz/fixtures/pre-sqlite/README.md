# Stores from before SQLite

Immutable stores the Groove engine wrote. Current code must refuse each with
`unsupported_storage_codecs` (`ErrorCode::UnsupportedStorageCodecs`, or
`JazzError::UnsupportedStorage` from the native client) and leave it
byte-for-byte unchanged; see `crates/jazz/tests/storage_corpus.rs`.

- `published-alpha54-native-rocksdb.tar.gz.base64`: the RocksDB root produced
  by the distributed alpha.54 Linux NAPI artifact (provenance:
  `../published-alpha54-storage.md`, `../published-alpha54-provenance.json`).
- `published-alpha56-legacy-edge-receipt-rocksdb.tar.gz.base64`: a client root
  produced by the distributed alpha.56 NAPI/jazz-tools packages (provenance:
  `../published-alpha56-legacy-edge-receipt.md` and `.json`).
- `groove-linear-history-rocksdb.tar.gz.base64`: the last Groove format
  (linear row history, as alpha.59 writes it), from Groove's corpus producer.
- `groove-linear-history.sqlite.gz.base64`: the same root through Groove's
  SQLite adapter: a SQLite file with tables but no Jazz storage-format stamp.

Each tarball holds one RocksDB directory. Previously these lived beside the
Groove storage corpus in `crates/jazz/fixtures/` on main.
