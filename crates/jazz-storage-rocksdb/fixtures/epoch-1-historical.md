# Epoch-1 RocksDB historical physical fixture

This committed archive contains a RocksDB epoch-1 store, encoded as base64 so
the binary fixture remains reviewable. Its decoded archive SHA-256 is
`58c9198a4eb2373b6cd475177f7cbbbc0482ce5c037d388630565fd000659202`.

It was generated at settlement baseline `8b946278e`, not during the gate. It
contains the superseded internal column family and `raw-v3` marker. It is
negative evidence for the alpha format reset, not a fixture that a final V1
adapter may reopen. The final V1 positive corpus is produced separately by the
current official producer and must never overwrite this historical archive.

The archived store also contains the canonical
RocksDB `JSM1` manifest with Groove's complete epoch-1 codec base, plus the rows in Groove's backend-neutral
`epoch-1-ordered-kv.pack`. RocksDB SST, log, manifest, and option files remain
adapter-private physical implementation artifacts, not interchange bytes.

The gate verifies the archive checksum before extraction, inspects the archive
through RocksDB's read-only API, then extracts a separate copy for current
mixed writes and reopen. Derived Jazz state is deliberately absent: it is
discarded/rebuilt after an authoritative future migration rather than preserved
as historical authoritative bytes. Alpha stores before epoch 1 are unsupported.
