# Epoch-1 RocksDB historical physical fixture

This committed archive contains a RocksDB epoch-1 store, encoded as base64 so
the binary fixture remains reviewable. Its decoded archive SHA-256 is
`468ba3377ed1b219d332c56af40f8e5d5d0cabc7ca085e3ae88c66434e7efa49`.

It was generated at settlement baseline `8b946278e`, not during the gate. It
contains the fixed internal column family, `raw-v3` marker, and canonical
RocksDB `JSM1` manifest plus the rows in Groove's backend-neutral
`epoch-1-ordered-kv.pack`. RocksDB SST, log, manifest, and option files remain
adapter-private physical implementation artifacts, not interchange bytes.

The gate verifies the archive checksum before extraction, inspects the archive
through RocksDB's read-only API, then extracts a separate copy for current
mixed writes and reopen. Derived Jazz state is deliberately absent: it is
discarded/rebuilt after an authoritative future migration rather than preserved
as historical authoritative bytes. Alpha stores before epoch 1 are unsupported.
