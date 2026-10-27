# SQLite sync v3 and storage format 3

Release matching server, browser/WASM, Node/NAPI and React Native artifacts
together. This build reads **jazz.sqlite.sync.v3** and **SQLite storage format
3**. It rejects older semantic frames and stores; it does not migrate, delete
or reset them. The v1 and v2 fixture directories record historical contracts
and remain unchanged.

Each semantic frame starts with `4a 53 51 4c 03` (`JSQL`, version 3), followed
by one canonical Postcard message. The outer websocket Hello and Error carrier
stays at v6. Matching Hello versions do not make different semantic codecs
compatible: the semantic decoder rejects the old header before delivery.

The original 47 message tags retain their v2 numbers and fields, except for rejected
write results. A rejection now carries an optional stable reason name before
its display message. The body layout is:

```text
Result::Err(1) | WireError::Rejected(1) | Option<String> reason | String message
```

An unnamed rejection with message `no` changes from `01 01 02 6e 6f` to
`01 01 00 02 6e 6f`. A named rejection uses Option tag `01`, then the UTF-8
reason name and message, each preceded by its unsigned LEB128 byte length.
Conflict, accepted and other-error results keep their field bytes. Unknown
reason names survive wire decoding; the application error reports no known
reason while retaining its message.

This change affects `Fate`, `FateV1` and `OriginFateV2`, as well as raw results
stored in SQLite. The raw result encoding is **jazz.sqlite.write-fate.v2**,
protected by the enclosing file's format-3 stamp. The relay upload journal
remains **jazz.sqlite.relay-upload.v2** (`JRU\x02`): it stores a Commit body,
whose layout has not changed. Durable content hashes, catalogue identities,
permission evidence and serialisable read-content preimages also keep their
existing encodings.

The native relay uses upstream's **ABI 3**. Its coded operation error adds an
optional reason name between the code and display message. JavaScript and
the native host must agree on that ABI as well as the semantic wire version.

`assemble.py` builds the new wire fixtures from frozen v2 field bytes and
explicit integer, UTF-8, Option and Result encodings. It does not call a Jazz
serializer and refuses to replace an existing fixture with different values.
`error-cases.json` pins named and unnamed rejections in every receipt envelope,
plus an unknown reason name. `../sqlite_sync_wire_v3.json` pins the original 47 tags,
26 extended forms, two rejected fates, carrier frames and malformed batches.
The append-only tag 47, `jazz.sqlite.sync.writer-owner.v1`, has independent
fixtures in `../sqlite_writer_owner_v1.json`. Together these cover all 48
current tags. The [writer ownership compatibility notes](../../../../dev/proofs/sqlite-writer-owner-attestation.md)
describe its trust, queueing and coordinated-release requirements.
`../sqlite_write_fate_v2.json` pins the raw durable results.

The format-3 authority and replica snapshots live in
[the storage corpus](../sqlite-storage-v3/README.md). They retain history,
schema lenses, large values, a structured rejection and pending offline
writes. Older stamped stores are refused before journal configuration or
schema writes, leaving both their contents and sidecar inventory unchanged.

Codec admission grants no identity or permission authority. Private-owner
admission, trusted-host checks, accepted-history evidence and the requirement
for a real authority receipt still apply.
