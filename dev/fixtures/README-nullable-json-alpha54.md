# Published alpha.54 nullable JSON fixture

`crates/jazz/fixtures/published-alpha54-nullable-json-rocksdb.tar.gz.base64`
contains a RocksDB database produced by the actual npm `jazz-napi` and
`jazz-tools` 2.0.0-alpha.54 packages. The adjacent provenance JSON pins npm
integrities, the Linux native binary's shipped provenance, and the archive
SHA256. Current-source artifact producers must never regenerate this fixture.

The producer uses public schema builders and the published NAPI typed-cell
boundary. Alpha.54's ordinary optional JSON cell carrier rejects _every_
present value. To create valid existing JSON records, the producer supplies
published scalar-carrier bytes through that same typed NAPI boundary while
keeping the registered schema nullable. This is a persisted compatibility
receipt, not a claim that alpha.54's high-level optional JSON writes worked.

The eight synthetic rows cover omission, object history, inline root null,
nested null, an array, the JSON string `"null"`, an indirect object, and an
indirect whitespace-padded root null. The object has two immutable versions.
The producer never inserts column null, which the old descriptor cannot encode.

Explicit maintainer production uses separately extracted, integrity-verified
published packages and a fresh output directory:

```sh
node dev/fixtures/produce-alpha54-nullable-json.mjs TOOLS_PACKAGE NAPI_PACKAGE NEW_OUTPUT
node dev/fixtures/produce-alpha54-nullable-json.mjs TOOLS_PACKAGE NAPI_PACKAGE NEW_OUTPUT --reopen
```

The second command must run in a new process: the shipped runtime retains
RocksDB handles after `close` until the producer process exits. It decodes and
checks every complete public row against its expected JSON source. Archive the
`rocksdb-epoch-1` child after both processes exit, with sorted paths and fixed
archive metadata; record its SHA256 in the provenance and the consumer test.

The current consumer opens a fresh copy using the public Db, checks values and
null filters, clears the object and updates a different column, then cold
reopens again. An internal history-byte subset check supplements public reads
to prove that normalization never rewrites original authored records.
