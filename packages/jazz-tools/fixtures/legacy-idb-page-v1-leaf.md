# Archived opaque page fixture

`legacy-idb-page-v1-leaf.hex` is an exact copy of
`crates/idb-tree/fixtures/page-v1-leaf.hex` from commit
`9ed3a362a731c65f826cd1dfa6479814f7f42362`, the parent of the SQLite replacement
commit `a852b86825`.

The text file has 95 bytes and decodes to 47 bytes. Its text-file SHA-256 is:

`68189b9487bf3d0933b87656ea0e9374cbb95fdab97aa08ebe638e34a5124fa2`

The IndexedDB adapter test stores these bytes without interpreting them, then
checks that they survive close and reopen unchanged. The fixture retains the
original Rust-produced byte sample after removal of the crate that produced it.
It does not restore a Groove decoder or test SQLite page compatibility.

SQLite browser roots use the separate `browser-sqlite-root-v1.json` fixture.
