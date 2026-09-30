---
"jazz-tools": patch
---

Stores that hold rows now declare the `jazz.author-alias.v1` storage codec family: row authors and transaction authors are stored as 4-byte node-local aliases into a `jazz_authors` table. A store written by an earlier linear-history build (which stores full author records there) is refused when it is opened, with a typed `UnsupportedStorageCodecs` error naming `jazz.author-alias.v1`, instead of misreading those records. There is no migration: delete the old store (or keep the previous release).
