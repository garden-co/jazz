---
"jazz-tools": patch
---

Node and React Native dev runtimes no longer fail to reopen local storage when a schema adds tables: reopening a RocksDB or SQLite store to add column families now keeps the store's codec profile instead of failing the pinned-profile check.
