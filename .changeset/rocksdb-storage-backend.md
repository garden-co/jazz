---
"jazz-tools": patch
"jazz-napi": patch
---

Replace Fjall with RocksDB as the default persistent storage engine for server, Node.js client, and CLI.

**BREAKING:** Server data stored with Fjall is not compatible — existing servers must start from a clean data directory.
