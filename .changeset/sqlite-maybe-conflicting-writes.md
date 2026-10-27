---
"jazz-tools": patch
"jazz-napi": patch
"jazz-wasm": patch
---

The sync server again records what each uploaded write had seen of its row, so it can tell which accepted writes maybe conflict. Commits now carry that base on the wire and servers keep it in storage format 1, which is not released yet: a client and server built before and after this change cannot exchange commits, and a development store holding uploads queued by an earlier build no longer opens.
