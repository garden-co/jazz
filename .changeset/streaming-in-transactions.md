---
"jazz-tools": patch
---

Transactions now take `insertStreaming`, `updateStreaming` and `upsertStreaming`: the streamed value is staged in the transaction and commits or rolls back with it, in local-first, React Native and thin clients alike.
