---
"jazz-tools": patch
---

Transactions now take `insertStreaming`, `updateStreaming` and `upsertStreaming`: the streamed value is staged in the transaction and commits or rolls back with it, in local-first, React Native and thin clients alike. A streamed value is held for its transaction for the store's staging max age (10 minutes by default); a transaction left open longer is refused at commit with `streaming upload expired`.
