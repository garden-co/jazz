---
"jazz-napi": patch
"jazz-wasm": patch
---

Surface owner-busy errors from `setLargeValueStagingPolicy` instead of reporting that the new limits were applied.

Keep the serving connection progressing after its final propagated query usage closes, so subsequent writes can settle and unrelated subscriptions remain live.

Publish maintained include and array-subquery child edits without requiring a root-row replacement.
