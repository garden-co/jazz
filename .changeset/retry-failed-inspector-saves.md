---
"jazz-tools": patch
---

Save inspector mutations atomically, settle queued edits filtered out by schema changes without opening an empty transaction, keep ambiguous committed saves visibly pending and non-discardable across navigation, and confirm retained results without resubmitting them or later edits.
