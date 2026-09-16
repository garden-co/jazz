---
"jazz-tools": patch
---

Require all non-nullable insert fields, including array fields that previously defaulted to `[]`. Report upsert input validation failures through `onMutationError` and the write handle’s `wait()`; transaction failures are available through `commit().wait()`. Missing upsert fields are validated asynchronously because they may be valid for an update but invalid for a new row.

[PR #2717](https://github.com/garden-co/jazz/pull/2717).
