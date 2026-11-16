---
"jazz-tools": patch
---

Apply a serializable transaction wait's `timeoutMs` to a commit that has not returned its transaction ID yet, so a stalled commit no longer outlasts the timeout. A commit that fails still surfaces its own error unchanged.
