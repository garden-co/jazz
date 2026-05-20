---
"jazz-tools": patch
---

Fix: reject stale transactional writes when the authority sees that a sealed transaction's staged row parents no longer match the current visible row frontier.
