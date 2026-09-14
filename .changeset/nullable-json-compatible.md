---
"jazz-tools": patch
---

Fix explicit null inserts and clearing updates for optional JSON columns while preserving published storage and wire descriptors. Optional JSON root null and column null now share logical null semantics, including filtering and policies; existing persisted JSON remains readable without a database reset.
