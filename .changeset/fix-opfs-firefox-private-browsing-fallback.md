---
"jazz-tools": patch
---

Fall back to ephemeral in-memory storage when OPFS is blocked by a SecurityError (Firefox private browsing, Safari private mode). Jazz now initialises successfully without persistence instead of failing to load entirely.
