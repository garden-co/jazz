---
"jazz-tools": patch
---

Fix writes failing with `graph field not found: __jazz_claim_typed…` when permissions are published with `deploy` and an insert checks a table whose read policy compares a session claim. Deployed permissions now behave like the same permissions supplied at server startup.
