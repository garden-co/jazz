---
"jazz-tools": patch
---

Admin-secret clients now bypass local row-policy enforcement so writes still reach the sync server, where permissions are actually checked.
