---
"jazz-tools": patch
---

Close manually managed transactions when a pending read fails during commit. The staged transaction is rolled back, the original read error is preserved, and explicit rollback remains available if cleanup fails.
