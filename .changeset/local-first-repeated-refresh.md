---
"jazz-tools": patch
---

Local-first clients now keep their connection for as long as they run. Token refreshes keep working after the first one, a client whose token has expired renews it and reconnects on its own, and an account context that fails to refresh logs the error and tries again.
