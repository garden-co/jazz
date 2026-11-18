---
"jazz-tools": patch
---

A client or browser worker now refuses a credential refresh or reconnect whose credential is for a different user than the one it already connected as. Before, such a refresh reconnected the store upstream as the other user, so one user's local replica synced under another's identity. The refusal is an error with reason `identity_mismatch`. Renewing the credential for the same user works as before.
