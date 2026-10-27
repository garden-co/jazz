---
"jazz-tools": patch
---

Restoring a row a client has not loaded now fails with the same `not_observed` error as updating one ("... is not loaded locally; read or subscribe to the row before updating it"), instead of claiming the row does not exist.
