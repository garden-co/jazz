---
"jazz-tools": patch
---

Updating a row that this client has not loaded yet now fails with a clear `not_observed` error ("... is not loaded locally; read or subscribe to the row before updating it") instead of a misleading "read policy denied UPDATE ... requires read permission on the target row", which was reported even when the table's read rule is `always()`. Rows the policy genuinely hides are still denied.
