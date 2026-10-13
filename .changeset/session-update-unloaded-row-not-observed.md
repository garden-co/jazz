---
"jazz-tools": patch
---

A backend `update()` inside `forRequest`/`forSession` on a row this backend instance has not loaded yet (for example right after a restart with an empty data dir) now fails with a clear `not_observed` error ("... row ... is not loaded locally; read or subscribe to the row before updating it") instead of the misleading "read policy denied UPDATE ... requires read permission on the target row". Rows the backend holds but the session may not read are still denied as before.
