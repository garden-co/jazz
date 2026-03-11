---
"jazz-tools": patch
"jazz-napi": patch
"jazz-rn": patch
---

Switch the native persistent storage engine from SurrealKV to Fjall for the CLI, NAPI bindings, and React Native bindings.

Native local data now lives in Fjall-backed stores and uses `.fjall` database paths by default.
