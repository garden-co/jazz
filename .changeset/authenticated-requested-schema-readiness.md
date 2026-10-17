---
"jazz-tools": patch
"jazz-wasm": patch
"jazz-napi": patch
"jazz-rn": patch
---

Require admission of the database's requested schema before reporting authenticated catalogue readiness. A retained older catalogue can still provide stable table identities, but cannot bypass migration startup admission. Preserve authenticated capture draining, original admission errors, and offline readiness for already admitted schemas.
