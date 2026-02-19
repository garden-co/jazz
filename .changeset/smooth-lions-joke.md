---
"jazz-tools": patch
---

Removed the legacy `coField` and `Encoders` exports and completed the runtime schema migration to the new schema descriptors. Apps still using the old schema APIs should migrate to the current `co`/zod based schemas.
