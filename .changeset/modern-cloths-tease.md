---
"jazz-tools": patch
---

fix `jazz-tools build` bootstrap behavior by routing through the TypeScript schema CLI when `schema/current.ts` exists and `schema/current.sql` is missing
