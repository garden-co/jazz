---
"jazz-tools": patch
---

Stop generating `declare` phantom fields in query builder schema output so Expo/Metro can transpile generated `schema/app.ts` files without requiring Flow `allowDeclareFields`.
