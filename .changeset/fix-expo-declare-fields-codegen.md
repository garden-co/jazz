---
"jazz-tools": patch
---

Fix generated `schema/app.ts` query builder class fields for Expo compatibility by replacing `declare readonly` phantom fields with `readonly ...!:`.

Expo's Babel pipeline rejects `declare` class fields without Flow-specific options, causing generated schemas to fail compilation in React Native apps. This change keeps the same type inference intent and does not change runtime behavior.
