---
"jazz-tools": patch
---

`jazz-tools generate` now parenthesises an array whose element is a union. An array of an enum (`{ "array": { "enum": ["low", "high"] } }`) used to be typed `"low" | "high"[]` in `jazz.generated.ts`, which TypeScript reads as "low, or an array of highs". It is now `("low" | "high")[]`. `generate` leaves bindings whose `source` still matches `jazz.app.json` as they are, so an app with such a column picks the fix up by deleting `jazz.generated.ts` and running `jazz-tools generate`.
