---
"jazz-tools": patch
---

`jazz-tools generate` now parenthesises an array whose element is a union. An array of a payload enum with several cases (`{ "array": { "enum": { "ping": {…}, "pong": {} } } }`) used to be typed `{ type: "ping"; … } | { type: "pong" }[]` in `jazz.generated.ts`, which TypeScript reads as "a ping, or an array of pongs". It is now `({ type: "ping"; … } | { type: "pong" })[]`. `generate` leaves bindings whose `source` still matches `jazz.app.json` as they are, so an app with such a column picks the fix up by deleting `jazz.generated.ts` and running `jazz-tools generate`.
