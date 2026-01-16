---
"cojson": minor
---

Restricted the `Uniqueness` type to only accept specific values instead of any `JsonValue`. 
The allowed types are now: `string`, `number` (as integers), `boolean`, `null`, `undefined`, 
or an object with string values. Arrays, non-integer numbers, and objects with non-string values are no longer 
accepted and will throw an error.

Deprecated `number`, even if it is an integer, as it is not a valid uniqueness type in TS, if you want to continue using numbers, 
use a string instead or ignore the type check.
