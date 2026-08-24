---
"jazz-tools": patch
---

Compile timestamp/`Date`, floating-point, byte-array, and array permission policy literals that the TypeScript authoring API already accepts. Invalid and pre-epoch dates, tagged timestamps outside the non-negative safe-integer millisecond range, and non-finite floating-point literals now fail at the TypeScript authoring boundary; core compilation independently enforces finite floating-point values.
