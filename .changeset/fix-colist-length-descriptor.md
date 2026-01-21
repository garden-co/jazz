---
"jazz-tools": patch
---

Fixed `CoList` to return the correct length when calling `getOwnPropertyDescriptor` with `length`. Previously it was always returning 0.
