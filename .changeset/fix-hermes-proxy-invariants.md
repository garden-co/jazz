---
"jazz-tools": patch
---

**BREAKING:** The `in` operator on CoMap instances now returns `true` for all schema-defined keys, even if the value is `undefined` or has been deleted. This fixes a fatal `TypeError` on React Native 0.84+ (Hermes V1) caused by proxy invariant violations.

Previously, `"key" in coMap` returned `false` for unset/deleted optional properties. Now it returns `true` for any key with a schema descriptor, consistent with `Object.keys()` and `Object.getOwnPropertyDescriptor()`.

To check whether a key has an actual value set, use `coMap.$jazz.has("key")` instead of the `in` operator.

Also adds `configurable: true` to all internal property definitions (`$jazz`, `$isLoaded`, `[TypeSym]`, `_instanceID`) across all CoValue types to satisfy ES2015 proxy invariants enforced by Hermes V1.
