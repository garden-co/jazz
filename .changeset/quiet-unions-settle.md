---
"jazz-tools": patch
---

Fix `createJazzContext` hanging when an account migration loads a `co.discriminatedUnion()` CoValue whose stored value matches no declared variant. The runtime discriminator now throws a dedicated `SchemaUnionNoMatchingVariantError` that `SubscriptionScope` catches and surfaces as `UNAVAILABLE`, so `load()` settles instead of hanging. Other instantiation errors (e.g. `CoVector` dimension mismatches) still throw loudly.
