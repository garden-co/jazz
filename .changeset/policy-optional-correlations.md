---
"jazz-tools": patch
---

Permission policies can compare an optional column with a required column of the same type inside `exists.where(...)` (for example an optional invite code against a required one). Publishing such policies no longer fails with `OperandTypeMismatch`; a `null` value still never matches.

[PR #3732](https://github.com/garden-co/jazz/pull/3732).
