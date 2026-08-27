---
"jazz-tools": patch
---

Unify all public timestamps on Unix milliseconds. `$createdAt` and `$updatedAt`
now round-trip as ordinary JavaScript `Date`s without a provenance-only scale
conversion; numeric query and write inputs use the same millisecond unit.
Packed HLC values remain internal transaction/version ordering state.
