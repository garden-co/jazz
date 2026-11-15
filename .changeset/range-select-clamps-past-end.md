---
"jazz-tools": patch
---

A partial selection (`select({ body: { from, to } })`, and `{ fromUtf8, toUtf8 }` or a bytes range) now clamps its range to the value's current length instead of failing with "value range is out of bounds": a range that starts past the end reads as an empty string or empty bytes, one that runs past it reads what the value still covers. A subscription over a range therefore survives the value shrinking below it and follows the value as it grows again; one-shot reads of the same query clamp alike. Inverted ranges, ranges that split a code point or surrogate pair, the native `Db::read_value_range`, and `applyDiffs` pages still fail explicitly.
