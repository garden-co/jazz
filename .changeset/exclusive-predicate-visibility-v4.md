---
"jazz-tools": patch
---

Exclusive predicate reads now carry their root-row visibility mode and validate include-deleted output identity with both content and deletion witnesses. The wire protocol is v4 only; v3 peers are rejected before payload decoding. Pending exclusive transactions with incomplete read evidence remain blocked after restart rather than being uploaded with weaker semantics. Durable restart recovery is deferred to [#3228](https://github.com/garden-co/jazz/issues/3228).
