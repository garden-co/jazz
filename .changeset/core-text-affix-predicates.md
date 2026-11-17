---
"jazz-tools": patch
---

**Query core: text prefix and suffix predicates.** `Predicate::StartsWith`/`EndsWith` (and `RelationPredicate::StartsWith`/`EndsWith`) match text bytewise on UTF-8, case-sensitive, NULL never matching. On a non-text column the query is refused. They are appended to both predicate enums, so every existing query keeps its bytes. They are not supported in permission policies: a policy built through the Rust API that uses one is refused when the schema compiles.

A new optional wire feature, `FEATURE_TEXT_AFFIX_PREDICATES` (`1 << 13`), negotiates them in the `Hello`; the protocol version stays 6. Browser (wasm) clients advertise it. Over a link without it (an older server, which could not decode them and would drop the link), a read using them is refused with "not supported yet by this server", and a serializable commit whose reads use them gets a rejected fate. Neither is sent. The native relay's host-carried upstream does not report its features yet, so it refuses them too. The TypeScript query API does not expose them yet.
