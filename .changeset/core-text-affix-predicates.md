---
"jazz-tools": patch
---

**Query core: text prefix and suffix predicates.** `Predicate::StartsWith`/`EndsWith` (and `RelationPredicate::StartsWith`/`EndsWith`) match text bytewise on UTF-8, case-sensitive, NULL never matching; on a non-text column the query is refused. They are appended to both predicate enums, so every existing query keeps its bytes. A new optional wire feature, `FEATURE_TEXT_AFFIX_PREDICATES` (`1 << 13`), negotiates them in the `Hello`; the protocol version stays 6. Over a link without it (an older server, which could not decode them and would drop the link), a read using them is refused with "not supported yet by this server" and is not sent. The TypeScript query API does not expose them yet.
