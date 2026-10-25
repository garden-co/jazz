# SQLite local UNION ALL reads

SQLite evaluates full-row relation unions on the storage owner. Each leaf uses
the ordinary query compiler with the same schema view, read identity, policy,
accepted cut and large-value preprocessing. SQLite combines the leaves with
`UNION ALL`; it applies each arm's own window before the outer order and window.
No application-side table scan or row materialisation is used to merge arms.
The supported leaves have the same output table and full identity projection.
Nested unions retain their own windows. Renaming or narrowing projections,
history sources and nested array output remain explicit unsupported shapes.

A row appearing in two arms remains two occurrences. Current rows and removed
rows carry the declared leaf label in memory. Nested labels use the existing
length-prefixed `compose_union_arm_path` contract. SQL uses integer slots to
look up those labels; the slots never become public identities. Leaf traversal
sorts sibling labels, so reordering declarations changes neither occurrence
keys nor the final tie order. The outer query orders by its declared terms,
then ascending root ID, then this stable arm order.

Subscriptions track all tables read by every arm. They rerun the combined SQL
and diff by root, history version, joined sources and arm label. They do not
use the single-root-row recheck path, which would collapse duplicates. Packed
binding deltas and the Rust client address both updates and removals by the
complete occurrence.

## Existing encoding

The encoding remains **ResultKey v2**, defined in `SPEC/6_queries.md`:

```text
2 | root:16 | joined_count:u32be | joined:16* | arm_count:u32be |
(position:u32be | label_length:u32be | label:utf8)* |
(has_transaction:u8 [tx_time:u64be | tx_node:16]) * (1 + joined_count)
```

A current union row has no joined sources, one discriminator at position zero
and one zero transaction flag. The exact two-arm byte fixture is
`fixtures/sqlite_union_result_keys_v2.json`. Existing plain-row binding goldens
remain unchanged. Local query execution changes no query-IR or binding-envelope
encoding; the in-memory row fields are not serialized directly. Serializable
union transactions add a separately versioned evidence kind, described in
[the transaction compatibility note](sqlite-union-transaction-compatibility.md).
Existing SDK decoders already understand v2 union discriminators. No store
migration or alternate decoder is needed.

## Coverage boundary

Remote union reads and subscriptions are explicitly refused. Hydrating only a
union's final page cannot establish the inputs needed to reproduce arm-local
windows on a partial replica. Local-only reads can evaluate the admitted local
cache; they make no authority-coverage claim. Local serializable union reads record the complete query and compare both row
versions and semantic occurrence content at commit. Replica reads that require
remote union hydration still report the coverage error.

For example, `done=false UNION title=alpha ORDER BY title LIMIT 2` returns alpha
twice when alpha matches both arms. Changing alpha to `done=true` removes only
the first occurrence and brings beta into the page. A reader denied alpha by
the table policy sees it through neither arm. Bigint comparisons use the same
exact integer path as ordinary queries, and chunked text uses full-value ranks
at both the arm and outer ordering levels.
