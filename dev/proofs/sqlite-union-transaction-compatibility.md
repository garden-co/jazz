# SQLite serializable union evidence

A serializable UNION ALL read observes semantic occurrences. A supporting row
can move an unchanged root from arm `a` to arm `b`: its row ID, values and
publication version remain identical. Comparing only root versions misses that
change. Local commits and authority admission now compare the complete query's
ordered read content, including each union arm, before applying any staged write.
Empty reads still record the query, so a later matching row causes a conflict.
All arms use the ordinary snapshot, staged-prefix and permission compiler.

This supports local serializable union reads on an authority and `LocalOnly`
reads on a replica. Replica commits use normal upload, conflict and settlement
paths. Remote union hydration remains explicitly unsupported: its per-query
occurrence coverage is a separate requirement.

## Named encodings

**`jazz.sqlite.serializable-union-read-content.v1`** is BLAKE3 derive-key with
that role string and this exact postcard preimage:

```text
v1_plain_row_preimage_length:uleb128 | v1_plain_row_preimage:bytes |
arm_label_utf8_length:uleb128 | arm_label:utf8
```

The first byte string is the existing
`jazz.sqlite.serializable-read-content.v1` preimage: logical table, root ID,
deleted flag, projected cells, joined sources and root source version. Numeric
publication aliases remain excluded. The arm label is the semantic relation
label; nested unions use the existing length-prefixed path encoding. Plain rows
continue to use the original v1 content role and bytes, including plain reads in
a transaction that also reads a union.

**`jazz.sqlite.serializable-union-observed.v1`** appends
`WireKind::SerializableUnionObservedV1` at postcard discriminant **4**. Existing
kinds at discriminants 0–3 retain their bytes. Its fields, in order, are:

```text
snapshot:u64 | reads:Vec<WireRead> |
own:Vec<(local_position:u64, original_node:[u8;16], original_tx:u64)> |
content:Vec<Vec<[u8;32]>>
```

A transaction with any retained union query uses this kind, including an empty
union result. The authority requires it for those queries and rejects union
reads carried by the older serializable kinds. It validates the existing origin
constraints and content dimensions, re-executes every read through normal
permissions, and compares versions and content before entering the mutation
savepoint. Duplicate occurrences require distinct arm digests; one root's digest
cannot stand in for another arm of that root.

`fixtures/sqlite_union_read_content_v1.json` pins exact preimage bytes for two
arms and the appended commit-kind bytes. The fixture regression inspects an
actual mixed plain/union upload and also verifies the old plain digest. Existing
`sqlite_owner_read_content_v1.json` assertions remain unchanged.

## Compatibility and durability

The ordinary query and result-key encodings do not change. Existing plain
serializable uploads keep their original wire kind and bytes. Pending union
transactions persist the new commit kind, so their evidence survives close and
reopen. Older builds do not understand that appended kind and must be upgraded
before consuming files or connections that contain it. This is an unreleased
feature port for the coordinated SQLite/E2EE release; no downgrade decoder or
rewrite of older records is introduced.

Regression coverage includes duplicate roots, flag-only arm changes, empty-read
phantoms, staged writes and snapshot windows, offline acceptance and rejection,
file reopen, malformed evidence, duplicate-arm digest substitution and attempted
downgrades to all three older serializable kinds. A rejected transaction leaves
its staged companion write unapplied.
