# Merge strategies and convergence

Confirm these details against the installed schema DSL and Rust resolution code. A status-quo spec
can lag a newly added strategy.

## Concurrent-frontier model

Jazz stores row snapshots and ancestry. A linear history uses the latest whole-row state. When a
row has genuinely concurrent frontier tips, Jazz finds a most recent common ancestor and resolves
each changed column using the consumer schema's merge strategy.

That distinction matters: merge strategies describe how concurrent snapshots converge. They do not
prevent later sequential writes, validate application commands, or grant permissions.

## Strategies

### LWW

`lww` is implicit. For each concurrently changed column, the latest timestamp-ordered contributing
tip wins.

Use it for replaceable values such as a title, selected status, or current preference.

### Counter

```ts
const schema = {
  counters: s.table({
    value: s.int().default(0).merge("counter"),
  }),
};
```

`counter` is valid only for a non-nullable integer. Writers still store snapshots:

```ts
db.update(app.counters, id, { value: row.value + delta });
```

For a concurrent frontier, Jazz calculates each tip's delta from the common ancestor, sums those
deltas with checked integer arithmetic, and applies the sum to the ancestor. A base of `10` with
concurrent snapshots `12` and `9` resolves to `11`.

This does not protect two same-client commands derived from the same stale render. Serialize those
commands, maintain a synchronous local shadow, or model increments as operation rows.

### Grow-only set

```ts
const schema = {
  documents: s.table({
    tags: s.array(s.string()).default([]).merge("g-set"),
  }),
};
```

`g-set` is valid only for a non-nullable array. Concurrent contenders resolve to a canonical,
deduplicated union that includes the ancestor elements. Treat the result as a set and order it for
display separately.

A later sequential replacement can still remove elements because no concurrent merge is needed on
a linear frontier. Keep every mutation path append-only when removal must be forbidden, or represent
each addition as its own row when provenance, timestamps, deletion, or auditing matter.

## Consumer-schema-relative resolution

History stores original snapshots, not a permanently materialized strategy result. The reading
client's current schema chooses how a conflicting frontier resolves. During a merge-strategy
migration, old and new clients can therefore compute different visible values until the rollout is
coordinated.

Use `jazz-schema-evolution` to review and test both client versions. Test old and new reads against
the same concurrent history instead of validating only the latest client.

## Prefer operation rows when needed

Choose stable, append-only operation rows instead of snapshot merging when the product requires:

- exactly-once command identity or explicit deduplication;
- an audit trail or per-operation authorship;
- removal from a set;
- business validation of each increment or addition;
- replay-safe retries across uncertain acknowledgements.
