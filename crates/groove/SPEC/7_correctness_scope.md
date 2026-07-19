# groove — Specification · 7. Correctness, determinism & scope

## Overview

This chapter defines what "correct" means for groove, what the system
deliberately does _not_ promise, and where the correctness contract ends. It is
the boundary defended by the oracle tests and the reference point for reviewing
the system's semantic obligations.

Invariant digest:

- `INV-OK-1`: For every subscription, initial snapshot plus the consolidated sum of all received deltas MUST equal a fresh one-shot recomputation of that query against current storage.
- `INV-OK-3`: One-shot snapshot reads MUST NOT perturb retained subscription streams or consume future tick deltas.
- `INV-OK-13`: Persisted schema index reads MUST match a full-scan oracle over committed base-table state.
- `INV-OK-14`: Base-table writes and durable index/view writes MUST be committed through one storage-atomic batch; if the final batch fails after runtime state advances, the Database...
- `INV-QUERY-17`: SQL lowering MUST reject unsupported SELECT/set/join shapes explicitly, including SELECT DISTINCT, grouped/ordered/limited selects, non-inner joins, and non-UNION ALL...

## Details

### 7.1 The oracle property

The correctness contract reduces the entire engine to one equality. At each
successful commit/tick boundary, every live subscription denotes the same
multiset that would be obtained by evaluating the subscribed query from scratch
against current storage. The subscription's initial hydration snapshot plus the
consolidated sum of all delivered deltas — grouped by output record identity,
with integer weights summed — equals that fresh result (`INV-OK-1`).
One-shot reads (`query`/`query_graph`) and persisted index reads observe the
same semantics: a one-shot read matches a fresh recompute without perturbing any
retained subscription stream (`INV-OK-3`), and a persisted index read matches a
full-scan oracle over committed base state (`INV-OK-13`).

Correctness is **semantic, not algorithmic.** The implementation may share
nodes, reuse arrangements, memoize within a tick, or prepare reusable query
shapes, but those techniques are valid only insofar as they preserve the oracle
property. The mechanisms described in chapters 3–6 — including same-tick join
cross-term correction, `AsOf` logical-time discipline, and recursive
recompute-and-diff — serve this single equality. Oracle tests compare the engine
with a naive recompute under seeded interleavings, and the benchmark harness
(appendix B, non-normative) exercises the same property.

### 7.2 Supported and unsupported scope

groove exposes a full graph contract and a narrower SQL-lowerable contract. The
distinction is intentional: graph execution defines the system's complete
operator model, while SQL lowering accepts only the subset it can represent
exactly.

- **Graph-level (the full contract).** The graph model executes filters,
  projections, nullable-unwrap, inner equi-joins, anti-joins, `UNION ALL`,
  `ArgMaxBy`, and hand-built, non-nested recursion (ch. 6) as graph operators
  (ch. 3, 6). Anti-join and `ArgMaxBy` are executable and oracle-tested; "no
  aggregates" refers only to the SQL surface below, not to the graph contract.
- **SQL-lowerable (a subset).** The SQL surface accepts `SELECT … FROM` with
  supported predicates, projections, inner equi-joins, `UNION ALL`, and equality
  prepared parameters via `prepare_query`. Everything else is rejected, not
  approximated (ch. 3, `INV-QUERY-17`): `SELECT DISTINCT`, aggregates /
  `GROUP BY` / `HAVING`, `ORDER BY` / `LIMIT` / `OFFSET`, outer joins, derived
  tables, recursive CTEs, non-equality prepared parameters, and unsupported join
  keys.

### 7.3 Concurrency, durability, and the atomicity bound

groove bounds atomicity around a single writer and synchronous ticks; it does
not provide MVCC. Within a tick, base-table writes and durable index/view writes
are staged together, then flushed through one storage-atomic batch after the
tick succeeds (ch. 2, ch. 4, `INV-OK-14`). Runtime storage reads during that
tick see staged set/delete operations before committed storage, so same-tick
consumers observe prior staged writes. If the final storage batch fails after
in-memory state has advanced, the `Database` instance is poisoned and rejects
subsequent operations rather than serving potentially torn state.

### 7.4 Determinism

Determinism makes the oracle property practical to test and reason about.
Evaluation uses ordered state throughout (`BTreeMap`s in the reference
implementation, with no ambient-hash iteration order), `F64` values are never
NaN (ch. 2), and the benchmark harness exposes deterministic counters as hard
regression signals alongside the oracle. The correctness contract itself is
_multiset_ equality — the consolidated sum of §7.1, independent of delivery
order.

With identical inputs, the reference implementation replays to the same deltas
in the same order; this is a reference-implementation property, not part of the
contract. As a result, a divergence from the naive recompute is always a real
bug, never noise. Cross-operator delivery _order_ is reproducible but not itself
a normative guarantee — depend on the consolidated result, not on the order
deltas arrive in.

## Open Questions

### Open questions

No open questions in this chapter.
