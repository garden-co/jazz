# groove — Specification · 4. Incremental maintenance

This chapter defines the runtime maintenance protocol for an accepted change.
It covers how commits become weighted deltas, how a synchronous tick orders all
visible work, how shared arrangements support incremental joins, and how durable
nodes and subscribers observe the result. It is the runtime counterpart to the
operator semantics in chapter 3.

## 4.1 From commit to deltas

Incremental evaluation starts from a simple representation: a committed change is
translated into weighted table differences before any operator runs. Inserts
contribute `+record`, updates contribute `-old, +new`, and deletes contribute
`-old`. The resulting table deltas are the only base-table input to the
maintenance tick.

A batch is interpreted as an ordered overlay on storage. When multiple
operations affect the same key, each later operation sees the earlier writes in
that same batch, not just the state that existed before the batch began. Before
the tick starts, each table's deltas are consolidated to the net per-record
change (`INV-TICK-4`). Base table writes are staged before the tick, durable
`Persist` writes are appended during the tick, and both sets are flushed
together after a successful tick (ch. 2, `INV-STORAGE-18`, `INV-STORAGE-19`).

## 4.2 The tick

The tick is the unit of visible maintenance work. It gives every observer the
same ordering: storage-facing derived state is brought up to date first,
ordinary subscriptions are notified after that, and per-tick runtime state is
discarded before the next tick begins. A tick is **synchronous and
single-threaded**, and every tick runs the same fixed sequence:

1. **Retire dead bindings** — drain pending binding retractions.
2. **Advance logical time** — `advance_tick` bumps `current_tick` by one.
3. **Update persisted views** — evaluate durable (`Persist`) nodes, before any
   subscriber is notified.
4. **Notify ordinary subscriptions** — evaluate direct subscriptions.
5. **Route prepared shapes** — evaluate prepared-shape outputs and deliver them
   by `BindingKey` (ch. 5).
6. **Clean up** — drop dead subscriptions and clear the per-tick memo.

Every tick advances `current_tick` exactly once, and every durable (`Persist`)
node is evaluated before any subscription is notified (`INV-TICK-1`).

**Tick kinds.** The same discipline applies to different maintenance events. A
_commit tick_ carries table deltas. A _binding tick_ carries only binding
deltas, with no table deltas, to register and hydrate a new prepared binding
(ch. 5). Both are ticks, and both advance `current_tick` once. A _hydration
snapshot_ — a fresh subscription's initial result, or a one-shot query — does
**not** advance `current_tick` and does not perturb other subscriptions' future
deltas (`INV-TICK-19`). Within a single tick, recursive fixpoint iterations
advance an inner `SubTick`, never `current_tick` (§4.3, ch. 6).

The subscription contract (ch. 1) depends on a strict split between snapshots
and deltas. A new subscription receives exactly one initial hydration
`RecordDeltas` message — including an empty one for an empty result — before any
commit delta (`INV-TICK-2`). Commit ticks then carry only weighted _result_
deltas, never unchanged matching rows or base-table changes outside the result
(`INV-TICK-3`). Hydration and one-shot query evaluation are isolated from
existing subscriptions' future deltas (`INV-TICK-19`). The tick provides that
isolation with per-tick memoization keyed by `{scope, node, tick, sub_tick}`,
cleared after the tick (`INV-TICK-5`).

## 4.3 Arrangements: shared, logically-timed state

Arrangements are shared indexes for incrementally maintaining joins and
anti-joins. Instead of rebuilding an input for each consumer, the runtime keeps a
single arrangement for each identical, context-independent input and lets all
subscriptions and operators use that maintained state. The shared state is keyed
by `ArrangementKey { scope, input, fields, descriptor }` and stored as
arrangement state (`ArrangementState` in the reference implementation)
(`INV-TICK-6`).

Because arrangements are shared, they are explicitly governed by logical time.
Freshness is tracked as `AsOf<…, SubTick>`: advancing an arrangement to a lower
logical time fails (`OutOfOrderRuntimeState`) rather than serving stale data
(`INV-TICK-8`), and accumulating twice at the same `SubTick` is idempotent so
shared state absorbs each delta once (`INV-TICK-9`). A root-scope arrangement
represents base-table commit time, so it is stamped `sub_tick: 0`; only
context-dependent (recursive) arrangements use the evaluator's nonzero
`sub_tick` (`INV-TICK-7`).

An arrangement is updated in one of two modes:

| mode         | used by                                   | effect on arrangement state                                                                                                            |
| ------------ | ----------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `Accumulate` | commit/binding ticks, recursive sub-ticks | applies each delta once into the existing state at a new `SubTick`; re-accumulating at the same `SubTick` is idempotent (`INV-TICK-9`) |
| `Replace`    | hydration / one-shot snapshot evaluation  | rebuilds the arrangement from scratch instead of accumulating over existing state (`INV-TICK-12`)                                      |

For inner joins, the arrangement layer enforces the product rule and the
same-tick cross-term correction (ch. 3). Join output multiplies each delta
weight by the stored weight on the opposite side and subtracts one copy of the
same-tick cross term — formally `Δ(L⋈R) = ΔL·Rₐ + ΔR·Lₐ − ΔL·ΔR`, where
`Lₐ`/`Rₐ` are the maintained sides after this tick (§3.4) — so the cross term is
counted once (`INV-TICK-10`).

_Further invariants._ `INV-TICK-11` — anti-join output deltas represent the
left-visibility diff for keys whose left or right inputs changed.

## 4.4 Durable nodes and prepared-shape routing

Durable nodes make selected derived results part of the storage-facing state.
Because ordinary observers must see a tick only after that storage-facing state
has been maintained, durable schema indices are retained as runtime roots and
their `Persist` nodes are evaluated before subscriber notification (§4.2).
Prepared shapes participate in the same tick: binding deltas are processed,
outputs are evaluated after direct subscriptions, and those outputs are routed
by `BindingKey`. Chapter 5 specifies that behavior.

_Further invariants._ `INV-TICK-13` — a `Persist` node consolidates same-tick
deltas by durable key before writing storage; a unique target rejects a
conflicting positive delta. Same-tick durable reads observe staged `Persist`
writes through the tick storage overlay before falling through to committed
storage. `INV-TICK-14` — prepared-shape routing updates per-binding materialized
weights and delivers each output delta only to subscriptions whose `BindingKey`
matches (ch. 5).

## 4.5 Recursion in the tick

Recursive maintenance is part of the tick, but it does not advance the outer
logical time. Recursive operators maintain set-style accumulated facts and run a
bounded fixpoint inside the tick; chapter 6 gives the full semantics.

At the tick level, positive incremental recursive maintenance emits each newly
discovered fact at `+1` at most once and collapses duplicate derivations
(`INV-TICK-15`). Any tick with a negative table delta, or with unhydrated
recursive state, recomputes from storage and emits the diff against the previous
accumulated set (`INV-TICK-16`, prov — the binding output contract is the
minimal diff, `INV-REC-8`). After that recompute hydrates step arrangements,
later insert-only commits can return to the positive-incremental path
(`INV-REC-9`).

_Further invariants._ `INV-TICK-17` — recursion rejects non-positive frontier
facts rather than assigning bag-recursive semantics (ch. 6). `INV-TICK-18` —
recursive evaluation stops with `RecursiveIterationLimit` when the frontier is
still non-empty after `max_iters` (ch. 6). `INV-TICK-20` — contextual recursive
child state is not persisted in `operator_states` after recompute (ch. 6).

## 4.6 The unified arrangement model (target)

Every keyed structure in groove is one thing: an **arrangement** — an ordered
keyed store of records with three orthogonal attributes:

- **persistence** — `resident` (in-memory) or `durable` (column-family-backed,
  committed in the same storage batch as base writes).
- **covering** — `pk-ref` (values are primary-key references into a base
  arrangement; probes take a second hop) or `covering` (values are full record
  bytes; probes are self-contained at the cost of duplicated bytes).
- **delta-implicitness** — `implicit-1` (set semantics guaranteed by
  construction: presence is weight 1, deletes are plain deletes) or `explicit`
  (stored integer weights; required at and below any weight-multiplying
  operator — joins, bag unions, collapsing projections).

Under this model the existing structures are points in one space, not parallel
systems:

| structure              | persistence | covering | weights    | key              |
| ---------------------- | ----------- | -------- | ---------- | ---------------- |
| base/current table     | durable     | covering | implicit-1 | primary key      |
| declared index         | durable     | pk-ref   | implicit-1 | declared columns |
| join/operator state    | resident    | covering | explicit   | join key         |
| recursion scoped state | resident    | covering | explicit   | scope-qualified  |

**No parallel paths.** Table stores and traditional index maintenance MUST be
thin wrappers over the arrangement abstraction; a conformant implementation
has one write path, one probe interface, and one identity scheme
(`ArrangementKey`) for all of them (`INV-ARR-1`, target).

**All arrangements are ordered.** The arrangement interface is the ordered-KV
contract (ch. 2 `OrderedKvStorage`; the in-memory implementation backs
`resident`). Ordering buys uniform range probes (range-parameter bindings
probe the same arrangement as equality bindings) and deterministic iteration.
The accepted cost is O(log n) point probes for resident state where a hash map
was O(1); if measurement ever shows this on a hot path, a hash-accelerated
resident variant may be added **behind the same interface**, never as a
parallel path (`INV-ARR-2`, target).

**The binding-boundary keying rule.** Everything upstream of a binding join is
binding-independent by construction. Graph construction SHOULD maximize the
binding-free prefix (parameterized joins placed as far downstream as
semantics allow), and at the binding boundary the binding-independent side is
arranged **keyed by the parameter column(s)**, so one shared arrangement
serves every binding of every structurally identical shape prefix as a
point/prefix/range probe (`INV-ARR-3`, target).

**The persist-the-frontier rule.** The durable arrangement set is selected
deterministically — no cost model, no statistics:

1. base/current tables (durable by construction);
2. declared indexes (developer-anticipated boundary arrangements);
3. the maximal binding-free, set-semantic (`implicit-1`) frontier of every
   prepared shape — boundary arrangements are retained while any prepared
   shape references them (today that is the database lifetime, `INV-SHAPE-16`;
   if shape TTL/GC later relaxes that, an unreferenced durable arrangement
   first stops being maintained and is marked stale-at-position — resurrection
   catches up from the per-table change watermark — with byte deletion a
   separate lazy reclamation step). Persisting the frontier converts
   restart/re-prepare rebuild scans into loads.

Weight-multiplying downstream state (join outputs, result-sized operator
state) is NOT persisted by default: it is recomputed from persisted inputs.
This bounds durable write amplification to base tables plus the distinct
`(input, key-shape)` boundary arrangements across all prepared shapes,
deduplicated by arrangement identity (`INV-ARR-4`, target).

The `implicit-1` restriction on the persisted frontier is staging, not
doctrine. Concrete frontier inhabitants beyond plain tables/indexes, in v1
scope: tier-visible-current materializations (the arg-max/anti-join stack every
shape shares per table+tier — the global-current tables are the hand-built
precedent), literal-filter prefixes (workload-derived partial indexes), and
lens-projected current rows (the migration read-path materialization). The
known high-value intermediates that multiplicity excludes from v1 — policy
grant closures and reachability closures, both keyed by subject/seed and
shared across all identities — are the motivating cases for a later
explicit-weight durable arrangement extension (requires the weight-bearing
storage format: merge-operator/tombstone compaction).

**Incremental restart.** A durable arrangement hydrates from its own persisted
form, not by rescanning its inputs (`INV-ARR-5`, target).

Terminology: _arrangement_ is the spec term everywhere; "index" remains
acceptable user-facing shorthand for the declared durable pk-ref case.
"Covering" replaces ad-hoc "full record vs PK" phrasing. A _boundary
arrangement_ is the param-keyed arrangement at a shape's binding join.

## Open questions

No open questions in this chapter.
