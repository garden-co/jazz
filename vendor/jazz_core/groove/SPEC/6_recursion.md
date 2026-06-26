# groove — Specification · 6. Recursion & fixpoint

groove evaluates recursive (transitive-closure-style) queries by running a
bounded fixpoint *inside* a single tick. This chapter defines the recursive
operator, its monotone set semantics, and the recompute fallback for
retractions. It builds directly on the tick and arrangement machinery of
chapter 4.

## 6.1 The recursive operator

Recursive queries are expressed as an explicit graph construct. A recursive
node pairs an initial derivation with an iterative derivation, and the iterative
derivation receives the facts accepted in the previous iteration through a
scoped frontier source. In the reference API this is built with
`GraphBuilder::recursive(seed, step, frontier, max_iters)` and
`GraphBuilder::frontier_source(frontier, output)` inside the step graph. SQL
`WITH RECURSIVE` is not lowered; the planner rejects it with
`UnsupportedQuery("recursive CTE lowering is not implemented yet")`
(`INV-REC-14`).

The recursive node has exactly two child graphs: the seed graph and the step
graph. Their compiled output descriptors must be identical (`INV-REC-1`), so
that every fact produced by either graph belongs to the same recursive relation.
The step graph reads a **frontier source**, identified by `FrontierName`, rather
than a stored weighted record set. The frontier is the evaluator-supplied set of
facts newly discovered in the previous iteration. `FrontierName` is scoped to
its recursive node: it names the channel by which the evaluator hands that
iteration's accepted facts to the step graph, and it is meaningful only inside
that step evaluation, not as a globally named weighted record set. A frontier
source with no bound deltas in the current context yields an empty weighted
record set with its declared descriptor (`INV-REC-2`). `arg_max_by` is not
permitted inside a recursive seed or step graph (`INV-REC-13`). Nested recursive
graphs are rejected at graph validation/compilation time rather than accepted
under ambiguous recursive scope (`INV-REC-15`).

## 6.2 Monotone set semantics and the fixpoint

Recursive evaluation produces a set, not a multiset. The accumulated recursive
result (`RecursiveState::accumulated` in the reference implementation) follows
**monotone set semantics**: each accepted fact is stored and emitted at weight
`1`, and a fact already accumulated is never re-emitted nor has its weight
increased by duplicate derivations (`INV-REC-3`). A positive recursive delta of
weight > 1 collapses to one accepted fact (`INV-REC-4`).

The accumulated set is maintained per recursive node and per evaluation scope
(`OperatorStateKey`). A plain subscription evaluates the node at root scope. A
recursive node inside a prepared shape has a distinct scope, and therefore a
distinct accumulated set, for each binding context.

The fixpoint begins by evaluating the seed and accepting only facts not already
in the accumulated set. That accepted set becomes the frontier bound to
`recursive.frontier`; the step graph is then evaluated against that frontier,
newly discovered facts are accepted, and the process repeats until the accepted
frontier is empty (`INV-REC-6`). In a from-scratch recompute (§6.3), the
fixpoint runs over the *full* seed output. In positive-incremental maintenance,
it runs over the seed's delta for that tick and again accepts only facts not
already in `accumulated`. Cyclic input converges because each iteration is
deduplicated against the accumulated set. As a safety bound, evaluation stops
with `RecursiveIterationLimit { node, max_iters }` when the frontier is still
non-empty after `max_iters` iterations (`INV-REC-7`).

*Further invariants.* `INV-REC-5` — positive-only recursive evaluation rejects a
non-positive frontier delta (`UnsupportedNonMonotoneRecursion`); non-monotone
change is handled by recompute (§6.3), not by propagating negative frontiers
through the loop.

## 6.3 Retractions: recompute and diff

Recursive maintenance does not propagate negative frontiers through the loop.
When a change retracts recursive facts, such as a base-table delete or an
anti-join input change, the node performs a **full recompute from storage, then
a diff against the previous accumulated set, so subscribers receive only the net
recursive delta (`INV-REC-8`)**. This preserves minimal output even though the
algorithm is expensive. Because an update lowers to `-old, +new` (§4.1), any
update touching a recursion input carries a negative delta and therefore takes
the recompute path rather than the incremental one. A conforming engine MAY
recompute more often than strictly necessary, but it MUST still emit the same
minimal diff (`INV-REC-8`). After a recompute, the recursive step arrangements
are hydrated from the full table snapshots and the full accumulated weighted
record set before any later positive-incremental use (`INV-REC-9`).

## 6.4 Scope and logical time

Recursive sub-iterations use logical time (ch. 4). Frontier-dependent graph
fragments are scoped under the recursive node and use the recursive `sub_tick`.
Context-independent base-table arrangements stay root-scoped (`sub_tick = 0`),
remain shareable with non-recursive consumers, and absorb a public tick's table
delta exactly once (`INV-REC-10`). Hydrating a new subscriber to an
already-shared recursive node returns the full current accumulated result and
does not consume or suppress future tick deltas for existing subscribers
(`INV-REC-11`).

*Further invariants.* `INV-REC-12` — recursive recompute does not persist
per-context child operator state in the runtime state maps after it completes.

## Open questions

- ✅ **Insert-only incrementality.** After a recompute hydrates recursive step
  arrangements, the next insert-only commit over recursion inputs uses the
  positive-incremental fixpoint again. Non-positive input deltas still recompute
  and diff so the emitted result remains the minimal net delta (`INV-REC-8`).
- 🔶 **Conservative recompute trigger.** A conforming engine MAY recompute more
  often than strictly necessary while still emitting the same minimal diff
  (`INV-REC-8`); the implementation recomputes on any table delta against
  non-empty recursive state.
