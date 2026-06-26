# groove — Specification · 5. Prepared shapes & bindings-as-data

A prepared shape is a parameterized query whose parameters are *data flowing
through the graph*, not literals baked into graph identity. This is groove's
work-sharing mechanism: thousands of bound instances of one shape share a single
maintained graph and its arrangements (ch. 4). This chapter defines the shape
APIs, the binding lifecycle, and output routing.

The vocabulary is easy to conflate, so fix it first:

| term | what it is |
|---|---|
| **prepared shape** (`PreparedShapeId`) | the parameterized graph itself, registered once and shared by all of its bindings |
| **binding source name** (`binding_source_shape`) | the stable string that names the `BindingSource` weighted record set *inside* that graph — a name, not the shape id |
| **binding** (`BindingKey`) | one concrete parameter tuple bound against the shape |
| **bound subscription** | a subscriber attached to one binding; many subscribers can share a binding (it is reference-counted) |

For a shape `posts WHERE author_id = $author`, the binding source name might be
`"by_author"`; `author_id = "u7"` is a binding; and two UI panes both watching
u7's posts are two bound subscriptions sharing that one binding.

## 5.1 What a prepared shape is

A prepared shape separates a query's reusable structure from the parameter
values that select one bound result. The shape is a graph containing one or more
binding-source nodes (`BindingSource`, constructed by
`GraphBuilder::binding_source` and represented as
`OpType::BindingSource { shape }`). Each binding source presents the active
parameter assignments as a runtime-maintained weighted record set of
`BindingKey`s. Downstream operators join against that record set exactly as they
would join against an ordinary table, so the parameters participate in query
evaluation as data.

This is why prepared shapes must be entered through the prepared APIs. A graph
that contains a `BindingSource` has no standalone meaning: the binding rows are
supplied by the prepared-shape machinery, not by literals embedded in the graph.
Ordinary `query_snapshot`, `subscribe`, and `subscribe_query` therefore reject
such graphs (`INV-SHAPE-1`).

Shape identity is structural. Graphs are hash-consed by `NodeDescriptor`
(ch. 3), so identical prepares share the same nodes and arrangements. Sharing is
defined for structurally identical graphs; the status of one binding source name
shared by two *different* graphs is the open question recorded below.

## 5.2 The APIs

Prepared shapes can be registered either from an already-built graph or from a
parameterized query. At the graph level,
`Database::prepare(graph, binding_source_shape, binding_descriptor,
output_key_fields)` registers a retained `PreparedShapeId`. The
`binding_source_shape` argument is the binding source *name* (§5.1), not the
shape id. A caller then uses `Database::bind_shape(id, &[Value])` to create a
`Subscription` for one concrete parameter tuple.

The SQL-ish path preserves the same model while binding by parameter name.
`Database::prepare_query(Query)` requires at least one parameter; plain
`subscribe_query` rejects parameterized queries. `Database::bind(&shape,
&[(name, Value)])` accepts exactly one value per parameter name, rejects
missing, duplicate, or unknown names, and passes values in prepared-parameter
order (`INV-SHAPE-14`). The supplied values must conform to the shape's
`binding_descriptor`; otherwise binding fails before hydration (`INV-SHAPE-15`).

*Further invariants.* `INV-SHAPE-2` — `prepare_query` rejects parameter-free
queries and lowers only equality `column = parameter` predicates into binding
joins. `INV-SHAPE-3` — a prepared-query output includes every binding key column
not already projected, rejecting output-name collisions with parameter names.
`INV-SHAPE-4` — graph-level `prepare` rejects an `output_key_fields` entry absent
from the graph output descriptor (`ShapeKeyFieldNotFound`).

## 5.3 The binding lifecycle

A binding source represents active parameter tuples, not subscriber identities.
Its weighted record set therefore has **set semantics**: for each active
`BindingKey`, evaluation snapshots contain exactly one row at weight `+1`,
regardless of how many subscribers share that key (`INV-SHAPE-5`). Per-key
reference counts determine when that set changes:

- Binding a key whose refcount goes 0→1 injects exactly one `+1` `BindingDelta`,
  runs a table-delta-free tick (§4.2) that evaluates the shape for that key and
  updates the per-key materialized multiset, then serves the new subscriber its
  initial snapshot from that multiset (`INV-SHAPE-6`).
- Binding an already-active key injects nothing and serves the new subscriber
  from the per-key materialized snapshot (`INV-SHAPE-7`).
- Unsubscribing decrements the refcount and injects a `-1` binding delta only
  when the *last* reference is removed (`INV-SHAPE-10`).

Shared bindings also require a strict retraction order. Retractions discovered
through dropped receivers during notification are queued, then drained before
any subsequent table or binding deltas and before any prepare or bind hydration
snapshot (`INV-SHAPE-11`). This prevents a dead subscriber's pending retraction
from corrupting a freshly hydrated sibling that shares the same binding source.

## 5.4 Output routing

The shared graph computes rows for all active bindings, so each output delta
must be routed back to the binding that owns it. Each shape output row is
projected through `output_key_fields` into a `BindingKey`; that key's
materialized multiset is updated, and the delta is sent only to subscribers
registered for that key (`INV-SHAPE-8`). A shape commit tick therefore delivers
to each bound subscriber exactly the changes to *its* parameterized result.

*Further invariants.* `INV-SHAPE-9` — the per-key materialized snapshot is a
weighted multiset; a delta bringing a record to weight zero removes it.
`INV-SHAPE-17` — a normal-mode `BindingSource` tick emits only `BindingDelta`s
whose `shape` matches the source and whose descriptor matches the node output.
`INV-SHAPE-18` — prepared recursive shapes route retractions (from base deletes
or anti-join changes) to the correct bound subscriber.

## 5.5 Hydration, sharing, and composition

Hydration establishes the arrangements that a prepared shape will share across
its bindings. Shape-graph hydration reads full table snapshots plus current
binding snapshots in `ArrangementUpdateMode::Replace` (ch. 4). The critical
sharing guarantee is that preparing a second identical shape over an
already-active binding source must not replace shared arrangements with an empty
snapshot or otherwise wipe existing bindings (`INV-SHAPE-12`). A `BindingSource`
in `Replace` mode reads current binding snapshots, not pending or incremental
deltas (`INV-SHAPE-13`).

Because a binding source is just another weighted record set, prepared shapes
compose with joins, anti-joins, nullable-unwrap, `ArgMaxBy`, and recursion: the
binding columns participate as data. Recursive fixpoint semantics are chapter 6;
in this chapter, a binding source is simply another input weighted record set.
Prepared shapes are retained for the lifetime of the database (`INV-SHAPE-16`);
the API does not define shape drop.

## Open questions

- 🔶 **Same-name binding sources across distinct shapes.** The README calls
  sharing one `binding_source_shape` string across *identical* graphs a sharp
  edge, but a regression test exercises two *different* sibling shapes sharing one
  source. Decide whether distinct shapes sharing a source name is supported or
  forbidden.
- 🔶 **`prepare`-time binding-source validation.** `prepare` does not clearly
  reject a graph whose `BindingSource` names don't match the supplied
  `binding_source_shape`, nor check descriptor compatibility when the same source
  name is re-prepared with a different descriptor (`or_insert_with` keeps the
  first). Decide what `prepare` must validate.
