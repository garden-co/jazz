# groove — Specification · 5. Prepared shapes & bindings-as-data

## Overview

A prepared shape is a parameterized query whose parameters are _data flowing
through the graph_, not literals baked into graph identity. This is groove's
work-sharing mechanism: thousands of bound instances of one shape share a single
maintained graph and its arrangements (ch. 4). This chapter defines the shape
APIs, the binding lifecycle, and output routing.

The vocabulary is easy to conflate, so fix it first:

| term                                             | what it is                                                                                                          |
| ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------- |
| **prepared shape** (`PreparedShapeId`)           | the parameterized graph itself, registered once and shared by all of its bindings                                   |
| **binding source name** (`binding_source_shape`) | the stable string that names the `BindingSource` weighted record set _inside_ that graph — a name, not the shape id |
| **binding** (`BindingKey`)                       | one concrete parameter tuple bound against the shape                                                                |
| **bound subscription**                           | a subscriber attached to one binding; many subscribers can share a binding (it is reference-counted)                |

For a shape `posts WHERE author_id = $author`, the binding source name might be
`"by_author"`; `author_id = "u7"` is a binding; and two UI panes both watching
u7's posts are two bound subscriptions sharing that one binding.

Invariant digest:

- `INV-SHAPE-1`: Graphs containing BindingSource MUST NOT be evaluated through ordinary querysnapshot, subscribe, or subscribequery; they MUST be evaluated only through prepared-shape...
- `INV-SHAPE-2`: Database::preparequery MUST reject queries without parameters and MUST lower only equality column = parameter / parameter = column predicates into binding joins.
- `INV-SHAPE-3`: A prepared-query internal graph output MUST include every binding key column needed for routing, while PreparedShape::output and bound subscription rows MUST expose on...
- `INV-SHAPE-4`: Graph-level prepare MUST reject any outputkeyfields entry absent from the graph output descriptor.
- `INV-SHAPE-5`: A binding source weighted record set MUST expose set semantics: for each active BindingKey, evaluation snapshots contain exactly one row with weight +1, regardless of...
- `INV-SHAPE-6`: Binding a key whose refcount transitions 0 -> 1 MUST inject exactly one +1 BindingDelta in a table-delta-free tick before serving the subscriber snapshot.
- `INV-SHAPE-7`: Binding an already-active key MUST NOT inject another binding-source delta, and MUST serve the new subscriber from the per-key materialized snapshot.
- `INV-SHAPE-8`: Shape deltas MUST be routed by projecting the prepared output record through outputkeyfields, or the explicit routing graph output through routingkeyfields, into the b...
- `INV-SHAPE-9`: A prepared binding's materialized snapshot MUST be maintained as a weighted multiset where deltas that bring a record weight to zero remove that record.
- `INV-SHAPE-10`: Unsubscribing a shape subscription MUST decrement the binding refcount and MUST inject a -1 binding delta only when the last reference is removed.
- `INV-SHAPE-11`: Binding retractions discovered via dropped receivers during notification MUST be queued, then drained before subsequent user table/binding deltas and before prepare/bi...
- `INV-SHAPE-12`: Preparing an identical shape over an already-active binding source MUST NOT replace shared arrangements with an empty binding snapshot or otherwise wipe existing bindi...
- `INV-SHAPE-13`: During shape graph hydration, BindingSource nodes in ArrangementUpdateMode::Replace MUST read current binding snapshots, not pending/incremental binding deltas.
- `INV-SHAPE-14`: Database::bind MUST accept exactly one value for each prepared parameter name, MUST reject missing/duplicate/unknown names, and MUST pass values to bindshape in prepar...
- `INV-SHAPE-15`: Binding values MUST conform to the prepared shape's bindingdescriptor; mismatched type/arity MUST fail before subscription hydration.
- `INV-SHAPE-16`: Prepared shapes MUST retain their output graph nodes for the lifetime of the database unless/until an explicit shape-drop API exists.
- `INV-SHAPE-17`: A BindingSource tick in normal accumulate mode MUST emit only BindingDeltas whose shape matches the source's BindingSourceOp.shape and whose descriptor matches the nod...
- `INV-SHAPE-18`: Prepared recursive shapes MUST route retractions caused by base-table deletes or anti-join changes to the correct bound subscriber result.

## Details

### 5.1 What a prepared shape is

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
Ordinary `query_snapshot`, `subscribe_one_sink`, and `subscribe_query` APIs
therefore reject such graphs (`INV-SHAPE-1`).

Shape identity is structural. Graphs are hash-consed by `NodeDescriptor`
(ch. 3), so identical prepares share the same nodes and arrangements. Sharing is
defined for structurally identical graphs; the status of one binding source name
shared by two _different_ graphs is the open question recorded below.

### 5.2 The APIs

Prepared shapes can be registered either from an already-built graph or from a
parameterized query. At the graph level,
`Database::prepare_one_sink(graph, binding_source_shape, binding_descriptor,
output_key_fields)` registers a retained `PreparedShapeId`. The
`binding_source_shape` argument is the binding source _name_ (§5.1), not the
shape id. A caller then uses `Database::bind_shape_one_sink(id, &[Value])` to create a
`Subscription` for one concrete parameter tuple.
Graph-level callers that need a clean output graph plus a richer internal
routing graph use
`Database::prepare_one_sink_with_routing(output_graph, routing_graph, binding_source_shape,
binding_descriptor, routing_key_fields)`: subscribers observe `output_graph`,
while routing keys are projected from `routing_graph`.

The SQL-ish path preserves the same model while binding by parameter name.
`Database::prepare_query(Query)` requires at least one parameter; plain
`subscribe_query` rejects parameterized queries. `Database::bind(&shape,
&[(name, Value)])` accepts exactly one value per parameter name, rejects
missing, duplicate, or unknown names, and passes values in prepared-parameter
order (`INV-SHAPE-14`). The supplied values must conform to the shape's
`binding_descriptor`; otherwise binding fails before hydration (`INV-SHAPE-15`).

_Further invariants._ `INV-SHAPE-2` — `prepare_query` rejects parameter-free
queries and lowers only equality `column = parameter` predicates into binding
joins. `INV-SHAPE-3` — a prepared-query's internal graph output includes every
binding key column needed for routing, rejecting output-name collisions with
parameter names, but `PreparedShape::output` and bound subscription
notifications expose only the public query projection. Graph-level callers that
build internal routed outputs directly can bind with an explicit public output
descriptor to expose clean rows while retaining hidden routing fields
internally; callers with separate clean and routed graphs can instead use
`prepare_one_sink_with_routing`. `INV-SHAPE-4` — graph-level `prepare_one_sink` rejects an
`output_key_fields` entry absent from the graph output descriptor
(`ShapeKeyFieldNotFound`).

### 5.3 The binding lifecycle

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
  when the _last_ reference is removed (`INV-SHAPE-10`).

Shared bindings also require a strict retraction order. Retractions discovered
through dropped receivers during notification are queued, then drained before
any subsequent table or binding deltas and before any prepare or bind hydration
snapshot (`INV-SHAPE-11`). This prevents a dead subscriber's pending retraction
from corrupting a freshly hydrated sibling that shares the same binding source.

### 5.4 Output routing

The shared graph computes rows for all active bindings, so each output delta
must be routed back to the binding that owns it. Each shape output row is
projected through `output_key_fields` into a `BindingKey`; that key's
materialized multiset is updated, and the delta is sent only to subscribers
registered for that key (`INV-SHAPE-8`). Prepared-query subscribers receive a
notification projection over that internal row, so hidden binding fields can
route the delta without appearing in public rows. A shape commit tick therefore
delivers to each bound subscriber exactly the changes to _its_ parameterized
result. Graph-level prepared shapes use the same split when bound with an
explicit public output descriptor: routing observes the internal output, while
the subscriber stream receives the projected public record. For
`prepare_one_sink_with_routing`, the runtime evaluates the routing graph for prepared
ticks, projects routing rows into binding keys, then projects those same rows
back to the clean output descriptor before maintaining per-binding snapshots and
notifying subscribers. The routing graph output must therefore contain every
subscriber-visible output field with the same field name, plus the hidden
routing key fields. Hidden routing fields are internal provenance only; they are
not part of subscription snapshots, notifications, or `subscription_output`.

_Further invariants._ `INV-SHAPE-9` — the per-key materialized snapshot is a
weighted multiset; a delta bringing a record to weight zero removes it.
`INV-SHAPE-17` — a normal-mode `BindingSource` tick emits only `BindingDelta`s
whose `shape` matches the source and whose descriptor matches the node output.
`INV-SHAPE-18` — prepared recursive shapes route retractions (from base deletes
or anti-join changes) to the correct bound subscriber.

### 5.5 Hydration, sharing, and composition

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

## Open Questions

### Open questions

- 🔶 **Same-name binding sources across distinct shapes.** The README calls
  sharing one `binding_source_shape` string across _identical_ graphs a sharp
  edge, but a regression test exercises two _different_ sibling shapes sharing one
  source. Decide whether distinct shapes sharing a source name is supported or
  forbidden.
- 🔶 **`prepare`-time binding-source validation.** `prepare` does not clearly
  reject a graph whose `BindingSource` names don't match the supplied
  `binding_source_shape`, nor check descriptor compatibility when the same source
  name is re-prepared with a different descriptor (`or_insert_with` keeps the
  first). Decide what `prepare` must validate.
- 🔶 **Partitioned sink operator.** Routed multisink bindings currently append
  per-binding filter/project tails to route a shared shape's internal output into
  public sink rows. Decide whether groove should add a first-class partitioned
  sink operator so hot shapes avoid evaluating binding-count by sink-count tails
  while preserving the same "route-carrying graph -> binding partition -> public
  sink" semantics.
- 🔶 **Correlated array-subquery bindings.** Jazz array-subquery sharing can be
  expressed as parent correlation keys flowing through prepared-shape style
  binding sources. Decide whether this is ordinary prepared-shape routing or a
  specialized correlation source.
