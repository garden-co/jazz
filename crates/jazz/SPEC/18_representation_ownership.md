# jazz — Representation ownership map

This is the short map of the representations a query crosses. It answers two
questions that are easy to blur together: which module owns a representation,
and whether it is a public API value, a cross-process wire value, or an
internal compiler/runtime value.

```text
public Query / RelationQuery
        │  validate and normalize
        ▼
NormalizedRowSetShape + ProgramBinding
        │  lower
        ▼
QueryProgram { LoweredGraph, typed output schemas }
        │  execute / maintain
        ▼
MaintainedSubscriptionView + terminal operations
        │  reduce top-level edits in Rust
        ▼
Indexed root row deltas + descendant terminal operations
        │  publish through NAPI or WASM
        ▼
TypeScript subscription result
```

Peer sync enters this pipeline before local execution, never after the remote
collector:

```text
authority-maintained authorization and coverage
        │  safe canonical source closure + residual-program identity
        ▼
receiver source/frontier reconciliation  ◀── eligible local pending inputs
        │
        ▼
the same receiver-local LoweredGraph
        │
        ▼
receiver-local terminal operations → application result
```

There is no authority-terminal arrow into the root delta reducer. An ordered
terminal operation belongs to the Groove execution that produced it. It may
cross the local binding ABI after Rust has reduced/indexed it, but it never
crosses peer sync as result truth (`INV-SYNC-36`).

## Ownership and boundary

| Stage              | Authoritative owner                                                                                                                                                      | Representation                                                                     | Boundary                                           | Rule that must survive it                                                                                                                                                                |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------- | -------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Query              | `jazz::db` public API and `node/query_eval.rs`                                                                                                                           | `Query`, `RelationQuery`, query options                                            | public host API                                    | Validate public input before execution; no host-specific query semantics.                                                                                                                |
| Normalized query   | `node/query_engine/input.rs`, with normalizers in `node/query_eval.rs`                                                                                                   | `NormalizedRowSetShape`, `RowSetProgramInput`, `ProgramBinding`                    | internal compiler IR                               | Shape identity is canonical and binding-independent; values/claims are bindings, not a second query shape.                                                                               |
| Lowered graph      | `node/query_engine/lowering.rs`                                                                                                                                          | `QueryProgram`, `LoweredGraph`, `LoweredTerminal`                                  | internal compiler/runtime contract                 | One graph and its typed outputs serve ordinary rows as well as structured include/relation views; consumers select the outputs they need rather than defining another query meaning.     |
| Terminal schema    | `node/query_engine/schemas.rs`                                                                                                                                           | `OutputTerminalSchema`, `AppRowSchema`, `ProgramFactSchema`                        | internal typed output contract                     | App rows and internal facts are separate outputs of the same program. Hidden routing/provenance fields never become app fields.                                                          |
| Maintained state   | `node/maintained_subscription_view.rs`; consumed by `node/query_eval.rs`, `node/views.rs`, and `peer.rs`; `db.rs` turns derived transitions into app subscription events | `MaintainedSubscriptionView`, membership/payload/fact indexes, `ResultTransitions` | internal runtime state                             | The maintained path consumes typed terminal deltas; it does not use a second semantic diff engine. Facts such as policy or version witnesses inform maintenance but are not result rows. |
| Root delta reducer | `db.rs`, using the internal `TerminalRootLayout` prepared by `node/maintained_subscription_view.rs`                                                                      | ordered subscription snapshot plus indexed added/updated/removed occurrences       | Rust subscription event                            | Apply every top-level terminal edit exactly once in Rust and publish its authoritative pre/post positions; never forward a parallel root terminal edit.                                  |
| Binding payload    | `binding_codec.rs`; NAPI and WASM are thin host adapters                                                                                                                 | packed root rows, occurrence identities, explicit indices, descendant operations   | binding ABI, distinct from peer sync wire protocol | Both hosts carry the same Rust-indexed root delta. Terminal operations crossing this boundary must have a non-empty path.                                                                |
| TypeScript reducer | `packages/jazz-tools/src/runtime/subscription-manager.ts` and `native-runtime/native-row-codec.ts`                                                                       | indexed root changes plus nested collection edits                                  | public TypeScript result                           | Trust producer root positions and apply only descendant edits to already decoded or deferred root object trees. Reject any root terminal operation.                                      |

## What is public, wire, and internal

- **Public API:** `Query`/`RelationQuery`, `Db` reads and subscriptions, and the
  TypeScript rows exposed after decoding. They are ergonomic application values,
  not compiler or transport data structures.
- **Peer wire protocol:** `WireFrame` in `wire.rs`, containing the
  postcard-encoded `SyncMessage` defined in `protocol.rs`. This is the only
  portable transport-byte contract. Its target subscription representation is
  canonical authored facts, ordered catalogue/lens lineage, and
  authority-filtered witness/admission facts — never projected app rows,
  collector indices, structural terminal operations, or a terminal cache as
  truth (ch. 8 §8.4.1). Bindings move these bytes but do not treat
  `SyncMessage` as their application API.
- **Binding ABI:** descriptors and packed `Record` row bytes, aligned
  occurrence identities and root indices, plus descendant terminal operations.
  It is deliberately separate from peer wire protocol and may use native host
  objects for non-hot-path core types.
- **Internal only:** normalized shapes, bindings, lowered graphs, terminal
  schemas, maintained facts, routing fields, and physical carrier choices.
  Their names and layouts are not public compatibility promises.

### Frozen binding byte envelopes

Where a NAPI or WASM host emits a binary subscription payload, it uses the
shared postcard layout produced by `binding_codec.rs`; TypeScript uses the
matching production reader. These are exactly one postcard value per binding
payload: decoders MUST reject trailing bytes rather than treating the input as
a prefix of a larger carrier (`INV-WIRE-1`). A transport that needs several
payloads supplies separate byte slices; it does not concatenate them.
Postcard strings use strict UTF-8 decoding; malformed text is malformed input,
not a replacement-character-compatible spelling.
Every binding `u64` uses its shortest base-128 spelling; redundant
continuations, overlong encodings, and values beyond `u64` are malformed. The
v1 binding fields are exposed as JavaScript numbers, so their TypeScript reader
MUST reject a decoded value above `Number.MAX_SAFE_INTEGER` rather than losing
index or count precision.

The v1 relation snapshot field order is `root_count, rows`; each row batch is
`table, descriptor, rows`; and each row is `row_id, deleted, raw`. The v1
subscription delta field order is `added, updated, removed,
added_occurrence_keys, updated_occurrence_keys, removed_occurrence_keys,
added_indices, updated_previous_indices, updated_indices, removed_indices`.
Every occurrence-key and index sidecar is aligned with its corresponding row
sequence. `removed` entries are `table, row_id`. These positions, postcard
enum discriminants in nested descriptors, and raw Groove record bytes are
frozen once emitted: future ABI evolution requires a new explicitly versioned
envelope, never a reordered field, permissive fallback, or in-place migration.

Named-cell input has the explicit v1 postcard field order `descriptor, raw`.
Its descriptor is an ordered vector of `name: Option<String>, value_type`;
recursive records and enum case payloads use that same name/type grammar.
It never includes `FieldIdentity`, a compiler slot, or a local physical ID.
The NAPI, WASM, and React Native foreground adapters call the same
`binding_codec::decode_named_cells` reader. Before installing logical name
bindings it rejects trailing or
noncanonical framing, noncanonical packed rows, duplicate or absent top-level
names, unknown types, more than 1024 type/case nodes, and depth 128 or greater.
Scalar type discriminants match the output type grammar; internal scalar tags
retain their existing encoding and remain subject to the owning schema's
validation. This input role is independent of the immutable peer-version
`JVRR` envelope described in chapter 16.

`binding_codec_golden.json` is the frozen cross-language corpus for this v1
binding layout. Rust creates the hard-coded semantic cases and exact bytes;
the TypeScript reader independently decodes them, and the generated NAPI and
WASM artifacts directly return the Rust-owned corpus in the binding
compatibility matrix. A change needs this corpus, both binding paths, and the
SPEC decision reviewed together. Terminal operations remain JSON-native
metadata rather than an alternative row byte layout.

`wire_frame_artifact_corpus.json` is the companion v1 host-artifact rejection
receipt. The generated NAPI and WASM artifacts execute **every** exact Hello
and semantic-message frame from the complete frozen frame manifests, plus the
frozen structured-error and malformed, unsupported-version, trailing-byte,
corrupt-compressed, malformed-semantic, unsupported-semantic, and
trailing-semantic inputs from that corpus. TypeScript supplies the bytes and
asserts acceptance or rejection through the owning Rust frame, negotiation,
compression, and semantic-payload decoders. An exact executed-name set makes
sampling or silently omitting a newly added manifest family fail. This proves
host-artifact reachability without treating two host bridges as independent
encoders.

## Non-negotiable representation rules

1. **Rows are the unit of authorization and sync.** Read policy admits or
   denies a row; synchronization carries complete ordinary rows/versions, not
   column-level permission patches. A query's `select` is application-result
   presentation only. It must not introduce hidden cells, per-column access
   rules, or a second partial-row sync format.
   A Groove large-value descriptor (ch. 19) is the complete atomic cell carried
   by that row. Its immutable backing chunks may be fetched lazily through the
   authorized root capability; they are neither hidden Jazz cells nor partial
   row-sync facts.
2. **Projection is downstream of row semantics.** The source resolver preserves
   the complete current-row material needed by query, policy, and maintenance;
   node materialization may then apply the requested app projection. Internal
   route/provenance slots remain compiler bookkeeping fields and never become
   hidden application cells.
3. **Rust owns root materialization.** The maintained-view layer creates an
   internal `TerminalRootLayout` from the typed app-row schema. `db.rs` uses it
   to decode and apply root inserts, updates, removals, and moves to the retained
   subscription snapshot. The binding publishes ordinary packed root rows with
   occurrence identities and explicit positions; TypeScript never decodes a
   root terminal payload or reconstructs root ordering.
4. **One graph, many consumers.** Typed fact terminals support membership,
   payload, replacement, policy, settlement, and coverage maintenance for both
   ordinary and structured include/relation results. They must remain internal
   unless a public API explicitly exposes a corresponding result; app output
   must not reimplement their diffing logic.
5. **No projection carrier crosses peer sync.** A `ProjectedAppRow`, packed
   terminal record, or relabeled raw source bytes is not a protocol escape hatch.
   The receiver decodes a canonical version in its authored schema, projects it
   through the ordered lineage, and uses local IVM to produce its terminal.
   That preserves source/witness identity and lets recovery discard every local
   terminal cache without losing replicated truth.

## Pointers for changes

- Change public query meaning: start in `SPEC/6_queries.md` and
  `node/query_engine/input.rs`.
- Change storage/current-row or graph semantics: start in `SPEC/14_lowering_to_groove.md`
  and `node/query_engine/lowering.rs`.
- Change maintained delivery: start in `SPEC/16_maintained_subscription_views.md`
  and `node/maintained_subscription_view.rs`.
- Change NAPI/WASM subscription output: preserve the shared indexed payload in
  `binding_codec.rs`, keep both adapters free of root-layout decoding, and
  update the binding golden plus `subscription-manager.test.ts`.

This map intentionally defines ownership, not a second source of query or
protocol semantics. The numbered chapters and invariant registries remain
authoritative.
