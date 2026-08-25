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
        │  publish through NAPI or WASM
        ▼
TerminalRootLayout + packed Record bytes
        │  register layout once, decode many operations
        ▼
TypeScript subscription result
```

## Ownership and boundary

| Stage              | Authoritative owner                                                                                                                                                      | Representation                                                                     | Boundary                                           | Rule that must survive it                                                                                                                                                                |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------- | -------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Query              | `jazz::db` public API and `node/query_eval.rs`                                                                                                                           | `Query`, `RelationQuery`, query options                                            | public host API                                    | Validate public input before execution; no host-specific query semantics.                                                                                                                |
| Normalized query   | `node/query_engine/input.rs`, with normalizers in `node/query_eval.rs`                                                                                                   | `NormalizedRowSetShape`, `RowSetProgramInput`, `ProgramBinding`                    | internal compiler IR                               | Shape identity is canonical and binding-independent; values/claims are bindings, not a second query shape.                                                                               |
| Lowered graph      | `node/query_engine/lowering.rs`                                                                                                                                          | `QueryProgram`, `LoweredGraph`, `LoweredTerminal`                                  | internal compiler/runtime contract                 | One graph and its typed outputs serve ordinary rows as well as structured include/relation views; consumers select the outputs they need rather than defining another query meaning.     |
| Terminal schema    | `node/query_engine/schemas.rs`                                                                                                                                           | `OutputTerminalSchema`, `AppRowSchema`, `ProgramFactSchema`                        | internal typed output contract                     | App rows and internal facts are separate outputs of the same program. Hidden routing/provenance fields never become app fields.                                                          |
| Maintained state   | `node/maintained_subscription_view.rs`; consumed by `node/query_eval.rs`, `node/views.rs`, and `peer.rs`; `db.rs` turns derived transitions into app subscription events | `MaintainedSubscriptionView`, membership/payload/fact indexes, `ResultTransitions` | internal runtime state                             | The maintained path consumes typed terminal deltas; it does not use a second semantic diff engine. Facts such as policy or version witnesses inform maintenance but are not result rows. |
| Binding payload    | `node/maintained_subscription_view.rs` creates `TerminalRootLayout` from `AppRowSchema`; `db.rs` defines/exposes it in subscription events; NAPI and WASM adapt it       | layout ID, encoded `RecordDescriptor`, terminal operation, packed `Record` bytes   | binding ABI, distinct from peer sync wire protocol | Rust publishes an immutable layout before operations naming it. NAPI and WASM carry the same contract; they do not invent per-host layouts.                                              |
| TypeScript decoder | `packages/jazz-tools/src/runtime/subscription-manager.ts` and `native-runtime/native-row-codec.ts`                                                                       | registered `NativeTerminalRootLayout` and compiled decoder                         | public TypeScript result                           | Register once by layout ID; reject redefinition, unknown IDs, bad descriptor/slot compatibility, or a root-key mismatch.                                                                 |

## What is public, wire, and internal

- **Public API:** `Query`/`RelationQuery`, `Db` reads and subscriptions, and the
  TypeScript rows exposed after decoding. They are ergonomic application values,
  not compiler or transport data structures.
- **Peer wire protocol:** `WireFrame` in `wire.rs`, containing the
  postcard-encoded `SyncMessage` defined in `protocol.rs`. This is the only
  portable transport-byte contract. Its target subscription representation is
  canonical authored facts, ordered catalogue/lens lineage, and
  authority-filtered witness/admission facts — never projected app rows or a
  terminal cache as truth (ch. 8 §8.4.1). Bindings move these bytes but do not
  treat `SyncMessage` as their application API.
- **Binding ABI:** descriptors and packed `Record` row bytes, plus
  `TerminalRootLayout` and terminal operations. It is deliberately separate
  from peer wire protocol and may use native host objects for non-hot-path core
  types.
- **Internal only:** normalized shapes, bindings, lowered graphs, terminal
  schemas, maintained facts, routing fields, and physical carrier choices.
  Their names and layouts are not public compatibility promises.

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
   node materialization may then apply the requested app projection. A terminal
   layout separately omits internal route/provenance slots from its app payload.
   Those are compiler bookkeeping fields, not hidden application cells.
3. **The producer owns physical decoding facts.** The maintained-view layer
   creates `TerminalRootLayout` from the typed app-row schema. It binds the
   exact descriptor, root-key slot, public field slots, and each field's carrier
   (`CurrentRow` or `Logical`); `db.rs` carries that contract to bindings.
   TypeScript compiles a decoder from the early-bound layout rather than
   guessing from a query projection at each operation.
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
- Change NAPI/WASM output: preserve `TerminalRootLayout` in both adapters and
  update the fast TypeScript matrix test
  `packages/jazz-tools/src/runtime/terminal-layout-contract-matrix.test.ts`.

This map intentionally defines ownership, not a second source of query or
protocol semantics. The numbered chapters and invariant registries remain
authoritative.
