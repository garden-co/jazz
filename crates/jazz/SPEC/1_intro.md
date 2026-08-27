# jazz — Specification · 1. Introduction

## Overview

jazz is a local-first, distributed, real-time database with full edit history,
RLS-authorized sync, and two transaction kinds: eventually-consistent
`mergeable` transactions and serializable `exclusive` transactions. Its query
and maintenance semantics are defined by **lowering everything onto
[`groove`](../../groove/SPEC/1_intro.md)**, the incremental-view-maintenance
engine. jazz adds distribution, history, and authorization around that engine;
it is not a second query engine. This document is both the design and the
contract.

Invariant digest:

- `INV-EDGE-8`: Edge acceptance of a mergeable transaction MUST be a final authorization outcome; core MUST NOT re-evaluate or reject it solely because policy changed concurrently aft...
- `INV-EDGE-12`: Topology v1 MUST be star-shaped: edges connect upstream to core; edges MUST NOT sync with other edges as peers for authority or merge coordination.
- `groove/SPEC/INVARIANTS.md::INV-SHAPE-16`: Prepared shapes MUST retain their output graph nodes while the shape remains registered.
- `INV-TX-1`: A transaction MUST NOT expose `open` writes to ordinary reads or subscriptions before commit.

## Details

### 1.1 How to read this document

This SPEC is **the contract**, ordered so that the concepts needed to understand
jazz appear before the mechanisms that rely on them. The following document
conventions are guidance for maintaining that contract, not product semantics.
The SPEC has two kinds of file:

- **Numbered chapters (`1_`…`N_`) are normative** — they define the data model,
  semantics, protocol, and invariants any conformant implementation must honor.
- **Letter-prefixed appendices (`A_`, `B_`…) are implementation guidance** —
  they are non-normative material on implementation discipline, benchmarks,
  performance levers, meta-learnings, and testing. They may change without
  changing the contract.

**One home for every decision.** Every chapter uses the same top-level shape:
`## Overview`, `## Details`, then `## Open Questions`. The overview is the
team-onboarding entry point. Details hold both normative body text and clearly
marked implementation-status notes; Open Questions hold unresolved design
decisions. Guidance appendices are entirely non-normative.

**Chapter map**

| #   | chapter                                                                               | one line                                                                       |
| --- | ------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| 1   | Introduction                                                                          | this file: what jazz is, principles, conventions                               |
| 2   | Data model & identity                                                                 | tables, columns, schema, rows, the id types                                    |
| 3   | Transactions & durability                                                             | mergeable/exclusive, fates, durability tiers, commit units                     |
| 4   | History, domination & merging                                                         | argmax history, column-LWW, current state                                      |
| 5   | Reads & snapshots                                                                     | current, point-in-time, visibility                                             |
| 6   | Queries                                                                               | shapes, bindings, content-addressing, matched include paths, query-driven sync |
| 7   | Authorization (RLS)                                                                   | policies as shapes; read/write; claim-binding                                  |
| 8   | Sync protocol                                                                         | the peer layer: view updates, commit units, fates, subscriptions               |
| 9   | Topology & the edge tier                                                              | client/relay/edge/core trust ladder; edge authority & cache                    |
| 10  | Schema evolution: lenses & migrations                                                 | multi-schema coexistence                                                       |
| 11  | Partitioned history, overlay views & time travel                                      | schema branch columns; live/frozen bases; contribution merges                  |
| 13  | The high-level `Db` API                                                               | the runtime-typed surface, subscriptions, sync/serve, identity/auth            |
| 14  | Lowering to groove                                                                    | how every jazz concept maps onto groove                                        |
| 15  | Sharding                                                                              | exploratory; mostly open questions                                             |
| 16  | Maintained subscription views                                                         | target serving architecture for query-driven sync                              |
| 17  | Integrability roadmap                                                                 | TS/WASM/NAPI, server shell, protocol, storage, topology                        |
| 18  | Representation ownership                                                              | public, wire, binding and internal carrier boundaries                          |
| 19  | Large-value capabilities                                                              | authorization, proxy, publication and retention of Groove chunks               |
| A–E | _guidance:_ implementation discipline · benchmarks · performance · testing · glossary |
| —   | _registry:_ `INVARIANTS.md`                                                           | out-of-band: every `INV-` id → test + impl                                     |

**If you are not reading front to back:** to build an app on jazz, read ch. 1
and then **ch. 13 (the `Db` API)**. The API chapter appears late in the
normative order because it depends on the concepts below it, but it is the
surface an application calls. Dip back into 2–8 as the API references them. You
do not need the groove spec to build an app: lowering to groove (ch. 14) is an
implementation concern, not an app-facing one. A rough reading path: minimum
for a local app is ch. 1, 13, §2.3, §7.1; add §3.3 / §5.1 / §8.1 / §9.2 for
client–server sync. `string` and `bytes` use the ordinary-value baseline in
§2.3.1. To operate a deployment, read ch. 3, 8, and 9.

### 1.2 Design principles

The following principles define the shape of jazz before any individual
mechanism is specified. They are normative intent, not mechanism.

1. **Everything queryable lowers to groove.** jazz has one query substrate.
   Schemas become groove schemas, mutations become groove batches, queries and
   sync views become groove subscriptions, and RLS policies become groove
   prepared shapes (ch. 14).
2. **One sync protocol; tiers are roles, not code.** Distribution is expressed
   through roles in a single protocol. Every hop (UI ↔ worker, worker ↔ edge,
   edge ↔ core) speaks that protocol; tiers differ only in role flags (fate
   authority, durability guarantee, eviction). Inserting a tier is a deployment
   change, not a protocol change (ch. 8–9).
3. **Transactions are atomic upstream units.** A transaction is assembled locally
   in an `open` state and syncs upstream _only at commit_, as one idempotent
   `CommitUnit`; the core holds no open-transaction state (ch. 3). Downstream
   subscription delivery is view-atomic, not transport-atomic: a `ViewUpdate` may
   carry only the subset of an exclusive transaction needed by the maintained
   subscription view, and those rows become visible only when that view's
   required exclusive payload is complete (ch. 8).
4. **Full history is first-class — at the core.** The core is
   history-complete; downstream nodes may hold arbitrary evicted or partial
   subsets. No protocol step may assume a downstream node has complete history
   (ch. 4).
5. **Every column has a declared class.** Sync and ingest behavior derive
   mechanically from the column's class: _replicated-immutable_ (the only thing
   shipped), _upstream-decided mutable state_ (fate/global*seq, written by the
   authority), or \_node-local derived state* (currency, global-current; never
   shipped) (ch. 2–3).

### 1.3 Conventions

**Normative keywords.** MUST / MUST NOT / SHOULD / MAY carry their
[RFC 2119](https://www.rfc-editor.org/rfc/rfc2119) meaning. They are used only
for load-bearing statements; unmarked prose is explanatory.

**Implementation names.** Rust type, file, and table names (`ids.rs`,
`NodeState`, `jazz_global_changes`, …) are reference-implementation anchors.
They help identify concrete machinery, but the normative contract is the
behavior described here, which any conformant implementation must honor however
it spells things.

**Invariants are the unit of convergence.** Each chapter gives every invariant
a stable id `INV-<AREA>-<n>` (e.g. `INV-TX-1`). An invariant states the simple,
timeless behavior required when jazz is working as designed. It does not record
rollout state, a missing test, or a list of currently supported cases. The id
appears beside the normative statement; finer or edge-case invariants may appear
in a short _Further invariants_ block at the end of their subsection.

**Implementation status is separate from the contract.** A clearly marked
**Implementation status** note in Details records what the current implementation
does, including a named regression test when one exists. This is where
implementation-specific case breakdowns and test coverage belong. The
out-of-band `SPEC/INVARIANTS.md` registry mirrors invariant text and links it to
implementation anchors and tests; it does not determine the contract.

**Open questions are localized and issue-backed.** Each chapter ends with an
`## Open Questions` section holding only that chapter's unresolved design
decisions, each marked and linked to its GitHub issue. There is no central
TODO: a missing test or an implementation gap belongs in a Details status note,
while an unsettled intended contract belongs beside the thing it qualifies as an
open question and in its issue discussion. A reference to an invariant owned by
the other spec is written with its spec name, for example
`groove/SPEC/INVARIANTS.md::INV-SHAPE-16`.

### 1.4 Terminology

Terms are defined where they are introduced. The load-bearing terms needed
up front are listed here; the full set is in appendix E:

- **mergeable / exclusive** — the two transaction kinds: eventually-consistent
  column-LWW vs serializable compare-and-set (ch. 3).
- **fate** — an upstream authority's verdict on a transaction: `Pending` /
  `Accepted` / `Rejected` (ch. 3).
- **durability tier** — how far a write has settled: `None` / `Local` / `Edge`
  / `Global` (ch. 3).
- **shape / binding** — a content-addressed query graph and a concrete
  parameter assignment against it; the unit of query-driven sync (ch. 6).
- **policy** — an RLS read/write rule expressed as a shape, claim-bound to an
  identity (ch. 7).
- **node roles** — client / relay / edge / core, the trust ladder (ch. 9).

## Open Questions

None.
