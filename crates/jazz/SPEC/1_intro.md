# jazz — Specification · 1. Introduction

jazz is a local-first, distributed, real-time database with full edit history,
RLS-authorized sync, and two transaction kinds: eventually-consistent
`mergeable` transactions and serializable `exclusive` transactions. Its query
and maintenance semantics are defined by **lowering everything onto
[`groove`](../../groove/SPEC/1_intro.md)**, the incremental-view-maintenance
engine. jazz adds distribution, history, and authorization around that engine;
it is not a second query engine. This document is both the design and the
contract.

## 1.1 How to read this document

This SPEC is **the contract**, ordered so that the concepts needed to understand
jazz appear before the mechanisms that rely on them. It has two kinds of file:

- **Numbered chapters (`1_`…`N_`) are normative** — they define the data model,
  semantics, protocol, and invariants any conformant implementation must honor.
- **Letter-prefixed appendices (`A_`, `B_`…) are implementation guidance** —
  they are non-normative material on implementation discipline, benchmarks,
  performance levers, meta-learnings, and testing. They may change without
  changing the contract.

**One home for every decision.** Within a chapter, normative content comes
first. Non-normative material follows in clearly marked trailing sections:
`## Open questions` for unresolved decisions (most chapters have one), and
`## In flight` for operational detail still in progress, such as benchmark
specifics, measured findings, or slice plans. A chapter carries an `## In
flight` section only while it has such material; several chapters have none.
Guidance appendices are entirely non-normative. This is the single placement
rule for the system: as work settles, it moves _upward_ — in-flight detail into
the normative body, and open questions into resolved prose. A chapter is "done"
when it has no `## In flight` section left. Nothing about the system lives
outside the spec.

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
| 11  | Time-travel & branches                                                                | settled-history reads; snapshot-base branches                                  |
| 12  | Large values                                                                          | `text`/`blob` columns and the op-log                                           |
| 13  | The high-level `Db` API                                                               | the runtime-typed surface, subscriptions, sync/serve, identity/auth            |
| 14  | Lowering to groove                                                                    | how every jazz concept maps onto groove                                        |
| 15  | Sharding                                                                              | exploratory; mostly open questions                                             |
| 16  | Maintained subscription views                                                         | target serving architecture for query-driven sync                              |
| 17  | Integrability roadmap                                                                 | TS/WASM/NAPI, server shell, protocol, storage, topology                        |
| A–E | _guidance:_ implementation discipline · benchmarks · performance · testing · glossary |
| —   | _registry:_ `INVARIANTS.md`                                                           | out-of-band: every `INV-` id → test + impl                                     |

**If you are not reading front to back:** to build an app on jazz, read ch. 1
and then **ch. 13 (the `Db` API)**. The API chapter appears late in the
normative order because it depends on the concepts below it, but it is the
surface an application calls. Dip back into 2–8 as the API references them. You
do not need the groove spec to build an app: lowering to groove (ch. 14) is an
implementation concern, not an app-facing one. A rough reading path: minimum
for a local app is ch. 1, 13, §2.3, §7.1; add §3.3 / §5.1 / §8.1 / §9.2 for
client–server sync; add §12.1–12.4 for `text`/`blob` columns. To operate a
deployment, read ch. 3, 8, and 9.

## 1.2 Design principles

The following principles define the shape of jazz before any individual
mechanism is specified. They are normative intent, not mechanism.

1. **Everything queryable lowers to groove.** jazz has one query substrate.
   Schemas become groove schemas, mutations become groove batches, queries and
   sync views become groove subscriptions, and RLS policies become groove
   prepared shapes (ch. 14). The one deliberate exception is large-value content
   _bytes_, which live in a raw content store below the table/IVM layer (ch. 12,
   ch. 14); their op _metadata_ still lowers normally.
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

## 1.3 Conventions

**Normative keywords.** MUST / MUST NOT / SHOULD / MAY carry their
[RFC 2119](https://www.rfc-editor.org/rfc/rfc2119) meaning. They are used only
for load-bearing statements; unmarked prose is explanatory.

**Implementation names.** Rust type, file, and table names (`ids.rs`,
`NodeState`, `jazz_global_changes`, …) are reference-implementation anchors.
They help identify concrete machinery, but the normative contract is the
behavior described here, which any conformant implementation must honor however
it spells things.

**Invariants are the unit of convergence.** Each chapter gives every invariant
a stable id `INV-<AREA>-<n>` (e.g. `INV-TX-1`). Load-bearing invariants are
stated in the section where the topic is discussed, as ordinary prose with the
id in parentheses. Finer or edge-case invariants are collected in a short
_Further invariants_ block at the end of the subsection they belong to, close to
their context but easy to skip.

**Every invariant has a status and a coverage.** These are two orthogonal axes:

- **Status** — the invariant's standing in the design and implementation:
  `now` (in force in the implementation and the default contract state),
  `target` (a committed design point, not yet in force), `open` (the design
  itself is unsettled — see the chapter's _Open questions_), or `prov` (true in
  the implementation but not a hard requirement; a conformant implementation may
  differ).
- **Coverage** — whether an enforcing test exists: `✓` or `untested`.

In the chapters, status appears inline at the id **only when it is not `now`** —
e.g. "star topology (`INV-EDGE-12`, target)" — so settled prose stays
clean and not-yet-enforced behavior is visible exactly where you read it.
Coverage is never shown in the chapters; it changes as tests land.

The id is the anchor; the full mapping of every id to its status, coverage,
enforcing test, and implementation lives in one out-of-band registry
(`SPEC/INVARIANTS.md`), never in the chapters. A registry row reads, e.g.:

| id           | invariant                                            | enforced by (test)                                                                         | impl                                                                                                                         | status | coverage |
| ------------ | ---------------------------------------------------- | ------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------- | ------ | -------- |
| `INV-EDGE-8` | edge mergeable fates are final; core never re-judges | `jazz::tests::four_tier::edge_accepted_mergeable_is_final_at_core_after_policy_revocation` | `node/ingest.rs::NodeState::finalize_edge_accepted_mergeable_commit_unit_once`; `peer.rs::ingest_edge_mergeable_commit_unit` | now    | ✓        |

The rule is simple: every id used in a chapter has a registry row; every `now`
invariant trends toward coverage `✓`; and an untested `now` is visible debt.
`target` and `open` invariants are coverage-exempt by definition, because
unbuilt or undecided behavior cannot be tested. A CI check can later assert
every referenced test exists, every chapter id appears, and no `now` row is
silently untested. A reference to an invariant owned by the other spec is
written with its spec name. For example, a Groove-owned id is written as groove
`INV-SHAPE-16`; it points into `groove/SPEC/INVARIANTS.md` and is exempt from
Jazz's local-row rule.

**Open questions are localized.** Each chapter ends with an `## Open questions`
section holding only that chapter's unresolved decisions, each tagged `🔶`.
There is no central TODO; an open edge lives beside the thing it qualifies. A
`🔶` bullet flags an open _work item_, which may be an undecided design, an
unbuilt `target`, or simply a missing test for a `now` invariant. That work-item
marker is distinct from the design-status axis above: an invariant id appearing
under `🔶` with no status tag is still `now` (the open work is its coverage or
enforcement, not its standing).

## 1.4 Terminology

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
