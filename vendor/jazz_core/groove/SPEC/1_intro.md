# groove — Specification · 1. Introduction

groove is an embedded database built around incremental view maintenance
(IVM). It supports both one-shot queries over stored data and subscriptions to
maintained views; in both cases, common work is shared inside the engine rather
than repeated by each query.

The system follows the [DBSP "Automatic Incremental View Maintenance for Rich
Query Languages"](https://arxiv.org/abs/2203.16684) approach in a from-scratch
implementation.

The storage model assumes a simple ordered key-value store, such as RocksDB.

groove provides the local substrate for `jazz`, the distributed database (see
jazz `SPEC/`, ch. 14). References to the jazz spec are motivational only; they
are never required to understand or validate groove.

## 1.1 How to read this document

- **Numbered chapters (`1_`…`N_`) are normative**
- **Letter-prefixed appendices (`A_`, `B_`…) are implementation guidance**

**One home, settling over time.** Each chapter begins with the settled
normative material. Non-normative material, when present, is kept in two
clearly marked trailing sections: `## Open questions` for unresolved decisions
(most chapters have one), and `## In flight` for operational detail still in
progress.

**Chapter map**

| #   | chapter                                              | one line                                                              |
| --- | ---------------------------------------------------- | --------------------------------------------------------------------- |
| 1   | Introduction                                         | this file: what groove is, conventions                                |
| 2   | Data & storage model                                 | weighted record sets, records, keys, the `OrderedKvStorage` interface |
| 3   | Queries & operators                                  | the query graph, filter/join/project/aggregate                        |
| 4   | Incremental maintenance                              | the tick: deltas → arrangements → outputs                             |
| 5   | Prepared shapes & bindings-as-data                   | the work-sharing core                                                 |
| 6   | Recursion & fixpoint                                 | a fixpoint inside every commit                                        |
| 7   | Correctness, determinism & scope                     | the oracle property, deliberate limits                                |
| A   | _guidance:_ implementation map                       | where to read the code                                                |
| B   | _guidance:_ benchmarks, performance & meta-learnings | the suite, its predictions, levers, findings                          |
| —   | _registry:_ `INVARIANTS.md`                          | out-of-band: every `INV-` id → test + impl                            |

## 1.2 Conventions

**Normative keywords.** MUST / MUST NOT / SHOULD / MAY carry their
[RFC 2119](https://www.rfc-editor.org/rfc/rfc2119) meaning. We apply them to
load-bearing statements deliberately rather than to every sentence; unmarked
prose is explanatory.

**Invariants force code-spec convergence.** Stable invariant ids
(`INV-<AREA>-<n>`, for example `INV-TICK-1`) are the anchors that connect the
normative text, implementation, and tests. Important invariants are stated
where the relevant behavior is specified, as ordinary prose with the id in
parentheses. Finer or edge-case invariants are collected in a short _Further
invariants_ block at the end of each subsection.

**Every invariant has a status and a coverage.** These are orthogonal axes:

- **Status** — its design standing: `now` (in force in the current
  implementation; the contract — the default), `target` (a committed design,
  not yet in force), `open` (the design itself is unsettled — see the chapter's
  _Open questions_), or `prov` (true in the implementation but not a hard
  requirement; a conformant engine may differ).
- **Coverage** — whether an enforcing test exists: `✓` or `untested`.

Only non-`now` status appears inline in the chapters. The complete mapping from
each id to its status, coverage, enforcing test, and implementation lives in
the out-of-band registry (`SPEC/INVARIANTS.md`).

## 1.3 Terminology

Terms are defined where they are introduced. The load-bearing terms used
throughout the specification are:

- **weighted record set** _(a Z-set, in DBSP terms)_ — a multiset of **records**
  with integer weights (`+n` present, `-n` removed). This is the single data
  type that flows on every edge of a query graph.
- **delta** — the weighted change to a weighted record set produced by one commit.
- **arrangement** _(a term of art from incremental view maintenance —
  the heaviest piece of jargon here)_ — the engine's equivalent of a **database
  index that keeps itself up to date**. An arrangement is a maintained, indexed
  copy of a weighted record set, shared across the query graph, so a join or
  aggregate reads its inputs from the index and updates from changes instead of
  rescanning from scratch (ch. 4).
- **prepared shape** — a parameterized query graph whose bindings are _data_
  flowing through a maintained `BindingSource` weighted record set (ch. 5).
- **tick** — one synchronous propagation of a delta batch through the graph to
  every affected subscription (ch. 4).
- **`OrderedKvStorage`** — the ordered key/value interface groove is implemented
  over (RocksDB in production; an in-memory store in tests) (ch. 2).
