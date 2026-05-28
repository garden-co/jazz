# Orientation

## Overview

This document describes how to implement Jazz as a local-first relational
database on top of a simple embedded database. SQLite is the recommended first
implementation target, and examples use SQLite syntax, but SQLite is not part
of Jazz's public semantics.

Jazz is Jazz because it combines four product ideas in one database model:

1. **Local-first sync.** Every replica can write locally, keep durable local
   state, subscribe to relational queries, and sync only the history and
   metadata needed for those queries.
2. **Branching history everywhere.** Writes produce durable row history.
   Branches are visibility views over that history, not copied databases.
   Historical snapshots are normal read modes.
3. **Policy-first relational access.** Row-level security participates in query
   planning, write validation, sync delivery, and authority validation.
4. **Multi-version schemas with lenses.** Multiple schema versions can coexist.
   Lenses describe how older stored data is read through newer schema views and
   how writes move forward.

The embedded database provides local ACID transactions, durable storage,
B-tree indexes, and a query planner. Jazz provides distributed semantics:
stable public identities, transaction outcomes, row history, branch views,
observed facts, sync bundles, subscriptions, policies, conflicts, and schema
lenses.

The design separates:

- the public product model, which uses stable public ids and application schema
  concepts
- distributed semantics, expressed as transactions, outcomes, durability
  receipts, visibility relations, observed facts, and projections over history
- embedded-database lowering, expressed as generated tables, indexes, compact
  local ids, and database-specific query plans

This is not a product API proposal. It is a semantic and architectural spec for
the core that future product APIs should lower into.

## 1. How To Read

The document uses "must", "should", and "may" in their ordinary engineering
sense. They are design intent, not standards-track compliance language.

The main body defines behavior first and storage lowering later. SQL blocks are
lowering sketches unless explicitly described as semantic requirements.

Open issue sections are part of the spec. They identify places where the next
implementation attempt should either choose a behavior or record why the
behavior remains intentionally open.

## 2. One Running Example

The examples use a small app with:

- `projects`
- `todos`
- Alice and Bob as users
- one branch called `draft`
- a policy saying a user can read a todo when they can read its project

A simplified schema might look like:

```ts
export const schema = defineApp({
  tables: {
    projects: table({
      title: text(),
    }),
    todos: table({
      title: text(),
      done: boolean(),
      project: ref("projects"),
      // Declared index intent. The engine may also generate indexes.
      byProjectAndCreated: indexOnly(["project", "$createdAt"]),
    }),
  },
});
```

A query might ask:

```ts
db.todos.all({
  where: { done: false },
  include: { project: true },
  orderBy: [{ field: "$createdAt", direction: "desc" }],
});
```

This query returns semantic todo rows. Internally, it also observes facts: which
todo rows were results, which project rows were needed for includes and policy,
which predicate/range/page-boundary facts made the answer valid, which branch
view was used, and which catalogue revision interpreted the query.

Those observed facts are why Jazz can sync query scope rather than whole tables.
A result payload alone is not enough to keep another replica correct.

## 3. Core Invariants

The core invariants are:

- Public ids are stable across replicas.
- Physical ids are local implementation details and never cross API or wire
  boundaries.
- Every write is represented as one sealed transaction.
- Row history is append-only with respect to application state.
- Transaction outcome and durability determine which history is visible.
- Rejected transactions remain stored but are invisible to ordinary reads.
- Current projections are rebuildable serving indexes over history.
- Branch visibility is independent from global acceptance.
- Queries produce semantic rows plus observed facts.
- Sync bundles are derived from observed facts; they are not table dumps.
- Policies are evaluated before row delivery and before accepting writes.
- Schema/catalogue state is explicit and versioned.
- Subscriptions use the same query semantics as one-shot reads.

These invariants are more important than any particular SQLite table layout.

## 4. Goals And Non-Goals

The core must provide:

- local ACID writes using embedded database transactions
- stable public row and transaction identities across replicas
- optimistic local writes
- authority-observed acceptance and rejection
- append-only row history as the source of truth
- fast current reads using derived projection tables
- historical reads over accepted history
- branch reads over explicit source provenance
- query-scoped sync
- live subscriptions with semantic row diffs
- row-granular exclusive conflict correctness
- per-column metadata for merge, invalidation, policy explanations, and UI
- SQL-lowerable policy and schema-lens hooks

The core should support:

- compact storage and memory representation for hot metadata
- generated covering and partial indexes derived from schema/query intent
- deterministic projection rebuild
- projection-diff effects for incoming sync and subscriptions
- typed read/write sets for authority validation
- branch provenance precise enough for future multi-base branches

This spec does not specify:

- final TypeScript DSL syntax
- final wire encoding
- final authentication system
- production policy language
- production schema-lens completeness
- networking transports
- userland history-range compression
- garbage collection of history
- final UI behavior for conflict resolution
