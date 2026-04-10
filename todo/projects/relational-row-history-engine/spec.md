# Table-First Row Histories and Visible State

This document records the shape of the current storage foundation and the slices that still build on top of it.

The guiding idea is simple:

- user data lives in raw tables
- the engine stores a small set of reserved system fields next to that data
- current reads come from compact visible entries
- replay, sync, and recovery work from row histories rather than inventing a separate history model

## What Exists Today

Jazz now treats each user table as a table-first storage surface with three closely related pieces:

### 1. Application payload

The columns defined in `schema.ts`.

### 2. Reserved engine fields

The engine-managed facts that make local-first behavior work, including:

- stable row identity
- branch view
- row-version identity
- ancestry
- visibility state
- durability tier
- deletion markers
- metadata

### 3. Current visible state plus retained history

For each logical row, Jazz keeps:

- a visible entry for current reads
- a history of stored row versions

That is what lets the runtime answer both "what is true now?" and "how did we get here?" without switching mental models midway through the stack.

## Core Model

The important nouns are:

- **logical row** — the stable row identity an app thinks of as "this todo"
- **row version** — one concrete version of that logical row
- **row history** — the row-local ancestry graph of versions
- **visible entry** — the compact current winner for one `(branch, row_id)`
- **catalogue entry** — schema/lens metadata stored in the separate catalogue lane

## Physical Layout

Conceptually, one user table looks like this:

```text
todos
  visible: (branch, row_id) -> current winner + tier fallbacks + current payload
  history: (row_id, version_id) -> stored row versions over time
```

The current implementation keeps the application payload encoded through `row_format` and stores the engine-managed fields alongside it on the row-history records and visible entries.

That gives Jazz one coherent storage story:

- raw tables
- row histories
- visible entries
- indices
- catalogue rows

## Reserved System Fields

The important fields are:

- `$row_id`
- `$branch`
- `$version_id`
- `$parents`
- `$state`
- `$confirmed_tier`
- `$is_deleted`
- `$metadata`
- actor/provenance fields such as `created_by` and `updated_by`

The exact encoded representation can evolve, but the architectural point should stay the same: these are ordinary engine-managed table fields, not a second hidden world the rest of the runtime has to reconstruct later.

## Current Read Path

Ordinary queries are visible-first:

1. index scans find candidate row ids
2. materialization loads the visible entry for the requested branch
3. the runtime only falls back to row history when a lower-tier winner differs from the current visible winner

This keeps current-state reads compact while still giving the runtime durable history to replay from.

## Current Write Path

A direct write:

1. appends a row version to history
2. computes the current visible winner
3. updates visible entries and indices
4. emits row-visibility changes for query/sync consumers

Because reads and replay both use the same row-history language, restart and reconnect behavior can recover from durable state instead of waiting for lucky live callbacks.

## Current Module Boundaries

The implementation now lines up with the model directly:

- `row_format` — shared binary row/value encoding
- `row_histories` — row-history types and reducer logic
- `storage` — raw tables, indices, row locators, visible/history persistence
- `catalogue` — schemas and lenses
- `RuntimeCore` — orchestration plus a small monotonic clock

## What Still Comes Next

This foundation was always meant to support two follow-up slices:

### Slice 2: transactions, authorities, and fate

Add opt-in multi-row transactions with:

- staging
- accepted/rejected outcomes
- authority-driven settlement
- replayable restart/reconnect behavior

### Slice 3: public history queries

Expose the history-aware features already implicit in the engine through query APIs such as:

- history mode
- as-of reads
- explicit branch views

## Practical Takeaway

If you want one sentence to remember:

> Jazz now stores user data as raw tables with engine-managed row metadata, and the same row-history model powers current reads, replay, sync, and durability.
