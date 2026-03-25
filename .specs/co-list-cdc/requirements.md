# Requirements

## Introduction

`CoList` should gain a first-class CDC capability for Jazz app developers.
The feature should let developers use the schema returned by `const MyList = co.list(Todo)` to query historical changes and subscribe to future changes, including nested descendant changes selected through a Jazz `resolve` query.
The CDC feature is intended to be replayable, cursor-based, transaction-ordered, and best-effort when descendants cannot be resolved.

## User Stories & Acceptance Criteria

### US-1: Query CoList CDC history

**As a** Jazz developer, **I want** to query historical CDC records from a `CoList` schema, **so that** I can bootstrap downstream consumers from an existing list state.

**Acceptance Criteria:**
- The system shall expose CoList CDC through a high-level static API on `CoListSchema`.
- When a caller requests CDC history for a CoList ID without a cursor, the system shall return CDC batches from the earliest available change.
- When a caller requests CDC history with an opaque cursor, the system shall return only CDC batches after that cursor.
- The system shall order returned CDC batches by underlying transaction order.
- The system shall return a new opaque cursor that represents the end of the returned history window.

### US-2: Subscribe to live CoList CDC

**As a** Jazz developer, **I want** to subscribe to CoList CDC updates, **so that** I can keep downstream consumers in sync after bootstrap.

**Acceptance Criteria:**
- When a caller starts a CDC subscription without a cursor, the system shall tail future changes from subscription start and shall not replay existing history.
- When a caller starts a CDC subscription with an opaque cursor, the system shall replay backlog after that cursor and then continue tailing future changes in the same subscription flow.
- While a subscription remains active, the system shall deliver CDC batches in transaction order.
- When multiple CDC-relevant changes occur in one underlying transaction, the system shall deliver them as one transaction batch.

### US-3: Capture nested descendant changes

**As a** Jazz developer, **I want** CDC to include nested descendant changes, **so that** a CoList can act as the root of a change stream for a resolved object graph.

**Acceptance Criteria:**
- The system shall let callers define CDC scope with a Jazz `resolve` query.
- When the resolve query includes referenced descendants, the system shall include CDC changes originating from those descendants.
- Where a descendant is outside the requested resolve scope, the system shall not emit CDC changes for that descendant.
- Where a `CoList` item is a referenced CoValue, the system shall treat descendant changes inside the requested resolve scope as CDC-relevant changes for the parent `CoList` stream.

### US-4: Resume from replayable cursors

**As a** CDC consumer, **I want** a replayable cursor, **so that** I can resume processing without re-reading the full stream.

**Acceptance Criteria:**
- The system shall use opaque cursors as the public resume token.
- When a caller resumes from a previously returned cursor, the system shall continue after the last acknowledged CDC batch represented by that cursor.
- The system shall keep cursor internals private from the public API contract.
- The system shall produce cursor values for both history queries and live subscriptions.

### US-5: Best-effort behavior for unresolved descendants

**As a** Jazz developer, **I want** CDC to continue when some descendants cannot be resolved, **so that** one inaccessible child does not break the whole stream.

**Acceptance Criteria:**
- When a CDC query or subscription encounters an unauthorized, unavailable, or otherwise unresolvable descendant inside the requested resolve scope, the system shall continue processing other eligible changes.
- The system shall surface skipped descendants or resolution failures as metadata on the affected CDC batch or change record.
- The system shall not abort the full `CoList` CDC stream solely because one descendant could not be resolved.

## Assumptions And Defaults

- The feature directory is `.specs/co-list-cdc/`.
- The public surface is a high-level static API on the `CoListSchema` returned by `co.list(...)`.
- CDC scope selection reuses Jazz's existing `resolve` query model.
- Live delivery is transaction-batched rather than flattened into one callback per individual change.
- A live subscription without a cursor tails from "now" by default.
- Exact method names and exact event payload fields will be finalized in the design step.
