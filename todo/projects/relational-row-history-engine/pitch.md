# Table-First Row History Engine

Jazz now has a cleaner foundation for everything that feels "local first":

- current table reads
- reconnect and replay
- multi-tier sync
- future transaction semantics

The key idea is easy to state:

> user data lives in raw tables, and the engine-managed facts needed for branching, visibility, durability, and history live alongside that data as row metadata.

## What Landed

Slice 1 established a new storage/runtime shape:

- current reads are driven by compact visible entries
- history is stored as row versions
- sync payloads speak in row-version terms
- storage is the source of truth for row state
- `row_format` and `row_histories` now match the actual architecture directly

That gives the runtime one coherent language from storage all the way up to subscriptions.

## Why It Matters

This makes the system easier to explain and easier to extend:

- table data is still table data
- the engine fields that make local-first behavior work are explicit
- current reads stay fast
- replay and sync no longer need a different conceptual model from local storage

It also gives the next features a much better footing.

## The Next Two Slices

### Slice 2

Build transaction semantics on top of the existing row-history substrate:

- opt-in multi-row transactions
- staging
- accepted/rejected fate
- authority-driven settlement

### Slice 3

Expose history-aware reads directly:

- history queries
- as-of queries
- explicit branch views

## One Illustrative Sketch

```text
app writes todo
  -> append row version to history
  -> update visible entry for current branch
  -> update indices
  -> emit row-version sync
  -> subscriptions settle from the same visible state
```

That is the spirit of the whole effort: one table-first model, reused everywhere instead of translated back and forth across layers.
