# Scopes

```text
┌──────────────────────────────────────────────────────┐
│ Scope 1: Table-First Row Histories                  │
│ raw tables + visible entries + row histories        │
│ status: landed                                       │
└────────────────────────┬─────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────┐
│ Scope 2: Transactions, Authorities, and Fate        │
│ staging, acceptance/rejection, and tx semantics     │
│ status: next                                         │
└────────────────────────┬─────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────┐
│ Scope 3: Public History and As-Of Queries           │
│ expose history-aware query APIs                     │
│ status: later                                        │
└──────────────────────────────────────────────────────┘
```

## Scope 1 — landed foundation

- [x] User rows now follow the table-first row-history model.
- [x] `row_histories` is the canonical row-history module.
- [x] `row_format` is the shared binary row codec used across user rows, visible entries, and catalogue rows.
- [x] Production row state is storage-backed rather than cached in a separate row/object manager.
- [x] Current reads are visible-first.
- [x] Sync payloads are row-version and row-state oriented.
- [x] Browser and native runtimes use the same storage/sync model, with platform-specific backends underneath.

## Scope 2 — transactions, authorities, and fate

- [ ] Introduce opt-in multi-row transactions on top of the existing row-history substrate.
- [ ] Add staging rows that do not affect ordinary visible reads until accepted.
- [ ] Add accepted/rejected terminal outcomes driven by an authority role.
- [ ] Persist enough transaction state for reconnect and restart to replay cleanly.
- [ ] Keep direct visible writes working beside the new transactional path.
- [ ] Add integration tests for accepted, rejected, replayed, and restarted transactional flows.

## Scope 3 — public history queries

- [ ] Add explicit history-oriented query modes.
- [ ] Add as-of queries over historical row versions.
- [ ] Add explicit branch-view query APIs.
- [ ] Decide which engine-managed fields should become first-class queryable metadata.
- [ ] Add realistic integration and performance coverage for historical query shapes.
