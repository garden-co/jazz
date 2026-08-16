# Launch TODOs

## Public schema administration API

Design a public schema administration API that makes the catalogue invariant
explicit: activating a non-genesis schema and publishing its lineage-defining
lens are one atomic operation.

The current administrative surface still reflects an older schema-first model
in which a schema may be uploaded as a draft and a migration supplied later.
Before launch, decide whether that draft workflow should remain public, become
an explicitly named staging API, or be replaced by a single atomic publication
operation.

The design must specify:

- how callers submit and validate the target schema, source schema, lens, table
  partition changes, and current-write-pointer transition;
- whether drafts have durable public identities and how they are listed,
  replaced, abandoned, and garbage-collected;
- idempotency, optimistic concurrency, authorization, auditability, and error
  behavior for retries and competing administrators;
- how generated/client schema tooling obtains the lineage bundle it must
  publish; and
- how the existing administrative endpoints migrate without creating a path
  that can expose a non-genesis schema before its lens is active.

This item records an open API-design task. It does not reopen the runtime
invariant: runtime activation remains atomic across schema and lens.
