# Globally Consistent Transactions — TODO (Launch)

Minimally viable version of globally consistent transactions: per-table, per-policy, or per-constraint scope.

## Design Idea

MVCC on top of the existing commit graph, with a single global transaction authority instance that determines which conflicting transactions win.

- **High latency is OK** — this is for correctness, not speed (e.g., unique constraints, balance checks)
- Transactions are submitted to the authority, which serializes them
- Losing transactions are rejected; clients retry or surface the conflict
- Scope can be narrow: per-table or per-constraint, not necessarily whole-database

## Open Questions

- Authority placement — dedicated service, or a role the core server plays?
- Scope granularity — per-table? per-constraint? per-policy?
- How does this interact with optimistic local writes — does the client speculatively apply, then roll back on rejection?
- Failure mode — what if the authority is unreachable? Queue and retry, or block?
- Can this reuse the existing commit graph's causal ordering, or does it need a separate log?

Related: `../c_later/edge_transaction_authorities.md`
