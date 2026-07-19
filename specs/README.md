# Architecture Specs

The authoritative Jazz and groove architecture contracts live with the crates:

- [`crates/jazz/SPEC/`](../crates/jazz/SPEC/) — Jazz data model, transactions,
  authorization, sync, topology, API, lowering, branches, large values,
  sharding, maintained subscriptions, integrability, benchmarks, performance,
  testing, and glossary.
- [`crates/groove/SPEC/`](../crates/groove/SPEC/) — groove storage, operators,
  incremental maintenance, prepared shapes, recursion, correctness scope,
  implementation map, and benchmarks.

This top-level directory remains only for repo-level routing and temporary
`jazz-private` move candidates. Historical alpha status-quo notes and public
TODO specs were subsumed into the crate SPEC chapters; the per-file audit trail
is [`dev/SPECS_SUBSUMPTION.md`](../dev/SPECS_SUBSUMPTION.md).
