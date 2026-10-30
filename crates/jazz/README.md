# jazz

jazz is a local-first, distributed, real-time database with full edit history,
RLS-authorized sync, and two transaction kinds — eventually-consistent
`mergeable` and `serializable` — stored in and queried through SQLite.

## The design and contract live in [`SPEC/`](SPEC/)

Start at [`SPEC/1_intro.md`](SPEC/1_intro.md) — it has the chapter map and reading
order. The normative chapters (ch. 1–17) cover the data model, transactions,
history & merging, reads, queries, authorization, the sync protocol, Core-only
topology, lenses/migrations, branches, the `Db` API, storage, sharding,
maintained subscription views, and integrability. Guidance appendices (A–E)
cover implementation discipline, benchmarks, performance, testing, and the
glossary. Invariant →
test/impl mapping is in [`SPEC/INVARIANTS.md`](SPEC/INVARIANTS.md).

_Building an app?_ Read ch. 1, then ch. 13 (the `Db` API).

Operational and in-flight material lives _inside_ the spec, in each chapter's
clearly-marked `In flight` section after its normative content (benchmark
specifics in [appendix B](SPEC/B_benchmarks.md), and the performance backlog in
[appendix C](SPEC/C_performance.md)).
