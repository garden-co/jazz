# groove

groove is an embedded database (a Rust library, like SQLite or RocksDB — no server)
organized around one idea: **instead of running queries against data, you subscribe
to queries, and the database pushes you the exact changes to their results as
writes are committed.**

It is a small, from-scratch implementation of the approach described in
[DBSP: Automatic Incremental View Maintenance for Rich Query Languages](https://arxiv.org/abs/2203.16684)
(see also the [reference implementation](https://docs.rs/dbsp/latest/dbsp/)),
built on RocksDB for durable storage. groove is the substrate the
[`jazz`](../jazz/README.md) distributed database lowers onto.

## The design and contract live in [`SPEC/`](SPEC/)

Start at [`SPEC/1_intro.md`](SPEC/1_intro.md) for the chapter map and reading
order. The normative chapters (ch. 1–7) cover the storage model, queries &
operators, incremental maintenance (the tick), prepared shapes & bindings-as-data,
recursion, and correctness/scope. Appendix A is the implementation map, appendix B
the benchmark methodology, and [`SPEC/INVARIANTS.md`](SPEC/INVARIANTS.md) maps each
invariant to its enforcing test and impl.

Operational and in-flight material lives _inside_ the spec: benchmark commands,
knobs, workflow, and retained results are in
[appendix B's `In flight` section](SPEC/B_benchmarks.md).
