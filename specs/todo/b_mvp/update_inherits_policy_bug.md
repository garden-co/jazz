# UPDATE with INHERITS Policy Bug — TODO (MVP)

UPDATE may fail with PolicyDenied even when an INHERITS chain should grant access. Needs investigation of PolicyGraph wiring for UPDATE USING.

> `crates/groove/src/query_manager/policy_graph.rs`
