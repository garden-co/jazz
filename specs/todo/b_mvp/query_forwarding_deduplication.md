# Query Forwarding Deduplication — TODO (MVP)

The implementation sends queries upstream but does not track per-server forwarded query state. `query_origin: HashMap<QueryId, HashSet<ClientId>>` tracks which clients originated a query, but not which servers received it. This means:

- No deduplication of forwarded queries
- No cleanup of server-side subscriptions when queries are removed

> `crates/groove/src/sync_manager.rs:390` (query_origin), `675-696` (send methods)
