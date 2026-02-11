# Silent Query Compilation Errors — TODO (MVP)

When query compilation fails server-side (invalid query, schema mismatch), the client is NOT notified. The subscription is silently dropped. Should send `SyncPayload::Error` with schema hash and error message.

> `crates/groove/src/query_manager/manager.rs:2110-2122` — `continue` on compilation failure with TODO comment
