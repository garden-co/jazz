---
"jazz-tools": patch
"jazz-rn": patch
---

Fix React Native cold-start on offline and unblock initial subscriptions when the transport can't reach the server.

- `jazz-rn` now regenerates its UniFFI bindings for the `insert` / `insert_with_session` signatures introduced with the caller-supplied UUIDv7 APIs, so the native library and JS adapter agree at startup and Jazz initializes in the app.
- `jazz-rn` now calls `rehydrate_schema_manager_from_catalogue` after opening SQLite, matching the WASM runtime, so offline cold-starts can decode previously-persisted rows against their original schema/permissions history.
- `jazz-tools` bounds the "hold remote query frontier while transport connects" wait so a never-completing transport no longer stalls first subscription delivery forever. Pending servers now clear on a new `TransportInbound::ConnectFailed` event (fired from the connect/handshake error paths in both tokio and wasm run loops), with a 2s safety-net timeout for hung connects. The frontier hold also re-evaluates live at settle time so offline or hung first-connect cases release immediately once pending clears.

No public-API break. RowPolicyMode selection, persisted-row wire format, and transport handshake semantics are unchanged.
