---
"jazz-tools": patch
---

**Native bindings core (`jazz-ffi`): Swift, Kotlin (UniFFI) and Go (C ABI) over `jazz-app`.**

- A JSON-in/JSON-out layer over `jazz-app` for languages other than Rust and TS: open an app from its `jazz.app.json` (as a local client, or as a backend acting as a session user), read with queries, insert/update/delete, and subscribe to deltas. UniFFI (pinned `=0.32.2`) generates the Swift and Kotlin sources; Go links a hand-written C ABI (`crates/jazz-ffi/include/jazz_ffi.h`).
- Contract v2 (additive; v1 stays valid): `include` (nested related rows) and `select` in queries, transactions (`JazzHandle.begin()` / C `txBegin`, `txCommit`, `txRollback`) whose reads see their own writes, read and durability tiers, `waitForSync`, and with `"contract": 2` in the open config the error kinds `permission`, `notSupported` and `conflict`.
- **Breaking when you regenerate Swift or Kotlin bindings:** `JazzError` gains the variants `Permission`, `NotSupported` and `Conflict`, so an exhaustive Swift `switch` or Kotlin `when` over it needs those cases or a default. `catch` clauses are unaffected, and hosts without `"contract": 2` never receive them.
- **Breaking for Swift and Kotlin listeners:** `SubscriptionListener.on_error` takes `(kind, message)` (`kind` as the C ABI's error kinds: `closed`, `refused`, `io`, and with contract 2 `permission`, `notSupported`, `conflict`) instead of `(message)`; implementations must adopt the new signature.
- Not supported yet (refused by name): connecting as an end user with a JWT, device accounts and sessions, mergeable-text edits, streamed files.
