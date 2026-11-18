---
"jazz-tools": patch
---

**Rust: a user's handle leaves the shared client's lifecycle alone.** On a handle from `Endpoint::for_user`, `disconnect()`, `reconnect()`, `mutation_errors()` and `delete_client_storage()` now fail with `Error::Unsupported`: the handle shares the backend's trusted client with every user, so they would move its link for all of them, show every user's refusals, or (for `delete_client_storage`) report success having done nothing. Call them on the backend's own handle.
