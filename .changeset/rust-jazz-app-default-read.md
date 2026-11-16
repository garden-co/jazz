---
"jazz-tools": patch
---

**Rust (`jazz-app`): reads that name no tier now read as TypeScript's do.** This applies to `jazz.all`, `one`, `get` and `subscribe`.

- **A backend, or a backend client opened with `Config::credential`:** reads the server's answer (`ReadTier::Remote`), as TypeScript does in a server runtime. Before, a backend client never saw rows other clients had written unless it asked for `ReadTier::Remote` itself.
- **An end user's client (`Config::user`):** reads local data first, and its first load waits up to `DEFAULT_FIRST_LOAD_REMOTE_WAIT` (5 s) for the server, as TypeScript's browser and React Native clients do.
- **No server:** reads stay local.

`all_with(query, tier)` and `subscribe_with(query, tier)` still read exactly at the tier they name. Reads inside a transaction stay local. To get the old behaviour, call `all_with(query, ReadTier::LocalFirst)`.
