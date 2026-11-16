---
"jazz-tools": patch
---

Kotlin: `jazz.transaction { }` is mergeable by default, like TypeScript's (`serializable = true` for a transaction whose conflicting commit throws). Also: `disconnect()` / `reconnect()`, `deleteClientStorage()`, and `mutationErrors`, a Flow of writes the server refused that nobody waited on.
