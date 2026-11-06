---
"jazz-tools": patch
---

A backend client's `session.close()` (from `createJazzClient` in `jazz-tools/backend`) now uploads the writes it holds before it closes. Before, a script that inserted rows and then closed exited cleanly with none of them on the server. It waits up to 10 seconds, or as long as `close({ timeoutMs })` says; `timeoutMs: 0` closes at once. It stops waiting early when the server refused this client or it is offline for good. Writes still not uploaded then are handled by the driver: a `persistent` driver keeps them and uploads them when the client opens again, so `close()` only warns. With the `memory` driver they are lost, so `close()` still closes, then rejects with an error that says how many writes were lost and why (with `timeoutMs: 0` it warns instead). Writes the server rejects are reported as before. `process.exit()` with writes still pending still loses them, so close the session or await `db.waitForSync()` first.
