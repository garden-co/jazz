---
"jazz-tools": patch
---

A backend client's `session.close()` (from `createJazzClient` in `jazz-tools/backend`) now uploads the writes it holds before it closes. Before, a script that inserted rows and then closed exited cleanly with none of them on the server. It waits up to 10 seconds. If writes still haven't reached the server by then, or the server refused this client, `close()` still closes, then rejects with an error that says how many writes were lost and why. A refused client gets that error at once, with no wait. Writes the server rejects are reported as before. `process.exit()` with writes still pending still loses them, so close the session or await `db.waitForSync()` first.
