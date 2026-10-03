---
"jazz-tools": patch
---

Keep Global write waits responsive while inbound routing is suspended. Wait for actual transport progress instead of repeatedly waking on the same queued frames, so host tasks can unblock routing without treating transport activity as write acceptance.

Report scheduled core-tick failures on connected runtimes through the existing terminal transport error path without also throwing them as uncaught exceptions. Preserve connection-generation isolation, native foreground liveness checks, and fatal diagnostics for runtimes without a server transport.
