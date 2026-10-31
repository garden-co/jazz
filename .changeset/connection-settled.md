---
"jazz-tools": patch
---

Every sync upstream now tells its clients the position the whole connection is settled through, in a new `ConnectionSettled` message after a tick's rows, settles and fates. A read's settle speaks for that read alone, so an upstream may settle reads at different positions; what a client knows of its upstream moves only by the connection's position. This prepares the client for sharded upstreams and changes nothing a client reads from an unsharded server.
