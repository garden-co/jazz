---
"jazz-tools": patch
---

Thin clients get their first results sooner: the schema and the app's live queries now go out right behind the connection's hello, without waiting for the server's hello or for the schema's answer. The first results arrive one round trip after the socket opens instead of three. At 40 ms RTT, a StagePlan thin reload's first result came 87 ms sooner. The server already read a connection's messages only after admitting it, and then in order. SPEC §8.1.2 now states that as part of the protocol, with server tests that pin it.
