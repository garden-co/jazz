---
"jazz-tools": patch
---

Thin clients get their first results sooner. The schema and the app's live queries now go out right behind the connection's hello, without waiting for the server's hello or for the schema's answer. The first results arrive one round trip after the socket opens instead of three. At 40 ms RTT, a StagePlan thin reload's first result came 89 ms sooner. The server already read a connection's messages only after admitting it, and then in order. SPEC §8.1.2 now states that as part of the protocol, with server tests that pin it.

Mixed versions: every wire-v6 server (alpha.60 and later) serves thin clients. Older servers speak wire v3 and refuse the client's hello before reading anything after it, so frames sent ahead never reach them.
