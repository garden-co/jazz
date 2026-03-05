---
"jazz-tools": patch
---

Wait for the initial server event stream handshake before returning from `JazzClient::connect`, preventing `EdgeServer` settled queries from racing the connection after server restart.
