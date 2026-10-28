---
"jazz-tools": patch
---

A server that cannot serve a message a client sent now ends that link with an error the client does not retry, instead of asking it to reconnect and resend the same message forever. A client waiting for a write to reach the server no longer spins while inbound frames wait for the next pump pass, which could starve its timers and exhaust the heap.
