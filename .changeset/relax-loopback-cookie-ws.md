---
"jazz-tools": patch
---

Allow cookie-backed WebSocket auth between loopback hosts on different ports.
This keeps cookie auth same-origin by default, but treats `localhost`,
`*.localhost`, `127.0.0.1`, and `::1` as trusted local development peers.
