---
"jazz-tools": minor
---

Add an opt-in, in-band server→client `Redirect` message: a sync server can tell a client to reconnect to a different public URL without dropping the connection. It is gated by the new `--public-url` / `JAZZ_PUBLIC_URL` server option and an `x-jazz-forwarded` request header — the server keeps serving, and older clients that don't recognize the message simply ignore it (no capability handshake). On receipt, the client re-points its transport to the advertised origin (keeping its `/apps/<id>/ws` path) and reconnects, preserving in-memory state. Enables edge-fallback routing, where a client proxied to the region its app actually lives in is handed a direct URL so the long-lived session bypasses the proxy.
