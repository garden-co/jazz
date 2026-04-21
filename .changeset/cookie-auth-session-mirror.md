---
"jazz-tools": patch
---

Add HttpOnly cookie auth support to `jazz-tools` with a mirrored browser
`cookieSession` for local permission evaluation. Servers can now accept JWT auth
from a configured auth cookie, and cookie-backed websocket handshakes are
restricted to same-origin requests.
