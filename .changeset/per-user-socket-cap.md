---
"jazz-tools": patch
---

The server no longer evicts a user's open tabs when several of them connect at once. The cap on WebSocket connections per signed-in user goes from 4 to 16, so thin-client tabs (one socket each) and several devices fit. Past the cap, the server now evicts the connection it heard from least recently, such as a half-open socket left by a reconnect, instead of the oldest live one.
