---
"jazz-tools": patch
---

On hosts that can't upgrade sync sockets (`next dev`, `next start`, a fetch handler), the browser console no longer shows errors that only look like failures:

- Once a backend has said it serves the HTTP stream instead (426 + `x-jazz-stream`), new pages, tabs and workers go straight to the stream for that endpoint for an hour, instead of failing a WebSocket first.
- Ending a stream session on sign-out no longer gets a 403, 404 or 410: the client ends the session on the backend before letting its response go, and the backend lets the stream's own client end it once signed out (not someone signed in as another user), and answers 204 for a session that already ended.
