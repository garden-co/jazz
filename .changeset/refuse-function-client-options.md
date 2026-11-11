---
"jazz-tools": patch
---

`createJazzClient` now refuses an option that holds a function or class, such as a `WebSocket` class, as soon as it is called. Before, the option reached the client's config and broke later, in the first sign-in, with "could not be cloned". No client option takes one; the client uses the platform's own WebSocket.
