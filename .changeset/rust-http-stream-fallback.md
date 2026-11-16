---
"jazz-tools": patch
---

**Rust (`jazz-app`): the HTTP stream fallback.** A Rust client of an app's main backend (a backend client, or a user signing in through its `/jazz`) now falls back from the WebSocket to the backend's HTTP stream, as TypeScript clients do, on hosts that can't upgrade sockets:

- **When it switches:** a socket that fails before opening asks the backend. A `createJazz` backend that can't upgrade sockets says so, and the client switches at once. If the backend says it upgrades sockets but 3 in a row fail anyway (2 that get no answer), something in between doesn't pass upgrades on, and the client switches then.
- **How long:** the switch lasts while the client runs. A stream refused before it opens switches back to sockets.
- **Idle sessions:** an idle stream session isn't given up after the socket's 45 s of silence, since a backend sends nothing on an idle stream.
- **Credentials:** each stream request carries the bearer the session connected with. When the client renews its bearer (at 80% of the token's life), it replaces the stream session with a new one carrying the fresh bearer, as it reconnects a socket. A new session at each renewal is expected, not a dropped link.
- **Other failures:** a socket that fails for any other reason fails as before.
