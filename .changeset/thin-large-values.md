---
"jazz-tools": patch
---

Thin clients read and write rows holding values over 2 MiB (up to 32 MiB per request or result): such a request or result now travels in parts instead of overflowing one frame and closing the connection. A result over 32 MiB fails only its own query with a `TooLarge` error, and a write that large is refused before it is sent (use `insertStreaming` / `updateStreaming`); the connection and other queries carry on.
