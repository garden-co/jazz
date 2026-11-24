---
"jazz-tools": patch
---

`createJazz` decides who a client's socket is admitted as in Rust, on Node and in Cloudflare Workers, and no longer passes unknown fields of a client's handshake on to the core.
