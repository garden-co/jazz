---
"jazz-tools": patch
---

A browser on a backend that upgrades sync sockets (`attach()`) now falls back to the HTTP stream when it sits behind something that doesn't pass WebSocket upgrades on, such as `vite preview` with `ws: false`, a framework's fetch proxy, or some PaaS edges. Before, it retried the socket forever and writes never synced, with no error. After three sockets in a row fail while the backend itself answers, it switches to the stream and logs a warning that says why.
