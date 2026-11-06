---
"jazz-tools": patch
---

A browser on a backend that upgrades sync sockets (`attach()`) now falls back to the HTTP stream when it sits behind something that doesn't pass WebSocket upgrades on, such as `vite preview` with `ws: false`, a framework's fetch proxy, or some PaaS edges. Before, it retried the socket forever and writes never synced, with no error. After three sockets in a row fail while the backend itself answers, it switches to the stream for as long as the page stays open, and logs a warning that says why. A backend that is down or restarting in between starts the count again. If the backend refuses the page's origin (a proxy that rewrites the Host header), the client says so once and stays on the socket, because the stream would be refused too.
