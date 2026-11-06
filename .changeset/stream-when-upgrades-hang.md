---
"jazz-tools": patch
---

A browser behind a proxy that never answers WebSocket upgrades now syncs. Examples are `vite preview` without `ws`, or a fetch-based proxy in a framework's server hooks. Before, the socket stayed connecting forever, so the HTTP stream fallback never started and nothing synced. Now a socket that hasn't opened after 10 s counts as failed. If the backend answers plain requests and says it upgrades sockets, one such hang is enough to switch that page to the HTTP stream, with a console warning that says why.
