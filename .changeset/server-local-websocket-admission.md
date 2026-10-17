---
"jazz-tools": patch
---

Scope WebSocket connection limits and admission cleanup to each server instance, preventing independent servers in one process from evicting each other's clients.
