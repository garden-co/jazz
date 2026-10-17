---
"jazz-tools": patch
---

Fix a client staying on the slower initial-sync flush cadence after a reconnect or resubscribe in which every referenced payload was already stored locally.
