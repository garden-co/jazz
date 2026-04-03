---
"jazz-tools": patch
---

Fix browser worker reconnect after network loss when offline `worker`-tier writes were queued locally.

The worker now aborts its stale upstream events stream before scheduling reconnect after sync POST failures, which lets later writes promote normally once network access returns. This also adds browser regression coverage for the split-context reconnect case where one client stays online while another writes offline and reconnects.
