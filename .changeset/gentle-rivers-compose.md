---
"jazz-tools": patch
---

Prevent staged large values from being evicted or reclaimed while accepted or reactivated references are still resident but not yet durable. Eviction now defers while any resident large-value publication owns lifecycle serialization, avoiding cross-receipt deadlocks, and reclamation reuses the durable metadata read when there is no staged override.
