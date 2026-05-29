---
"jazz-tools": patch
"jazz-rn": patch
---

Fix a `batched_tick` deadlock that left React Native apps spinning at 100% CPU when a server-side query subscription was deferred waiting for its catalogue/schema.

- `jazz-tools`: `RuntimeCore::batched_tick` now drains parked sync messages before deciding whether to reschedule, so a `CatalogueEntryUpdated` that arrives while a subscription is parked actually unblocks it instead of sitting in the parked queue forever. Progressless ticks no longer re-arm the scheduler, breaking the reschedule hot loop.
- `jazz-rn`: `request_batched_tick` is now deferred off the JS thread (mirroring `schedule_mutation_error_delivery` and `NapiScheduler`) so the JS callback can't synchronously re-enter `batched_tick` and starve `setInterval` / rendering.
