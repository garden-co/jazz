---
"jazz-tools": patch
---

`vercel()`: concurrent first requests on a cold deploy all open the store. An instance that loses the race to create the core's Sandbox ("A sandbox with the name … already exists") now waits for the winner's Sandbox, resuming it if needed, within the wake deadline, instead of failing its request with a 503.
