---
"jazz-tools": patch
---

Writes no longer carry the device's clock as an explicit `$updatedAt`. The server stamps each write when it accepts it, so an edit made offline gets the `$updatedAt` and `$acceptedAt` of when it synced, history ordered by `$acceptedAt` ends with the current row, and a device whose clock runs fast is no longer refused with `client_clock_too_far_ahead`. Writes that pass `updatedAt` explicitly keep that time.
