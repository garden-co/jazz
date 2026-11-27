---
"jazz-tools": patch
---

Go: `DB.Close`, `DB.Shutdown` and `Session.Close` no longer hang while a call waits for an unreachable server. That call fails with `jazz.ErrClosed` (what it sent may still apply), `Shutdown` keeps to its context's deadline plus up to 2 seconds to end sync, and calls made while closing fail at once. Closing cancels the token provider's context and waits for the provider to return; a provider may call `Close` itself. `Close` now returns what failed as it closed, having closed all the same.
