---
"jazz-tools": patch
---

A browser tab whose shared-worker connection failed now closes that connection and lets go of its page lock as soon as the failure is seen, so the worker no longer counts the dead connection as a live tab until the page goes away. A tab that reported `held-by-another-tab` and is then refused for another reason (for example an older deploy's schema) now reports that refusal instead of still showing `held-by-another-tab` (an unresponsive worker on a retry doesn't clear it). The `StoreHeldByAnotherTabError` docs now say that a failed wait doesn't resume when the tab joins: its writes still go through, so call `wait()` again then.

`linkWithBackend()` without a guest to link (signed out, or already signed in through the backend) now rejects with an `AccountAuthError` with code `local_first_account_required` and a message that points to `loginWithBackend()`, instead of "Linking requires a logged-in account". The `DbReplacedError` message now tells React users to read the Db with `useDb()` instead of keeping an earlier one.
