---
"jazz-tools": patch
---

A local-first guest who signs up keeps its data however the session notices the sign-in. Asking the backend who is signed in (`backend-user`) no longer creates the user's account, and when the user has none yet the session links the guest to them instead of signing them in. Before, the session's own check on start, on focus or after `reauthenticate()` gave the new user an account first, and the guest's `linkWithBackend()` was then refused for good.

If the backend refuses that link, the session signs the user in to their own account instead and doesn't try the link again, and on shared devices `linkGuestOnSignUp: false` turns the automatic link off. Two tabs linking the same guest at once now both succeed when the backend revokes guest keys after a link.
