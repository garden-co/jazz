---
"jazz-tools": patch
---

Vue's `<JazzProvider>` now takes its options in kebab-case as Vue templates write them. Before, `:clear-on-sign-out="true"` reached the session as `clear-on-sign-out` and was ignored, so signing out kept the account's local data. `clear-on-sign-out` and `link-guest-on-sign-up` are now declared props (a bare attribute counts as true), and any other attribute is camelized before it reaches the client.
