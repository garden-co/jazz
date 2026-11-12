---
"jazz-tools": patch
---

With `clearOnSignOut`, a tab still open as a guest that another tab signed up (linking the guest to the new user) now signs out once that user signs out, and wipes its storage as a sign-out does, instead of staying open as their account until it reloads. It notices through the account store's `storage` event and on its next backend check.
