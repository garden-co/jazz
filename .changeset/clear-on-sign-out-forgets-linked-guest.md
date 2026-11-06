---
"jazz-tools": patch
---

With `clearOnSignOut`, a guest who signs up no longer leaves their guest signing root on the device. Before, the root stayed in browser storage after the sign-up and the sign-out, so the next person on a shared device could reopen that account with `restoreLocalFirst` and see the user's data. Jazz now forgets a guest's root as soon as the backend links it to a user, since the backend signs that user in from then on. A guest who never signs up keeps their root, which is the only key to their account. So does a device without `clearOnSignOut`. `guestKeyAfterLink: "revoke"` on the backend still also retires the key on the server.
