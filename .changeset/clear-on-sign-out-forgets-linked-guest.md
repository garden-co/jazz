---
"jazz-tools": patch
---

With `clearOnSignOut`, a guest who signs up no longer leaves their guest signing root on the device. Before, the root stayed in browser storage after the sign-up and the sign-out, so the next person on a shared device could reopen that account with `restoreLocalFirst` and see the user's data. Jazz now forgets a guest's root as soon as the backend links it to a user, since the backend signs that user in from then on. The account store also keeps a short list of the accounts it forgot (account ids, never keys), so another tab that still had the guest open cannot write the root back. Restoring the root on purpose with `restoreLocalFirst` takes it off that list.

A guest who never signs up keeps their root, which is the only key to their account. So does a device without `clearOnSignOut`. A copy of the root the app saved itself, for example from `exportLocalFirstSecret`, is the app's to delete. Tabs running an older Jazz version don't know the list: their next write drops it, after which they, and tabs on this version that still hold the root, can write it back until they reload. `guestKeyAfterLink: "revoke"` on the backend still also retires the key on the server.
