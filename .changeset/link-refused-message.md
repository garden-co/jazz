---
"jazz-tools": patch
---

When the backend refuses to link a guest because the signed-in user already has an account (most often from another device), `linkWithBackend()` now says so and tells the app to call `loginWithBackend()` instead. The error keeps its `link_refused` code, and the guest stays signed in.
