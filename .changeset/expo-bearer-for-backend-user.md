---
"jazz-tools": patch
---

React Native and Expo sessions signed in through an app's backend that authenticates by bearer, not cookie, now ask the backend who is signed in with the app's bearer, as sign-in already did. Before, that check went without it and never got an answer. Thanks to Xelson for this fix.
