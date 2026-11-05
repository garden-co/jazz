---
"jazz-tools": patch
---

React Native and Expo sessions that follow the app's backend (`createJazzClient` from `jazz-tools/react-native` and `jazz-tools/expo`) no longer report `signed-out` before the backend has answered who is signed in, as browser sessions already don't. Waiting for that first answer on start is now capped at 5 seconds on every platform: a slower backend opens the session signed out, and it signs the user in once the answer arrives.
