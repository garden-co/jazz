---
"jazz-tools": patch
---

Opening a backend-authenticated session with no account on the device (`createJazzClient` in the browser, React Native and Expo) now waits at most 5 seconds for the backend's first answer about who is signed in, instead of up to the 30-second auth request deadline. A slower backend (cold-starting or hung) opens the session `signed-out`; the check keeps running, and the user is signed in when the answer arrives.
