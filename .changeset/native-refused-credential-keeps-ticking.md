---
"jazz-tools": patch
"jazz-rn": patch
---

React Native: a refused credential no longer fails the native foreground's ticks. The native socket stops and reports the refusal as connection state, the same way the web socket path does, so local reads and writes keep working while the app renews its token, and the renewed token reconnects.
