---
"jazz-tools": patch
---

Move Jazz client creation into the default React and React Native `JazzProvider` so Strict Mode remounts do not trigger extra startup delays, while still exposing `JazzClientProvider` for apps that need to supply their own client instance.
