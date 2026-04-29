---
"jazz-tools": patch
---

Fix `jazz-tools/expo` fetch polyfill rejecting URL and Request inputs.

`expo/fetch`'s native bridge only accepts a string for the first argument, so calling `fetch(new URL(...))` or `fetch(new Request(...))` — both valid per the WHATWG spec, and the form better-auth's client uses — failed with "The 2nd argument cannot be cast to type URL". The polyfill now normalises URL and Request inputs to a string URL plus a merged init (including the request body) before delegating to `expo/fetch`.
