---
"jazz-tools": patch
---

Keep standalone Node, browser, and React Native subscriptions loading until the first complete local result is ready, avoiding a premature empty snapshot when matching rows are already stored. Browser and React Native foreground databases wait for their persistent worker or relay to supply the initial inputs. Genuinely empty results still finish loading without waiting for server sync, and initialized subscriptions do not return to loading on disconnect. Preserve joined-row identities when resetting subscription inputs.

[PR #2986](https://github.com/garden-co/jazz/pull/2986).
