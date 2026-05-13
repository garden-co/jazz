---
"jazz-tools": patch
"jazz-wasm": patch
"jazz-napi": patch
"jazz-rn": patch
---

Fix loss of reactivity for query subscriptions with deeply-nested includes. Subscriptions built from depth-2+ `via` include chains (`org.include({ todoViaOrg: { user_checkViaTodo: true } })`) now correctly receive deltas when a row at the bottom of the chain is inserted, updated, or deleted. Previously only the immediate child table was tracked as a dependency of the outer subscription, so mutations further down the chain were silently missed.
