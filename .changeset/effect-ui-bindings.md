---
"jazz-tools": patch
---

Add `jazz-tools/effect-ui`, experimental bindings for [Effect UI](https://ui.effect.website/) (`@effect/ui`, which has no npm release yet). A live query is a keyed Subscription (`liveQuery`) over `Jazz.stream`; who is signed in is the `JazzSession` service, whose state is a Subscription (`sessionState`) and whose `JazzSession.jazz` layer provides `Jazz` to a component keyed on the signed-in account. Writes are your own `Command.Service`s over the `Jazz` service.

`JazzError`, `JazzWriteRejected` and `JazzNoAccount` from `jazz-tools/effect` are now `Schema.TaggedError` classes, so they can be part of a Schema (an Effect UI message, an RPC error). Their tags, fields and messages are unchanged.

Constructing one now validates its fields: `new JazzError({})` or a non-string `operation` throws a Schema validation error where it was accepted before. Errors Jazz itself raises are unaffected; `instanceof`, tags, `cause`, equality and yielding work as before.
