---
"jazz-tools": patch
---

`jazz.nodeHandler` passes requests for the app's own routes on without touching their body, so mounting it after a body parser such as `express.json()` no longer turns every other POST into a 500 ("Response body object should not be disturbed or locked"). A request for a Jazz route whose body another handler already read now fails with an error that says to mount `jazz.nodeHandler` before body parsers; a buffer left by `express.raw()` is used as the body.
