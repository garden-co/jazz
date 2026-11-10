---
"jazz-tools": patch
---

Destructuring the removed `isCreator` in `definePermissions` now fails with an error that names its replacement. An unsupported permission condition now names the rule it was passed to (such as `policy.todos.allowRead.where`) and what it got.
