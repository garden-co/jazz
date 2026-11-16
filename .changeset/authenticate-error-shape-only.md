---
"jazz-tools": patch
---

The error for an `authenticate` answer without a `userId` names the answer's shape (its keys and the types of its values), never the values themselves, so a token or session returned by mistake doesn't end up in logs.
