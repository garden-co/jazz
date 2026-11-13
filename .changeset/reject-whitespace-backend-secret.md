---
"jazz-tools": patch
---

A whitespace-only backend secret (`"  "`, a stray newline: usually an environment variable that is set but blank) is now refused up front with `invalid_backend_secret: backendSecret is only whitespace…`, by `createJazzContext` and by a Node `createJazzClient` opened with `initial: { backendSecret }`, instead of being sent to the server and failing later as a refused connection. Any other non-empty secret is still passed on unchanged.
