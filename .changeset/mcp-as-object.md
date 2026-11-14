---
"jazz-tools": patch
---

The Inspector MCP server no longer ignores an `as` given as an object. `as` takes a user id or `{ userId, claims }` (the HTTP API's shape), on every tool that runs as a user. Anything else (an empty or unknown-field object, a number, `null`, an empty string, claims given twice) is refused with an error. It used to be dropped, so the call ran with the backend's authority and answered every row.
