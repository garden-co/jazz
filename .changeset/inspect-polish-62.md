---
"jazz-tools": patch
---

Inspector polish:

- An agent token's label (what it asked access for) now shows in `whoami` and in every audit event and default audit line of that token.
- A token's lifetime is checked before anyone approves it. `inspect login --ttl` and MCP `request_access` refuse less than a minute, and the approval page refuses more than the app issues: `whoami` now tells it `maxTokenTtlSeconds`.
- The MCP server's `write` tool types each op (`kind`: `insert`, `update` or `delete`, with the fields each needs), and its `query` argument documents its `where` operators, `select`, `orderBy`, `limit` and `offset`.
- A write op without a known `kind` is refused with an error that names the `kind` field.
