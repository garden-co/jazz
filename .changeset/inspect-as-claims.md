---
"jazz-tools": patch
---

Viewing data as a user in the Inspector, `jazz-tools inspect --as` and the MCP server's `as` can now include that user's session claims, so policies that read `session.claims` answer as they would for that user. Use `--claims '<json>'` in the CLI, `claims` in MCP tools, and the "with claims" field in the Inspector (deep links carry them too). Without claims a view-as runs as a session with none, which used to give empty or refused answers silently; now, when the app's permissions read claims, the answer carries a `warning` and the Inspector shows "no claims".
