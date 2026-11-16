---
"jazz-tools": patch
---

`jazz-tools inspect` without a token, against an app whose `inspect` hook refuses it, now says there's no token and to run `jazz-tools inspect login` (the MCP server: to call `request_access`), instead of only "the app's inspect hook refused this request".
