---
"jazz-tools": patch
---

Fix the MCP server crashing with `ERR_UNKNOWN_BUILTIN_MODULE` on Node.js < 22 (and non-Node runtimes). The text-search fallback used when `node:sqlite` is unavailable no longer transitively imports `node:sqlite`: the pure MDX parsing helpers now live in a sqlite-free module, so the fallback loads and serves docs instead of failing. The fallback also emits a loud deprecation warning, since Node.js < 22 is no longer supported.
