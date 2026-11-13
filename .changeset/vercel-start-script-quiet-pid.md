---
"jazz-tools": patch
---

The `vercel()` core's start script no longer writes "/proc/<pid>/cmdline: No such file or directory" to the Sandbox's `/data/server.log` on a Drive's first start or after the Sandbox stopped.
