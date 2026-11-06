---
"jazz-tools": patch
---

Report scheduled core-tick failures on connected runtimes through the existing
terminal transport error path without also throwing them as uncaught exceptions.
Recheck queued work before running it and preserve connection-generation
isolation, terminal foreground retirement and fatal diagnostics without transport.
