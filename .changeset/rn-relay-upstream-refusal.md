---
"jazz-tools": patch
"jazz-rn": patch
---

A React Native relay whose database refuses its upstream (for example, it cannot mirror the server's catalogue) now stops that connection and reports the refusal as a non-retrying upstream failure, with a warning in the log, instead of keeping a socket open that never syncs and reporting nothing.
