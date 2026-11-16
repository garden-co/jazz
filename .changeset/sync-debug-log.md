---
"jazz-tools": patch
---

In the browser, `logLevel: "debug"` now logs sync traffic in the page's and the worker's consoles: each message between a tab, its worker and the server (its kind, the read it is about, what it carries and its size), and each subscription delivery with whether and how it settled. Native hosts don't map `logLevel` to these logs yet: a Jazz server started from Node reads `RUST_LOG` (`RUST_LOG=jazz::sync=debug`), and Node and React Native clients don't print them.
