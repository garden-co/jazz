---
"jazz-tools": patch
---

BREAKING CHANGE: Solid's `JazzProvider` now accepts a reactive `config` prop and manages the client lifecycle. It no longer accepts `client`; use `JazzClientProvider` when passing a client created with `createSolidJazzClient`.
