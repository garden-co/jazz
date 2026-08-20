---
"jazz-tools": patch
---

BREAKING CHANGE: `JazzSvelteProvider` now accepts a reactive `config` prop instead of a pre-created `client`. The provider creates the client itself and waits for the previous client to shut down before starting a replacement. Use `JazzSvelteClientProvider` to provide a pre-created client whose lifecycle remains owned by the caller.
