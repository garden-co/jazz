---
"jazz-tools": patch
---

Expose an established native transport's terminal future so socket close and failure are observable even while the semantic transport is idle. This adds a required `terminal` field to `ConnectedNativeTransport`; external custom connector implementations that construct this struct with a literal must add a future resolving to `NativeTransportTerminal::Closed` or `NativeTransportTerminal::Failed`.
