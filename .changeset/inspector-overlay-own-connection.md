---
"jazz-tools": patch
---

The embedded inspector overlay now connects to the data layer through its **own worker connection** instead of the postMessage "devtools protocol" bridge. The overlay reads the host app's connection config from a same-origin `window.__jazzInspectorHost` handle, opens its own client (inheriting the host's credential — local-first secret, admin, or JWT), and receives the host's active subscriptions via a one-way push for Live Query. The separate browser-extension/devtools build and the bridge API are removed: `attachDevTools`, `createExtensionJazzClient`, `createEmbeddedJazzClient`, and the `DevToolsAttachment` type are no longer exported.
