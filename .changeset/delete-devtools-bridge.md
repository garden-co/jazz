---
"jazz-tools": patch
---

The postMessage "devtools protocol" bridge and the separate browser-extension/devtools build are deleted now that the inspector overlay talks to the data layer through its own worker connection: `attachDevTools`, `createExtensionJazzClient`, `createEmbeddedJazzClient`, and the `DevToolsAttachment` type are no longer exported.
