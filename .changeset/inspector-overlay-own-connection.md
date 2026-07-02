---
"jazz-tools": patch
---

The embedded inspector overlay now connects to the data layer through its **own worker connection** instead of the postMessage "devtools protocol" bridge. The overlay reads the host app's connection config from a same-origin `window.__jazzInspectorHost` handle and **joins the host's local store** — it reuses the host's OPFS namespace, broker SharedWorker URL, and identity, so it sees the host's actual local data (including unsynced local-only rows) and works offline; no `serverUrl` is required. It receives the host's active subscriptions via a one-way push for Live Query. The separate browser-extension/devtools build and the bridge API are removed: `attachDevTools`, `createExtensionJazzClient`, `createEmbeddedJazzClient`, and the `DevToolsAttachment` type are no longer exported.
