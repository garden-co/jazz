---
"jazz-tools": patch
"@jazz/rust": patch
---

New permissions reach every session at once. When a backend publishes a permissions bundle the core has not seen, the core ends every session pinned to another bundle with a retryable close (`NotReady`, retry later). Each client reconnects through whichever backend serves it now and is pinned again by that backend. This applies to every deployment of the app: a preview or canary deployment that publishes new permissions reconnects the sessions of all other deployments too, and they pin themselves to their own bundle again. Publishing a bundle the core already has ends no one: a cold start, a deployment that publishes the same permissions as another, or a rollback to an earlier bundle. Unpinned links (the system's, an admin's) are never ended. Schema-only changes still reach sessions through lenses, without a reconnect.

There is no revocation list. To retire permissions that grant too much, deploy fixed ones and delete every deployment that still serves the old ones, including older production and preview deployments that are still reachable.
