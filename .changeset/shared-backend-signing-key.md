---
"jazz-tools": patch
---

Fix guest sign-ups failing with `invalid_link_challenge` on `vercel()` (and `remote()` with a `backendCredential`) when the link challenge and the link reach two function instances (jazz#5553). A backend without the app's secret now derives its link-challenge and session-renewal keys from a per-app key the core hands its trusted backend, fetched once on first use, so every instance shares them.

Core: new admin route `GET /apps/{id}/admin/backend-signing-key` (admin secret, or a backend credential stating admin access). A core with a configured admin secret derives the key from it; a core that trusts a workload generates it once and keeps it in the app's store (new host table `_jazz_host_backend_signing_key`), so it survives core restarts. Durable Object cores derive it from their core secret. Behind an older core without the route, each backend instance keeps its own keys as before and warns once to upgrade the core.
