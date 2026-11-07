---
"jazz-tools": patch
---

`vercel()` now refuses, at start, a Blob store that another environment's core already claimed: `createJazz` fails with "Blob store … serves the production environment, not preview; connect a separate Blob store to each environment". Before, a preview core given production's store started, acknowledged writes, and only logged a warning while it retried uploading their large values. A store connected through the API or CLI is connected to every environment, so give each environment its own.
