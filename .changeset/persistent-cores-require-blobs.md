---
"jazz-tools": patch
---

**Breaking change.** Persistent Jazz cores require an explicit `blobs` destination for large values. On upgrade, select the destination your core already uses.

- **Native SQLite.** Use `sqlite({ path, blobs: files() })` to keep existing blobs in `<path>/blobs`.
- **Vercel Sandbox.** Set `blobs` on `vercel({ ... })`. If you relied on automatic selection, use `vercelBlob()` for a connected Vercel Blob store or `"sqlite"` if the Sandbox kept blobs in its database.
- **Cloudflare Durable Objects.** Replace the direct `JazzCore` re-export in the core Worker with `export const JazzCore = createJazzCore({ blobs: … })`. Use `blobs: (env) => r2(env.JAZZ_BLOBS)` for an existing R2 binding or `blobs: "sqlite"` for inline storage.

All three cores support `"sqlite"`, `s3(...)` for S3-compatible buckets, and `vercelBlob(...)`. Native SQLite and Vercel Sandbox also support `files()` on persistent disk. Cloudflare supports `r2(...)` for direct bucket access.
