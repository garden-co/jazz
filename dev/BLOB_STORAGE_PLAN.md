# Blob storage across Jazz core providers

Track remaining work here. Remove each item once its checks pass. Keep the step
numbers stable so we can refer to them during implementation.

The target is for native SQLite, Vercel Sandbox, and Durable Object cores to
support SQLite, S3-compatible buckets, and Vercel Blob for large-value bytes.
Native and Sandbox cores also support files on durable disk. The native R2
binding is specific to Cloudflare. This plan assumes existing buckets and
covers configuring a destination for a core. Bucket provisioning and migrations
between external stores are separate decisions.

## Proposed configuration

Every persistent core requires a `blobs` destination. Choose `"sqlite"` to keep
large-value bytes inline. For Cloudflare,
configure the Worker that exports `JazzCore`; every connecting backend uses
that core's configuration. Native SQLite and Vercel Sandbox support explicit bucket credentials. The remaining changes apply to Durable Objects.

**Native SQLite with Vercel Blob.** The Node backend reads the token from its
environment and passes its value to `vercelBlob()`.

```ts
sqlite({
  path: "./data",
  blobs: vercelBlob({
    storeId: "store_example",
    credentials: { readWriteToken: process.env.APP_BLOB_TOKEN! },
  }),
});
```

**Vercel Sandbox with an S3-compatible bucket.** Set the two secrets in the
Vercel project environment. The application reads them and the provider supplies them to the
core process on Sandbox start or wake. Secret values must stay out of generated
startup scripts, arguments, logs and persisted store settings.

```ts
vercel({
  namespace: ({ environment }) => environment,
  region: "iad1",
  blobs: s3({
    bucket: "app-chunks",
    region: "auto",
    endpoint: "https://<account-id>.r2.cloudflarestorage.com",
    credentials: {
      accessKeyId: process.env.APP_BUCKET_KEY_ID!,
      secretAccessKey: process.env.APP_BUCKET_SECRET!,
    },
  }),
});
```

**Durable Object with an S3-compatible bucket.** Configure storage in the core
Worker with a factory that receives its bindings. The connecting backend
continues to use `durableObjects({ binding, secret })`.

```ts
export const JazzCore = createJazzCore({
  blobs: (env: Env) =>
    s3({
      bucket: "app-chunks",
      region: "us-east-1",
      credentials: {
        accessKeyId: env.APP_BUCKET_KEY_ID,
        secretAccessKey: env.APP_BUCKET_SECRET,
      },
    }),
});
```

### Resolution rules

- Require `blobs` in the TypeScript options for native SQLite, deployed Vercel
  Sandboxes, and the DO core factory. Reject missing configuration before
  opening or provisioning a core, including when a bucket is already bound.
  Select native R2 explicitly with `blobs: (env) => r2(env.JAZZ_BLOBS)`.
- On upgrade, set `blobs` to the destination the existing core uses. For
  example, migrate a native SQLite store that used the old default to
  `sqlite({ path, blobs: files() })`. Verify existing blobs can still be read
  and retain the checks that prevent reopening with an incompatible destination.
- A backend connecting via `remote()` or `durableObjects()` leaves destination
  selection to the deployed core.
- Omitted credentials keep native S3's AWS credential chain and the Sandbox's
  workload OIDC defaults for AWS S3 and Vercel Blob. For a DO using S3, read
  Worker secrets `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optional
  `AWS_SESSION_TOKEN`. For Vercel Blob outside Vercel, support
  `BLOB_READ_WRITE_TOKEN`. The native R2 binding supplies its own access.
- Explicit S3 credentials take `accessKeyId`, `secretAccessKey`, and an optional
  `sessionToken`. Explicit Vercel Blob credentials take `readWriteToken`.
  A supplied credentials object takes precedence over ambient credentials.
  Reject missing or empty required values with an error naming the field.
  Keep secret values out of errors, logs, and persisted store configuration.
  The credential shapes can evolve as each adapter is implemented. Files and
  inline SQLite need no bucket credentials.
  Bucket credentials are separate from the credential admitting a backend to Jazz.

## Remaining work

### 4. Separate DO chunk handling from R2 access

Isolate the R2 binding calls so other adapters can share retries, checksum and
ownership checks, and retention of each SQLite chunk until its upload succeeds.

### 5. Add S3-compatible storage to the DO core

Implement signed S3 requests with Worker `fetch`. Configure the bucket and
credentials on the core Worker so every connecting backend uses the same
destination. Verify reads after restart, integrity and ownership checks, and
that failed uploads retain the SQLite copy for retry.

### 6. Add Vercel Blob to the DO core

Implement private Blob reads, writes, and deletes using credentials configured
on the core Worker. Run the same restart, integrity, retry, and ownership
checks as for S3.
