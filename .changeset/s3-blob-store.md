---
"jazz-tools": patch
"jazz-napi": patch
"@jazz/rust": patch
---

Large values can live in an S3-compatible bucket: `sqlite({ blobs: s3() })`, `vercel({ blobs: s3() })`, or `jazz-tools server --blobs s3`. On Vercel, connect an AWS S3 bucket to the project through the Marketplace: the core reads `S3_BUCKET_NAME`, `AWS_REGION` and `AWS_ROLE_ARN`, and assumes that role with the function's own OIDC token, so you set no keys. Elsewhere it uses `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` (or `AWS_ROLE_ARN` with `AWS_WEB_IDENTITY_TOKEN_FILE`). `s3({ endpoint })` reaches R2 or MinIO. Every chunk is put with its SHA-256, so the bucket rejects a corrupted upload. Bucket I/O runs beside the core's sync loop, so a slow bucket never stalls sync. A core warns when its bucket is in a different region from its Vercel region. A store on `"sqlite"` may move to `s3()`. Any other move is refused as not supported yet.
