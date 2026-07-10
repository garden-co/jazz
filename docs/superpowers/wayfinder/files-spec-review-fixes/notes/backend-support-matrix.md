# S3-compatible backend support matrix

Research date: 2026-07-10. Backends: AWS S3, Cloudflare R2, MinIO (community server, `minio/minio`), Tigris. All claims cite primary sources (vendor docs, release notes, source code). Items that could not be confirmed from a primary source are marked **unverified**.

Context: uploads go to a staging key and are "released" to a final immutable key; object ids are client-generated randoms, so conditional writes are a belt against self-collision, not the primary safety mechanism.

## Summary matrix

| Capability                                                                             | AWS S3               | Cloudflare R2                               | MinIO                                                        | Tigris                                      |
| -------------------------------------------------------------------------------------- | -------------------- | ------------------------------------------- | ------------------------------------------------------------ | ------------------------------------------- |
| `If-None-Match: *` on PutObject                                                        | ✅ (GA 2024-08-20)   | ✅                                          | ✅ (wildcard since RELEASE.2024-05-07) [^m1]                 | ✅                                          |
| `If-None-Match` on CompleteMultipartUpload                                             | ✅                   | ⚠️ [^r1]                                    | ✅ (RELEASE.2024-05-27)                                      | ❌ not documented [^t1]                     |
| `If-Match` on writes                                                                   | ✅ (GA 2024-11)      | ✅                                          | ✅ (since 2023, PR #16551)                                   | ✅                                          |
| Conditional copy vs **destination**                                                    | ✅ (GA 2025-10)      | ✅ via `cf-copy-destination-if-*` extension | ❌ not documented                                            | ⚠️ ambiguous [^t2]                          |
| Precondition failure code                                                              | 412 (+409/404 races) | 412                                         | 412                                                          | 412                                         |
| `x-amz-metadata-directive`, default COPY                                               | ✅                   | ✅ [^r2]                                    | ✅ [^m2]                                                     | **unverified** [^t3]                        |
| CopyObject single-copy limit                                                           | 5 GB                 | ~5 GiB (4.995 GiB per-request cap) [^r3]    | 5 GB [^m2]                                                   | **unverified**                              |
| Store emits per-object `Content-Type` / `Content-Disposition` / `Cache-Control` on GET | ✅                   | ✅                                          | ✅ [^m3]                                                     | ✅                                          |
| Store natively emits arbitrary headers (e.g. `X-Content-Type-Options: nosniff`)        | ❌ (CDN required)    | ⚠️ zone-level, custom domain only           | ❌ (proxy required)                                          | ✅ per-bucket "Additional Headers"          |
| Prefix-scoped expiration (`Filter.Prefix` + `Expiration.Days`)                         | ✅                   | ✅                                          | ✅                                                           | ✅ (multi-rule + prefix only since 2026-05) |
| `AbortIncompleteMultipartUpload` lifecycle action                                      | ✅                   | ✅                                          | ❌ in ILM; server auto-cleanup instead [^m4]                 | ❌ not documented [^t4]                     |
| Lifecycle tag filters                                                                  | ✅                   | ❌                                          | ✅                                                           | ❌ not documented                           |
| Lifecycle granularity                                                                  | days                 | days                                        | days (ILM); stale-upload cleanup is a duration (default 24h) | days (UTC-midnight rounding)                |

[^m1]: Exact-ETag conditional writes existed since 2023; the S3-style `*` wildcard landed May 2024. See §1.

[^r1]: Historical release note says conditional multipart "publish" is handled; current compatibility matrix does not list conditional headers for CompleteMultipartUpload. See §1.

[^r2]: Header listed as supported; COPY-default not independently documented by Cloudflare. See §2.

[^r3]: R2 documents a 4.995 GiB single-request cap; no CopyObject-specific limit is documented. See §2.

[^m2]: From maintainer statements on the official repo, not reference docs. See §2.

[^m3]: Standard S3 PutObject/GetObject semantics; MinIO publishes no per-header reference page. See §3.

[^m4]: Confirmed unsupported in source (`internal/bucket/lifecycle/rule.go` FIXME). See §4.

[^t1]: Tigris conditional-operations doc covers GetObject/PutObject/DeleteObject/CopyObject headers; multipart is never mentioned. See §1.

[^t2]: Docs describe `If-None-Match` on "writes" and list CopyObject among conditional operations, but only source-side `X-Amz-Copy-Source-If-*` headers are specified for copy. See §2.

[^t3]: CopyObject is listed as supported in Tigris's S3 compatibility table with no per-header detail. See §2.

---

## 1. `If-None-Match: *` conditional writes

### AWS S3

- **PutObject: supported.** GA announced [Aug 20, 2024](https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/): "you can add the HTTP if-none-match conditional header along with PutObject and CompleteMultipartUpload API requests."
- **CompleteMultipartUpload: supported**, same announcement and the [conditional-writes user guide](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html), which lists `If-None-Match` as usable with PutObject, CompleteMultipartUpload, and CopyObject. "The `If-None-Match` header expects the \* (asterisk) value."
- **`If-Match` on writes: supported** since [Nov 2024](https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-functionality-conditional-writes/) ("S3 PutObject or CompleteMultipartUpload API requests in both S3 general purpose and directory buckets").
- **Failure modes** ([conditional-writes guide](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)):
  - Existing object → `412 Precondition Failed`.
  - Concurrency: "You can also receive a `409 Conflict` response in the case of concurrent requests if a delete request to an object succeeds before a conditional write operation on that object completes." For PutObject you may retry; "When using `CompleteMultipartUpload`, the entire multipart upload must be re-initiated with `CreateMultipartUpload` ... after receiving a `409 Conflict` error."
  - `If-Match` + concurrent delete → `404 Not Found`.
  - Conditions ignore in-progress MPUs: a conditional PutObject can land mid-MPU and the later conditional CompleteMultipartUpload then fails with 412 (documented scenario).
  - Conditional writes require SigV4 signing.
- Enforcement via bucket policy (`s3:if-none-match` / `s3:if-match` condition keys) GA [Nov 2024](https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-enforcement-conditional-write-operations-general-purpose-buckets/).

### Cloudflare R2

- **PutObject: supported.** The [S3 API compatibility matrix](https://developers.cloudflare.com/r2/api/s3/api/) lists under PutObject: "✅ Conditional Operations: ✅ If-Match ✅ If-Modified-Since ✅ If-None-Match ✅ If-Unmodified-Since". Wildcard supported: [release note 2022-07-30](https://developers.cloudflare.com/r2/platform/release-notes/): "`If-Match` / `If-None-Match` headers now support arrays of ETags, Weak ETags and wildcard (`*`) as per the HTTP standard."
- **CompleteMultipartUpload: ⚠️ ambiguous / not currently documented as a completion-time header.**
  - [Release note 2022-05-27](https://developers.cloudflare.com/r2/platform/release-notes/): "If conditional headers are provided to S3 API `UploadObject` or `CreateMultipartUpload` operations, and the object exists, a `412 Precondition Failed` status code will be returned if these checks are not met" — i.e. for multipart, R2 historically attaches the condition to **CreateMultipartUpload**, not Complete.
  - [Release note 2023-08-11](https://developers.cloudflare.com/r2/platform/release-notes/): "Users can now complete conditional multipart publish operations. When a condition failure occurs when publishing an upload, the upload is no longer available and is treated as aborted." So the condition IS re-evaluated at publish (complete) time — but the failed upload is auto-aborted and cannot be retried.
  - The current [compatibility matrix](https://developers.cloudflare.com/r2/api/s3/api/) lists **no** conditional headers under CreateMultipartUpload or CompleteMultipartUpload (its only listed gaps there are `x-amz-expected-bucket-owner` and `x-amz-request-payer`). Whether an `If-None-Match: *` header placed on the CompleteMultipartUpload request itself is honored is **unverified**.
  - The Workers binding is unambiguous: [`createMultipartUpload` options and `complete()`](https://developers.cloudflare.com/r2/api/workers/workers-api-reference/) accept no `onlyIf` conditions (only `put()` does).
- **`If-Match` on writes: supported** (PutObject matrix above; bindings `onlyIf` supports `etagMatches`/`etagDoesNotMatch`).
- **Failure mode:** `412 Precondition Failed` (release notes above). No S3-style 409 concurrent-conflict behavior is documented.

### MinIO (community server; repo archived read-only 2025/2026 era — see note below)

- **PutObject: supported.**
  - Exact-ETag `If-Match`/`If-None-Match` on uploads since [PR #16551](https://github.com/minio/minio/pull/16551) (merged 2023-02-07). Maintainer in [discussion #20318](https://github.com/minio/minio/discussions/20318): "we implemented it back in 2023 #16551 — AWS simply copied our implementation."
  - S3-style **wildcard** (`If-None-Match: *`) via [PR #19682 "support ETag value to be '\*'"](https://github.com/minio/minio/pull/19682), first shipped in [RELEASE.2024-05-07T06-41-25Z](https://github.com/minio/minio/releases/tag/RELEASE.2024-05-07T06-41-25Z) (verified: the PR's merge commit is an ancestor of that tag; the release notes list the PR). A user in [discussion #20318](https://github.com/minio/minio/discussions/20318) confirmed S3-equivalent wildcard behavior on RELEASE.2024-09-13T20-26-02Z after seeing it fail on a 2023 build.
- **CompleteMultipartUpload: supported** via [PR #19713 "verify preconditions during CompleteMultipart"](https://github.com/minio/minio/pull/19713), first shipped in [RELEASE.2024-05-27T19-17-46Z](https://github.com/minio/minio/releases/tag/RELEASE.2024-05-27T19-17-46Z) (release notes list the PR).
- **`If-Match` on writes: supported** (PR #16551 above; [MinIO blog](https://blog.min.io/leading-the-way-minios-conditional-write-feature-for-modern-data-workloads/) describes both headers on uploads).
- **Failure mode:** `412 Precondition Failed` per the [MinIO blog](https://blog.min.io/leading-the-way-minios-conditional-write-feature-for-modern-data-workloads/). No documented 409 concurrent-conflict code. Caveats: [PR #21550](https://github.com/minio/minio/pull/21550) (Sep 2025) fixed error returns for conditional writes on non-existing objects; [issue #21727](https://github.com/minio/minio/issues/21727) (multi-ETag `If-None-Match` on reads) remains open.
- **Note:** `minio/minio` was archived (read-only) on 2025-04/2026-04 per GitHub banner observed during research ("archived on April 25" shown on issue pages) — community-edition behavior is frozen; current vendor docs live under MinIO AIStor.

### Tigris

- **PutObject: supported.** [Conditional operations doc](https://www.tigrisdata.com/docs/objects/conditionals/): "Use `If-None-Match: \"*\"` to write only if the object does not already exist. Returns ... `412 Precondition Failed` on a matching PUT." Also `If-Match`, `If-Modified-Since`, `If-Unmodified-Since`; multiple conditions AND together; "Conditional operations like If-Match always evaluate against the latest state" (Tigris is strongly consistent for these).
- **CompleteMultipartUpload: ❌ not documented.** The [conditionals doc](https://www.tigrisdata.com/docs/objects/conditionals/) never mentions multipart; the [multipart doc](https://www.tigrisdata.com/docs/objects/multipart-uploads/) never mentions conditions; the [S3 compatibility table](https://www.tigrisdata.com/docs/api/s3/) lists conditional headers only in connection with object read/write ops, not CompleteMultipartUpload. Treat conditional completion as **unsupported (unverified — absence of documentation, not a documented refusal)**.
- **`If-Match` on writes: supported** ([conditionals doc](https://www.tigrisdata.com/docs/objects/conditionals/)).
- **Failure mode:** `412 Precondition Failed` (writes), `304 Not Modified` (reads). No other codes documented.

## 2. `CopyObject` metadata behavior

Terminology guard: `x-amz-copy-source-if-*` headers condition on the **source** object (copy only if the source is unchanged). A conditional **destination** write (`If-None-Match: *` semantics — "don't clobber the target") is a different feature; the two must not be conflated.

### Metadata directive (COPY vs REPLACE)

- **AWS S3**: [CopyObject API reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html): "When copying an object, you can preserve all metadata (the default) or specify new metadata. If this header isn't specified, `COPY` is the default behavior." `Cache-Control`, `Content-Disposition`, `Content-Type` are request headers on CopyObject (used with REPLACE) and are **system-defined metadata stored with the object** (see §3 metadata table) — with COPY (default) they carry over from the source. Only `x-amz-website-redirect-location` is documented as not copied.
- **Cloudflare R2**: [compatibility matrix](https://developers.cloudflare.com/r2/api/s3/api/) CopyObject: "✅ Operation Metadata: ✅ x-amz-metadata-directive ✅ System Metadata: ✅ Content-Type ✅ Cache-Control ✅ Content-Disposition ✅ Content-Encoding ✅ Content-Language ✅ Expires". The COPY-as-default is standard S3 semantics; R2 does not restate the default (**default unverified but implied by "S3 compatible"**). Note tags don't survive anything on R2 (`x-amz-tagging`/`x-amz-tagging-directive` ❌ — R2 has no object tags).
- **MinIO**: supports `x-amz-metadata-directive` with `REPLACE` per maintainer guidance in [discussion #11545 "How to modify metadata only"](https://github.com/minio/minio/discussions/11545); default-COPY behavior follows S3. No reference-doc page enumerates this (**directive support confirmed via official repo discussions/issues, e.g. [#19450](https://github.com/minio/minio/issues/19450), not docs**).
- **Tigris**: CopyObject is listed as supported in the [S3 compatibility table](https://www.tigrisdata.com/docs/api/s3/) with no per-header detail. Whether `x-amz-metadata-directive` (and directive-default) is honored is **unverified**. (Tigris's own blog describes CopyObject-based rename flows, implying metadata-only copies work, but the directive is not documented.)

### Conditional copy against the DESTINATION

- **AWS S3: ✅ supported** since [Oct 2025](https://aws.amazon.com/about-aws/whats-new/2025/10/amazon-s3-conditional-write-functionality-copy-operations/): "perform conditional copy operations through S3 CopyObject by including either the HTTP if-none-match header to verify object existence or the HTTP if-match header." The [CopyObject reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html) documents plain `If-None-Match` ("Copies the object only if the object key name at the destination does not already exist ... Otherwise ... `412 Precondition Failed` ... If a concurrent operation occurs ... `409 ConditionalRequestConflict`. Expects the '\*' character") and plain `If-Match` on the destination.
- **Cloudflare R2: ✅ supported** via Cloudflare-specific extension headers `cf-copy-destination-if-match / -if-none-match / -if-modified-since / -if-unmodified-since` — "allow the copy operation to be conditional on the state of the destination object"; on failure "the `CopyObject` operation will be rejected with a `412 PreconditionFailed` error code" ([extensions doc](https://developers.cloudflare.com/r2/api/s3/extensions/), shipped [2023-06-16](https://developers.cloudflare.com/r2/platform/release-notes/)). Note: the AWS SDK's plain `If-None-Match` on CopyObject is NOT listed in R2's CopyObject matrix — you must send the `cf-` headers.
- **MinIO: ❌ not documented.** No destination-conditional copy appears in MinIO PRs/releases/docs surveyed (conditional-write PRs #16551/#19682/#19713 cover PUT and CompleteMultipart only). **Unverified whether plain `If-None-Match` on CopyObject is evaluated or ignored — do not rely on it.**
- **Tigris: ⚠️ ambiguous.** The [conditionals doc](https://www.tigrisdata.com/docs/objects/conditionals/) lists CopyObject among conditional operations but only specifies the source-side `X-Amz-Copy-Source-If-*` headers for it; destination-side `If-None-Match` on copy is **unverified**.

### Copy size limits

- **AWS S3**: "You create a copy of your object up to 5 GB in size in a single atomic action ... to copy an object greater than 5 GB, you must use ... UploadPartCopy" ([CopyObject reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html)).
- **Cloudflare R2**: no CopyObject-specific limit documented; the [limits page](https://developers.cloudflare.com/r2/platform/limits/) caps any single-request upload at 4.995 GiB (and notes the cap applies to "copying into a part of a multipart upload"). Treat single-copy as ~5 GiB; larger requires multipart copy (**exact CopyObject cap unverified**).
- **MinIO**: "CopyObject method works but only for files less than 5 GB"; larger objects need the compose/multipart-copy path ([discussion #11545](https://github.com/minio/minio/discussions/11545)).
- **Tigris**: **unverified** — no published copy limit found; objects up to 5 TB via multipart ([multipart doc](https://www.tigrisdata.com/docs/objects/multipart-uploads/): "Standard S3 multipart semantics apply (e.g., large objects up to 5 TB)").

## 3. Natively-emittable response headers (public-read bucket, CDN pointed straight at it)

### AWS S3

- Per-object `Cache-Control`, `Content-Disposition`, `Content-Type` are **user-modifiable system-defined metadata** set at upload and returned as response headers on GET — the [object-metadata doc](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html) table lists all three with "Can user modify the value? Yes".
- **Arbitrary/custom headers: ❌ no native way.** User-defined metadata "must begin with `x-amz-meta-` ... When you retrieve the object using the REST API, this prefix is returned" ([same doc](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html)) — so you can never make raw S3 emit `X-Content-Type-Options: nosniff`. It requires CloudFront (response headers policy / CloudFront Functions / Lambda@Edge) in front.
- **CDN option:** CloudFront managed [`SecurityHeadersPolicy`](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/using-managed-response-headers-policies.html) (ID `67f7725c-6f97-4210-82d7-5512b31e9d03`): "CloudFront adds `X-Content-Type-Options: nosniff` to all responses" (plus `Referrer-Policy`, `Strict-Transport-Security`, `X-Frame-Options: SAMEORIGIN`, `X-XSS-Protection`).

### Cloudflare R2

- Per-object HTTP metadata (`contentType`, `contentDisposition`, `cacheControl`, `contentEncoding`, `contentLanguage`, `cacheExpiry`) is stored at upload and echoed on GET: "Generally, these fields match the HTTP metadata passed when the object was created" ([Workers API reference, `httpMetadata`](https://developers.cloudflare.com/r2/api/workers/workers-api-reference/)); the S3-side PutObject matrix marks all system metadata headers ✅ ([compat matrix](https://developers.cloudflare.com/r2/api/s3/api/)).
- **Arbitrary headers: not per-object.** Injection is a Cloudflare **zone** feature, which requires the bucket behind a **custom domain**: "To use features like WAF custom rules, caching, access controls, or Bot Management, you must configure your bucket behind a custom domain," and "Public access through `r2.dev` subdomains is rate-limited and should only be used for development purposes" ([public buckets doc](https://developers.cloudflare.com/r2/buckets/public-buckets/)). On a custom domain, the Managed Transform ["Add security headers"](https://developers.cloudflare.com/rules/transform/managed-transforms/reference/) adds `x-content-type-options: nosniff` (plus `x-xss-protection`, `x-frame-options: SAMEORIGIN`, `referrer-policy: same-origin`, `expect-ct`); custom Transform Rules can set individual static response headers. On plain `r2.dev` there is no documented header-injection mechanism.

### MinIO

- Serves stored system metadata (`Content-Type` etc.) on GET as part of its S3-compatible API; user metadata comes back prefixed `x-amz-meta-*` (S3 semantics; MinIO publishes no dedicated header-behavior reference — **behavior standard but doc citation unavailable; verify against the deployed release**).
- **Arbitrary response headers: ❌ nothing native.** No MinIO server/bucket configuration for injecting custom response headers (e.g. nosniff) was found in AIStor docs or the community repo. Self-hosted MinIO typically sits behind a reverse proxy, which is where such headers get added (e.g. nginx `add_header`). This is an **absence-of-feature finding**, not a cited limitation.

### Tigris

- Per-bucket default `Cache-Control`: "This lets you set the default Cache-Control header for objects in the bucket" ([bucket settings](https://www.tigrisdata.com/docs/buckets/settings/)); per-object system metadata upload follows the S3 API ([S3 compatibility](https://www.tigrisdata.com/docs/api/s3/)).
- **Arbitrary headers: ✅ native, per-bucket.** The bucket settings "Additional Headers" section: "You can configure additional headers for objects stored in your buckets ... set the `X-Content-Type-Options` header to `nosniff`" ([bucket settings](https://www.tigrisdata.com/docs/buckets/settings/)). Tigris is the only one of the four where the store itself can emit nosniff with no CDN/proxy in front. Custom domains are supported via CNAME ([same page](https://www.tigrisdata.com/docs/buckets/settings/)).

## 4. Lifecycle parity

### Prefix-scoped expiration (`Filter.Prefix` + `Expiration.Days`)

- **AWS S3: ✅.** Rules filter by key prefix (also tags, object size); `Expiration` with `Days`; up to 1,000 rules/bucket ([lifecycle elements doc](https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html)).
- **R2: ✅.** Prefix conditions (`"Prefix": "logs/"`) and age in days; 1,000-rule maximum ([object lifecycles doc](https://developers.cloudflare.com/r2/buckets/object-lifecycles/)).
- **MinIO: ✅.** `mc ilm rule add` has `--prefix` ("Restrict the management rule to a specific object prefix") and `--expire-days` ([mc ilm rule add reference](https://docs.min.io/aistor/reference/cli/mc-ilm-rule/mc-ilm-rule-add/); overview: [AIStor object lifecycle management](https://docs.min.io/enterprise/aistor-object-store/administration/object-lifecycle-management/)). Tag filters also available (`--tags`).
- **Tigris: ✅, recent.** "Each rule can be scoped to a key prefix using `Filter.Prefix`"; expiration after N days or on a date; up to 10 rules/bucket; configured via the standard S3 `put-bucket-lifecycle-configuration` API, CLI, or dashboard ([object expiration doc](https://www.tigrisdata.com/docs/buckets/objects-expiration/); `PutBucketLifecycleConfiguration` ✅ in the [S3 compatibility table](https://www.tigrisdata.com/docs/api/s3/)). **Caveat:** multiple rules + prefix filters shipped only on [2026-05-26](https://www.tigrisdata.com/blog/lifecycle-rules-prefix-filters/) — "Before this update, you got one lifecycle rule per bucket, applied to every object in it."

### `AbortIncompleteMultipartUpload`

- **AWS S3: ✅.** `AbortIncompleteMultipartUpload` action with days-after-initiation; cannot be combined with a tag filter ([lifecycle elements doc](https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html); how-to: [abort-incomplete-MPU lifecycle config](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpu-abort-incomplete-mpu-lifecycle-config.html)).
- **R2: ✅.** "Example: abort all incomplete multipart uploads after a week" with `"AbortIncompleteMultipartUpload": { "DaysAfterInitiation": 7 }` ([object lifecycles doc](https://developers.cloudflare.com/r2/buckets/object-lifecycles/)).
- **MinIO: ❌ in ILM.** Source of truth, [`internal/bucket/lifecycle/rule.go`](https://github.com/minio/minio/blob/master/internal/bucket/lifecycle/rule.go): `// FIXME: add a type to catch unsupported AbortIncompleteMultipartUpload` — the element is not implemented (and `mc ilm rule add` has no corresponding flag). Instead, the server auto-expires stale multipart uploads via `api.stale_uploads_expiry` / `MINIO_API_STALE_UPLOADS_EXPIRY`, default **24h**, swept per `stale_uploads_cleanup_interval` ([config reference in-repo](https://github.com/minio/minio/blob/master/docs/config/README.md)). Net effect: cleanup exists but is server-global config, not per-bucket/per-prefix lifecycle policy.
- **Tigris: ❌ not documented.** Neither the [object expiration doc](https://www.tigrisdata.com/docs/buckets/objects-expiration/), the [May 2026 lifecycle blog](https://www.tigrisdata.com/blog/lifecycle-rules-prefix-filters/), the [bucket settings page](https://www.tigrisdata.com/docs/buckets/settings/), nor the [multipart doc](https://www.tigrisdata.com/docs/objects/multipart-uploads/) mention incomplete-multipart cleanup. Whether an `AbortIncompleteMultipartUpload` element in a `PutBucketLifecycleConfiguration` body is honored, ignored, or rejected is **unverified — assume abandoned MPUs accrue until aborted client-side (`AbortMultipartUpload` is supported per the [S3 compatibility table](https://www.tigrisdata.com/docs/api/s3/))**.

### R2 tag-filter constraint

Still holds. The [R2 lifecycle doc](https://developers.cloudflare.com/r2/buckets/object-lifecycles/) documents only prefix and age/date conditions — no tag filters. Consistently, R2 doesn't support object tagging at all (`x-amz-tagging` ❌ on PutObject/CopyObject in the [compat matrix](https://developers.cloudflare.com/r2/api/s3/api/)).

### Granularity

- **S3:** days (integer); "rounding up the resulting time to the next day at midnight UTC" ([lifecycle elements doc](https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html)). Date-based rules are also midnight UTC. Nothing finer.
- **R2:** days (`Days`, `DaysAfterInitiation`) in all documented conditions ([lifecycle doc](https://developers.cloudflare.com/r2/buckets/object-lifecycles/)).
- **MinIO:** ILM in days; the scanner is lazy ("may ... not detect an object as eligible ... until after the lifecycle rule period has passed", [AIStor ILM doc](https://docs.min.io/enterprise/aistor-object-store/administration/object-lifecycle-management/)). The stale-upload cleanup config is a Go duration (default `24h`), so sub-day is possible there — but only for MPU garbage, not object expiration.
- **Tigris:** days/date; "Tigris always rounds the expiration time to UTC midnight" ([object expiration doc](https://www.tigrisdata.com/docs/buckets/objects-expiration/)). Separately, a bucket-default TTL setting exists ([bucket settings](https://www.tigrisdata.com/docs/buckets/settings/)).

## Implications

- **Conditional completion can't be mandated uniformly.** `If-None-Match: *` on CompleteMultipartUpload is solid on S3 (GA 2024-08) and MinIO (≥ RELEASE.2024-05-27T19-17-46Z), murky on R2 (condition attaches at Create; publish-time failure auto-aborts the upload; current compat matrix silent), and undocumented on Tigris. Options: (a) require it only where verified and degrade to unconditional completion elsewhere, (b) attach the condition at CreateMultipartUpload for R2 and treat Tigris MPUs as unguarded, or (c) drop the belt for MPUs entirely — random ids already make collision the pathological case.
- **Conditional single-part PUT is safe to require everywhere** — all four support `If-None-Match: *` on PutObject with a 412 on conflict. If the belt matters, biasing the release path toward single-part writes maximizes portability.
- **A "release copy" (staging → final) can be destination-guarded on S3 (plain `If-None-Match`, GA Oct 2025) and R2 (`cf-copy-destination-if-none-match` extension header — different header!), but not verifiably on MinIO or Tigris.** Source-conditioned `x-amz-copy-source-if-*` headers exist on all four and are NOT a substitute. Copy also caps at ~5 GB single-shot on S3/R2/MinIO, so a copy-based release needs an UploadPartCopy path for large files anyway.
- **Copy preserves `Content-Type`/`Content-Disposition`/`Cache-Control` by default (COPY directive) on S3 and R2 per docs; MinIO follows S3 semantics per maintainers; Tigris is unverified** — if release-by-copy is chosen, the spec should either re-send metadata with `REPLACE` (belt-and-braces, works everywhere) or gate on per-backend verification.
- **nosniff cannot be a store-level requirement across backends.** Only Tigris emits `X-Content-Type-Options: nosniff` natively (per-bucket Additional Headers). S3 needs CloudFront (managed SecurityHeadersPolicy), R2 needs a custom domain + zone Managed Transform/Transform Rule (not available on bare r2.dev), MinIO needs a reverse proxy. Wording it as a **deployment requirement** ("the public serving layer MUST add nosniff") is the only formulation all four can satisfy; per-object `Content-Type`/`Content-Disposition`/`Cache-Control` CAN be a store-level requirement everywhere.
- **Staging-prefix hygiene diverges on MPUs:** prefix-scoped expiration of staged objects works on all four (Tigris only since 2026-05, max 10 rules), but abandoned multipart uploads are only lifecycle-managed on S3 and R2; MinIO relies on a server-global 24h auto-purge and Tigris has no documented cleanup — the spec should not assume `AbortIncompleteMultipartUpload` exists, and may need a client/ops-side sweeper (`ListMultipartUploads` + `AbortMultipartUpload`) as the portable fallback.
