// Where a core keeps the bytes of large values once they settle.
import type { S3BlobStore, VercelBlobStore } from "jazz-wasm/durable-object/blob-stores.js";
export type { S3BlobStore, VercelBlobStore } from "jazz-wasm/durable-object/blob-stores.js";

/**
 * Where a store keeps the bytes of large values once they settle. Rows and
 * the chunk index stay in the database either way.
 * - `files()`: one file per chunk in a directory.
 * - `s3()`: one object per chunk in an S3-compatible bucket.
 * - `vercelBlob()`: one private blob per chunk in a Vercel Blob store.
 * - `"sqlite"`: inside the database file. Works, but the file grows with
 *   every large value, so it is not recommended.
 */
export type BlobStore =
  | { readonly kind: "files"; readonly dir?: string }
  | S3BlobStore
  | VercelBlobStore
  | "sqlite";

export function validateBlobCredentials(blobs: BlobStore): void {
  if (blobs === "sqlite" || blobs.kind === "files" || blobs.credentials === undefined) return;
  if (blobs.kind === "vercel-blob") {
    requireCredential("readWriteToken", blobs.credentials?.readWriteToken);
    return;
  }
  requireCredential("accessKeyId", blobs.credentials?.accessKeyId);
  requireCredential("secretAccessKey", blobs.credentials?.secretAccessKey);
  if (blobs.credentials.sessionToken !== undefined) {
    requireCredential("sessionToken", blobs.credentials.sessionToken);
  }
}

function requireCredential(field: string, value: unknown) {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`blobs.credentials.${field} must be a nonempty string`);
  }
}

/**
 * Large-value bytes as files in `dir`, by default `blobs/` in the store's
 * directory. A store keeps the blob store it was first opened with; one that
 * started on `"sqlite"` may move to `files()` or `s3()` later.
 */
export function files(options: { dir?: string } = {}): Extract<BlobStore, { kind: "files" }> {
  return { kind: "files", ...(options.dir === undefined ? {} : { dir: options.dir }) };
}

/**
 * Large-value bytes in an S3-compatible bucket: AWS S3 (on Vercel, the
 * Marketplace's AWS integration sets `S3_BUCKET_NAME`, `AWS_REGION` and
 * `AWS_ROLE_ARN`, and the core assumes that role with the function's own
 * OIDC token), MinIO, or Cloudflare R2 through R2's S3 API (an `endpoint`
 * of `https://<account id>.r2.cloudflarestorage.com`, `region: "auto"` and an
 * R2 API token's keys). On a Durable Object, configure the core Worker with
 * this helper or use a direct R2 binding via `r2(bucket)`. Native SQLite finds
 * credentials as the AWS SDKs do: `AWS_ACCESS_KEY_ID` and
 * `AWS_SECRET_ACCESS_KEY`, then `AWS_ROLE_ARN` with
 * `AWS_WEB_IDENTITY_TOKEN_FILE`, then the role ECS, Fargate, EKS Pod
 * Identity or Lambda attaches to the container, then an EC2 instance
 * profile (IMDSv2). Every chunk is put with its SHA-256, so
 * the bucket itself rejects corrupted uploads.
 *
 * Each store keeps its objects under a prefix of its own (it deletes the
 * chunks it no longer needs), so one bucket serves many stores. A
 * `vercel()` core also claims the bucket for its Vercel environment, and a
 * core of another environment is refused there: give production and
 * previews a bucket each.
 */
export function s3(options: Omit<S3BlobStore, "kind"> = {}): S3BlobStore {
  const set = Object.entries(options).filter(([, value]) => value !== undefined);
  return { kind: "s3", ...Object.fromEntries(set) };
}

/**
 * Large-value bytes in a private Vercel Blob store. With `sqlite()` or `vercel()`, supply
 * `credentials: { readWriteToken }` (native SQLite also reads `BLOB_READ_WRITE_TOKEN`).
 * On Vercel, omitting credentials uses the connected store and authenticates
 * with the function's workload OIDC token. Browsers send and fetch large values
 * through the app's `/jazz` route. The core reads and writes the blobs.
 * Blobs are private, and each is checked against its SHA-256 when read.
 *
 * One Blob store serves one Vercel environment: any function of the
 * environment's OIDC token reaches the whole store, so the first core
 * claims it for its environment, and a core of another environment is
 * refused. Connect a separate Blob store to production and to previews.
 */
export function vercelBlob(options: Omit<VercelBlobStore, "kind"> = {}): VercelBlobStore {
  const set = Object.entries(options).filter(([, value]) => value !== undefined);
  return { kind: "vercel-blob", ...Object.fromEntries(set) };
}

/** What a `vercel()` core is started with for `vercelBlob()`. */
export interface VercelBlob {
  useReadWriteToken?: boolean;
  storeId: string;
  prefix?: string;
}

/**
 * The Blob store connected to the project for OIDC: `BLOB_STORE_ID`. A
 * connection with only `BLOB_READ_WRITE_TOKEN` does not count: the core
 * authenticates with the function's OIDC token alone, and the token never
 * reaches it.
 */
export function connectedBlobStore(env: Record<string, string | undefined>): string | undefined {
  return env.BLOB_STORE_ID?.replace(/^store_/, "") || undefined;
}

/** Resolve `vercelBlob()` for a `vercel()` core from its options and the function's environment. */
export function resolveVercelBlob(
  blobs: VercelBlobStore,
  env: Record<string, string | undefined>,
): VercelBlob {
  const tokenStore = /^vercel_blob_rw_([^_]+)_/.exec(blobs.credentials?.readWriteToken ?? "")?.[1];
  const storeId = blobs.storeId?.replace(/^store_/, "") ?? tokenStore ?? connectedBlobStore(env);
  if (!storeId) {
    throw new Error(
      "vercel({ blobs: vercelBlob() }): BLOB_STORE_ID is not set. Connect a Blob store to this project (Storage tab in the Vercel dashboard), or pass vercelBlob({ storeId }).",
    );
  }
  if (!/^[a-zA-Z0-9-]+$/.test(storeId)) throw new Error("vercelBlob(): invalid storeId");
  return {
    storeId,
    ...(blobs.credentials && { useReadWriteToken: true }),
    ...(blobs.prefix === undefined ? {} : { prefix: blobs.prefix }),
  };
}

/** The AWS region a Vercel region runs in. */
export function awsRegionOfVercel(region: string): string | undefined {
  return VERCEL_TO_AWS[region];
}

const VERCEL_TO_AWS: Record<string, string> = {
  iad1: "us-east-1",
  cle1: "us-east-2",
  sfo1: "us-west-1",
  pdx1: "us-west-2",
  yul1: "ca-central-1",
  gru1: "sa-east-1",
  dub1: "eu-west-1",
  lhr1: "eu-west-2",
  cdg1: "eu-west-3",
  fra1: "eu-central-1",
  arn1: "eu-north-1",
  cpt1: "af-south-1",
  dxb1: "me-central-1",
  bom1: "ap-south-1",
  sin1: "ap-southeast-1",
  syd1: "ap-southeast-2",
  hkg1: "ap-east-1",
  hnd1: "ap-northeast-1",
  icn1: "ap-northeast-2",
  kix1: "ap-northeast-3",
};

/** What a `vercel()` core is started with for `s3()`. */
export interface VercelS3 {
  bucket: string;
  region: string;
  roleArn?: string;
  endpoint?: string;
  prefix?: string;
}

/**
 * Resolve `s3()` for a `vercel()` core from its options and the function's
 * environment (the Marketplace's AWS integration), warning when the bucket
 * is in another region than the Sandbox.
 *
 * Vercel sets `AWS_REGION` to the function's own region unless the project
 * sets it, so without `s3({ region })` the region is only a first guess:
 * the core signs for the region the bucket answers it is in.
 */
export function resolveVercelS3(
  blobs: S3BlobStore,
  sandboxRegion: string,
  env: Record<string, string | undefined>,
  warn: (message: string) => void = console.warn,
): VercelS3 {
  const bucket = blobs.bucket ?? env.S3_BUCKET_NAME;
  const region = blobs.region ?? env.AWS_REGION ?? (blobs.endpoint ? "auto" : undefined);
  const roleArn = blobs.credentials ? undefined : env.AWS_ROLE_ARN;
  const missing = [
    bucket ? null : "S3_BUCKET_NAME (or s3({ bucket }))",
    region ? null : "AWS_REGION (or s3({ region }))",
    roleArn || blobs.credentials ? null : "AWS_ROLE_ARN",
  ].filter((name): name is string => name !== null);
  if (missing.length > 0) {
    throw new Error(
      `vercel({ blobs: s3() }): ${missing.join(", ")} not set. Connect an AWS S3 bucket to this project through the Vercel Marketplace, which sets them, or set them yourself.`,
    );
  }
  const aws = awsRegionOfVercel(sandboxRegion);
  if (!blobs.endpoint && aws && aws !== region) {
    warn(
      `vercel({ blobs: s3() }): the bucket ${bucket} is in ${region}, but the core runs in ${sandboxRegion} (${aws}). Every chunk crosses regions; put the bucket in ${aws}.`,
    );
  }
  return {
    bucket: bucket!,
    region: region!,
    ...(roleArn && { roleArn }),
    ...(blobs.endpoint === undefined ? {} : { endpoint: blobs.endpoint }),
    ...(blobs.prefix === undefined ? {} : { prefix: blobs.prefix }),
  };
}
