// Where a core keeps the bytes of large values once they settle.

/** An S3-compatible bucket for large-value bytes; see {@link s3}. */
export interface S3BlobStore {
  readonly kind: "s3";
  /** Defaults to `S3_BUCKET_NAME`. */
  readonly bucket?: string;
  /** Defaults to `AWS_REGION`; `auto` for R2. */
  readonly region?: string;
  /** An S3-compatible endpoint other than AWS, e.g. R2's. */
  readonly endpoint?: string;
  /**
   * Prepended to every object name. Defaults to `jazz/<appId>/<store id>/`,
   * one per store, so stores sharing a bucket (preview and production) never
   * share objects. A prefix is claimed by the first store that uses it, and
   * any other store is refused there.
   */
  readonly prefix?: string;
}

/**
 * Where a store keeps the bytes of large values once they settle. Rows and
 * the chunk index stay in the database either way.
 * - `files()`: one file per chunk in a directory (the default for `sqlite()`).
 * - `s3()`: one object per chunk in an S3-compatible bucket.
 * - `"sqlite"`: inside the database file. Works, but the file grows with
 *   every large value, so it is not recommended.
 */
export type BlobStore = { readonly kind: "files"; readonly dir?: string } | S3BlobStore | "sqlite";

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
 * OIDC token), Cloudflare R2 or MinIO. Elsewhere the core reads
 * `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, or `AWS_ROLE_ARN` with
 * `AWS_WEB_IDENTITY_TOKEN_FILE`. Every chunk is put with its SHA-256, so
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

/** A Vercel Blob store for large-value bytes; see {@link vercelBlob}. */
export interface VercelBlobStore {
  readonly kind: "vercel-blob";
  /** Defaults to `BLOB_STORE_ID`, set when a Blob store is connected to the project. */
  readonly storeId?: string;
  /**
   * Prepended to every blob's pathname. Defaults to `jazz/<appId>/<store id>/`,
   * one per core, so preview and production cores sharing a Blob store never
   * share blobs. A prefix is claimed by the first core that uses it.
   */
  readonly prefix?: string;
}

/**
 * Large-value bytes in a Vercel Blob store, for a `vercel()` core (and its
 * default whenever `BLOB_STORE_ID` is set). Browsers send and fetch large
 * values through the app's `/jazz` route; the core writes and reads the
 * blobs itself, so nothing but connecting a Blob store is needed. It
 * authenticates with the function's own OIDC token, which reaches it with
 * each request, and the Sandbox holds only the store's id.
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
  const storeId = blobs.storeId?.replace(/^store_/, "") ?? connectedBlobStore(env);
  if (!storeId) {
    throw new Error(
      "vercel({ blobs: vercelBlob() }): BLOB_STORE_ID is not set. Connect a Blob store to this project (Storage tab in the Vercel dashboard), or pass vercelBlob({ storeId }).",
    );
  }
  return { storeId, ...(blobs.prefix === undefined ? {} : { prefix: blobs.prefix }) };
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
  roleArn: string;
  endpoint?: string;
  prefix?: string;
}

/**
 * Resolve `s3()` for a `vercel()` core from its options and the function's
 * environment (the Marketplace's AWS integration), warning when the bucket
 * is in another region than the Sandbox.
 */
export function resolveVercelS3(
  blobs: S3BlobStore,
  sandboxRegion: string,
  env: Record<string, string | undefined>,
  warn: (message: string) => void = console.warn,
): VercelS3 {
  const bucket = blobs.bucket ?? env.S3_BUCKET_NAME;
  const region = blobs.region ?? env.AWS_REGION ?? (blobs.endpoint ? "auto" : undefined);
  const roleArn = env.AWS_ROLE_ARN;
  const missing = [
    bucket ? null : "S3_BUCKET_NAME (or s3({ bucket }))",
    region ? null : "AWS_REGION (or s3({ region }))",
    roleArn ? null : "AWS_ROLE_ARN",
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
    roleArn: roleArn!,
    ...(blobs.endpoint === undefined ? {} : { endpoint: blobs.endpoint }),
    ...(blobs.prefix === undefined ? {} : { prefix: blobs.prefix }),
  };
}
