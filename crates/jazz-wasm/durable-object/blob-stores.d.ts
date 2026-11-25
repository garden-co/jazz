/** An S3-compatible bucket for large-value bytes. */
export interface S3BlobStore {
  readonly kind: "s3";
  /** Explicit keys take precedence over the host's AWS credentials. */
  readonly credentials?: {
    readonly accessKeyId: string;
    readonly secretAccessKey: string;
    readonly sessionToken?: string;
  };
  /** Defaults to `S3_BUCKET_NAME`. */
  readonly bucket?: string;
  /** Defaults to `AWS_REGION`; `auto` for R2. */
  readonly region?: string;
  /** An S3-compatible endpoint other than AWS, e.g. R2's. */
  readonly endpoint?: string;
  /**
   * Prepended to every object name. Defaults to `jazz/<store id>/`,
   * one per store, so stores sharing a bucket (a preview core per branch)
   * never share objects. A prefix is claimed by the first store that uses
   * it, and any other store is refused there. A `vercel()` core also claims
   * the whole bucket for its environment, so production and previews each
   * need a bucket of their own.
   */
  readonly prefix?: string;
}

/** A Vercel Blob store for large-value bytes. */
export interface VercelBlobStore {
  readonly kind: "vercel-blob";
  /** Use this token instead of ambient credentials or Sandbox workload OIDC. */
  readonly credentials?: { readonly readWriteToken: string };
  /** Defaults to `BLOB_STORE_ID`, set when a Blob store is connected to the project. */
  readonly storeId?: string;
  /**
   * Prepended to every blob's pathname. Defaults to `jazz/<store id>/`,
   * one per core, so the preview cores of different branches sharing a Blob
   * store never share blobs. A prefix is claimed by the first core that uses
   * it. On Vercel, the Blob store itself serves one environment: production and
   * previews each need their own.
   */
  readonly prefix?: string;
}

/** An opened DO transport. Keys include the prefix claimed by the host. */
export type OpenedBlobStore = {
  readonly prefixOption?: string;
  /** Return null for a missing object and reject bodies larger than limit. */
  get(key: string, limit: number): Promise<Uint8Array | null>;
  /** Create the ownership marker with overwrites disabled. The caller reads it back. */
  createOwner(key: string, bytes: Uint8Array): Promise<unknown>;
  /** Resolve only after storage accepts these bytes under their SHA-256. */
  put(key: string, bytes: Uint8Array, sha256: string): Promise<unknown>;
} & (
  | { deleteMany(keys: string[]): Promise<void>; remove?: never }
  | { remove(key: string): Promise<void>; deleteMany?: never }
);
