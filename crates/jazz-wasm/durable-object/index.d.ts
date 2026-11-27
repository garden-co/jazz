import type { S3BlobStore, VercelBlobStore } from "./blob-stores.js";
import type { DurableObject } from "cloudflare:workers";

/** The bindings the Jazz core's Worker and Durable Object read. */
export interface JazzCoreEnv {
  /** The Durable Object namespace of `JazzCore` (SQLite-backed). */
  JAZZ: DurableObjectNamespace;
  /**
   * Admits the app's backend; at least 32 characters, stable across
   * deployments. Optional for a backend in the same Worker: without it the
   * object makes one on its first start and hands it out only over its
   * binding (`coreCredential`).
   */
  JAZZ_CORE_SECRET?: string;
  /** A public schema's JSON, served until one is published (optional). */
  JAZZ_SCHEMA?: string;
  /**
   * An R2 bucket available to select with r2(). With explicit SQLite storage, bytes stay
   * in the object's SQLite. Objects go under `JAZZ_BLOBS_PREFIX`, claimed for
   * this app by a `.jazz-owner` object.
   */
  JAZZ_BLOBS?: R2Bucket;
  /** Where in `JAZZ_BLOBS` this app's objects go; defaults to `jazz/<app id>/`. */
  JAZZ_BLOBS_PREFIX?: string;
  /** Defaults for s3() when the option is omitted. */
  S3_BUCKET_NAME?: string;
  AWS_REGION?: string;
  AWS_ACCESS_KEY_ID?: string;
  AWS_SECRET_ACCESS_KEY?: string;
  AWS_SESSION_TOKEN?: string;
  /** Defaults for vercelBlob() when the option is omitted. */
  BLOB_STORE_ID?: string;
  BLOB_READ_WRITE_TOKEN?: string;
}

/** Select an existing R2 bucket for the core's large values. */
export declare function r2(bucket: R2Bucket): { kind: "r2"; bucket: R2Bucket };

export declare function createJazzCore<Env extends JazzCoreEnv = JazzCoreEnv>(options: {
  blobs:
    | "sqlite"
    | ((env: Env) => "sqlite" | ReturnType<typeof r2> | S3BlobStore | VercelBlobStore);
}): {
  new (...args: ConstructorParameters<typeof DurableObject<Env>>): DurableObject<Env> & {
    /** Available only over the object binding. */
    coreCredential(): string;
  };
};

/** The Worker's fetch: answers /health locally and forwards core requests to the object. */
export declare function routeJazzRequest(request: Request, env: JazzCoreEnv): Promise<Response>;
