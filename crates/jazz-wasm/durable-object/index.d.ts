import type { DurableObject } from "cloudflare:workers";

/** The bindings the Jazz core's Worker and Durable Object read. */
export interface JazzCoreEnv {
  /** The Durable Object namespace of `JazzCore` (SQLite-backed). */
  JAZZ: DurableObjectNamespace;
  /** Admits the app's backend; at least 32 characters, stable across deployments. */
  JAZZ_CORE_SECRET: string;
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
}

/** Select an existing R2 bucket for the core's large values. */
export declare function r2(bucket: R2Bucket): { kind: "r2"; bucket: R2Bucket };

export declare function createJazzCore<Env extends JazzCoreEnv = JazzCoreEnv>(options: {
  blobs: "sqlite" | ((env: Env) => "sqlite" | ReturnType<typeof r2>);
}): typeof DurableObject<Env>;

/** The Worker's fetch: answers /health locally and forwards core requests to the object. */
export declare function routeJazzRequest(request: Request, env: JazzCoreEnv): Promise<Response>;
