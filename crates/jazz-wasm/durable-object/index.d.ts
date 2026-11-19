import type { DurableObject } from "cloudflare:workers";

/** The bindings the Jazz core's Worker and Durable Object read. */
export interface JazzCoreEnv {
  /** The Durable Object namespace of `JazzCore` (SQLite-backed). */
  JAZZ: DurableObjectNamespace;
  /** Admits the app's backend; at least 32 characters, stable across deployments. */
  JAZZ_CORE_SECRET: string;
  /** A public schema's JSON, served until one is published (optional). */
  JAZZ_SCHEMA?: string;
}

/** The Durable Object class holding an app's Jazz core. */
export declare const JazzCore: typeof DurableObject<JazzCoreEnv>;

/** `JazzCore`, for callers that prefer a factory. */
export declare function createJazzCore(): typeof JazzCore;

/** The Worker's fetch: answers /health locally and forwards core requests to the object. */
export declare function routeJazzRequest(request: Request, env: JazzCoreEnv): Promise<Response>;
