/**
 * Application context for Jazz client connections.
 */

import type { StorageDriver, WasmSchema } from "../drivers/types.js";

/**
 * Runtime source overrides for Jazz WASM and worker startup.
 *
 * These are primarily used by browser and edge-style runtimes.
 */
export interface RuntimeSourcesConfig {
  /**
   * Base URL for Jazz runtime files.
   *
   * When set, Jazz derives `jazz_wasm_bg.wasm` and the browser broker worker.
   */
  baseUrl?: string;

  /** Explicit URL for the WASM binary. Overrides `baseUrl`. */
  wasmUrl?: string;

  /** Explicit URL for the browser broker SharedWorker entry script. Overrides `baseUrl`. */
  brokerWorkerUrl?: string;

  /**
   * Immutable version for browser runtime URL overrides.
   *
   * When browser worker assets are configured with `baseUrl`, `wasmUrl`, or
   * `brokerWorkerUrl`, this value is added to their URLs and must change with
   * the deployed WASM/worker build. This prevents a long-lived SharedWorker
   * from silently reusing bytes from an older deployment at the same URL.
   */
  wasmVersion?: string;

  /** Explicit in-memory WASM source bytes. Overrides URL-based resolution. */
  wasmSource?: BufferSource;

  /** Explicit compiled WASM module. Highest-precedence bootstrap input. */
  wasmModule?: WebAssembly.Module;

  /** @internal Stable identity for an in-memory WASM input sent to a worker. */
  workerWasmAssetIdentity?: string;

  /** @internal Pre-attached worker peer used by the same-origin inspector. */
  browserWorkerPort?: MessagePort;
}

/**
 * Mirrors the Rust `AuthMode` enum in `crates/jazz-tools/src/public_schema.rs`.
 */
export type AuthMode = "external" | "local-first" | "anonymous";

/**
 * Session context for policy evaluation.
 */
export interface Session {
  /** Validated JWT issuer (`iss`). */
  issuer: string;
  /** User identifier */
  user_id: string;
  /** User-defined claims (roles, teams, etc.) */
  claims: Record<string, unknown>;
  /** Auth mode — derived from the JWT's `iss` claim. */
  authMode: AuthMode;
}

/**
 * A session that Jazz has admitted for use by a client binding.
 *
 * `user` is the opaque, canonical JSON encoding of the admitted JWT's exact
 * `[iss, sub]` pair. Policies see it as `session.user`, and Jazz records the
 * same identity in `$createdBy` and
 * `$updatedBy`. It is deliberately distinct from `user_id`: the latter is the
 * provider-controlled raw JWT `sub`.
 *
 * This is not a user-row reference, display name, or raw `sub`. Applications
 * must obtain it from an admitted session rather than constructing it
 * themselves. Local interning is an implementation detail and is never
 * exposed here.
 */
export interface PublicSession extends Session {
  readonly user: string;
}

/**
 * Configuration for connecting to Jazz.
 */
export interface AppContext {
  /** Application identifier (used for isolation) */
  appId: string;

  /** Optional client ID (generated if not provided) */
  clientId?: string;

  /** Schema definition */
  schema: WasmSchema;

  /** Optional server URL for sync */
  serverUrl?: string;

  /** Optional runtime source overrides for WASM loading. */
  runtimeSources?: RuntimeSourcesConfig;

  /** Storage driver mode (defaults to persistent). */
  driver?: StorageDriver;

  /** Environment (e.g., "dev", "prod") */
  env?: string;

  // Authentication fields

  /**
   * JWT token for frontend authentication.
   * Sent as `Authorization: Bearer <token>`.
   */
  jwtToken?: string;

  /**
   * Mirrored session used for local permission evaluation when auth rides on
   * an HttpOnly cookie instead of a JS-readable bearer token.
   */
  cookieSession?: Session;

  /** @internal Session produced by a first-party reserved-issuer auth flow. */
  trustedReservedSession?: Session;

  /**
   * Backend secret for session impersonation.
   * Enables backend session-scoped operations as any user.
   */
  backendSecret?: string;

  /**
   * Admin secret for privileged sync and `/admin/*` catalogue endpoints.
   * On `/ws`, a valid admin secret authenticates this client as the backend.
   */
  adminSecret?: string;

  /**
   * Durability tier identity for this node (or identities for multi-role nodes).
   * Set for server nodes to enable durability notifications.
   * Clients typically leave this undefined.
   */
  tier?: "local" | "edge" | "global" | Array<"local" | "edge" | "global">;

  /**
   * Default durability tier for reads and writes when no explicit tier is provided.
   */
  defaultDurabilityTier?: "local" | "edge" | "global";
}
