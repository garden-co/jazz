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
   * When set, Jazz derives:
   * - `jazz_wasm_bg.wasm`
   * - `worker/jazz-worker.js`
   */
  baseUrl?: string;

  /** Explicit URL for the WASM binary. Overrides `baseUrl`. */
  wasmUrl?: string;

  /** Explicit URL for the worker entry script. Overrides `baseUrl`. */
  workerUrl?: string;

  /** Explicit in-memory WASM source bytes. Overrides URL-based resolution. */
  wasmSource?: BufferSource;

  /** Explicit compiled WASM module. Highest-precedence bootstrap input. */
  wasmModule?: WebAssembly.Module;
}

/**
 * Session context for policy evaluation.
 */
export interface Session {
  /** User identifier */
  user_id: string;
  /** Additional claims (roles, teams, etc.) */
  claims: Record<string, unknown>;
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
  /** Optional route prefix for multi-tenant servers (e.g. `/apps/<appId>`). */
  serverPathPrefix?: string;

  /** Optional runtime source overrides for WASM and worker loading. */
  runtimeSources?: RuntimeSourcesConfig;

  /** Storage driver mode (defaults to persistent). */
  driver?: StorageDriver;

  /** Environment (e.g., "dev", "prod") */
  env?: string;

  /** User branch name (default: "main") */
  userBranch?: string;

  // Authentication fields

  /**
   * JWT token for frontend authentication.
   * Sent as `Authorization: Bearer <token>`.
   */
  jwtToken?: string;

  /**
   * Backend secret for session impersonation.
   * Enables `forSession()` to act as any user.
   */
  backendSecret?: string;

  /**
   * Admin secret for schema/policy sync.
   * Required to sync catalogue objects.
   */
  adminSecret?: string;

  /**
   * Durability tier identity for this node (or identities for multi-role nodes).
   * Set for server nodes to enable durability notifications.
   * Clients typically leave this undefined.
   */
  tier?: "worker" | "edge" | "global" | Array<"worker" | "edge" | "global">;

  /**
   * Default durability tier for reads and writes when no explicit tier is provided.
   */
  defaultDurabilityTier?: "worker" | "edge" | "global";
}
