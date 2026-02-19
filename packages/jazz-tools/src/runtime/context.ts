/**
 * Application context for Jazz client connections.
 */

import type { StorageDriver, WasmSchema } from "../drivers/types.js";

/** Local auth mode for client-generated identities. */
export type LocalAuthMode = "anonymous" | "demo";

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

  /** Storage driver implementation (optional — storage is in-memory by default) */
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
   * Local auth mode for client-generated identities.
   * Sent as `X-Jazz-Local-Mode`.
   */
  localAuthMode?: LocalAuthMode;

  /**
   * Client-generated auth token for anonymous/demo identity.
   * Sent as `X-Jazz-Local-Token`.
   */
  localAuthToken?: string;

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
   * Persistence tier for this node.
   * Set for server nodes to enable ack emission.
   * Clients typically leave this undefined.
   */
  tier?: "worker" | "edge" | "core";
}
