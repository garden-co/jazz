/**
 * Application context for Jazz client connections.
 */

import type { StorageDriver, Schema } from "../drivers/types.js";

/**
 * Configuration for connecting to Jazz.
 */
export interface AppContext {
  /** Application identifier (used for isolation) */
  appId: string;

  /** Optional client ID (generated if not provided) */
  clientId?: string;

  /** Schema definition */
  schema: Schema;

  /** Optional server URL for sync */
  serverUrl?: string;

  /** Storage driver implementation */
  driver: StorageDriver;

  /** Environment (e.g., "dev", "prod") */
  env?: string;

  /** User branch name (default: "main") */
  userBranch?: string;
}
