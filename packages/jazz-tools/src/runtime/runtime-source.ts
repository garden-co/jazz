import type { WasmSchema } from "../drivers/types.js";
import type { JazzClient } from "./client.js";
import type { DbConfig } from "./db.js";
import type { AuthFailureReason } from "./auth-state.js";

export interface RuntimeTokenOptions {
  secret: string;
  audience: string;
  ttlSeconds: number;
  nowSeconds: bigint;
}

export interface RuntimeClientContext<RuntimeConfig extends DbConfig = DbConfig> {
  config: RuntimeConfig;
  schema: WasmSchema;
  onAuthFailure: (reason: AuthFailureReason) => void;
  /**
   * Browser-only identity leased by the durable SharedWorker before the
   * synchronous foreground client is constructed.
   */
  foregroundNodeLease?: ForegroundNodeLease;
}

/** Internal foreground TxId lease, never exposed as application API. */
export interface ForegroundNodeLease {
  readonly node: Uint8Array;
  readonly confirmedTxTime: bigint;
  /** Atomically persists runtime high-water and makes this node reusable. */
  returnWithHighWater(highWater: bigint): Promise<void>;
  /** Permanently retires this node when clean handoff is not certain. */
  retire(): Promise<void>;
}

export interface RuntimeTelemetryContext<RuntimeConfig extends DbConfig = DbConfig> {
  config: RuntimeConfig;
  collectorUrl: string;
  runtimeThread: "main" | "worker";
}

export interface BrowserWorkerConnection {
  ready(): Promise<void>;
  waitForServerConnection(): Promise<void>;
  updateAuth(authJson: string, sessionClaims: Record<string, unknown>): Promise<void>;
  disconnect(): Promise<void>;
  reconnect(authJson: string, sessionClaims: Record<string, unknown>): Promise<void>;
  deleteStorage(): Promise<void>;
  flushLocal(): Promise<void>;
  openInspectorControlPort(): Promise<MessagePort>;
  shutdown(): Promise<void>;
  /** Present only after an authenticated Inspector control-port attachment. */
  getAuthenticatedInspectorAttachmentPhysicalDbName?(): string | null;
}

export interface BrowserFollowerConnection {
  ready(): Promise<void>;
  flushLocal(): Promise<void>;
  waitForServerConnection(): Promise<void>;
  updateAuth(authJson: string, sessionClaims: Record<string, unknown>): void;
  detachForReconnect(): void;
  shutdown(): Promise<void>;
}

export interface BrowserWorkerConnectionContext<RuntimeConfig extends DbConfig = DbConfig> {
  config: RuntimeConfig;
  schema: WasmSchema;
  client: JazzClient;
  onAuthFailure: (reason: AuthFailureReason) => void;
  onAuthRestored: () => void;
  /** The worker namespace's explicit offline state changed. */
  onExplicitOfflineChange?: (offline: boolean) => void;
  onFailure: (error: unknown) => void;
  onStorageReset?: () => void;
  onStorageInvalidated?: () => void;
}

export interface BrowserFollowerConnectionContext<RuntimeConfig extends DbConfig = DbConfig> {
  config: RuntimeConfig;
  client: JazzClient;
  port: MessagePort;
  onAuthFailure: (reason: AuthFailureReason) => void;
  onAuthRestored: () => void;
  /** The worker namespace's explicit offline state changed. */
  onExplicitOfflineChange?: (offline: boolean) => void;
  onFailure: (error: unknown) => void;
  onStorageReset?: () => void;
  onStorageInvalidated?: () => void;
}

/**
 * Internal source for loading and wiring the native runtime.
 *
 * This keeps platform/source differences (WASM, NAPI, browser storage, React
 * Native support status) out of Db. The active database path is native-runtime backed:
 * implementations preload the runtime, then create JazzClient instances for
 * concrete schemas.
 */
export abstract class RuntimeSource<RuntimeConfig extends DbConfig = DbConfig> {
  /** Set to true when this source can host browser persistence in a dedicated worker. */
  readonly supportsBrowserWorker: boolean = false;
  /** Set to false when the runtime must receive schemas exactly as declared. */
  readonly supportsPolicyBypass: boolean = true;

  async load(config: RuntimeConfig): Promise<void> {
    await this.loadRuntime(config);
  }

  protected async loadRuntime(_config: RuntimeConfig): Promise<unknown> {
    return undefined;
  }

  abstract createClient(context: RuntimeClientContext<RuntimeConfig>): JazzClient;

  /**
   * Optional pre-runtime foreground identity lease for non-browser hosts.
   * Browser persistence uses the dedicated SharedWorker variant below.
   */
  async acquireForegroundNodeLease(
    _config: RuntimeConfig,
  ): Promise<ForegroundNodeLease | undefined> {
    return undefined;
  }

  acquireBrowserForegroundNodeLease(_config: RuntimeConfig): Promise<ForegroundNodeLease> {
    return Promise.reject(
      new Error("Db runtime source does not support browser foreground leases"),
    );
  }

  createBrowserWorkerConnection(
    _context: BrowserWorkerConnectionContext<RuntimeConfig>,
  ): BrowserWorkerConnection {
    throw new Error("Db runtime source does not support browser worker connections");
  }

  createBrowserFollowerConnection(
    _context: BrowserFollowerConnectionContext<RuntimeConfig>,
  ): BrowserFollowerConnection {
    throw new Error("Db runtime source does not support browser follower connections");
  }

  installTelemetry(
    _context: RuntimeTelemetryContext<RuntimeConfig>,
  ): (() => void) | null | undefined {
    return null;
  }

  mintLocalFirstToken(_options: RuntimeTokenOptions): string {
    throw new Error("Db runtime source does not support local-first auth");
  }

  mintAnonymousToken(_options: RuntimeTokenOptions): string {
    throw new Error("Db runtime source does not support anonymous auth");
  }
}
