import type { WasmSchema } from "../drivers/types.js";
import type { JazzClient, LocalBatchRecord } from "./client.js";
import type { DbConfig } from "./db.js";
import type { AuthFailureReason } from "./sync-transport.js";

export interface BackendTokenOptions {
  secret: string;
  audience: string;
  ttlSeconds: number;
  nowSeconds: bigint;
}

export interface DbBackendClientContext<BackendConfig extends DbConfig = DbConfig> {
  config: BackendConfig;
  schema: WasmSchema;
  schemaJson: string;
  hasWorker: boolean;
  useBinaryEncoding: boolean;
  onAuthFailure: (reason: AuthFailureReason) => void;
  onRejectedBatchAcknowledged: (batchId: string) => void;
}

export interface DbBackendTelemetryContext<BackendConfig extends DbConfig = DbConfig> {
  config: BackendConfig;
  collectorUrl: string;
  runtimeThread: "main" | "worker";
}

export interface DbBackendHost {
  isShuttingDown(): boolean;
  onAuthFailure(reason: AuthFailureReason): void;
  onMutationErrorReplay(client: JazzClient, batch: LocalBatchRecord): void;
  getTelemetryCollectorUrl(): string | undefined;
  shutdownClientsForStorageReset(): Promise<void>;
}

export interface DbBackendCreateContext<BackendConfig extends DbConfig = DbConfig> {
  config: BackendConfig;
  host: DbBackendHost;
}

export interface DbBackend<BackendConfig extends DbConfig = DbConfig> {
  readonly hasWorker: boolean;
  createClient(context: DbBackendClientContext<BackendConfig>): JazzClient;
  ensureReady(): Promise<void>;
  waitForUpstreamServerConnection(): Promise<void>;
  updateAuth(auth: { jwtToken?: string }): void;
  acknowledgeRejectedBatch(batchId: string): void;
  deleteClientStorage(): Promise<void>;
  shutdown(): Promise<void>;
}

class DirectDbBackend<
  BackendConfig extends DbConfig = DbConfig,
> implements DbBackend<BackendConfig> {
  readonly hasWorker = false;

  constructor(
    private readonly createDirectClient: (
      context: DbBackendClientContext<BackendConfig>,
    ) => JazzClient,
  ) {}

  createClient(context: DbBackendClientContext<BackendConfig>): JazzClient {
    const client = this.createDirectClient({
      ...context,
      hasWorker: false,
      useBinaryEncoding: false,
    });

    if (context.config.serverUrl) {
      client.connectTransport(context.config.serverUrl, {
        jwt_token: context.config.jwtToken,
        admin_secret: context.config.adminSecret,
      });
    }

    return client;
  }

  async ensureReady(): Promise<void> {}

  async waitForUpstreamServerConnection(): Promise<void> {}

  updateAuth(_auth: { jwtToken?: string }): void {}

  acknowledgeRejectedBatch(_batchId: string): void {}

  async deleteClientStorage(): Promise<void> {
    throw new Error(
      "deleteClientStorage() is only available on browser worker-backed Db instances.",
    );
  }

  async shutdown(): Promise<void> {}
}

export abstract class DbBackendModule<BackendConfig extends DbConfig = DbConfig> {
  /** Set to false for backends, such as React Native, that cannot use browser workers. */
  readonly supportsBrowserWorker: boolean = true;
  /** Set to false when the runtime must receive schemas exactly as declared. */
  readonly supportsPolicyBypass: boolean = true;
  private hasLoadedResources = false;
  private loadedResourcesValue: unknown;

  async load(config: BackendConfig): Promise<void> {
    if (this.hasLoadedResources) {
      return;
    }

    this.loadedResourcesValue = await this.loadResources(config);
    this.hasLoadedResources = true;
  }

  protected abstract loadResources(config: BackendConfig): Promise<unknown>;

  protected get loadedResources(): unknown {
    if (!this.hasLoadedResources) {
      throw new Error("Db backend module is not loaded");
    }
    return this.loadedResourcesValue;
  }

  abstract createClient(context: DbBackendClientContext<BackendConfig>): JazzClient;

  async createBackend(
    _context: DbBackendCreateContext<BackendConfig>,
  ): Promise<DbBackend<BackendConfig>> {
    return new DirectDbBackend((clientContext) => this.createClient(clientContext));
  }

  installTelemetry(
    _context: DbBackendTelemetryContext<BackendConfig>,
  ): (() => void) | null | undefined {
    return null;
  }

  mintLocalFirstToken(_options: BackendTokenOptions): string {
    throw new Error("Db backend module does not support local-first auth");
  }

  mintAnonymousToken(_options: BackendTokenOptions): string {
    throw new Error("Db backend module does not support anonymous auth");
  }
}
