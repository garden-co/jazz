import type { DurabilityTier } from "../client.js";
import { resolveClientSessionSync } from "../client-session.js";
import type { Session } from "../context.js";
import type { BrowserWorkerConnection } from "../runtime-source.js";
import { reloadAfterStorageInvalidation } from "../browser-storage-invalidation.js";
import {
  ConnectionManager,
  type ConnectionManagerClientInput,
  type DbForConnection,
} from "./types.js";

/**
 * Every persistent browser tab is an in-memory client of one SharedWorker
 * runtime. There are no tab roles, elections, ownership locks, or follower
 * handoffs; SharedWorker identity supplies the namespace-wide singleton.
 */
export class BrowserConnectionManager extends ConnectionManager {
  private connection: BrowserWorkerConnection | null = null;
  private connectionReady: Promise<void> | null = null;
  private connectionError: Error | null = null;
  private disconnected = false;
  private storageReset: Promise<void> | null = null;

  constructor(host: DbForConnection) {
    super(host);
  }

  async start(): Promise<void> {}

  protected override onClientCreated({ schema, client }: ConnectionManagerClientInput): void {
    const connection = this.host.runtimeSource.createBrowserWorkerConnection({
      config: { ...this.host.config },
      schema,
      client,
      // Retained only until the generic runtime-source contract is renamed.
      // The SharedWorker topology has no leadership generation or lock.
      leadershipId: 1,
      workerLockName: "",
      onAuthFailure: (reason) => this.host.markUnauthenticated(reason),
      onAuthRestored: () => this.host.clearAuthError(),
      onFailure: (error) => {
        if (this.connection !== connection) return;
        this.connectionError = asError(error);
      },
      onStorageReset: () => this.beginStorageReset(connection),
      onStorageInvalidated: () => this.reloadAfterStorageInvalidation(connection),
      onFollowerPortClosed: () => undefined,
    });
    this.connection = connection;
    this.connectionReady = connection.ready().catch((error: unknown) => {
      this.connectionError = asError(error);
      throw error;
    });
    void this.connectionReady.catch(() => undefined);
  }

  async ensureReady(tier?: DurabilityTier): Promise<void> {
    await this.storageReset;
    if (this.connectionError) throw this.connectionError;
    await this.connectionReady;
    if (this.connectionError) throw this.connectionError;
    if (this.disconnected && tier !== "local") {
      throw new Error("Db is disconnected");
    }
    if (this.host.config.serverUrl && tier !== "local") {
      await this.connection?.waitForServerConnection();
    }
  }

  shouldDeferSubscriptionStart(tier?: DurabilityTier): boolean {
    return tier === "edge" || tier === "global";
  }

  async disconnect(): Promise<void> {
    if (!this.host.config.serverUrl) {
      throw new Error("Db.disconnect() requires a configured serverUrl.");
    }
    await this.connectionReady;
    await this.connection?.disconnect();
    this.disconnected = true;
  }

  async reconnect(): Promise<void> {
    if (!this.host.config.serverUrl) {
      throw new Error("Db.reconnect() requires a configured serverUrl.");
    }
    await this.connectionReady;
    await this.connection?.reconnect(
      JSON.stringify(runtimeAuth(this.host.config)),
      runtimeSessionClaims(this.host.config),
    );
    this.disconnected = false;
  }

  override updateAuth(auth: { jwtToken?: string; cookieSession?: Session }): void {
    super.updateAuth(auth);
    void this.connection?.updateAuth(
      JSON.stringify(runtimeAuth(this.host.config)),
      runtimeSessionClaims(this.host.config),
    );
  }

  async deleteClientStorage(): Promise<void> {
    await this.connectionReady;
    await this.connection?.deleteStorage();
    await this.storageReset;
  }

  private beginStorageReset(connection: BrowserWorkerConnection): void {
    if (this.connection !== connection || this.storageReset) return;
    this.connection = null;
    this.connectionReady = null;
    const client = this.detachClient();
    this.storageReset = Promise.all([connection.shutdown(), client?.shutdown()])
      .then(() => undefined)
      .finally(() => {
        this.connectionError = null;
        this.storageReset = null;
      });
  }

  private reloadAfterStorageInvalidation(connection: BrowserWorkerConnection): void {
    if (this.connection !== connection) return;
    this.connectionError = new Error("IndexedDB storage was externally invalidated");
    reloadAfterStorageInvalidation();
  }

  override async shutdown(): Promise<void> {
    const connection = this.connection;
    this.connection = null;
    this.connectionReady = null;
    await connection?.shutdown();
    await super.shutdown();
  }
}

function runtimeAuth(config: DbForConnection["config"]): Record<string, unknown> {
  return {
    jwt_token: config.jwtToken ?? null,
    ...(config.adminSecret ? { admin_secret: config.adminSecret } : {}),
    ...(config.backendSecret ? { backend_secret: config.backendSecret } : {}),
    ...(config.cookieSession ? { backend_session: config.cookieSession } : {}),
  };
}

function runtimeSessionClaims(config: DbForConnection["config"]): Record<string, unknown> {
  return resolveClientSessionSync(config)?.claims ?? {};
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}
