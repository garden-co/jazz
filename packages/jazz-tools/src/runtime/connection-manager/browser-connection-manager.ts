import type { DurabilityTier } from "../client.js";
import type { Session } from "../context.js";
import type { BrowserWorkerConnection } from "../runtime-source.js";
import { resolveClientSessionSync } from "../client-session.js";
import {
  ConnectionManager,
  type ConnectionManagerClientInput,
  type DbForConnection,
} from "./types.js";

/**
 * Connects a main-thread, optimistic in-memory Db to one persistent OPFS
 * runtime in a dedicated worker. This manager deliberately has no tab-sharing
 * or leader-election responsibility.
 */
export class BrowserConnectionManager extends ConnectionManager {
  private bridge: BrowserWorkerConnection | null = null;
  private bridgeError: Error | null = null;
  private isDisconnected = false;
  private reconnectWaiters: Array<() => void> = [];

  constructor(host: DbForConnection) {
    super(host);
  }

  async start(): Promise<void> {}

  protected override onClientCreated({ schema, client }: ConnectionManagerClientInput): void {
    this.bridgeError = null;
    const bridge = this.host.runtimeSource.createBrowserWorkerConnection({
      config: { ...this.host.config },
      schema,
      client,
      onAuthFailure: (reason) => this.host.markUnauthenticated(reason),
      onFailure: (error) => {
        if (this.bridge !== bridge) return;
        this.bridgeError = asError(error);
      },
    });
    this.bridge = bridge;
    if (this.isDisconnected) {
      void bridge.ready().then(() => bridge.disconnect());
    }
  }

  async ensureReady(tier?: DurabilityTier): Promise<void> {
    if (this.bridgeError) throw this.bridgeError;
    const bridge = this.bridge;
    if (!bridge) return;
    await bridge.ready();
    if (tier == null || tier === "local") return;
    if (this.isDisconnected) {
      await new Promise<void>((resolve) => this.reconnectWaiters.push(resolve));
    }
    await bridge.waitForServerConnection();
  }

  shouldDeferSubscriptionStart(tier?: DurabilityTier): boolean {
    return tier === "edge" || tier === "global";
  }

  async disconnect(): Promise<void> {
    if (!this.host.config.serverUrl) {
      throw new Error("Db.disconnect() requires a configured serverUrl.");
    }
    this.isDisconnected = true;
    await this.bridge?.disconnect();
  }

  async reconnect(): Promise<void> {
    if (!this.host.config.serverUrl) {
      throw new Error("Db.reconnect() requires a configured serverUrl.");
    }
    this.isDisconnected = false;
    await this.bridge?.reconnect(
      JSON.stringify(runtimeAuth(this.host.config)),
      runtimeSessionClaims(this.host.config),
    );
    for (const resolve of this.reconnectWaiters.splice(0)) resolve();
  }

  override updateAuth(auth: { jwtToken?: string; cookieSession?: Session }): void {
    super.updateAuth(auth);
    this.bridge?.updateAuth(
      JSON.stringify(runtimeAuth(this.host.config)),
      runtimeSessionClaims(this.host.config),
    );
  }

  async deleteClientStorage(): Promise<void> {
    const driver = this.host.config.driver ?? { type: "persistent" };
    if (driver.type !== "persistent") {
      throw new Error("deleteClientStorage() is only available when driver.type='persistent'.");
    }
    const client = this.getCurrentClient() ?? this.getClient({});
    try {
      await this.bridge?.deleteStorage();
      await client.shutdown();
    } finally {
      this.bridge = null;
      this.clearClient();
    }
  }

  override async shutdown(): Promise<void> {
    try {
      await this.bridge?.shutdown();
    } finally {
      this.bridge = null;
      for (const resolve of this.reconnectWaiters.splice(0)) resolve();
      await super.shutdown();
    }
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
