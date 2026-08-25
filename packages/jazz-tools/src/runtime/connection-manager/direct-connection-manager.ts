import type { DurabilityTier } from "../client.js";
import {
  ConnectionManager,
  type ConnectionManagerClientInput,
  type DbForConnection,
} from "./types.js";

/** Manages a Db whose runtime connects directly to the configured server. */
export class DirectConnectionManager extends ConnectionManager {
  private isDisconnected = false;
  private reconnectWaiters: Array<() => void> = [];

  constructor(host: DbForConnection) {
    super(host);
  }

  async start(): Promise<void> {}

  protected override onClientCreated({ client }: ConnectionManagerClientInput): void {
    if (this.isDisconnected) {
      // Establish the runtime's reconnect barrier for clients created while the
      // Db is explicitly offline.
      void client.disconnectTransport().catch(() => undefined);
      return;
    }
    this.connectClient(client);
  }

  private connectClient(client: ConnectionManagerClientInput["client"]): void {
    const { config } = this.host;
    if (!config.serverUrl) return;
    client.connectTransport(config.serverUrl, {
      jwt_token: config.jwtToken,
      admin_secret: config.adminSecret,
      backend_secret: config.backendSecret,
      backend_session: config.cookieSession,
    });
  }

  async ensureReady(tier?: DurabilityTier, _signal?: AbortSignal): Promise<void> {
    if (!this.isDisconnected || tier === "local") return;
    await new Promise<void>((resolve) => {
      this.reconnectWaiters.push(resolve);
    });
  }

  shouldDeferSubscriptionStart(_tier?: DurabilityTier): boolean {
    return false;
  }
  isExplicitlyOffline(): boolean {
    return this.isDisconnected;
  }
  async waitForReconnect(signal?: AbortSignal): Promise<void> {
    if (!this.isDisconnected) return;
    await new Promise<void>((resolve) => {
      const onAbort = () => resolve();
      signal?.addEventListener("abort", onAbort, { once: true });
      this.reconnectWaiters.push(() => {
        signal?.removeEventListener("abort", onAbort);
        resolve();
      });
    });
  }

  async disconnect(): Promise<void> {
    if (!this.host.config.serverUrl) {
      throw new Error("Db.disconnect() requires a configured serverUrl.");
    }
    this.isDisconnected = true;
    await this.clientEntry?.client.disconnectTransport();
  }

  async reconnect(): Promise<void> {
    if (!this.host.config.serverUrl) {
      throw new Error("Db.reconnect() requires a configured serverUrl.");
    }
    this.isDisconnected = false;
    const client = this.clientEntry?.client;
    if (client) this.connectClient(client);
    for (const resolve of this.reconnectWaiters.splice(0)) {
      resolve();
    }
  }

  async deleteClientStorage(): Promise<void> {
    const driver = this.host.config.driver ?? { type: "persistent" };
    if (driver.type !== "persistent") {
      throw new Error("deleteClientStorage() is only available when driver.type='persistent'.");
    }
    if (typeof window === "undefined") {
      console.error("deleteClientStorage() is only available in browser runtimes.");
      return;
    }

    const client = this.getCurrentClient() ?? this.getClient({});
    try {
      await client.clearClientStorage();
      await client.shutdown();
    } finally {
      this.clearClient();
    }
  }
}
