import type { DurabilityTier } from "../client.js";
import {
  ConnectionManager,
  type ConnectionManagerClientInput,
  type DbForConnection,
} from "./types.js";

/** Manages a Db whose runtime connects directly to the configured server. */
export class DirectConnectionManager extends ConnectionManager {
  private isDisconnected = false;
  private reconnectWaiters = new Set<() => void>();
  private transportTransition: Promise<void> = Promise.resolve();

  constructor(host: DbForConnection) {
    super(host);
  }

  async start(): Promise<void> {}

  protected override onClientCreated({ client }: ConnectionManagerClientInput): void {
    if (this.isDisconnected) {
      // Establish the runtime's reconnect barrier for clients created while the
      // Db is explicitly offline.
      void this.enqueueTransportTransition(() => client.disconnectTransport()).catch(
        () => undefined,
      );
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

  async ensureReady(tier?: DurabilityTier, signal?: AbortSignal): Promise<void> {
    if (tier === "local") return;
    for (;;) {
      while (this.isDisconnected) {
        await this.waitForReconnect(signal);
        if (signal?.aborted) return;
      }
      await this.transportTransition;
      if (!this.isDisconnected || signal?.aborted) return;
    }
  }

  shouldDeferSubscriptionStart(_tier?: DurabilityTier): boolean {
    return false;
  }
  isExplicitlyOffline(): boolean {
    return this.isDisconnected;
  }
  async waitForReconnect(signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) return;
    if (!this.isDisconnected) {
      await this.transportTransition;
      if (!this.isDisconnected) return;
    }
    await new Promise<void>((resolve) => {
      const finish = () => {
        this.reconnectWaiters.delete(finish);
        signal?.removeEventListener("abort", onAbort);
        resolve();
      };
      const onAbort = () => finish();
      signal?.addEventListener("abort", onAbort, { once: true });
      this.reconnectWaiters.add(finish);
    });
  }

  async disconnect(): Promise<void> {
    if (!this.host.config.serverUrl) {
      throw new Error("Db.disconnect() requires a configured serverUrl.");
    }
    await this.enqueueTransportTransition(async () => {
      await this.clientEntry?.client.disconnectTransport();
      // An in-flight or failed disconnect is not permission for a
      // RemoteIfPossible read to fall back locally.
      this.isDisconnected = true;
    });
  }

  async reconnect(): Promise<void> {
    if (!this.host.config.serverUrl) {
      throw new Error("Db.reconnect() requires a configured serverUrl.");
    }
    await this.enqueueTransportTransition(async () => {
      const client = this.clientEntry?.client;
      if (client) this.connectClient(client);
      this.isDisconnected = false;
    });
    if (!this.isDisconnected) {
      const waiters = Array.from(this.reconnectWaiters);
      this.reconnectWaiters.clear();
      for (const resolve of waiters) resolve();
    }
  }

  private enqueueTransportTransition(run: () => void | Promise<void>): Promise<void> {
    const transition = this.transportTransition.then(run, run);
    this.transportTransition = transition.catch(() => undefined);
    return transition;
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
