import type { DurabilityTier } from "../client.js";
import {
  ConnectionManager,
  type ConnectionManagerClientInput,
  type DbForConnection,
} from "./types.js";
import type { DirectRuntimeConnection } from "../runtime-source.js";

/** Manages a Db whose runtime connects directly to the configured server. */
export class DirectConnectionManager extends ConnectionManager {
  private isDisconnected = false;
  private reconnectWaiters = new Set<() => void>();
  private transportTransition: Promise<void> = Promise.resolve();
  private connection: DirectRuntimeConnection | null = null;
  private connectionReady: Promise<void> | null = null;
  private connectionError: Error | null = null;
  private connectionErrorEpoch = 0;

  constructor(host: DbForConnection) {
    super(host);
  }

  async start(): Promise<void> {}

  protected override onClientCreated({ schema, client }: ConnectionManagerClientInput): void {
    let connection: DirectRuntimeConnection | null = null;
    connection =
      this.host.runtimeSource.createDirectConnection?.({
        config: this.host.config,
        schema,
        client,
        onFailure: (error) => {
          if (connection) this.recordConnectionError(connection, error);
        },
      }) ?? null;
    if (connection) {
      this.connection = connection;
      this.trackConnectionReady(connection, connection.ready());
      if (this.isDisconnected) {
        void this.enqueueTransportTransition(() => connection.disconnect()).catch(() => undefined);
      }
      return;
    }
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
      backend_session: config.cookieSession,
    });
  }

  async ensureReady(tier?: DurabilityTier, signal?: AbortSignal): Promise<void> {
    if (this.connectionError) throw this.connectionError;
    await this.connectionReady;
    if (this.connectionError) throw this.connectionError;
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
      if (this.connection) await this.connection.disconnect();
      else await this.clientEntry?.client.disconnectTransport();
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
      const connection = this.connection;
      if (connection) {
        // A successful reconnect replaces a rejected initial `ready()` receipt.
        // Only clear an error observed before this attempt: a concurrent native
        // failure must remain visible to subsequent ensureReady() calls.
        const errorEpoch = this.connectionErrorEpoch;
        const ready = this.trackConnectionReady(connection, connection.reconnect());
        await ready;
        if (this.connection === connection && this.connectionErrorEpoch === errorEpoch) {
          this.connectionError = null;
        }
      } else {
        const client = this.clientEntry?.client;
        if (client) this.connectClient(client);
      }
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

  private trackConnectionReady(
    connection: DirectRuntimeConnection,
    ready: Promise<void>,
  ): Promise<void> {
    const tracked = ready.catch((error: unknown) => {
      this.recordConnectionError(connection, error);
      throw error;
    });
    this.connectionReady = tracked;
    void tracked.catch(() => undefined);
    return tracked;
  }

  private recordConnectionError(connection: DirectRuntimeConnection, error: unknown): void {
    if (this.connection !== connection) return;
    this.connectionErrorEpoch += 1;
    this.connectionError = asError(error);
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

  override async shutdown(): Promise<void> {
    try {
      await this.connection?.shutdown();
    } finally {
      this.connection = null;
      this.connectionReady = null;
      await super.shutdown();
    }
  }
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}
